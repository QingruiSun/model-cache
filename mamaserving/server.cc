#include "ModelSliceReader.h"
#include "alimama.grpc.pb.h"
#include "alimama.pb.h"
#include "ourmama.grpc.pb.h"
#include "ourmama.pb.h"
#include <arpa/inet.h>
#include <boost/filesystem.hpp>
#include <boost/format.hpp>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <deque>
#include <dlfcn.h>
#include <etcd/Client.hpp>
#include <etcd/Response.hpp>
#include <etcd/Watcher.hpp>
#include <grpcpp/grpcpp.h>
#include <hdfs/hdfs.h>
#include <ifaddrs.h>
#include <iostream>
#include <memory>
#include <mutex>
#include <netinet/in.h>
#include <optional>
#include <queue>
#include <shared_mutex>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <unordered_map>

using MamaRequest = alimama::proto::Request;
using MamaResponse = alimama::proto::Response;
using SliceRequest = alimama::proto::SliceRequest;
using alimama::proto::ModelService;

using IntraReq = ourmama::proto::IntraReq;
using IntraResp = ourmama::proto::IntraResp;
using ourmama::proto::IntraService;

using grpc::ServerContext;
using grpc::Status;

using RawBuffer = char *;

#define likely(x) __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)

constexpr size_t KB = 1024;
constexpr size_t MB = 1024 * KB;

constexpr int queryInterval = 2500; // milliseconds
constexpr int intraPort = 50050;

const std::string etcdUrl = "http://etcd:2379";
const std::string etcdIntraDiscoveryFmt = "/intra/%1%";
const std::string etcdServiceDiscoveryFmt = "/services/modelservice/%1%:50051";
const std::string etcdVersionSyncPrefixFmt = "/version-%1%/";
const std::string etcdVersionSyncNodeFmt = "/version-%1%/%2%";

bool gIsExiting = false;
int gNodeId;
int gNodeNum;
std::string gNodeIp;
std::vector<std::string> gNodeIps;

#define FMT(fmt, ...) boost::str(boost::format(fmt) % __VA_ARGS__)

void SleepMilli(int ms) {
  std::this_thread::sleep_for(std::chrono::milliseconds(ms));
}

// trim from end (in place)
static inline void rtrim(std::string &s) {
  s.erase(std::find_if(s.rbegin(), s.rend(),
                       [](unsigned char ch) { return !std::isspace(ch); })
              .base(),
          s.end());
}

namespace EtcdService {
#define LAUNCH_ETCD_TASK(keyStr, opStr)                                        \
  try {                                                                        \
    etcd::Response response = task.get();                                      \
    std::cout << "[etcd] " << opStr << " " << (keyStr)                         \
              << (response.is_ok() ? " succeeds" : " failed") << std::endl;    \
  } catch (const std::exception &e) {                                          \
    std::cout << "[etcd] An exception occurred during " << opStr << " "        \
              << (keyStr) << std::endl;                                        \
  }

void set(std::string key, std::string value) {
  etcd::Client client(etcdUrl);
  auto task = client.set(key, value);
  LAUNCH_ETCD_TASK(key, "set");
}

void rm(std::string key) {
  etcd::Client client(etcdUrl);
  auto task = client.rm(key);
  LAUNCH_ETCD_TASK(key, "rm");
}

#undef LAUNCH_ETCD_TASK

void RegisterIntra() {
  etcd::Client client(etcdUrl);
  while (true) {
    try {
      auto response =
          client
              .set(FMT(etcdIntraDiscoveryFmt, gNodeIp), std::getenv("NODE_ID"))
              .get();
      if (response.is_ok()) {
        std::cout << "RegisterIntra success" << std::endl;
        return;
      } else {
        std::cout << "RegisterIntra: failed; reason: "
                  << response.error_message() << "; retrying..." << std::endl;
        SleepMilli(queryInterval);
      }
    } catch (const std::exception &e) {
      std::cout << "RegisterIntra: exception: " << e.what() << std::endl;
      SleepMilli(queryInterval);
    }
  }
}

void GetAllNodesIp() {
  etcd::Client client(etcdUrl);
  gNodeIps.resize(gNodeNum);

  while (true) {
    try {
      auto response = client.ls("/intra/").get();
      if (response.is_ok()) {
        if (response.keys().size() != gNodeNum) {
          std::cout << "GetAllNodesIp: only " << response.keys().size()
                    << " nodes are discovered, retrying..." << std::endl;
          SleepMilli(queryInterval);
          continue;
        }

        for (auto &key : response.keys()) {
          auto ipAddr = key.substr(key.find_last_of('/') + 1);
          // TODO: add error handling
          auto nodeId = std::stoi(client.get(key).get().value().as_string());
          gNodeIps[nodeId - 1] = std::move(ipAddr);
        }
        return;

      } else {
        std::cout << "GetAllNodesIp failed: " << response.error_code() << " "
                  << response.error_message() << std::endl;
        SleepMilli(queryInterval);
      }
    } catch (const std::exception &e) {
      std::cout << "An exception occurred during GetAllNodesIp: " << e.what()
                << std::endl;
      return;
    }
  }
}

static void RegisterModelService() {
  set(FMT(etcdServiceDiscoveryFmt, gNodeIp), "");
}

static void RegisterVersion(std::string modelVersion) {
  set(FMT(etcdVersionSyncNodeFmt, modelVersion % gNodeId), "");
}

static void UnregisterVersion(std::string modelVersion) {
  rm(FMT(etcdVersionSyncNodeFmt, modelVersion % gNodeId));
}
} // namespace EtcdService

class SliceCache {
private:
  std::string targetPath(int slice) {
    static boost::format fmter("./model_%1%/model_slice.%2$03d");
    return boost::str(boost::format(fmter) % Version_ % slice);
  }

  int start_;
  int cnt_;
  std::vector<ModelSliceReader *> readers_;

  inline static std::queue<SliceCache *> caches_;
  inline static std::mutex mutCv_;
  inline static std::shared_mutex mutP_;
  inline static std::condition_variable cvCache_;

  void load() {
    readers_.resize(cnt_);
    for (int i = 0; i < cnt_; i++) {
      // TODO: what if a version switch happens during load
      readers_[i] = new ModelSliceReader();
      readers_[i]->Load(targetPath(start_ + i));
    }
  }

  void unload() {
    for (auto reader : readers_) {
      reader->Unload();
      delete reader;
    }
  }

  static void resolveWatcherEvent(const etcd::Response &_resp,
                                  std::atomic<int> &syncCnt) {
    std::cout << "Watcher: current synccnt: " << syncCnt << std::endl;
    if ((++syncCnt) == gNodeNum)
      cvCache_.notify_all();
  }

public:
  const std::string Version_;
  uint32_t GenId_;

  using SyncRecord = std::pair<std::atomic<int> *, etcd::Watcher *>;

  SliceCache(std::string version, uint32_t genId, int start, int cnt)
      : Version_(version), GenId_(genId), start_(start), cnt_(cnt) {}

  static SliceCache *Get() {
    std::shared_lock lk(mutP_);
    return caches_.back();
  }

  static SliceCache *Get(uint32_t genId) {
    std::shared_lock lk(mutP_);
    return caches_.back()->GenId_ == genId    ? caches_.back()
           : caches_.front()->GenId_ == genId ? caches_.front()
                                              : nullptr;
  }

  /* must be called before DoSync */
  static SyncRecord ScheduleSync(std::string version) {
    static std::atomic<int> syncCnt;

    syncCnt = 0;
    auto start =
        boost::str(boost::format(etcdVersionSyncNodeFmt) % version % 0);
    auto end =
        boost::str(boost::format(etcdVersionSyncNodeFmt) % version % gNodeNum);
    return {&syncCnt, new etcd::Watcher(etcdUrl, start, end,
                                        [&](const etcd::Response &_resp) {
                                          resolveWatcherEvent(_resp, syncCnt);
                                        })};
  }

  static void DoSync(SyncRecord record, std::string version, uint32_t genId,
                     int start, int cnt) {
    mutP_.lock_shared();
    auto currentCacheSize = caches_.size();
    mutP_.unlock_shared();

    if (currentCacheSize >= 2) {
      mutP_.lock();
      auto oldCache = caches_.front();
      caches_.pop();
      mutP_.unlock();

      delete oldCache;
    }

    std::atomic<int> *syncCnt = record.first;
    etcd::Watcher *pWatcher = record.second;

    std::cout << "SliceCache::DoSync" << std::endl;
    // 1. Prepare the new version
    auto newCache = new SliceCache(version, genId, start, cnt);
    newCache->load();
    // 2. Tell everyone else I'm ready to switch
    EtcdService::RegisterVersion(version);
    // 3. Wait for everyone else to get ready
    {
      std::unique_lock lock(mutCv_);
      cvCache_.wait(lock, [=] { return *syncCnt == gNodeNum; });
    }

    std::cout << "SliceCache::DoSync: everyone is ready" << std::endl;
    {
      std::unique_lock lock(mutP_);
      caches_.push(newCache);
    }

    pWatcher->Cancel();
    delete pWatcher;
  }

  void Read(int slice, size_t seek, size_t len, RawBuffer buffer) {
    readers_[slice - start_]->Read(seek, len, buffer);
  }

  ~SliceCache() {
    EtcdService::UnregisterVersion(Version_);
    unload();
  }
}; // class SliceCache

std::string GetIpAddr() {
  struct ifaddrs *addresses;
  if (getifaddrs(&addresses) == -1) {
    return "";
  }

  std::string ipAddr;
  for (struct ifaddrs *addr = addresses; addr != nullptr;
       addr = addr->ifa_next) {
    if (addr->ifa_addr == nullptr || addr->ifa_addr->sa_family != AF_INET)
      continue;

    const struct sockaddr_in *sockaddr =
        reinterpret_cast<const struct sockaddr_in *>(addr->ifa_addr);
    ipAddr = inet_ntoa(sockaddr->sin_addr);
    if (strcmp(ipAddr.c_str(), "127.0.0.1") == 0)
      continue;

    break;
  }

  freeifaddrs(addresses);
  return ipAddr;
}

class IntraRespDataManager {
private:
  static constexpr size_t MAX_RING_SIZE = 0x6000;

  std::atomic<size_t> id_ = 0;

  IntraRespDataManager() = default;
  IntraRespDataManager(const IntraRespDataManager &) = delete;
  IntraRespDataManager &operator=(const IntraRespDataManager &) = delete;

  struct Record {
    MamaResponse *pMessage;
    std::condition_variable cv;
    std::atomic<uint8_t> remains;
    std::shared_mutex mutex;
  } records_[MAX_RING_SIZE];

public:
  static IntraRespDataManager &GetInstance() {
    static IntraRespDataManager instance;
    return instance;
  }

  size_t GetId() { return id_++; }

  MamaResponse *GetMessage(size_t id) {
    return records_[id % MAX_RING_SIZE].pMessage;
  }

  std::pair<size_t, std::condition_variable *> Register(MamaResponse *pMessage,
                                                        uint8_t remains) {
    size_t id = id_++;
    size_t ringId = id % MAX_RING_SIZE;
    auto &record = records_[ringId];
    record.mutex.lock();

    record.pMessage = pMessage;
    record.remains = remains;

    record.mutex.unlock();
    return {id, &record.cv};
  }

  bool IsBatchFinished(size_t id) {
    size_t ringId = id % MAX_RING_SIZE;
    auto &record = records_[ringId];
    record.mutex.lock_shared();

    bool ret = record.remains == 0;

    record.mutex.unlock_shared();
    return ret;
  }

  void ReportFinishedResponseFor(size_t id) {
    size_t ringId = id % MAX_RING_SIZE;
    auto &record = records_[ringId];
    record.mutex.lock();

    bool finished = (--record.remains) == 0;

    record.mutex.unlock();

    if (finished)
      record.cv.notify_one();
  }
}; // class IntraRespDataManager

class IntraServiceClient final {
private:
  struct AsyncClientCall {
    grpc::ClientContext context;
    std::unique_ptr<grpc::ClientAsyncResponseReader<IntraResp>> responseReader;
    IntraResp reply;
    grpc::Status status;
  };

public:
  IntraServiceClient(std::shared_ptr<grpc::Channel> channel)
      : stub_(IntraService::NewStub(channel)) {
    stop_ = false;
    sendReqThread_ = std::thread(&IntraServiceClient::SendRequest, this);
    parseRespThread_ = std::thread(&IntraServiceClient::ParseResponse, this);
  }

  ~IntraServiceClient() {
    stop_ = true;
    condition_.notify_all();
    sendReqThread_.detach();
    parseRespThread_.detach();
  }

  void EnqueueIntraReq(const IntraReq intraReq) {
    std::unique_lock<std::mutex> lock(queueMutex_);
    queue_.push(intraReq);
    condition_.notify_one();
  }

  void SendRequest() {
    while (!stop_) {
      std::queue<IntraReq> tempQueue = {};
      {
        std::unique_lock<std::mutex> lock(queueMutex_);
        condition_.wait(lock, [this] { return !queue_.empty(); });
        std::swap(tempQueue, queue_);
      }

      while (!tempQueue.empty()) {
        AsyncClientCall *call = new AsyncClientCall();
        call->responseReader =
            stub_->AsyncGet(&call->context, tempQueue.front(), &cq_);
        tempQueue.pop();
        call->responseReader->Finish(&call->reply, &call->status, (void *)call);
      }
    }
  }

  void ParseResponse() {
    void *tag;
    bool ok = false;
    IntraRespDataManager &respman = IntraRespDataManager::GetInstance();

    while (!stop_) {
      cq_.Next(&tag, &ok);
      if (unlikely(!ok)) {
        std::cout << "cq_.Next failed" << std::endl;
        continue;
      }
      AsyncClientCall *call = static_cast<AsyncClientCall *>(tag);
      auto &intraResp = call->reply;

      // TODO: can we remove this copy?
      auto pMessage = respman.GetMessage(intraResp.id());
      if (!pMessage)
        std::cout << "pMessage is nullptr for " << intraResp.id() << std::endl;
      const int N = intraResp.data().size();
      for (int i = 0; i < N; ++i) {
        const auto &slice = intraResp.data(i);
        const char *copyFrom = slice.data();
        char *copyTo =
            pMessage->mutable_slice_data(intraResp.offset(i))->data();
        memcpy(copyTo, copyFrom, slice.size());
      }
      respman.ReportFinishedResponseFor(intraResp.id());

      delete call;
    }
  }

  grpc::CompletionQueue cq_; // CompletionQueue is thread safe.

private:
  std::unique_ptr<IntraService::Stub> stub_;
  std::queue<IntraReq> queue_;
  std::mutex queueMutex_;
  std::condition_variable condition_;
  bool stop_ = false;
  std::thread sendReqThread_;
  std::thread parseRespThread_;
}; // class IntraServiceClient

std::vector<IntraServiceClient *> gIntraClients;

void EstablishIntraConn() {
  gIntraClients.resize(gNodeNum);
  for (int i = 0; i < gNodeNum; i++) {
    if (i == gNodeId)
      gIntraClients[i] = nullptr;
    else
      gIntraClients[i] = new IntraServiceClient(
          grpc::CreateChannel(FMT("%1%:%2%", gNodeIps[i] % intraPort),
                              grpc::InsecureChannelCredentials()));
  }
}

void LocalRead(SliceCache *pSc, uint64_t slice, uint64_t seek, uint64_t len,
               RawBuffer buffer) {
  pSc->Read(slice, seek, len, buffer);
}

class IntraServiceImpl final {
private:
  std::unique_ptr<grpc::ServerCompletionQueue> cq_;
  IntraService::AsyncService service_;
  std::unique_ptr<grpc::Server> server_;

  class CallData {
  private:
    IntraService::AsyncService *service_;
    grpc::ServerCompletionQueue *cq_;
    grpc::ServerContext ctx_;
    grpc::ServerAsyncResponseWriter<IntraResp> responder_;
    IntraReq intraReq_;
    IntraResp intraResp_;
    enum CallStatus { CREATE, PROCESS, FINISH };
    CallStatus status_;

    void processRequest(const IntraReq &request, IntraResp *reply) {
      uint64_t slice, seek, len;
      const auto &sliceReqs = request.slice_req();
      const auto genId = request.gen();
      auto pCache = SliceCache::Get(genId);
      if (!pCache) {
        std::cout << "[FATAL] pCache is nullptr for " << genId << std::endl;
        return;
      }

      const int N = sliceReqs.size();
      for (int i = 0; i < N; ++i) {
        const auto &req = sliceReqs[i];
        slice = req.slice_partition();
        seek = req.data_start();
        len = req.data_len();

        reply->add_offset(request.offset(i));
        auto d = reply->add_data();
        d->resize(len);

        LocalRead(pCache, slice, seek, len, d->data());
      }

      reply->set_id(request.id());
    }

  public:
    CallData(IntraService::AsyncService *service,
             grpc::ServerCompletionQueue *cq)
        : service_(service), cq_(cq), responder_(&ctx_), status_(CREATE) {
      Proceed();
    }

    void Proceed() {
      if (status_ == CREATE) {
        status_ = PROCESS;
        service_->RequestGet(&ctx_, &intraReq_, &responder_, cq_, cq_, this);
      } else if (status_ == PROCESS) {
        new CallData(service_, cq_);
        processRequest(intraReq_, &intraResp_);
        status_ = FINISH;
        responder_.Finish(intraResp_, Status::OK, this);
      } else
        delete this;
    }
  }; // class CallData

  std::vector<std::thread> threads_;

public:
  void HandleRPCs() {
    new CallData(&service_, cq_.get());
    void *tag;
    bool ok;
    while (true) {
      cq_->Next(&tag, &ok);
      static_cast<CallData *>(tag)->Proceed();
    }
  }

  ~IntraServiceImpl() {
    for (auto &thread : threads_)
      thread.detach();
    server_->Shutdown();
    cq_->Shutdown();
  }

  void Run(uint16_t port) {
    std::string server_address = std::string("0.0.0.0:") + std::to_string(port);

    grpc::ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service_);
    cq_ = builder.AddCompletionQueue();
    server_ = builder.BuildAndStart();
    std::cout << "Server listening on " << server_address << std::endl;

    // constexpr int nThreads = 6;
    constexpr int nThreads = 1;
    for (int i = 0; i < nThreads; ++i)
      threads_.emplace_back(&IntraServiceImpl::HandleRPCs, this);
  }
}; // class IntraServiceImpl

std::pair<int, int> GetSliceRange(int sliceCount, int nodeId) {
  int avgSlice = sliceCount / gNodeNum;
  int quotient = sliceCount % gNodeNum;
  int start = avgSlice * nodeId + std::min(nodeId, quotient);

  return {start, start + avgSlice + (nodeId < quotient ? 1 : 0) - 1};
}

class VersionSystem {
private:
  class VersionTracker {
    std::unordered_map<std::string, std::pair<bool, size_t>> record_;
    VersionTracker() = default;

  public:
    size_t GetNSlices(std::string version) { return record_[version].second; }

    void SetNSlices(std::string version, size_t nSlices) {
      record_[version].second = nSlices;
    }

    bool IsDownloaded(std::string version) { return record_[version].first; }

    void SetDownloaded(std::string version) { record_[version].first = true; }

    static VersionTracker &GetInstance() {
      static VersionTracker instance;
      return instance;
    }
  }; // class VersionTracker

  int getVersionSliceCountFromHdfs(hdfsFS &fs, std::string modelVersion) {
    int numEntries = 0;
    std::string path = FMT("/model_%1%/", modelVersion);

    hdfsFileInfo *fileInfo = hdfsListDirectory(fs, path.c_str(), &numEntries);
    if (likely(fileInfo != nullptr && numEntries > 0)) {
      hdfsFreeFileInfo(fileInfo, numEntries);
      return numEntries - 2;
    } else {
      std::cout << "  hdfs: list directory " << path << " failed" << std::endl;
      return 0;
    }
  }

  // Assuming the version pointed by pSliceCaches.back() is the latest
  void loadToMem(SliceCache::SyncRecord syncRecord, std::string version,
                 uint32_t genId, int start, int cnt) {
    std::cout << "  Preparing new cache" << std::endl;
    SliceCache::DoSync(syncRecord, version, genId, start, cnt);
  }

  VersionSystem() = default;

public:
  std::string CurrentVersion_ = "WAIT";
  std::atomic<uint32_t> GenId_ = 0;

  static VersionSystem *GetInstance() {
    static VersionSystem instance;
    return &instance;
  }

  std::vector<std::string> LatestVersionOn(hdfsFS &fs) {
    constexpr size_t versionBufferSize = 1024;

    int numEntries;
    std::string order;
    std::vector<std::string> dirs;

    hdfsFileInfo *fileInfo = hdfsListDirectory(fs, "/", &numEntries);
    if (unlikely(fileInfo == nullptr || numEntries == 0)) {
      order = "WAIT";
      std::cout << "[WARN] no version found" << std::endl;
      goto cleanup;
    }

    for (int i = 0; i < numEntries; ++i) {
      boost::filesystem::path path(fileInfo[i].mName);
      std::string filename = path.filename().string();

      if (filename == "rollback.version") {
        char buffer[versionBufferSize];

        hdfsFile rbFile =
            hdfsOpenFile(fs, fileInfo[i].mName, O_RDONLY, 0, 0, 0);
        hdfsRead(fs, rbFile, buffer, versionBufferSize);
        hdfsCloseFile(fs, rbFile);

        order = std::string(buffer).substr(6);
        rtrim(order);
        // std::cout << "rollback.version == " << order << std::endl;
        goto cleanup;
      } else if (filename.find("model_") == 0) {
        // Starting with model_ means it could be valid version.
        // check if the model.done file exists, if not exists, we need wait
        // until model load to hdfs finished.
        std::string modelDonePath = FMT("/%1%/model.done", filename);
        if (hdfsGetPathInfo(fs, modelDonePath.c_str()) == NULL)
          continue;

        dirs.push_back(filename.substr(6)); // removing the 'model_' in front
      }
    }

    sort(dirs.rbegin(), dirs.rend());
    if (dirs.empty()) {
      order = "WAIT"; // this situation may occur when start.
    }

  cleanup:
    if (likely(fileInfo != nullptr && numEntries > 0))
      hdfsFreeFileInfo(fileInfo, numEntries);
    return order.empty() ? std::move(dirs) : std::vector{order};
  }

  void downloadVersionFrom(hdfsFS &fs, std::string version, int start,
                           int end) {
    static boost::format fmter(
        "/opt/hadoop-3.3.1/bin/hadoop fs -get "
        "hdfs://namenode:9000/model_%1%/model_slice.%2$03d "
        "./model_%1%/model_slice.%2$03d");

    std::string command = FMT("mkdir -p ./model_%1%", version);
    std::cout << "  dispatching command: " << command << std::endl;
    system(command.c_str());

    std::cout << "  Downloading version " << version << std::endl;
    std::cout << "  planned download: [" << start << ", " << end << "]"
              << std::endl;
    for (int i = start; i <= end; ++i) {
      command = boost::str(boost::format(fmter) % version % i);
      std::cout << "  dispatching command: " << command << std::endl;
      system(command.c_str());
    }

    VersionTracker::GetInstance().SetDownloaded(version);
  }

  /**
   Only downloads and loads into mem <b>if necessary</b>. This function is
   synchronous and supposedly takes a few minutes to finish on the slow path.
   */
  void DownloadVersionAndOptionallyLoad(hdfsFS &fs,
                                        std::vector<std::string> versions,
                                        bool shouldPreload = false) {
    auto versionTracker = VersionTracker::GetInstance();
    auto targetVer = versions[0];
    auto sliceCount = versionTracker.GetNSlices(targetVer);
    if (!sliceCount) {
      sliceCount = getVersionSliceCountFromHdfs(fs, targetVer);
      VersionTracker::GetInstance().SetNSlices(targetVer, sliceCount);
    }
    std::cout << "  slice count = " << sliceCount << std::endl;
    auto [start, end] = GetSliceRange(sliceCount, gNodeId);

    auto syncSchedule = SliceCache::ScheduleSync(targetVer);
    ++GenId_;

    /// 1. check if the version is already downloaded to local disk
    std::cout << "[1] checking if version " << targetVer << " is on disk"
              << std::endl;
    if (versionTracker.IsDownloaded(targetVer)) {
      std::cout << "  Loading into mem" << std::endl;
      loadToMem(syncSchedule, targetVer, GenId_, start, end - start + 1);
      return;
    }

    /// 2. the normal slow path
    std::cout << "[2] downloading versions[0] " << targetVer << " from hdfs"
              << std::endl;
    downloadVersionFrom(fs, targetVer, start, end);

    if (shouldPreload) {
      std::cout << "[3] attempting to preload other versions" << std::endl;
      for (int i = 1; i < versions.size(); ++i) {
        std::cout << "  Preloading versions[" << i << "]: " << versions[i]
                  << std::endl;
        downloadVersionFrom(fs, versions[i], start, end);
        std::cout << "  >> Preloading version " << versions[i] << " done"
                  << std::endl;
      }
    }

    std::cout << "  Loading into mem" << std::endl;
    loadToMem(syncSchedule, targetVer, GenId_, start, end - start + 1);
  }

  void Run() {
    hdfsFS fs = hdfsConnect("hdfs://namenode", 9000);

    while (!gIsExiting) {
      auto versions = LatestVersionOn(fs);
      auto newVersion = versions[0];
      if (newVersion != "WAIT" && CurrentVersion_ != newVersion) {
        std::cout << "Current version: " << CurrentVersion_
                  << ", new version: " << newVersion << std::endl;
        std::cout << "Getting version " << newVersion << std::endl;

        bool isFirstLoad = CurrentVersion_ == "WAIT";
        // We have the choice to download all the versions beforehand. As it
        // appears, it's not against the rules
        DownloadVersionAndOptionallyLoad(fs, versions, isFirstLoad);
        if (isFirstLoad) {
          // We're required to register this service onto Etcd
          std::cout << "Registering public interface" << std::endl;
          EtcdService::RegisterModelService();
        }
        CurrentVersion_ = newVersion;
      }
      SleepMilli(3500);
    }

    hdfsDisconnect(fs);
  }

  size_t GetNSlices(std::string version) {
    return VersionTracker::GetInstance().GetNSlices(version);
  }

  size_t GetNSlices() { return GetNSlices(CurrentVersion_); }
}; // class VersionSystem

class ModelServiceImpl final : public ModelService::Service {
private:
  using LocalReadTask = std::tuple<uint64_t, uint64_t, uint64_t, RawBuffer>;

  int getServerIdFor(int slice, int nSlices) {
    for (int i = 0; i < gNodeNum; ++i) {
      auto [start, end] = GetSliceRange(nSlices, i);
      if (start <= slice && slice <= end)
        return i;
    }
    std::cout << "ERROR: getServerIdFor(" << slice << ") failed" << std::endl;
    return 0;
  }

  /**
   Synchronous function. It will block until the data is ready.
   */
  void progress(const MamaRequest *request, MamaResponse *reply) {
    int serverNodeId;
    uint64_t slice, seek, len;
    auto reqsForNode = new IntraReq[gNodeNum];
    const auto &sliceRequests = request->slice_request();
    auto nSlices = VersionSystem::GetInstance()->GetNSlices();
    auto pCache = SliceCache::Get();

    int nNodesComm = 0;
    for (int i = 0; i < sliceRequests.size(); ++i) {
      const auto &request = sliceRequests[i];
      slice = request.slice_partition();
      seek = request.data_start();
      len = request.data_len();

      // Allocate the buffer directly in the message, so we don't have to
      // double the copying
      auto pMessage = reply->add_slice_data();
      pMessage->resize(len);
      RawBuffer buffer = pMessage->data();

      serverNodeId = getServerIdFor(slice, nSlices);
      if (serverNodeId == gNodeId) {
        // From what it appears on benchmarks, it looks like synchronous reading
        // is enough right now
        LocalRead(pCache, slice, seek, len, buffer);
      } else {
        auto &nodeReq = reqsForNode[serverNodeId];
        if (nodeReq.slice_req_size() == 0)
          ++nNodesComm;
        nodeReq.add_offset(i);
        nodeReq.add_slice_req()->CopyFrom(request);
      }
    }

    if (nNodesComm > 0) {
      std::mutex mutResponse;
      std::unique_lock lkResponse(mutResponse);

      auto &respman = IntraRespDataManager::GetInstance();
      auto [respmanId, pCvResponse] = respman.Register(reply, nNodesComm);

      for (int i = 0; i < gNodeNum; ++i) {
        if (reqsForNode[i].slice_req_size() > 0) {
          reqsForNode[i].set_id(respmanId);
          reqsForNode[i].set_gen(pCache->GenId_);
          gIntraClients[i]->EnqueueIntraReq(reqsForNode[i]);
        }
      }

      pCvResponse->wait(lkResponse,
                        [&] { return respman.IsBatchFinished(respmanId); });
    }

    delete[] reqsForNode;
    reply->set_status(0);
  }

public:
  Status Get(ServerContext *context, const MamaRequest *request,
             MamaResponse *reply) override {
    progress(request, reply);
    return Status::OK;
  }
}; // class ModelServiceImpl

static void RunPublicGrpc() {
  std::string serverAddr("0.0.0.0:50051");
  ModelServiceImpl service;

  grpc::ServerBuilder grpcServerBuilder;
  grpcServerBuilder.AddListeningPort(serverAddr,
                                     grpc::InsecureServerCredentials());
  grpcServerBuilder.RegisterService(&service);
  std::unique_ptr<grpc::Server> server(grpcServerBuilder.BuildAndStart());
  std::cout << "Server listening on " << serverAddr << std::endl;

  server->Wait();
  gIsExiting = true;
}

int ParseIntEnv(const char *envName) {
  const char *envStr = std::getenv(envName);
  return std::stoi(envStr);
}

/**
 * @brief Since we don't have a proper way to end this program, we need a way to
 * handle exits gracefully, so that `gprof` can generate the profiling data.
 * Excerpted from StackOverflow.
 */
void sigUsr1Handler(int sig) {
  std::cout << "Exiting on SIGUSR1" << std::endl;
  void (*_mcleanup)(void);
  _mcleanup = (void (*)(void))dlsym(RTLD_DEFAULT, "_mcleanup");
  if (_mcleanup == NULL)
    std::cout << "Unable to find gprof exit hook" << std::endl;
  else
    _mcleanup();
  _exit(0);
}

int main(int argc, char **argv) {
  signal(SIGUSR1, sigUsr1Handler);

  gNodeId = ParseIntEnv("NODE_ID") - 1;
  gNodeNum = ParseIntEnv("NODE_NUM");
  gNodeIp = GetIpAddr();

  IntraServiceImpl intraServer;
  intraServer.Run(intraPort); // Don't have to call the destructor explicitly
  EtcdService::RegisterIntra();
  EtcdService::GetAllNodesIp(); // synchronous
  EstablishIntraConn();

  std::vector<std::thread> threads;
  threads.emplace_back(&VersionSystem::Run, VersionSystem::GetInstance());
  threads.emplace_back(&RunPublicGrpc);

  for (auto &thread : threads)
    thread.join();

  return 0;
}
