#include "ModelSliceReader.h"
#include "alimama.grpc.pb.h"
#include "alimama.pb.h"
#include "ourmama.grpc.pb.h"
#include "ourmama.pb.h"
#include <arpa/inet.h>
#include <boost/filesystem.hpp>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <ctime>
#include <deque>
#include <dlfcn.h>
#include <etcd/Client.hpp>
#include <etcd/Response.hpp>
#include <etcd/Watcher.hpp>
#include <filesystem>
#include <fmt/core.h> // fmtlib is neater and faster than boost::format
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

#define BOOST_LOG_DYN_LINK 1
#include <boost/log/core.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/trivial.hpp>
namespace logging = boost::log;

using MamaRequest = alimama::proto::Request;
using MamaResponse = alimama::proto::Response;
using SliceRequest = alimama::proto::SliceRequest;
using alimama::proto::ModelService;

using IntraReq = ourmama::proto::IntraReq;
using IntraResp = ourmama::proto::IntraResp;
using google::protobuf::Empty;
using ourmama::proto::IntraService;
using ourmama::proto::PingService;

using grpc::ServerContext;
using grpc::Status;

using RawBuffer = char *;

#define likely(x) __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)

constexpr size_t KB = 1024;
constexpr size_t MB = 1024 * KB;
constexpr size_t GB = 1024 * MB;

constexpr int queryInterval = 1000; // milliseconds
constexpr int pingPort = 50049;
constexpr int intraPort = 50050;

const std::string etcdUrl = "http://etcd:2379";
const std::string etcdIntraDiscoveryFmt = "/intra/{}";
const std::string etcdVersionSyncNodeFmt = "/version-{}/{}";

bool gIsExiting = false;
int gNodeId;
int gNodeNum;
int gSliceCount, gAvgSlice, gQuotient;
std::string gNodeIp;
std::vector<std::string> gNodeIps;

void SleepMilli(int ms) {
  std::this_thread::sleep_for(std::chrono::milliseconds(ms));
}

#ifdef LOWQPS
std::atomic<int> tokenId;
std::atomic<int> grantId;
void RateLimiter() {
  while (!gIsExiting) {
    if (tokenId <= grantId + 20)
      ++tokenId;
    std::this_thread::sleep_for(std::chrono::microseconds(2000));
  }
}
#endif

// trim from end
static inline std::string rtrim(std::string s) {
  s.erase(std::find_if(s.rbegin(), s.rend(),
                       [](unsigned char ch) { return !std::isspace(ch); })
              .base(),
          s.end());
  return s;
}

namespace EtcdService {
#define LAUNCH_ETCD_TASK(keyStr, opStr)                                        \
  try {                                                                        \
    etcd::Response response = task.get();                                      \
    BOOST_LOG_TRIVIAL(info) << "[etcd] " << opStr << " " << (keyStr)           \
                            << (response.is_ok() ? " succeeds" : " failed");   \
  } catch (const std::exception &e) {                                          \
    BOOST_LOG_TRIVIAL(error)                                                   \
        << "[etcd] An exception occurred during " << opStr << " " << (keyStr); \
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
      auto response = client
                          .set(fmt::format(etcdIntraDiscoveryFmt, gNodeIp),
                               std::getenv("NODE_ID"))
                          .get();
      if (response.is_ok()) {
        BOOST_LOG_TRIVIAL(info) << "RegisterIntra success";
        return;
      } else {
        BOOST_LOG_TRIVIAL(warning)
            << "RegisterIntra: failed; reason: " << response.error_message()
            << "; retrying...";
        SleepMilli(queryInterval);
      }
    } catch (const std::exception &e) {
      BOOST_LOG_TRIVIAL(error) << "RegisterIntra: exception: " << e.what();
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
          BOOST_LOG_TRIVIAL(info)
              << "GetAllNodesIp: only " << response.keys().size()
              << " nodes are discovered, retrying...";
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
        BOOST_LOG_TRIVIAL(warning)
            << "GetAllNodesIp failed: " << response.error_code() << " "
            << response.error_message();
        SleepMilli(queryInterval);
      }
    } catch (const std::exception &e) {
      BOOST_LOG_TRIVIAL(error)
          << "An exception occurred during GetAllNodesIp: " << e.what();
      return;
    }
  }
}

static void RegisterAllModelServices() {
  for (int i = 1; i <= gNodeNum; ++i)
    set(fmt::format("/services/modelservice/node-{}:50051", i), "");
}

static void RegisterVersion(int gen) {
  set(fmt::format(etcdVersionSyncNodeFmt, gen, gNodeId), "");
}

static void UnregisterVersion(int gen) {
  rm(fmt::format(etcdVersionSyncNodeFmt, gen, gNodeId));
}
} // namespace EtcdService

std::atomic<int> gSyncCnt = 0;
etcd::Watcher *pWatcher = nullptr;

class SliceCache {
private:
  std::string targetPath(int slice) {
    return fmt::format("/tmp/model_{}/model_slice.{:03d}", Version_, slice);
  }

  int start_;
  int cnt_;
  std::vector<ModelSliceReader *> readers_;

  inline static std::queue<std::shared_ptr<SliceCache>> caches_;
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

public:
  const std::string Version_;
  uint32_t GenId_;

  static void WatcherCb(const etcd::Response &_resp) {
    BOOST_LOG_TRIVIAL(info) << "WatcherCb gSyncCnt: " << ++gSyncCnt;
    if (gSyncCnt == gNodeNum)
      cvCache_.notify_all();
  }

  /**
   * Must be called before `SliceCache::Load`.
   */
  static void ScheduleSync(int gen) {
    BOOST_LOG_TRIVIAL(info) << "ScheduleSync gen: " << gen;

    auto start = fmt::format(etcdVersionSyncNodeFmt, gen, 0);
    auto end = fmt::format(etcdVersionSyncNodeFmt, gen, gNodeNum);
    pWatcher = new etcd::Watcher(etcdUrl, start, end, WatcherCb);
  }

  static std::shared_ptr<SliceCache> Get() {
    std::shared_lock lk(mutP_);
    return caches_.back();
  }

  static std::shared_ptr<SliceCache> Get(uint32_t genId) {
    std::shared_lock lk(mutP_);
    return caches_.back()->GenId_ == genId    ? caches_.back()
           : caches_.front()->GenId_ == genId ? caches_.front()
                                              : nullptr;
  }

  static std::shared_ptr<SliceCache> Load(std::string version, uint32_t genId,
                                          int start, int cnt) {
    BOOST_LOG_TRIVIAL(info) << "SliceCache::Load";

    BOOST_LOG_TRIVIAL(info) << "1. Prepare the new version";
    std::shared_ptr<SliceCache> newCache =
        std::make_shared<SliceCache>(version, genId, start, cnt);
    newCache->load();

    return newCache;
  }

  static void MakeVisible(std::shared_ptr<SliceCache> newCache) {
    BOOST_LOG_TRIVIAL(info) << "2. Tell everyone else I'm ready to switch";
    EtcdService::RegisterVersion(newCache->GenId_);

    BOOST_LOG_TRIVIAL(info) << "3. Wait for everyone else to be ready";
    {
      std::unique_lock lock(mutCv_);
      cvCache_.wait(lock, [=] { return gSyncCnt == gNodeNum; });
      gSyncCnt = 0;
    }

    BOOST_LOG_TRIVIAL(info) << "SliceCache::Load: everyone is ready";
    delete pWatcher;
    ScheduleSync(newCache->GenId_ + 1);

    {
      std::unique_lock lock(mutP_);
      if (caches_.size() == 2)
        caches_.pop();
      caches_.push(newCache);
    }
  }

  SliceCache(std::string version, uint32_t genId, int start, int cnt)
      : Version_(version), GenId_(genId), start_(start), cnt_(cnt) {}

  void Read(int slice, size_t seek, size_t len, RawBuffer buffer) {
    readers_[slice - start_]->Read(seek, len, buffer);
  }

  ~SliceCache() {
    EtcdService::UnregisterVersion(GenId_);
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

class PingServiceClient final {
public:
  PingServiceClient(std::shared_ptr<grpc::Channel> channel)
      : stub_(PingService::NewStub(channel)) {}

  void Ping() {
    static auto _empty = Empty();

    grpc::ClientContext context;
    Empty _pemp;
    Status status = stub_->Ping(&context, _empty, &_pemp);
    if (!status.ok())
      BOOST_LOG_TRIVIAL(error)
          << "PingServiceClient::Ping: " << status.error_message();
    else
      BOOST_LOG_TRIVIAL(info) << "PingServiceClient::Ping: OK";
  }

private:
  std::unique_ptr<PingService::Stub> stub_;
}; // class PingServiceClient

PingServiceClient *pPingClient = nullptr;

std::condition_variable gCvPing;
bool gPinged = false;

class PingServiceImpl final : public PingService::Service {
public:
  Status Ping(grpc::ServerContext *context, const Empty *_noreq,
              Empty *_norep) override {
    gPinged = true;
    gCvPing.notify_one();
    return Status::OK;
  }

  void Run() {
    const auto addr = fmt::format("0.0.0.0:{}", pingPort);

    grpc::ServerBuilder grpcServerBuilder;
    grpcServerBuilder.AddListeningPort(addr, grpc::InsecureServerCredentials());
    grpcServerBuilder.RegisterService(this);
    std::unique_ptr<grpc::Server> server(grpcServerBuilder.BuildAndStart());
    BOOST_LOG_TRIVIAL(info) << "Server listening on " << addr;

    server->Wait();
  }
}; // class PingServiceImpl

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
    parseRespThread_ = std::thread(&IntraServiceClient::ParseResponse, this);
  }

  ~IntraServiceClient() { parseRespThread_.detach(); }

  void SendIntraReq(const IntraReq &intraReq) {
    AsyncClientCall *call = new AsyncClientCall();
    call->responseReader =
        stub_->PrepareAsyncGet(&call->context, intraReq, &cq_);
    call->responseReader->StartCall();
    call->responseReader->Finish(&call->reply, &call->status,
                                 static_cast<void *>(call));
  }

  void ParseResponse() {
    void *tag;
    bool ok = false;
    IntraRespDataManager &respman = IntraRespDataManager::GetInstance();

    while (!gIsExiting) {
      cq_.Next(&tag, &ok);
      if (unlikely(!ok)) {
        BOOST_LOG_TRIVIAL(error) << "cq_.Next failed";
        continue;
      }
      AsyncClientCall *call = static_cast<AsyncClientCall *>(tag);
      auto &intraResp = call->reply;

      auto pMessage = respman.GetMessage(intraResp.id());
      if (unlikely(!pMessage))
        BOOST_LOG_TRIVIAL(error)
            << "pMessage is nullptr for " << intraResp.id();

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

private:
  grpc::CompletionQueue cq_; // CompletionQueue is thread safe.
  std::unique_ptr<IntraService::Stub> stub_;
  std::thread parseRespThread_;
}; // class IntraServiceClient

std::vector<IntraServiceClient *> gIntraClients;

void EstablishIntraConn() {
  gIntraClients.resize(gNodeNum);
  for (int i = 0; i < gNodeNum; ++i) {
    if (i != gNodeId)
      gIntraClients[i] = new IntraServiceClient(
          grpc::CreateChannel(fmt::format("node-{}:{}", i + 1, intraPort),
                              grpc::InsecureChannelCredentials()));
  }
}

void LocalRead(std::shared_ptr<SliceCache> pSc, uint64_t slice, uint64_t seek,
               uint64_t len, RawBuffer buffer) {
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
      std::shared_ptr<SliceCache> pCache;
      while (!(pCache = SliceCache::Get(genId))) {
        BOOST_LOG_TRIVIAL(warning) << "pCache is nullptr for " << genId;
        std::this_thread::sleep_for(std::chrono::microseconds(50));
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
    void *tag = nullptr;
    bool ok;
    while (!gIsExiting) {
      cq_->Next(&tag, &ok);
      if (unlikely(!ok || !tag)) {
        BOOST_LOG_TRIVIAL(error) << "HandleRPCS: cq_->Next failed";
        continue;
      }
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
    std::string addr = fmt::format("0.0.0.0:{}", port);

    grpc::ServerBuilder builder;
    builder.AddListeningPort(addr, grpc::InsecureServerCredentials());
    builder.RegisterService(&service_);
    cq_ = builder.AddCompletionQueue();
    server_ = builder.BuildAndStart();
    BOOST_LOG_TRIVIAL(info) << "Server listening on " << addr;

    // constexpr int nThreads = 6;
    constexpr int nThreads = 1;
    for (int i = 0; i < nThreads; ++i)
      threads_.emplace_back(&IntraServiceImpl::HandleRPCs, this);
  }
}; // class IntraServiceImpl

std::pair<int, int> GetSliceRange(int nodeId) {
  int start = gAvgSlice * nodeId + std::min(nodeId, gQuotient);
  return {start, start + gAvgSlice + (nodeId < gQuotient ? 1 : 0) - 1};
}

class VersionSystem {
private:
  class VersionTracker {
    std::unordered_set<std::string> record_;
    VersionTracker() = default;

  public:
    bool IsDownloaded(std::string version) {
      return record_.count(version) > 0;
    }

    void SetDownloaded(std::string version) { record_.insert(version); }

    static VersionTracker &GetInstance() {
      static VersionTracker instance;
      return instance;
    }
  }; // class VersionTracker

  int getVersionSliceCountFromHdfs(hdfsFS &fs, std::string modelVersion) {
    int numEntries = 0;
    std::string path = fmt::format("/model_{}/", modelVersion);

    hdfsFileInfo *fileInfo = hdfsListDirectory(fs, path.c_str(), &numEntries);
    if (likely(fileInfo != nullptr && numEntries > 0)) {
      hdfsFreeFileInfo(fileInfo, numEntries);
      return numEntries - 2;
    } else {
      BOOST_LOG_TRIVIAL(info) << "  hdfs: list directory " << path << " failed";
      return 0;
    }
  }

  // Assuming the version pointed by pSliceCaches.back() is the latest
  void loadToMem(std::string version, uint32_t genId, int start, int cnt,
                 bool shouldPing) {
    BOOST_LOG_TRIVIAL(info) << "  Preparing new cache";
    auto newCache = SliceCache::Load(version, genId, start, cnt);
    if (shouldPing) {
      BOOST_LOG_TRIVIAL(info) << "  Pinging next in line";
      pPingClient->Ping();
    }
    SliceCache::MakeVisible(newCache);
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
    constexpr size_t versionBufferSize = 128;

    int numEntries;
    std::string order;
    std::vector<std::string> dirs;

    hdfsFileInfo *fileInfo = hdfsListDirectory(fs, "/", &numEntries);
    if (unlikely(fileInfo == nullptr || numEntries == 0)) {
      order = "WAIT";
      BOOST_LOG_TRIVIAL(warning) << "no version found";
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

        order = std::string(buffer).substr(
            6, std::string("YYYY_MM_DD_HH_mm_SS").size());
        // BOOST_LOG_TRIVIAL(info)  << "rollback.version == " << order <<
        // std::endl;
        goto cleanup;
      } else if (filename.find("model_") == 0) {
        // Starting with model_ means it could be valid version.
        // check if the model.done file exists, if not exists, we need wait
        // until model load to hdfs finished.
        auto modelDonePath = fmt::format("/{}/model.done", filename);
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

    BOOST_LOG_TRIVIAL(info) << "  Downloading version " << version;
    BOOST_LOG_TRIVIAL(info)
        << "  planned download: [" << start << ", " << end << "]";
    hdfsFS localFs = hdfsConnect("file://localhost/", 0);
    std::filesystem::create_directory(fmt::format("/tmp/model_{}", version));
    for (int i = start; i <= end; ++i) {
      BOOST_LOG_TRIVIAL(info) << "  Downloading slice " << i;
      hdfsCopy(
          fs, fmt::format("/model_{0}/model_slice.{1:03d}", version, i).c_str(),
          localFs,
          fmt::format("/tmp/model_{0}/model_slice.{1:03d}", version, i)
              .c_str());
    }
    hdfsDisconnect(localFs);

    VersionTracker::GetInstance().SetDownloaded(version);
  }

  /**
   Only downloads and loads into mem <b>if necessary</b>. This function is
   synchronous and supposedly takes a few minutes to finish on the slow path.
   */
  void LoadVersionsOnDemand(hdfsFS &fs,
                            const std::vector<std::string> &versions,
                            bool shouldPreload = false) {
    auto versionTracker = VersionTracker::GetInstance();
    auto targetVer = versions[0];
    if (!gSliceCount) {
      gSliceCount = getVersionSliceCountFromHdfs(fs, targetVer);
      gAvgSlice = gSliceCount / gNodeNum;
      gQuotient = gSliceCount % gNodeNum;
    }
    BOOST_LOG_TRIVIAL(info) << "  slice count = " << gSliceCount;
    auto [start, end] = GetSliceRange(gNodeId);

    ++GenId_;

    /// 1. check if the version is already downloaded to local disk
    BOOST_LOG_TRIVIAL(info)
        << "[1] checking if version " << targetVer << " is on disk";
    if (versionTracker.IsDownloaded(targetVer)) {
      goto memload;
    }

    /// 2. the normal slow path
    BOOST_LOG_TRIVIAL(info)
        << "[2] downloading version " << targetVer << " from hdfs";
    downloadVersionFrom(fs, targetVer, start, end);

    if (shouldPreload) {
      BOOST_LOG_TRIVIAL(info) << "[3] attempting to preload other versions";
      for (int i = 1; i < versions.size(); ++i) {
        BOOST_LOG_TRIVIAL(info)
            << "  Preloading versions[" << i << "]: " << versions[i];
        downloadVersionFrom(fs, versions[i], start, end);
        BOOST_LOG_TRIVIAL(info)
            << "  >> Preloading version " << versions[i] << " done";
      }
    }

  memload:
    BOOST_LOG_TRIVIAL(info) << "  Loading into mem";
    loadToMem(targetVer, GenId_, start, end - start + 1, !shouldPreload);
  }

  void Run() {
    hdfsFS fs = hdfsConnect("hdfs://namenode", 9000);

    while (!gIsExiting) {
      bool isFirstLoad = CurrentVersion_ == "WAIT";
      if (!isFirstLoad && gNodeId >= gNodeNum / 2) {
        std::mutex mut;
        std::unique_lock lk(mut);
        gCvPing.wait(lk, [] { return gPinged; });
      }

      auto versions = LatestVersionOn(fs);
      auto newVersion = versions[0];
      if (newVersion != "WAIT" && CurrentVersion_ != newVersion) {
        BOOST_LOG_TRIVIAL(info) << "Current version: " << CurrentVersion_
                                << ", new version: " << newVersion;

        // We have the choice to download all the versions beforehand. As it
        // appears, it's not against the rules
        LoadVersionsOnDemand(fs, versions, isFirstLoad);
        CurrentVersion_ = newVersion;

        if (gNodeId == 0 && isFirstLoad) {
          // We're required to register this service onto Etcd
          BOOST_LOG_TRIVIAL(info) << "Registering public interface";
          EtcdService::RegisterAllModelServices();
        }
      }

      gPinged = false;

      if (gNodeId < gNodeNum / 2)
        SleepMilli(3000);
    }

    hdfsDisconnect(fs);
  }
}; // class VersionSystem

class ModelServiceImpl final : public ModelService::Service {
private:
  int getServerIdFor(int slice) {
    for (int i = 0; i < gNodeNum; ++i) {
      auto [start, end] = GetSliceRange(i);
      if (start <= slice && slice <= end)
        return i;
    }
    BOOST_LOG_TRIVIAL(error) << "getServerIdFor(" << slice << ") failed";
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

      serverNodeId = getServerIdFor(slice);
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
          gIntraClients[i]->SendIntraReq(reqsForNode[i]);
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
#ifdef LOWQPS
    int localGrantId = grantId++;
    while (localGrantId > tokenId) {
      /* loop indefinitely */
    }
#endif
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
  BOOST_LOG_TRIVIAL(info) << "Server listening on " << serverAddr;

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
  BOOST_LOG_TRIVIAL(info) << "Exiting on SIGUSR1";
  void (*_mcleanup)(void);
  _mcleanup = (void (*)(void))dlsym(RTLD_DEFAULT, "_mcleanup");
  if (_mcleanup == NULL)
    BOOST_LOG_TRIVIAL(info) << "Unable to find gprof exit hook";
  else
    _mcleanup();
  _exit(0);
}

int main(int argc, char **argv) {
  signal(SIGUSR1, sigUsr1Handler);
  logging::core::get()->set_filter(logging::trivial::severity >=
                                   logging::trivial::info);

  gNodeId = ParseIntEnv("NODE_ID") - 1;
  gNodeNum = ParseIntEnv("NODE_NUM");
  gNodeIp = GetIpAddr();

  IntraServiceImpl intraServer;
  intraServer.Run(intraPort); // Don't have to call the destructor explicitly
  EtcdService::RegisterIntra();
  EtcdService::GetAllNodesIp(); // synchronous
  EstablishIntraConn();

  std::vector<std::thread> threads;
  PingServiceImpl pingServer;
  threads.emplace_back(&PingServiceImpl::Run, &pingServer);

  int nextInRing =
      1 + gNodeId + gNodeNum / 2 * (gNodeId < gNodeNum / 2 ? 1 : -1);
  pPingClient = new PingServiceClient(
      grpc::CreateChannel(fmt::format("node-{}:{}", nextInRing, pingPort),
                          grpc::InsecureChannelCredentials()));
  SliceCache::ScheduleSync(1);

  threads.emplace_back(&VersionSystem::Run, VersionSystem::GetInstance());
  threads.emplace_back(&RunPublicGrpc);

#ifdef LOWQPS
  threads.emplace_back(&RateLimiter);
#endif

  for (auto &thread : threads)
    thread.join();

  delete pPingClient;

  return 0;
}
