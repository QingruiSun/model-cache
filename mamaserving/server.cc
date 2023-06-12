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
#include <grpcpp/grpcpp.h>
#include <hdfs/hdfs.h>
#include <ifaddrs.h>
#include <iostream>
#include <memory>
#include <mutex>
#include <netinet/in.h>
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

constexpr int parallelDownloads = 2;
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

class SliceCache;
std::deque<SliceCache *> gSliceCaches;
class SliceCache {
private:
  std::string targetPath(int slice) {
    return FMT("./model_%1%/model_slice.%2$03d", Version_ % slice);
  }

  int start_;
  int cnt_;
  std::vector<ModelSliceReader *> readers_;

public:
  const std::string Version_;

  SliceCache(std::string version, int start, int cnt)
      : Version_(version), start_(start), cnt_(cnt) {}

  void Load() {
    readers_.resize(cnt_);
    for (int i = 0; i < cnt_; i++) {
      // TODO: what if a version switch happens during load
      readers_[i] = new ModelSliceReader();
      readers_[i]->Load(targetPath(start_ + i));
    }
  }

  bool Has(int slice) { return slice >= start_ && slice < start_ + cnt_; }

  void Unload() {
    for (auto reader : readers_) {
      reader->Unload();
      delete reader;
    }
  }

  void Read(int slice, size_t seek, size_t len, RawBuffer buffer) {
    readers_[slice - start_]->Read(seek, len, buffer);
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

class LocalDownloadTracker {
public:
  void RecordDownloadedSlice(std::string version, int slice) {
    std::cout << "  > " << version << "[" << slice << "] finished" << std::endl;
    std::unique_lock lock(mutex_);

    auto &versionRecord = ivr_[version];
    versionRecord.finishedSlices.insert(slice);
    if (versionRecord.finished()) {
      std::cout << "  >> " << version << " all finished" << std::endl;
      finishedVersions_.insert(version);
      versionCv_[version]->notify_one();
    }
  }

  bool IsSliceDownloaded(std::string version, int slice) {
    std::shared_lock lock(mutex_);

    auto &fins = ivr_[version].finishedSlices;
    return fins.count(slice) > 0;
  }

  bool IsVersionDownloaded(std::string version) {
    std::shared_lock lock(mutex_);
    return finishedVersions_.count(version) > 0;
  }

  void MarkNewDownload(std::string version, int requiredSlices,
                       std::condition_variable *cv) {
    std::unique_lock lock(mutex_);
    ivr_[version].requiredSlices = requiredSlices;
    versionCv_[version] = cv;
  }

private:
  struct InternalVersionRecord {
    int requiredSlices;
    std::unordered_set<int> finishedSlices;

    bool finished() { return requiredSlices == finishedSlices.size(); }
  };

  std::shared_mutex mutex_;
  std::unordered_map<std::string, InternalVersionRecord> ivr_;
  std::unordered_set<std::string> finishedVersions_;
  std::unordered_map<std::string, std::condition_variable *> versionCv_;
}; // class LocalDownloadTracker

class DownloadThreadPool {
private:
  struct HdfsModelPath {
    std::string version;
    uint64_t slice;

    HdfsModelPath(std::string version, uint64_t slice)
        : version(version), slice(slice) {}
  };

public:
  DownloadThreadPool(int nThreads, LocalDownloadTracker *pLocalDownloadTracker)
      : stop_(false), pLocalDownloadTracker_(pLocalDownloadTracker) {
    for (int i = 0; i < nThreads; ++i)
      workers_.emplace_back(&DownloadThreadPool::ThreadFunc, this);
  }

  ~DownloadThreadPool() {
    {
      std::unique_lock<std::mutex> lock(queueMutex_);
      stop_ = true;
    }
    condition_.notify_all();
    for (std::thread &worker : workers_)
      worker.join();
  }

  void DownloadSliceFromHdfs(std::string version, int slice) {
    std::string command = FMT("mkdir -p ./model_%1%", version);
    std::cout << "  dispatching command: " << command << std::endl;
    system(command.c_str());

    command = FMT("/opt/hadoop-3.3.1/bin/hadoop fs -get "
                  "hdfs://namenode:9000/model_%1%/model_slice.%2$03d "
                  "./model_%1%/model_slice.%2$03d",
                  version % slice);
    std::cout << "  dispatching command: " << command << std::endl;
    system(command.c_str());
    pLocalDownloadTracker_->RecordDownloadedSlice(version, slice);
  }

  void ThreadFunc() {
    while (true) {
      std::unique_lock<std::mutex> lock(this->queueMutex_);
      this->condition_.wait(
          lock, [this] { return this->stop_ || !this->tasks_.empty(); });
      if (this->stop_ && this->tasks_.empty())
        return;
      HdfsModelPath hdfsPath = tasks_.front();
      tasks_.pop();
      lock.unlock();
      DownloadSliceFromHdfs(hdfsPath.version, hdfsPath.slice);
    }
  }

  // we need call this function when see new model version or roll back, because
  // we don't need to download old slice from hdfs.
  void ClearAllTasks() {
    std::unique_lock<std::mutex> lock(queueMutex_);
    tasks_ = {}; // this clear queue
  }

  void InsertTask(std::string version, std::pair<int, int> &&sliceRange) {
    std::unique_lock<std::mutex> lock(queueMutex_);
    for (int i = sliceRange.first; i <= sliceRange.second; ++i)
      tasks_.emplace(version, i);
    this->condition_.notify_one();
  }

  // Harlan: I moved it to public since it is accessed in a lambda function,
  // and with GCC's impl of C++ 11 lambda, it can't access private members.
  std::queue<HdfsModelPath> tasks_;

private:
  std::vector<std::thread> workers_;
  std::mutex queueMutex_;
  std::condition_variable condition_;
  bool stop_;
  LocalDownloadTracker *pLocalDownloadTracker_;
}; // class DownLoadThreadPool

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
  set(FMT(etcdIntraDiscoveryFmt, gNodeIp), std::getenv("NODE_ID"));
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

void LocalRead(uint64_t slice, uint64_t seek, uint64_t len, RawBuffer buffer) {
  gSliceCaches.back()->Read(slice, seek, len, buffer);
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
      const int N = sliceReqs.size();
      reply->set_id(request.id());

      for (int i = 0; i < N; ++i) {
        const auto &req = sliceReqs[i];
        slice = req.slice_partition();
        seek = req.data_start();
        len = req.data_len();

        reply->add_offset(request.offset(i));
        auto d = reply->add_data();
        d->resize(len);

        LocalRead(slice, seek, len, d->data());
      }
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
  LocalDownloadTracker localDownloadTracker_;
  DownloadThreadPool *pThreadPool_;

  class VersionTracker {
    std::unordered_map<std::string, size_t> record_;
    VersionTracker() = default;

  public:
    size_t GetNSlices(std::string version) { return record_[version]; }

    void SetRecord(std::string version, size_t nSlices) {
      record_[version] = nSlices;
    }

    static VersionTracker &GetInstance() {
      static VersionTracker instance;
      return instance;
    }
  }; // class VersionTracker

  static void waitForOtherNodesToLoad(std::string version) {
    static boost::format fmter(etcdVersionSyncPrefixFmt);

    while (true) {
      etcd::Client client(etcdUrl);
      auto prefix = boost::str(boost::format(fmter) % version);
      auto task = client.ls(prefix);
      auto response = task.get();
      std::cout << "[etcd] ls " << prefix << " got " << response.keys().size()
                << " keys" << std::endl;
      if (response.keys().size() == gNodeNum)
        return;

      SleepMilli(queryInterval);
    }
  }

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
  SliceCache *loadToMem(std::string version, int start, int cnt) {
    if (gSliceCaches.size() > 2) {
      std::cout << "  popping out old cache" << std::endl;

      auto *pOldCache = gSliceCaches.front();
      const auto oldVersion = pOldCache->Version_;
      gSliceCaches.pop_front();
      pOldCache->Unload();
      EtcdService::UnregisterVersion(oldVersion);
      delete pOldCache;
    }

    std::cout << "  Preparing new cache" << std::endl;
    auto pCache = new SliceCache(version, start, cnt);
    pCache->Load();
    return pCache;
  }

  VersionSystem() = default;

public:
  std::string CurrentVersion_ = "WAIT";

  static VersionSystem *GetInstance() {
    static VersionSystem instance;
    return &instance;
  }

  void InjectDownloadThreadPool(DownloadThreadPool *pPool) {
    pThreadPool_ = pPool;
  }

  std::string LatestVersionOn(hdfsFS &fs) {
    constexpr size_t versionBufferSize = 1024;

    int numEntries;
    std::string ret;
    std::vector<std::string> dirs;

    hdfsFileInfo *fileInfo = hdfsListDirectory(fs, "/", &numEntries);
    if (unlikely(fileInfo == nullptr || numEntries == 0)) {
      ret = "WAIT";
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

        ret = std::string(buffer).substr(6);
        goto cleanup;
      } else if (filename.find("model_") == 0) {
        // Starting with model_ means it could be valid version.
        // check if the model.done file exists, if not exists, we need wait
        // until model load to hdfs finished.
        std::string modelDonePath = FMT("/%1%/model.done", filename);
        if (hdfsGetPathInfo(fs, modelDonePath.c_str()) == NULL) {
          std::cout << modelDonePath << " doesn't exist" << std::endl;
          continue;
        }

        dirs.push_back(filename.substr(6)); // removing the 'model_' in front
      }
    }

    sort(dirs.rbegin(), dirs.rend());
    if (!dirs.empty()) {
      ret = dirs[0];
    } else {
      ret = "WAIT"; // this situation may occur when start.
    }

  cleanup:
    if (likely(fileInfo != nullptr && numEntries > 0))
      hdfsFreeFileInfo(fileInfo, numEntries);
    return ret;
  }

  /**
   Only downloads and loads into mem <b>if necessary</b>. This function is
   synchronous and supposedly takes a few minutes to finish on the slow path.
   */
  void DownloadVersionAndLoadIntoMem(hdfsFS &fs, std::string version) {
    std::mutex mutDownload;
    std::unique_lock<std::mutex> lkDownload(mutDownload);
    std::condition_variable cvDownload;

    auto sliceCount = VersionTracker::GetInstance().GetNSlices(version);
    if (!sliceCount) {
      sliceCount = getVersionSliceCountFromHdfs(fs, version);
      VersionTracker::GetInstance().SetRecord(version, sliceCount);
    }
    std::cout << "  slice count = " << sliceCount << std::endl;
    auto [start, end] = GetSliceRange(sliceCount, gNodeId);

    SliceCache *newSliceCache = nullptr;

    // Harlan: I wanted to do `goto` but they crosses too many variable
    // initializations; macros suffices now
#define REGISTER_ETCD()                                                        \
  std::cout << "  Registering onto etcd" << std::endl;                         \
  EtcdService::RegisterVersion(version);                                       \
  waitForOtherNodesToLoad(version);                                            \
  assert(newSliceCache != nullptr);                                            \
  gSliceCaches.push_back(newSliceCache);                                       \
  std::cout << "  DownloadVersionAndLoadIntoMem exiting..." << std::endl;

#define LOAD_MEM()                                                             \
  std::cout << "  Loading into mem" << std::endl;                              \
  newSliceCache = loadToMem(version, start, end - start + 1);                  \
  REGISTER_ETCD();

    /// 1. check if the version is already loaded in mem
    std::cout << "[1] checking if version " << version << " is in ram"
              << std::endl;
    for (int i = 0; i < gSliceCaches.size(); ++i) {
      if (gSliceCaches[i]->Version_ == version) {
        newSliceCache = gSliceCaches[i];
        gSliceCaches.erase(gSliceCaches.begin() + i);
        REGISTER_ETCD();
        return;
      }
    }

    /// 2. check if the version is already downloaded to local disk
    std::cout << "[2] checking if version " << version << " is in disk"
              << std::endl;
    if (localDownloadTracker_.IsVersionDownloaded(version)) {
      LOAD_MEM();
      return;
    }

    /// 3. the normal slow path
    std::cout << "[3] downloading version " << version << " from hdfs"
              << std::endl;
    std::cout << "  planned download: [" << start << ", " << end << "]"
              << std::endl;
    localDownloadTracker_.MarkNewDownload(version, end - start + 1,
                                          &cvDownload);
    if (pThreadPool_) {
      // Cancel unfinished downloads, if any
      pThreadPool_->ClearAllTasks();
      delete pThreadPool_;
    }
    pThreadPool_ =
        new DownloadThreadPool(parallelDownloads, &localDownloadTracker_);
    std::cout << "  download pool created" << std::endl;
    InjectDownloadThreadPool(pThreadPool_);
    pThreadPool_->InsertTask(version, {start, end});

    // wait for download tasks to finish
    cvDownload.wait(lkDownload, [&] {
      return localDownloadTracker_.IsVersionDownloaded(version);
    });

    LOAD_MEM();

#undef LOAD_MEM
#undef REGISTER_ETCD
  }

  void Run() {
    hdfsFS fs = hdfsConnect("hdfs://namenode", 9000);

    while (!gIsExiting) {
      std::string newVersion = LatestVersionOn(fs);
      if (newVersion != "WAIT" && CurrentVersion_ != newVersion) {
        std::cout << "Current version: " << CurrentVersion_
                  << ", new version: " << newVersion << std::endl;
        std::cout << "Getting version " << newVersion << std::endl;
        DownloadVersionAndLoadIntoMem(fs, newVersion);
        if (CurrentVersion_ == "WAIT") {
          // We're required to register this service onto Etcd
          std::cout << "Registering public interface" << std::endl;
          EtcdService::RegisterModelService();
        }
        CurrentVersion_ = std::move(newVersion);
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(queryInterval));
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
   * @brief Unused function; might be useful in the future (though not very
   * likely).
   */
  void localReadAsync(std::vector<LocalReadTask> &&tasks) {
    constexpr int nParallelReads = 2;
    std::vector<std::thread> threads;

    std::string version;
    uint64_t slice, seek, len;
    RawBuffer buffer;

    const int nTasks = tasks.size();
    const int groupEnd = nTasks - (nTasks % nParallelReads);
    for (int start = 0; start < groupEnd; start += nParallelReads) {
      for (int subtask = 0; subtask < nParallelReads; ++subtask) {
        std::tie(slice, seek, len, buffer) = tasks[start + subtask];
        threads.emplace_back(&LocalRead, slice, seek, len, buffer);
      }
      for (auto &t : threads)
        t.join();
      threads.clear();
    }

    for (int i = groupEnd; i < nTasks; ++i) {
      std::tie(slice, seek, len, buffer) = tasks[i];
      threads.emplace_back(&LocalRead, slice, seek, len, buffer);
    }
    for (auto &t : threads)
      t.join();
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
        LocalRead(slice, seek, len, buffer);
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
      std::unique_lock<std::mutex> lkResponse(mutResponse);

      auto &respman = IntraRespDataManager::GetInstance();
      auto [respmanId, pCvResponse] = respman.Register(reply, nNodesComm);

      for (int i = 0; i < gNodeNum; ++i) {
        if (reqsForNode[i].slice_req_size() > 0) {
          reqsForNode[i].set_id(respmanId);
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
