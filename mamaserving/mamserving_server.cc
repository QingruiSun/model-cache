#include "ModelSliceReader.h"
#include "alimama.grpc.pb.h"
#include "alimama.pb.h"
#include "collab.grpc.pb.h"
#include "collab.pb.h"
#include "hdfs.h"
#include <arpa/inet.h>
#include <boost/log/core.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/sinks/text_file_backend.hpp>
#include <boost/log/sources/record_ostream.hpp>
#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/utility/setup/common_attributes.hpp>
#include <boost/log/utility/setup/file.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <chrono>
#include <cstdlib>
#include <deque>
#include <etcd/Client.hpp>
#include <etcd/Response.hpp>
#include <etcd/Watcher.hpp>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include <ifaddrs.h>
#include <iostream>
#include <memory>
#include <netinet/in.h>
#include <sys/socket.h>
#include <thread>

namespace logging = boost::log;
namespace sinks = boost::log::sinks;
namespace expr = boost::log::expressions;

using MamaRequest = alimama::proto::Request;
using MamaResponse = alimama::proto::Response;
using SliceRequest = alimama::proto::SliceRequest;
using alimama::proto::ModelService;

using IntraSliceReq = ourmama::proto::IntraSliceReq;
using IntraReq = ourmama::proto::IntraReq;
using BatchedIntraReq = ourmama::proto::BatchedIntraReq;
using IntraSliceResp = ourmama::proto::IntraSliceResp;
using IntraResp = ourmama::proto::IntraResp;
using ourmama::proto::IntraService;

using grpc::ServerContext;
using grpc::Status;

#define likely(x) __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)

constexpr size_t KB = 1024;
constexpr size_t MB = 1024 * KB;

constexpr int parallelDownloads = 6;
constexpr int queryInterval = 1000; // microseconds

bool exiting = false;

const std::string etcdUrl = "http://etcd:2379";
const std::string etcdPublishTaskPrefix = "/tasks/";
const std::string etcdFinishTaskPrefix = "/finished/";
const std::string etcdIntraDiscoveryFmt =
    "/intra/%1%" const std::string etcdServiceDiscoveryFmt =
        "/service/modeling/%1%";
const std::string etcdVersionSyncFolderFmt = "/version/%1%/";
const std::string etcdVersionSyncNodeFmt = "/version/%1%/%2%";

int gNodeId;
int gNodeNum;
vector<std::string> gNodeIps;

std::string GetIpAddr() {
  struct ifaddrs *addresses;
  if (getifaddrs(&addresses) == -1) {
    BOOST_LOG_TRIVIAL(error)
        << "server-" << gNodeId << " Error occurred in getifaddrs!";
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

    BOOST_LOG_TRIVIAL(info)
        << "The local ip address parse succeed, is " << ipAddr;
    break;
  }

  freeifaddrs(addresses);
  return ipAddr;
}

struct InternalVersionRecord {
  int requiredSlices;
  std::ordered_set<int> finishedSlices;

  bool finished() { return requiredSlices == finishedSlices.size(); }
};

class VersionDownloadInfo {
public:
  void RecordDownloadedSlice(std::string version, int slice) {
    std::unique_lock<std::mutex> lock(mutex_);

    auto &versionRecord = ivr_[version];
    versionRecord.finishedSlices.insert(slice);
    if (versionRecord.finished()) {
      finishedVersions_.insert(version);
      versionCv_[version]->notify_one();
    }
  }

  bool IsSliceDownloaded(std::string version, int slice) {
    std::unique_lock<std::mutex> lock(mutex_);
    auto &versionRecord = ivr_[version];
    return versionRecord.find(slice) != versionRecord.end();
  }

  bool IsVersionDownloaded(std::string version) {
    std::unique_lock<std::mutex> lock(mutex_);
    return finishedVersions_.find(version) != finishedVersions_.end();
  }

  void RecordVersionNeedDownload(std::string version, int slice_count,
                                 std::condition_variable *cv) {
    std::unique_lock<std::mutex> lock(mutex_);
    version_need_slice_count_[version] = slice_count;
    versionCv_[version] = cv;
  }

private:
  std::mutex mutex_;
  std::unordered_map<std::string, InternalVersionRecord> ivr_;
  std::unordered_set<std::string> finishedVersions_;
  std::unordered_map<std::string, std::condition_variable *> versionCv_;
}; // class VersionDownloadInfo

// we use a thread pool to download file from hdfs to control resouce usage
class DownloadThreadPool {
public:
  DownloadThreadPool(size_t num_threads, VersionDownloadInfo *pDownloadInfo)
      : stop_(false), pDownloadInfo_(pDownloadInfo) {
    for (size_t i = 0; i < num_threads; ++i)
      workers_.emplace_back(&DownloadThreadPool::ThreadFunc, this);
  }

  ~DownloadThreadPool() {
    {
      std::unique_lock<std::mutex> lock(queue_mutex_);
      stop_ = true;
    }
    condition_.notify_all();
    for (std::thread &worker : workers_)
      worker.join();
  }

  void DownloadSliceFromHdfs(std::string version, int slice) {
    std::string command =
        boost::format("%1%/bin/hadoop fs -get /root/model-%2%/slice-%3% .") %
        HADOOP_PATH % version % slice;
    system(command.c_str());
    pDownloadInfo_->RecordDownloadedSlice(version, slice);
  }

  void ThreadFunc() {
    while (true) {
      std::unique_lock<std::mutex> lock(this->queueMutex_);
      this->condition_.wait(
          lock, [this] { return this->stop_ || !this->tasks_.empty(); });
      if (this->stop_ && this->tasks_.empty()) {
        return;
      }
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

  void InsertTask(std::string version, int slice) {
    std::unique_lock<std::mutex> lock(queueMutex_);
    tasks_.emplace(version, slice);
  }

private:
  std::queue<HdfsModelPath> tasks_;
  std::vector<std::thread> workers_;
  std::mutex queueMutex_;
  std::condition_variable condition_;
  bool stop_;
  VersionDownloadInfo *pDownloadInfo_;

}; // class DownLoadThreadPool

class SliceCache {
private:
  std::string targetPath(int slice) {
    return boost::format("./model_slice.%03d%") % slice;
  }

public:
  std::string version;
  int start;
  int cnt;
  vector<ModelSliceReader *> readers;

  SliceCache(std::string version) : version(version) {}

  void Load() {
    readers.resize(cnt);
    for (int i = 0; i < cnt; i++) {
      readers[i] = new ModelSliceReader();
      readers[i]->Load(targetPath(start + i));
    }
  }

  bool Has(int slice) { return slice >= start && slice < start + cnt; }

  void Unload() {
    for (auto reader : readers) {
      reader->Unload();
      delete reader;
    }
  }

  void Read(int slice, size_t seek, size_t len, char *buffer) {
    readers[slice - start]->Read(seek, len, buffer);
  }
};

std::deque<SliceCache *> gSliceCaches;

class EtcdService {
private:
#define CHECK_IF_SUCCEEDED(keyStr, opStr)                                      \
  try {                                                                        \
    etcd::Response response = task.get();                                      \
    std::cout << opStr << " " << (keyStr)                                      \
              << (response.is_ok() ? " succeeds" : " failed") << std::endl;    \
  } catch (const std::exception &e) {                                          \
    std::cout << "An exception occurred during " << opStr << " " << (keyStr)   \
              << std::endl;                                                    \
  }

  static void set(std::string key, std::string value) {
    etcd::Client client(etcdUrl);
    auto task = client.set(key, value);
    CHECK_IF_SUCCEEDED(key, "set");
  }

  static void del(std::string key) {
    etcd::Client client(etcdUrl);
    auto task = client.del(key);
    CHECK_IF_SUCCEEDED(key, "del");
  }

public:
  static void RegisterIp(std::string ipAddr) {
    set(boost::format(etcdIntraDiscoveryFmt) % ipAddr, "");
  }

  static void GetNodeIps() {
    constexpr int nodeIpQueryInterval = 1;
    etcd::Client client(etcdUrl);
    auto task = client.get("/intra");

    while (true) {
      try {
        etcd::Response response = task.get();
        if (response.is_ok()) {
          if (response.kvs().size() != gNodeNum) {
            std::this_thread::sleep_for(
                std::chrono::seconds(nodeIpQueryInterval));
            continue;
          }
          for (auto &node : response.kvs()) {
            auto ipAddr = node.key().as_string();
            if (ipAddr == GetIpAddr())
              continue;
            gNodeIps.push_back(ipAddr);
            return;
          }

        } else {
          std::cout << "Get node ips from etcd failed" << std::endl;
        }
      } catch (const std::exception &e) {
        std::cout << "Get node ips from etcd failed" << std::endl;
      }
    }
  }

  static void RegisterModelService() {
    set(boost::format(etcdServiceDiscoveryFmt) % ipAddr, "")
  }

  static void RegisterVersion(std::string modelVersion) {
    set(boost::format(etcdVersionSyncNodeFmt) % modelVersion % gNodeId, "");
  }

  static void UnregisterVersion(std::string modelVersion) {
    del(boost::format(etcdVersionSyncNodeFmt) % modelVersion % gNodeId);
  }
}; // class EtcdService

class IntraServiceClient final {
public:
  IntraServiceClient(std::shared_ptr<Channel> channel)
      : stub_(InternalModelService::NewStub(channel)) {
    stop_ = false;
    sendReqThread_ = std::thread(&IntraServiceClient::SendRequest, this);
    parseRespThread_ = std::thread(&IntraServiceClient::ParseResponse, this);
  }

  void EnqueueIntraReq(const IntraReq &intraReq) {
    std::unique_lock<std::mutex> lock(queueMutex_);
    queue_.push(intraReq);
    condition_.notify_one();
  }

  void SendRequest() {
    ClientContext context; // TODO: on stack; wat happens when this goes out
                           // of scope?

    while (!stop_) {
      std::queue<IntraReq> tempQueue = {};
      {
        std::unique_lock<std::mutex> lock(queueMutex_);
        condition_.wait(lock, [this] { return !queue_.is_empty(); });
        std::swap(tempQueue, queue_);
      }

      // we can batch many `IntraReq`s to a `BatchedIntraReq`.
      BatchedIntraReq batchedReq;
      size_t targetMemSize = 0;
      while (!tempQueue.empty()) {
        batchedReq.add_internal_requests(tempQueue.front());
        targetMemSize += tempQueue.front().data_len();
        tempQueue.pop();
      }
      // TODO: I don't think I did it right
      respDataManager_.AssociateIdAndAddress(batchedReq.id(),
                                             new uint8_t[targetMemSize]);
      std::unique_ptr<ClientAsyncResponseReader<BatchedIntraReq>> rpc(
          stub_->AsyncGet(&context, batchedReq, &cq_));
      // new a reply, use tag let consumer can get this address, ugly
      // implementation
      auto reply = new BatchedIntraResp();
      rpc->Finished(reply, &status, (void *)reply);
    }
  }

  void ParseResponse() {
    IntraResp *batchedResp = nullptr;
    book ok = false;

    while (!stop_) {
      cq.Next(&batchedResp, &ok);
      if (unlikely(!ok || !batchedResp)) {
        BOOST_LOG_TRIVIAL(error) << "failed to get response from server";
        continue;
      }

      const std::vector<IntraResp> &intraResps =
          batchedResp->internal_responses();
      for (auto &intraResp : intraResps) {
        // every response show have a id to identify which request it belong
        // to, and use it to find location to put data.
        uint8_t *dataStartAddr =
            respDataManager_.GetDataAddress(intraResp.id());
        for (const auto &intraSliceResp : intraResp.InternalSliceResponse()) {
          uint8_t *dataWriteAddr = dataStartAddr + intraSliceResp.offset();
          auto &sliceData = intraSliceResp.slice_data();
          sliceData.copy(dataWriteAddr, sliceData.length());
        }
      }
      delete batchedResp;
      batchedResp = nullptr;
    } // while
  }

private:
  class IntraRespDataManager {
  private:
    unordered_map<int, std::pair<uint8_t *, bool>> dataPointers_;

  public:
    uint8_t *GetDataAddress(int id) { return dataPointers_[id].first; }

    void AssociateIdAndAddress(int id, uint8_t *address) {
      dataPointers_[id] = std::make_pair(address, false);
    }
  } respDataManager_; // class IntraRespDataManager

  std::unique_ptr<InternalModelService::stub> stub_;
  std::queue<IntraReq> queue_;
  std::mutex queueMutex_;
  std::condition_variable condition_;
  CompletionQueue cq_; // CompletionQueue is thread safe.
  bool stop_ = false;
  std::thread sendReqThread_;
  std::thread parseRespThread_;
}; // class IntraServiceClient

IntraServiceClient *gIntraClients;

void EstablishIntraConn() {
  int ipIter = 0;
  gIntraClients = new IntraServiceClient[gNodeNum];
  for (int i = 1; i <= gNodeNum; i++) {
    if (i == gNodeId)
      gIntraClients[i] = nullptr;
    else
      gIntraClients[i] = new IntraServiceClient(grpc::CreateChannel(
          gNodeIps[ipIter++], 50000, grpc::InsecureCredentials()));
  }
}

/**
 * @returns `true` if success, `false` otherwise.
 */
bool LocalRead(uint64_t slice, uint64_t seek, uint64_t len, uint8_t *buffer,
               std::string version) {
  for (auto pCache : gSliceCaches) {
    if (pCache->version == version && pCache->Has(slice)) {
      pCache->Read(slice, seek, len, buffer);
      return true;
    }
  }

  BOOST_LOG_TRIVIAL(error) << "LocalRead failed";
  return false;
}

class ModelServiceImpl final : public ModelService::Service {
private:
  // parse requests and send to productor.
  void
  convertSliceRequestToIntraReq(const std::vector<SliceRequest> &sliceRequests,
                                vector<uint8_t *> &buffers) {
    size_t totalOffset = 0;
    IntraReq intraReqs[gNodeNum];
    int serverNodeId;
    uint64_t slice, seek, len;

    for (int i = 0; i < sliceRequests.size(); ++i) {
      const auto &request = sliceRequests[i];
      slice = request.slice_partition();
      seek = request.data_start();
      len = request.data_len();
      serverNodeId = getSliceServer(request);
      if (serverNodeId == gNodeId) {
        std::thread(&LocalRead, slice, seek, len, buffers[i], /* version */,
                    false);
      } else { // this slice data don't belong to this server
        IntraSliceReq intraSliceReq;
        intraSliceReq.set_slice_partition(slice);
        intraSliceReq.set_data_start(seek);
        intraSliceReq.set_data_len(len);
        // we need record data offset of slice, because only this we can put
        // response to proper data.
        intraSliceReq.set_data_offset(totalOffset);
        intraReqs[server].add_slice_request(intraSliceReq);
      }

      totalOffset += len;
    }

    for (int i = 1; i <= gNodeNum; ++i)
      if (i != gNodeId)
        gIntraClients[i]->EnqueueIntraReq(intraReqs[i]);
  }

public:
  Status Get(ServerContext *context, const MamaRequest *request,
             MamaResponse *reply) override {
    const auto &sliceRequests = request->slices();
    vector<uint8_t *> buffers(sliceRequests.size());
    for (int i = 0; i < buffers.size(); i++)
      buffers[i] = new uint8_t[sliceRequests[i].data_len()];
    convertSliceRequestToIntraReq(sliceRequests);

    reply->set_status(0);
    return Status::OK;
  }
}; // class ModelServiceImpl

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
    grpc::ServerAsyncResponseWriter<BatchedIntraResp> responder_;
    BatchedIntraReq batchedReq_;
    IntraResp intraResp_;
    enum CallStatus { CREATE, PROCESS, FINISH };
    CallStatus status_;

  public:
    CallData(Greeter::AsyncService *service, ServerCompletionQueue *cq)
        : service_(service), cq_(cq), responder_(&ctx_), status_(CREATE) {
      // Invoke the serving logic right away.
      Proceed();
    }

    void Proceed() {
      if (status_ == CREATE) {
        status_ = PROCESS;
        service_->RequestGet(&ctx_, &batchReq_, &batchResp_, cq_, cq_, this);
      } else if (status_ == PROCESS) {
        new CallData(service_, cq_);
        for (const auto &intraReq : batchedReq_.reqs())
          processRequest(intraReq, &intraResp_);
        status_ = FINISH;
        responder_.Finish(batchedResp_, Status::OK, this);
      } else if (status_ == FINISH) {
        delete this;
      } else {
        BOOST_LOG_TRIVIAL(error) << "Unknown status";
      }
    }
  }; // class CallData

  void handleRPCs() {
    new CallData(&service_, cq_.get());
    void *tag;
    bool ok;
    while (true) {
      GPR_ASSERT(cq_->Next(&tag, &ok));
      GPR_ASSERT(ok);
      static_cast<CallData *>(tag)->Proceed();
    }
  }

  int getSliceServer(const SliceRequest &request) {
    int slice_partition = request.slice_partition();
    // TODO: ...
    return server;
  }

  void processRequest(const IntraReq &request, IntraResp *reply) {
    uint64_t slice, seek, len;
    const auto &sliceReqs = request.slice_reqs();
    const auto &version = request.version();
    for (int i = 0; i < sliceReqs.size(); i++) {
      const auto &req = request[i];
      slice = req.slice_partition();
      seek = req.data_start();
      len = req.data_len();
      IntraSliceResp intraSliceResp;
      auto buffer = new uint8_t[len];
      if (!LocalRead(slice, seek, len, buffer, version, false)) {
        BOOST_LOG_TRIVIAL(error) << "LocalRead failed";
        delete[] buffer;
        continue;
      }
      intraSliceResp.set_offset(req.data_offset());
      intraSliceResp.set_version(version);
      intraSliceResp.add_data(buffers[i], len);
      delete[] buffer;
      reply->add_slice_resp(intraSliceResp);
    }
  }

  std::vector<std::thread> threads_;

public:
  ~IntraServiceImpl() {
    for (auto &thread : threads_)
      thread.detach();
    server_->Shutdown();
    cq_->Shutdown();
  }

  // There is no shutdown handling in this code.
  void Run(uint16_t port) {
    std::string server_address = boost::format("0.0.0.0:%1%") % port;

    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service_);
    cq_ = builder.AddCompletionQueue();
    server_ = builder.BuildAndStart();
    std::cout << "Server listening on " << server_address << std::endl;

    for (int i = 0; i < 8; ++i)
      threads_.emplace_back(handleRPCs, this);
  }
}; // class IntraServiceImpl

class VersionSystem {
private:
  class GlobalDownloadSynker {
  private:
    std::mutex mutex_;
    std::unordered_map<std::string, int> nNodesFinished_;
    std::unordered_set<std::string> finishedVersions_;

  public:
    void IncrementCountFor(std::string modelVersion) {
      std::unique_lock<std::mutex> lock(mutex_);
      nNodesFinished_[modelVersion] = nNodesFinished_[modelVersion] + 1;
      if (info_[modelVersion] == gNodeNum)
        finishedVersions_.insert(modelVersion);
    }

    bool IsVersionFinished(std::string modelVersion) {
      std::unique_lock<std::mutex> lock(mutex_);
      return finishedVersions_.find(modelVersion) != finishedVersions_.end();
    }
  }; // class GlobalDownloadSynker

  DownloadThreadPool *pThreadPool_;
  VersionDownloadInfo downloadInfo_;
  GlobalDownloadSynker gdsync_;

  // when an event happen in watch diretory, this cb will be called;
  void watchOtherNodesLoadCb(etcd::Response const &resp) {
    if (resp.error_code()) {
      BOOST_LOG_TRIVIAL(error)
          << "node " << node_id << " recieve version load info failed "
          << resp.error_code();
    } else {
      if (resp.action() == "set") {
        BOOST_LOG_TRIVIAL(info)
            << "node " << node_id << " recieve version load info "
            << resp.key().as_string();

        std::string modelVersion = resp.key().as_string();
        gdsync_.IncrementCountFor(modelVersion);
        if (gdsync_.IsVersionFinished(modelVersion))
          this->downloadInfo_.AddVersion(modelVersion);
      } else {
        // don't do anything, as `del' is legal here
      }
    }
  }

  void watchOtherNodesLoad(std::string modelVersion) {
    etcd::Client client(etcd_url);
    for (int i = 1; i <= gNodeNum; ++i) {
      std::string key = boost::format(etcdVersionSyncFolderFmt) % modelVersion;
      auto cb = [this](etcd::Response const &resp) {
        this->watchOtherNodesLoadCb(resp);
      };

      // TODO: check if this gets dropped when function returns
      std::make_shared<etcd::Watcher>(client, key, cb, true);
    }
  }

  void loadToMem(std::string version) {
    if (gSliceCaches.size() == 2) {
      EtcdService::UnregisterVersion(gSliceCaches.front().GetVersion());
      gSliceCaches.pop_front();
    }

    gSliceCaches.push_back(new SliceCache(version));
    gSliceCaches.back().Load();
  }

public:
  std::string currentVersion = "WAIT";

  VersionSystem() = default;

  void InjectDownloadThreadPool(DownloadThreadPool *pPool) {
    pThreadPool = pPool;
  }

  std::string LatestVersionOn(hdfsFS &fs) {
    constexpr size_t versionBufferSize = 1024;

    int numEntries;
    std::string ret;
    std::vector<std::string> dirs;

    hdfsFileInfo *fileInfo = hdfsListDirectory(fs, "/root/", &numEntries);
    if (fileInfo == nullptr || numEntries == 0) {
      ret = "";
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

        ret = std::string(buffer);
        goto cleanup;
      }

      // check if the model.done file exists, if not exists, we need wait
      // until model load to hdfs finished.
      std::string modelDonePath = boost::format("/%1%/model.done") % filename;
      if (hdfsGetPathInfo(fs, modelDonePath.c_str()) == NULL) {
        std::cout << modelDonePath << " doesn't exist" << std::endl;
        continue;
      }

      dirs.push_back(std::move(filename));
    }

    sort(dirs.rbegin(), dirs.rend());
    if (!dirs.empty()) {
      ret = dirs[0];
    } else {
      ret = "WAIT"; // this situation may occur when start.
    }

  cleanup:
    hdfsFreeFileInfo(fileInfo, numEntries);
    return ret;
  }

  int GetVersionSliceCount(std::string modelVersion) {
    int numEntries = 0;
    hdfsFS fs = hdfsConnect("hdfs://namenode", 9000);
    std::string path = boost::format("/%1%/") % modelVersion;

    hdfsFileInfo *fileInfo = hdfsListDirectory(fs, path.c_str(), &numEntries);
    if (unlikely(fileInfo == nullptr || numEntries == 0)) {
      // TODO: handle this
      goto cleanup;
    }

  cleanup:
    hdfsFreeFileInfo(fileInfo, numEntries);
    return numEntries - 2;
  }

  std::pair<int, int> GetSliceRange(int sliceCount) {
    int avgSlice = sliceCount / gNodeNum;
    int reminant = sliceCount - avgSlice * gNodeId;
    int start = (avgSlice + 1) * (gNodeId - 1);

    return {start, start + std::min(avgSlice, reminant)};
  }

  /**
   Only downloads and loads into mem <b>if necessary</b>.
   */
  void DownloadVersionAndLoadIntoMem(std::string modelVersion) {
    /// 1. check if the version is already loaded in mem
    for (auto &pCache : gSliceCaches) {
      if (pCache->GetVersion() == modelVersion)
        goto etcdreg;
    }

    /// 2. check if the version is already downloaded to local disk
    if (pDownloadInfo_->IsVersionDownloaded(modelVersion)) {
      goto load;
    }

    /// 3. the normal slow path
    uint64_t sliceCount = GetVersionSliceCount(modelVersion);
    auto sliceRange = GetSliceRange(sliceCount);
    auto start = sliceRange.first;
    auto end = sliceRange.second;

    std::condition_variable downloadCv;
    std::mutex downloadMutex;
    std::unique_lock<std::mutex> downloadLock(downloadMutex);

    pDownloadInfo_->RecordVersionNeedDownload(modelVersion, sliceCount,
                                              &downloadCv);

    if (pThreadPool_) {
      delete pThreadPool_;
      pThreadPool_ = nullptr;
    }
    InjectDownloadThreadPool(new DownloadThreadPool(parallelDownloads));
    for (int i = start; i <= end; ++i)
      pThreadPool->InsertTask(modelVersion, i);

    // wait for download tasks to finish
    downloadCv.wait(downloadLock,
                    [] { pDownloadInfo_->IsVersionDownloaded(modelVersion) });

  load:
    loadToMem(modelVersion);

  etcdreg:
    EtcdService::RegisterVersion(modelVersion);
    watchOtherNodesLoad(modelVersion); // this function is async.
  }

  void Versioning() {
    hdfsFS fs = hdfsConnect("hdfs://namenode", 9000);

    while (!exiting) {
      std::string newVersion = LatestVersionOn(fs);
      if (newVersion != "WAIT" && currentVersion != newVersion)
        DownloadVersionAndLoadIntoMem(newVersion);

      if (currentVersion == "WAIT") {
        // We're required to register this service onto Etcd
        EtcdService::RegisterModelService();
      }
      currentVersion = std::move(newVersion);
      std::this_thread::sleep_for(std::chrono::microseconds(queryInterval));
    }

    hdfsDisconnect(fs);
  }
}; // class VersionSystem

class Log {
public:
  // when we enter cltr + c, process show exit elegantly, flush all log to
  // file.
  static void signal_handler(int signum) {
    if (signum == SIGINT) {
      BOOST_LOG_TRIVIAL(info) << "Received SIGINT, flushing logs";
      boost::log::core::get()->flush();
    }
    exit(0);
  }

  static void InitLog() {
    logging::add_file_log(
        keywords::file_name = "sample_%N.log", /*< file name pattern >*/
        keywords::rotation_size =
            10 * 1024 * 1024, /*< rotate files every 10 MiB... >*/
        keywords::time_based_rotation = sinks::file::rotation_at_time_point(
            0, 0, 0),                                 /*< ...or at midnight >*/
        keywords::format = "[%TimeStamp%]: %Message%" /*< log record format >*/
    );

    logging::core::get()->set_filter(logging::trivial::severity >=
                                     logging::trivial::info);

    logging::add_common_attributes();

    std::signal(SIGINT, signal_handler);
  }
}; // class Log

static void RunPublicGrpc() {
  std::string serverAddr("0.0.0.0:50051");
  ModelServiceImpl service;

  grpc::EnableDefaultHealthCheckService(true);
  grpc::reflection::InitProtoReflectionServerBuilderPlugin();
  grpc::ServerBuilder grpcServerBuilder;
  grpcServerBuilder.AddListeningPort(serverAddr,
                                     grpc::InsecureServerCredentials());
  grpcServerBuilder.RegisterService(&service);
  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << serverAddr << std::endl;

  server->Wait();
}

int ParseIntEnv(const char *envName) {
  const char *envStr = std::getenv(envName);
  return std::stoi(envStr);
}

int main(int argc, char **argv) {
  gNodeId = ParseIntEnv("NODE_ID");
  gNodeNum = ParseIntEnv("NODE_NUM");
  Log::InitLog();

  std::string nodeIp = GetIpAddr();
  EtcdService::RegisterIp(nodeIp);
  EtcdService::GetNodeIps(&gNodeIps); // synchronous
  EstablishIntraConn();

  IntraServiceImpl intraServer;
  intraServer.Run(50050);

  std::vector<std::thread> threads;
  VersionSystem vsys;
  thread.emplace_back(&VersionSystem::Versioning, &vsys);
  thread.emplace_back(&RunPublicGrpc);

  for (auto thread : threads)
    thread.join();

  return 0;
}
