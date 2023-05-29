#include "ModelSliceReader.h"
#include "alimama.grpc.pb.h"
#include "alimama.pb.h"
#include "hdfs.h"
#include <boost/format.h>
#include <chrono>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include <iostream>
#include <thread>

using MamaRequest = alimama::proto::Request;
using MamaResponse = alimama::proto::Response;
using alimama::proto::ModelService;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

constexpr size_t KB = 1024;
constexpr size_t MB = 1024 * KB;
constexpr size_t QUERY_STEP = 64 * MB; // TODO: optimize this

constexpr iPort hdfsNnPort = 9000;
constexpr int queryInterval = 1000; // microseconds

static bool exiting = false;

class ModelServiceImpl final : public ModelService::Service {
private:
  static char *buffer = nullptr;

  void tryAllocBuffer() {
    if (buffer == nullptr) {
      buffer = new char[QUERY_STEP];
    }
  }

  void freeBuffer() {
    if (buffer != nullptr) {
      delete[] buffer;
      buffer = nullptr;
    }
  }

  std::string targetPath(int slice) {
    return boost::format("./model_slice.%03d%") % slice;
  }

  int load(int slice, size_t seek, size_t len) {
    // XXX: move Load to slave nodes and add cache
    ModelSliceReader reader;
    if (reader.Load(targetPath(slice)))
      return 1;
    if (reader.Read(seek, len, buffer))
      return 2;
    return 0;
  }

public:
  Status Get(ServerContext *context, const MamaRequest *request,
             MamaResponse *reply) override {
    auto slice = static_cast<int>(request->slice_partition());
    auto seek = request->data_start();
    auto end = seek + request->data_len();

    tryAllocBuffer();

    for (; seek < end; seek += QUERY_STEP) {
      if (load(slice, seek, QUERY_STEP)) {
        reply->set_status(1);
        reply->clear_slice_data();
        return Status::Internal("load failed");
      }
      reply->add_slice_data(buffer, QUERY_STEP);
    }

    reply->set_status(0);
    return Status::OK;
  }
}; // class ModelServiceImpl

void RunServer() {
  std::string server_address("0.0.0.0:50051");
  ModelServiceImpl service;

  grpc::EnableDefaultHealthCheckService(true);
  grpc::reflection::InitProtoReflectionServerBuilderPlugin();
  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;

  server->Wait();
  freeBuffer();
}

std::string latestVersion(hdfsFs &fs) {
  constexpr size_t versionBufferSize = 1024;

  int numEntries;
  std::string ret;
  std::vector<std::string> dirs;

  hdfsFileInfo *fileInfo = hdfsListDirectory(fs, "/root", &numEntries);
  if (fileInfo == nullptr || numEntries == 0) {
    ret = "";
    goto cleanup;
  }

  for (int i = 0; i < numEntries; ++i) {
    boost::filesystem::path path(fileInfo[i].mName);
    std::string filename = path.filename().string();

    if (filename == "rollback.version") {
      char buffer[versionBufferSize];

      hdfsFile rbFile = hdfsOpenFile(fs, fileInfo[i].mName, O_RDONLY, 0, 0, 0);
      hdfsRead(fs, rbFile, buffer, versionBufferSize);
      hdfsCloseFile(fs, rbFile);

      ret = std::string(buffer);
      goto cleanup;
    }

    dirs.push_back(std::move(filename));
  }

  // XXX: Does Hadoop API sort the entries? If it does, remove the line below.
  sort(dirs.rbegin(), dirs.rend());
  ret = dirs[0];

cleanup:
  hdfsFreeFileInfo(fileInfo, numEntries);
  return ret;
}

void loadVersion(std::string version) {
  // XXX: invalidate cache on slave nodes. If pre-loading brings better perf
  // even when version switching is frequent, then add pre-loading as well.
}

void Versioning() {
  std::string version;
  char *hdfsNnIp = "";
  hdfsFS fs = hdfsConnect(hdfsNnIp, hdfsNnPort);

  while (!exiting) {
    std::string newVersion = latestVersion(fs);
    if (version != newVersion)
      loadVersion(newVersion);
    std::this_thread::sleep_for(std::chrono::microseconds(queryInterval)));
  }

  hdfsDisconnect(fs);
}

int main(int argc, char **argv) {
  std::thread versioningThread(Versioning);

  RunServer();
  exiting = true;
  versioningThread.join();

  return 0;
}
