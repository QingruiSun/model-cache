#include "alimama.grpc.pb.h"
#include "alimama.pb.h"
#include <cstdio>
#include <grpcpp/grpcpp.h>
#include <memory>
#include <optional>
#include <vector>

using namespace std;
using alimama::proto::ModelService;
using grpc::Channel;

struct Client {
  std::unique_ptr<ModelService::Stub> stub_;

  Client(shared_ptr<Channel> channel) : stub_(ModelService::NewStub(channel)) {}

  optional<alimama::proto::Response> Get(uint64_t slice, uint64_t offset,
                                         uint64_t len) {
    alimama::proto::Request request;
    auto *piece = request.add_slice_request();
    piece->set_slice_partition(slice);
    piece->set_data_start(offset);
    piece->set_data_len(len);

    alimama::proto::Response response;
    grpc::ClientContext context;

    grpc::Status status = stub_->Get(&context, request, &response);

    if (status.ok()) {
      return response;
    } else {
      printf("%d: %s\n", status.error_code(), status.error_message().c_str());
      return {};
    }
  }
};

int main(int argc, char **argv) {
  if (argc < 5) {
    printf("Invalid arguments\n");
    return -1;
  }

  std::string ip(argv[1]);
  std::string version(argv[2]);
  int slice = atoi(argv[3]);
  int offset = atoi(argv[4]);
  int len = atoi(argv[5]);

  if (strlen(argv[3]) != 3) {
    printf("Invalid slice length\n");
    return -1;
  }

  std::string target = ip + ":50051";
  Client client(
      grpc::CreateChannel(target, grpc::InsecureChannelCredentials()));
  auto reply = client.Get(slice, offset, len);

  if (reply->status() != 0) {
    printf("reply->status() = %d\n", reply->status());
    return -1;
  }

  auto *buffer = new uint8_t[len];
  string path =
      string("./model_") + version + "/" + "model_slice." + string(argv[3]);
  auto *file = fopen(path.c_str(), "rb");
  fseek(file, offset, SEEK_SET);
  if (fread(buffer, 1, len, file) != len) {
    printf("Error: read file\n");
    return -1;
  }
  fclose(file);

  auto str = reply->slice_data(0);
  for (int i = 0; i < len; ++i) {
    if (buffer[i] != str[i]) {
      printf("Error: buffer[%d] = %02x, str[%d] = %02x\n", i, buffer[i], i,
             str[i]);

      printf("actual: \n ");
      for (int i = 0; i < len; ++i)
        printf("%02x ", buffer[i]);
      printf("\n\n");

      printf("reply: \n  ");
      for (auto c : str)
        printf("%02x ", c);
      printf("\n");
      return -1;
    }
  }

  printf("pass\n");

  return 0;
}
