#include "alimama.grpc.pb.h"
#include "alimama.pb.h"
#include <grpcpp/grpcpp.h>
#include <iostream>
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
      std::cout << status.error_code() << ": " << status.error_message()
                << std::endl;
      return {};
    }
  }
};

int main(int argc, char **argv) {
  if (argc < 5) {
    cout << "Invalid arguments" << endl;
    return -1;
  }

  std::string ip(argv[1]);
  std::string version(argv[2]);
  int slice = atoi(argv[3]);
  int offset = atoi(argv[4]);
  int len = atoi(argv[5]);

  std::string target = ip + ":50051";
  Client client(
      grpc::CreateChannel(target, grpc::InsecureChannelCredentials()));
  auto reply = client.Get(slice, offset, len);
  cout << "!!reply = " << (!!reply) << endl;

  return 0;
}
