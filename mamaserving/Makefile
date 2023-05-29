HOST_SYSTEM = $(shell uname | cut -f 1 -d_)
SYSTEM ?= $(HOST_SYSTEM)
HADDOP_VERSION=3.3.1
HADOOP_HOME=/opt/hadoop-$(HADOOP_VERSION)
CPPFLAGS += `pkg-config --cflags protobuf grpc`
CXXFLAGS += -std=c++17

CXX = g++
LDFLAGS += -L/usr/local/lib `pkg-config --libs protobuf grpc++`\
           -pthread\
           -Wl,--no-as-needed -lgrpc++_reflection -Wl,--as-needed\
           -ldl
LDFLAGS += -lpthread -letcd-cpp-api -lprotobuf -lgrpc++ -lgrpc -lz -lcpprest -lssl -lcrypto -lboost_system -I/usr/local/include/etcd/proto/
LDFLAGS += -I${HADOOP_HOME}/src/c++/libhdfs -L${HADOOP_HOME}/libhdfs -lhdfs 

PROTOC = protoc
GRPC_CPP_PLUGIN = grpc_cpp_plugin
GRPC_CPP_PLUGIN_PATH ?= `which $(GRPC_CPP_PLUGIN)`

PROTOS_PATH = ./

vpath %.proto $(PROTOS_PATH)

mamaserving_server: alimama.pb.o alimama.grpc.pb.o mamaserving_server.o
	$(CXX) $^ $(LDFLAGS) -o $@

.PRECIOUS: %.grpc.pb.cc
%.grpc.pb.cc: %.proto
	$(PROTOC) -I $(PROTOS_PATH) --grpc_out=. --plugin=protoc-gen-grpc=$(GRPC_CPP_PLUGIN_PATH) $<

.PRECIOUS: %.pb.cc
%.pb.cc: %.proto
	$(PROTOC) -I $(PROTOS_PATH) --cpp_out=. $<

clean:
	rm -f *.o *.pb.cc *.pb.h mamaserving_server
