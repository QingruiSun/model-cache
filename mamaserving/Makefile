HADDOP_VERSION = 3.3.1
HADOOP_HOME = /opt/hadoop-$(HADOOP_VERSION)
CPPFLAGS += `pkg-config --cflags protobuf grpc`
CXXFLAGS += -std=c++17
CXXFLAGS += -Ofast -I./include -march=native #-pg
CXX = g++

#LDFLAGS += -pg
LDFLAGS += -L/usr/local/lib `pkg-config --libs protobuf grpc++` -pthread -Wl,--no-as-needed -lgrpc++_reflection -Wl,--as-needed -ldl
LDFLAGS += -lpthread -letcd-cpp-api -lprotobuf -lgrpc++ -lgrpc -lz -lcpprest -lssl -lcrypto
LDFLAGS += -lboost_system -lboost_filesystem -lmodel_slice_reader -lboost_log
LDFLAGS += -I/usr/local/include/etcd/proto/ -I/usr/local/include/hdfs -L${HADOOP_HOME}/libhdfs -lhdfs
LDFLAGS += -I./include

PROTOC = protoc
GRPC_CPP_PLUGIN = grpc_cpp_plugin
GRPC_CPP_PLUGIN_PATH ?= `which $(GRPC_CPP_PLUGIN)`

PROTOS_PATH = ./

vpath %.proto $(PROTOS_PATH)

server: alimama.pb.o alimama.grpc.pb.o ourmama.pb.o ourmama.grpc.pb.o format.o server.o
	$(CXX) $^ $(LDFLAGS) -o $@

.PRECIOUS: %.grpc.pb.cc
%.grpc.pb.cc: %.proto
	$(PROTOC) -I $(PROTOS_PATH) --grpc_out=. --plugin=protoc-gen-grpc=$(GRPC_CPP_PLUGIN_PATH) $<

.PRECIOUS: %.pb.cc
%.pb.cc: %.proto
	$(PROTOC) -I $(PROTOS_PATH) --cpp_out=. $<

clean:
	rm -f *.o *.pb.cc *.pb.h server
