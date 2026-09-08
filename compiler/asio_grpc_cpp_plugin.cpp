// protoc plugin entry point for the asio_grpc C++ code generator.
//
// Usage (via Bazel macro):
//   protoc --plugin=protoc-gen-asio-grpc=path/to/asio_grpc_cpp_plugin \
//          --asio-grpc_out=<output_dir> \
//          <proto_files>

#include <google/protobuf/compiler/plugin.h>
#include "cpp_generator.h"

int main(int argc, char* argv[]) {
    asio_grpc::CppGenerator generator;
    return google::protobuf::compiler::PluginMain(argc, argv, &generator);
}
