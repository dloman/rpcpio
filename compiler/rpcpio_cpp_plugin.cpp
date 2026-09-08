// protoc plugin entry point for the rpcpio C++ code generator.
//
// Usage (via Bazel macro):
//   protoc --plugin=protoc-gen-rpcpio=path/to/rpcpio_cpp_plugin \
//          --rpcpio_out=<output_dir> \
//          <proto_files>

#include <google/protobuf/compiler/plugin.h>
#include "cpp_generator.h"

int main(int argc, char* argv[]) {
    rpcpio::CppGenerator generator;
    return google::protobuf::compiler::PluginMain(argc, argv, &generator);
}
