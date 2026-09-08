#pragma once

#include <string>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/compiler/code_generator.h>
#include <google/protobuf/compiler/plugin.pb.h>
#include <google/protobuf/io/zero_copy_stream.h>

namespace rpcpio {

// Generate the .rpcpio.pb.h and .rpcpio.pb.cc files for all services
// in a FileDescriptor that has no streaming methods.
//
// Emits a precise, source-located error for any streaming method found and
// exits the plugin with a non-zero status.
class CppGenerator : public google::protobuf::compiler::CodeGenerator {
public:
    bool Generate(const google::protobuf::FileDescriptor* file,
                  const std::string&                      parameter,
                  google::protobuf::compiler::GeneratorContext* context,
                  std::string*                            error) const override;

    uint64_t GetSupportedFeatures() const override;
};

// ── Helpers (also used in tests) ─────────────────────────────────────────────

struct GeneratedHeader {
    std::string content;  // content of the .rpcpio.pb.h file
};

struct GeneratedSource {
    std::string content;  // content of the .rpcpio.pb.cc file
};

// Generate header content for a single file.
// Returns false and sets error if a streaming method is found.
bool GenerateHeader(const google::protobuf::FileDescriptor* file,
                    GeneratedHeader&                        out,
                    std::string*                            error);

// Generate source content for a single file.
bool GenerateSource(const google::protobuf::FileDescriptor* file,
                    GeneratedSource&                        out,
                    std::string*                            error);

// Derive the base name (without extension) from a proto file path.
// E.g. "foo/bar/baz.proto" → "foo/bar/baz"
std::string ProtoBaseName(const std::string& filename);

// Map a proto package "foo.bar.baz" to a C++ namespace "foo::bar::baz".
std::string PackageToNamespace(const std::string& package);

// Map a proto method path to the gRPC path string.
// E.g. service "helloworld.Greeter", method "SayHello" →
//      "/helloworld.Greeter/SayHello"
std::string MethodPath(const google::protobuf::MethodDescriptor* method);

} // namespace rpcpio
