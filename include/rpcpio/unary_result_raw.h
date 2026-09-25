#pragma once

#include <string>

#include "rpcpio/metadata.h"
#include "rpcpio/status.h"

namespace rpcpio {

// Type-erased result of a unary RPC using serialized protobuf bytes.
struct UnaryResultRaw {
    Status status;
    std::string response_bytes;
    MetadataMap initial_metadata;
    MetadataMap trailing_metadata;
    // Distinguishes a received zero-length message from no message at all.
    bool has_response = false;
};

} // namespace rpcpio
