#pragma once

#include <string>
#include "rpcpio/metadata.h"
#include "rpcpio/status.h"

namespace rpcpio {

// Type-erased result of a single unary RPC call on the wire.
struct UnaryResultRaw {
    Status      status;
    std::string response_bytes;
    MetadataMap initial_metadata;
    MetadataMap trailing_metadata;
    bool        has_response{false};
};

} // namespace rpcpio
