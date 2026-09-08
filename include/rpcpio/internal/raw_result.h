#pragma once

#include <string>
#include "rpcpio/metadata.h"
#include "rpcpio/status.h"

namespace rpcpio::internal {

// Type-erased result of a single unary RPC call on the wire.
// The response is held as serialized protobuf bytes so that Channel::UnaryCall
// (a header-only template) can deserialize it into the concrete Response type.
struct UnaryResultRaw {
    Status      status;
    std::string response_bytes;    // non-empty only when status.ok()
    MetadataMap initial_metadata;
    MetadataMap trailing_metadata;
};

} // namespace rpcpio::internal
