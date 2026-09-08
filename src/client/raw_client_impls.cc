#include "raw_client_impls.h"

#include "src/protocol/framing.h"

namespace rpcpio::internal {

Status RawClientWriterImpl::Enqueue(std::string_view proto_bytes) {
    std::string frame;
    if (!protocol::EncodeFrame(0, proto_bytes, frame)) {
        return Status{StatusCode::RESOURCE_EXHAUSTED, "message too large"};
    }
    pending_.push_back(std::move(frame));
    // Phase 2 extension: wake the deferred nghttp2 data generator so it
    // re-enters the generator callback and drains the new frame.
    if (req_) req_->resume();
    return Status{};
}

} // namespace rpcpio::internal
