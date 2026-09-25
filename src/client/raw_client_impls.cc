#include "raw_client_impls.h"

#include <boost/asio/post.hpp>
#include "src/protocol/framing.h"

namespace rpcpio::internal {

Status RawClientWriterImpl::Enqueue(std::string_view proto_bytes) {
    std::string frame;
    if (!protocol::EncodeFrame(0, proto_bytes, frame)) {
        return Status{StatusCode::RESOURCE_EXHAUSTED, "message too large"};
    }
    boost::asio::post(strand_,
        [self = shared_from_this(), frame = std::move(frame)]() mutable {
            self->pending_.push_back(std::move(frame));
            if (self->req_) self->req_->resume();
        });
    return Status{};
}

void RawClientWriterImpl::MarkWritesDone() {
    boost::asio::post(strand_, [self = shared_from_this()]() {
        self->writes_done_ = true;
        if (self->req_) self->req_->resume();
    });
}

} // namespace rpcpio::internal
