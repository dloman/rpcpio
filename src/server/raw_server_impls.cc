#include "raw_server_impls.h"

#include <string_view>
#include "rpcpio/internal/raw_server_reader.h"
#include "rpcpio/internal/raw_server_writer.h"
#include "src/protocol/framing.h"
#include "src/protocol/metadata_codec.h"

namespace rpcpio::internal {

// ── RawServerWriterImpl method bodies ────────────────────────────────────────

Status RawServerWriterImpl::Write(std::string_view proto_bytes) {
    if (finished_) {
        return Status{StatusCode::INTERNAL,
                      "RawServerWriterImpl::Write called after Finish"};
    }
    std::string frame;
    if (!protocol::EncodeFrame(0, proto_bytes, frame)) {
        return Status{StatusCode::RESOURCE_EXHAUSTED, "message too large to frame"};
    }
    pending_.push_back(std::move(frame));
    // Phase 2 nghttp2-asio extension: wake the deferred generator.
    resp_.resume();
    return Status{};
}

void RawServerWriterImpl::Finish(Status status) {
    if (finished_) return;
    finished_ = true;
    protocol::BuildTrailers(status, {}, trail_hdrs_);
    resp_.resume();
}

// ── RawServerWriter thin-wrapper ─────────────────────────────────────────────

RawServerWriter::RawServerWriter(
    std::shared_ptr<RawServerWriterImpl> impl) noexcept
    : impl_(std::move(impl))
{}

Status RawServerWriter::Write(std::string_view proto_bytes) {
    if (!impl_) {
        return Status{StatusCode::INTERNAL, "RawServerWriter: not initialized"};
    }
    return impl_->Write(proto_bytes);
}

void RawServerWriter::Finish(Status status) {
    if (impl_) impl_->Finish(std::move(status));
}

// ── RawServerReader thin-wrapper ─────────────────────────────────────────────
// RawServerReaderImpl is fully defined in raw_server_impls.h (just a queue
// wrapper); no additional method bodies are needed for the impl itself.

RawServerReader::RawServerReader(
    std::shared_ptr<RawServerReaderImpl> impl) noexcept
    : impl_(std::move(impl))
{}

boost::asio::awaitable<RawServerReader::ReadResult>
RawServerReader::Read() {
    auto result = co_await impl_->queue_.Pop();
    if (!result.ok()) {
        co_return ReadResult{result.status, std::nullopt};
    }
    if (result.eos()) {
        co_return ReadResult{Status{}, std::nullopt};
    }
    co_return ReadResult{Status{}, std::move(result.data)};
}

} // namespace rpcpio::internal
