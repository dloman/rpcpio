#include "server_streaming_call_state.h"

#include <algorithm>
#include <cstring>
#include <nghttp2/nghttp2.h>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/post.hpp>
#include "rpcpio/internal/raw_server_writer.h"
#include "src/protocol/compression.h"
#include "src/protocol/framing.h"
#include "src/protocol/metadata_codec.h"
#include "src/protocol/status_map.h"
#include "src/protocol/timeout.h"

namespace rpcpio::internal {

// ── ServerStreamingCallState ─────────────────────────────────────────────────

ServerStreamingCallState::ServerStreamingCallState(
    boost::asio::io_context&                        ioc,
    const nghttp2::asio_http2::server::request&     req,
    const nghttp2::asio_http2::server::response&    resp,
    RawServerStreamingHandler                       handler,
    std::size_t                                     max_request_size,
    std::size_t                                     max_metadata_size)
    : ioc_(ioc)
    , req_(req)
    , resp_(resp)
    , handler_(std::move(handler))
    , max_request_size_(max_request_size)
    , max_metadata_size_(max_metadata_size)
    , decoder_(max_request_size)
    , timer_(ioc)
    , writer_impl_(std::make_shared<RawServerWriterImpl>(resp))
{}

void ServerStreamingCallState::Start() {
    // Collect client metadata.
    MetadataMap client_meta;
    Status meta_status = protocol::Nghttp2HeadersToMetadata(
        req_.header(), client_meta, max_metadata_size_);
    if (!meta_status.ok()) {
        SendError(meta_status);
        return;
    }
    ctx_.set_client_metadata(std::move(client_meta));

    // Derive peer string.
    {
        auto it = req_.header().find(":authority");
        std::string peer =
            (it != req_.header().end()) ? it->second.value : "unknown";
        ctx_.set_peer(std::move(peer));
    }

    // Parse grpc-timeout and arm deadline timer.
    {
        auto tmo_it = req_.header().find("grpc-timeout");
        if (tmo_it != req_.header().end()) {
            auto dur = protocol::ParseTimeout(tmo_it->second.value);
            if (dur && dur->count() > 0) {
                auto deadline = std::chrono::system_clock::now() + *dur;
                ctx_.set_deadline(deadline);
                ctx_.set_has_deadline(true);

                timer_.expires_after(*dur);
                auto self = shared_from_this();
                timer_.async_wait([self](boost::system::error_code ec) {
                    if (ec == boost::asio::error::operation_aborted) return;
                    self->ctx_.trigger_cancel();
                    if (!self->responded_) {
                        self->SendError(
                            Status{StatusCode::DEADLINE_EXCEEDED,
                                   "deadline exceeded"});
                    }
                });
            }
        }
    }

    // Register DATA callback to accumulate the (single) request message.
    auto self = shared_from_this();
    req_.on_data([self](const uint8_t* data, std::size_t len) {
        self->OnData(data, len);
    });
}

void ServerStreamingCallState::OnData(const uint8_t* data, std::size_t len) {
    if (responded_) return;

    if (len == 0) {
        decoder_.MarkEos();
        OnRequestEnd();
        return;
    }

    decoder_.Feed({reinterpret_cast<const char*>(data), len});
    if (decoder_.error()) {
        SendError(Status{StatusCode::INVALID_ARGUMENT,
                         "request framing error: " + decoder_.error_message()});
    }
}

void ServerStreamingCallState::OnRequestEnd() {
    if (responded_) return;

    if (decoder_.error()) {
        SendError(Status{StatusCode::INVALID_ARGUMENT,
                         "request framing error: " + decoder_.error_message()});
        return;
    }
    if (!decoder_.done()) {
        SendError(Status{StatusCode::INVALID_ARGUMENT,
                         "incomplete request message"});
        return;
    }

    std::string req_bytes{decoder_.payload()};

    // Mark responded_ before spawning so that a racing deadline timer doesn't
    // also fire SendError.  The streaming response path manages its own
    // completion via the generator + writer_impl_->finished_.
    responded_ = true;
    timer_.cancel();

    // Send initial HTTP 200 headers and arm the generator.
    SendHeaders();

    auto self = shared_from_this();
    boost::asio::co_spawn(
        ioc_,
        [self, req_bytes = std::move(req_bytes)]()
                -> boost::asio::awaitable<void> {
            RawServerWriter writer(self->writer_impl_);
            Status status;
            try {
                status = co_await self->handler_(self->ctx_, req_bytes, writer);
            } catch (...) {
                status = Status{StatusCode::INTERNAL, "handler threw exception"};
            }
            // Ensure Finish() is called even if the handler forgot.
            if (!self->writer_impl_->finished_) {
                writer.Finish(std::move(status));
            }
        },
        boost::asio::detached);
}

void ServerStreamingCallState::SendError(Status status) {
    if (responded_) return;
    responded_ = true;
    timer_.cancel();

    // Trailers-only error response (no DATA frame).
    nghttp2::asio_http2::header_map hdrs;
    hdrs.emplace("content-type",
        nghttp2::asio_http2::header_value{"application/grpc+proto", false});
    hdrs.emplace("grpc-accept-encoding",
        nghttp2::asio_http2::header_value{protocol::AcceptEncodingValue(), false});
    protocol::BuildTrailers(status, {}, hdrs);

    resp_.write_head(200, std::move(hdrs));
    resp_.end();
}

void ServerStreamingCallState::SendHeaders() {
    nghttp2::asio_http2::header_map init_hdrs;
    init_hdrs.emplace("content-type",
        nghttp2::asio_http2::header_value{"application/grpc+proto", false});
    init_hdrs.emplace("grpc-accept-encoding",
        nghttp2::asio_http2::header_value{protocol::AcceptEncodingValue(), false});
    protocol::MetadataToNghttp2Headers(ctx_.initial_metadata(), init_hdrs);
    resp_.write_head(200, std::move(init_hdrs));

    // Capture writer_impl_ by value so the generator outlives this call.
    auto impl = writer_impl_;

    resp_.end(
        [impl](uint8_t* buf, std::size_t len, uint32_t* data_flags) mutable
        -> ssize_t {
            if (impl->pending_.empty()) {
                if (impl->finished_) {
                    // All data sent; emit EOF and hand off to write_trailer.
                    *data_flags =
                        NGHTTP2_DATA_FLAG_EOF | NGHTTP2_DATA_FLAG_NO_END_STREAM;
                    impl->resp_.write_trailer(impl->trail_hdrs_);
                    return 0;
                }
                // No data yet and not finished — defer until resume() wakes us.
                *data_flags = NGHTTP2_DATA_FLAG_DEFERRED;
                return 0;
            }

            // Drain as much of the front frame as fits in buf.
            auto& front = impl->pending_.front();
            const std::size_t remaining = front.size() - impl->offset_;
            const std::size_t n = std::min(remaining, len);
            std::memcpy(buf, front.data() + impl->offset_, n);
            impl->offset_ += n;
            if (impl->offset_ == front.size()) {
                impl->pending_.pop_front();
                impl->offset_ = 0;
            }
            return static_cast<ssize_t>(n);
        });
}

} // namespace rpcpio::internal
