#include "bidi_streaming_call_state.h"

#include <algorithm>
#include <cstring>
#include <nghttp2/nghttp2.h>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/post.hpp>
#include "rpcpio/internal/raw_server_reader.h"
#include "rpcpio/internal/raw_server_writer.h"
#include "src/protocol/compression.h"
#include "src/protocol/metadata_codec.h"
#include "src/protocol/status_map.h"
#include "src/protocol/timeout.h"

namespace rpcpio::internal {

// ── BidiStreamingCallState ───────────────────────────────────────────────────

BidiStreamingCallState::BidiStreamingCallState(
    boost::asio::io_context&                        ioc,
    const nghttp2::asio_http2::server::request&     req,
    const nghttp2::asio_http2::server::response&    resp,
    RawBidiStreamingHandler                         handler,
    std::size_t                                     max_message_size,
    std::size_t                                     max_metadata_size)
    : ioc_(ioc)
    , req_(req)
    , resp_(resp)
    , handler_(std::move(handler))
    , max_message_size_(max_message_size)
    , max_metadata_size_(max_metadata_size)
    , parser_(max_message_size)
    , timer_(ioc)
    , reader_impl_(std::make_shared<RawServerReaderImpl>(ioc))
    , writer_impl_(std::make_shared<RawServerWriterImpl>(resp))
{}

void BidiStreamingCallState::Start() {
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
                    // Unblock any pending Read().
                    self->reader_impl_->queue_.SetError(
                        Status{StatusCode::DEADLINE_EXCEEDED, "deadline exceeded"});
                    // Ensure the writer is finished so the generator closes the stream.
                    if (!self->writer_impl_->finished_) {
                        self->writer_impl_->Finish(
                            Status{StatusCode::DEADLINE_EXCEEDED, "deadline exceeded"});
                    }
                });
            }
        }
    }

    // Mark responded_ before starting: the streaming response path is managed
    // via the writer/generator pair, not SendError.
    responded_ = true;

    // Send initial HTTP 200 headers and arm the generator callback.
    SendHeaders();

    // Register the DATA callback to feed incoming request messages.
    auto self = shared_from_this();
    req_.on_data([self](const uint8_t* data, std::size_t len) {
        self->OnData(data, len);
    });

    // Start the handler coroutine; it interleaves Read() and Write() freely.
    boost::asio::co_spawn(
        ioc_,
        [self]() -> boost::asio::awaitable<void> {
            RawServerReader reader(self->reader_impl_);
            RawServerWriter writer(self->writer_impl_);
            Status status;
            try {
                status = co_await self->handler_(self->ctx_, reader, writer);
            } catch (...) {
                status = Status{StatusCode::INTERNAL, "handler threw exception"};
            }
            self->timer_.cancel();
            if (!self->writer_impl_->finished_) {
                writer.Finish(std::move(status));
            }
        },
        boost::asio::detached);
}

void BidiStreamingCallState::OnData(const uint8_t* data, std::size_t len) {
    if (len == 0) {
        // Client half-close.
        reader_impl_->queue_.SetEos();
        return;
    }

    auto status = parser_.Feed(data, len, [this](const std::string& payload) {
        reader_impl_->queue_.Push(payload);
    });

    if (!status.ok()) {
        reader_impl_->queue_.SetError(status);
        // Signal writer to close the stream with an error.
        if (!writer_impl_->finished_) {
            writer_impl_->Finish(status);
        }
    }
}

void BidiStreamingCallState::SendError(Status status) {
    // SendError is only called before responded_ is set (i.e., before SendHeaders).
    if (responded_) return;
    responded_ = true;
    timer_.cancel();

    // Trailers-only error response.
    nghttp2::asio_http2::header_map hdrs;
    hdrs.emplace("content-type",
        nghttp2::asio_http2::header_value{"application/grpc+proto", false});
    hdrs.emplace("grpc-accept-encoding",
        nghttp2::asio_http2::header_value{protocol::AcceptEncodingValue(), false});
    protocol::BuildTrailers(status, {}, hdrs);
    resp_.write_head(200, std::move(hdrs));
    resp_.end();
}

void BidiStreamingCallState::SendHeaders() {
    nghttp2::asio_http2::header_map init_hdrs;
    init_hdrs.emplace("content-type",
        nghttp2::asio_http2::header_value{"application/grpc+proto", false});
    init_hdrs.emplace("grpc-accept-encoding",
        nghttp2::asio_http2::header_value{protocol::AcceptEncodingValue(), false});
    protocol::MetadataToNghttp2Headers(ctx_.initial_metadata(), init_hdrs);
    resp_.write_head(200, std::move(init_hdrs));

    auto impl = writer_impl_;

    resp_.end(
        [impl](uint8_t* buf, std::size_t len, uint32_t* data_flags) mutable
        -> ssize_t {
            if (impl->pending_.empty()) {
                if (impl->finished_) {
                    *data_flags =
                        NGHTTP2_DATA_FLAG_EOF | NGHTTP2_DATA_FLAG_NO_END_STREAM;
                    impl->resp_.write_trailer(impl->trail_hdrs_);
                    return 0;
                }
                *data_flags = NGHTTP2_DATA_FLAG_DEFERRED;
                return 0;
            }

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
