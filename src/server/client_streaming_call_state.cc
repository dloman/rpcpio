#include "client_streaming_call_state.h"

#include <algorithm>
#include <cstring>
#include <nghttp2/nghttp2.h>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/post.hpp>
#include "rpcpio/internal/raw_server_reader.h"
#include "src/protocol/compression.h"
#include "src/protocol/framing.h"
#include "src/protocol/metadata_codec.h"
#include "src/protocol/status_map.h"
#include "src/protocol/timeout.h"

namespace rpcpio::internal {

// ── ClientStreamingCallState ─────────────────────────────────────────────────

ClientStreamingCallState::ClientStreamingCallState(
    boost::asio::io_context&                        ioc,
    const nghttp2::asio_http2::server::request&     req,
    const nghttp2::asio_http2::server::response&    resp,
    RawClientStreamingHandler                       handler,
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
{}

void ClientStreamingCallState::Start() {
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

    // Parse grpc-timeout and arm the deadline timer.
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
                    if (!self->responded_) {
                        self->SendError(
                            Status{StatusCode::DEADLINE_EXCEEDED,
                                   "deadline exceeded"});
                    }
                });
            }
        }
    }

    // Register DATA callback.  Decodes incoming LPM frames and pushes each
    // decoded proto payload into the reader queue.
    auto self = shared_from_this();
    req_.on_data([self](const uint8_t* data, std::size_t len) {
        self->OnData(data, len);
    });

    // Start the handler coroutine immediately; it will block in reader.Read()
    // until messages arrive via the queue.
    boost::asio::co_spawn(
        ioc_,
        [self]() -> boost::asio::awaitable<void> {
            RawServerReader reader(self->reader_impl_);
            std::string resp_bytes;
            Status status;
            try {
                status = co_await self->handler_(self->ctx_, reader, resp_bytes);
            } catch (...) {
                status = Status{StatusCode::INTERNAL, "handler threw exception"};
            }
            if (!self->responded_) {
                self->timer_.cancel();
                self->SendResponse(status, resp_bytes,
                                   self->ctx_.initial_metadata(),
                                   self->ctx_.trailing_metadata());
            }
        },
        boost::asio::detached);
}

void ClientStreamingCallState::OnData(const uint8_t* data, std::size_t len) {
    if (responded_) return;

    if (len == 0) {
        // Client half-close — signal EOS to the queue.
        reader_impl_->queue_.SetEos();
        return;
    }

    auto status = parser_.Feed(data, len, [this](const std::string& payload) {
        reader_impl_->queue_.Push(payload);
    });

    if (!status.ok()) {
        reader_impl_->queue_.SetError(status);
        SendError(status);
    }
}

// ── Sending helpers (mirror the unary ServerCallState pattern) ────────────────

static void AddCommonHeadersCS(nghttp2::asio_http2::header_map& hdrs) {
    hdrs.emplace("content-type",
        nghttp2::asio_http2::header_value{"application/grpc+proto", false});
    hdrs.emplace("grpc-accept-encoding",
        nghttp2::asio_http2::header_value{protocol::AcceptEncodingValue(), false});
}

void ClientStreamingCallState::SendError(Status status) {
    if (responded_) return;
    responded_ = true;
    timer_.cancel();

    nghttp2::asio_http2::header_map hdrs;
    AddCommonHeadersCS(hdrs);
    protocol::BuildTrailers(status, {}, hdrs);
    resp_.write_head(200, std::move(hdrs));
    resp_.end();
}

void ClientStreamingCallState::SendResponse(
    const Status&      status,
    std::string_view   resp_bytes,
    const MetadataMap& initial_meta,
    const MetadataMap& trailing_meta)
{
    if (responded_) return;
    responded_ = true;

    if (!status.ok() || resp_bytes.empty()) {
        // Trailers-only path.
        nghttp2::asio_http2::header_map hdrs;
        AddCommonHeadersCS(hdrs);
        protocol::MetadataToNghttp2Headers(initial_meta, hdrs);
        protocol::BuildTrailers(status, trailing_meta, hdrs);
        resp_.write_head(200, std::move(hdrs));
        resp_.end();
        return;
    }

    // Encode the response as a single LPM frame.
    std::string frame;
    protocol::EncodeFrame(0, resp_bytes, frame);

    nghttp2::asio_http2::header_map init_hdrs;
    AddCommonHeadersCS(init_hdrs);
    protocol::MetadataToNghttp2Headers(initial_meta, init_hdrs);
    resp_.write_head(200, std::move(init_hdrs));

    nghttp2::asio_http2::header_map trail_hdrs;
    protocol::BuildTrailers(status, trailing_meta, trail_hdrs);

    auto frame_shared = std::make_shared<std::string>(std::move(frame));
    std::size_t offset = 0;

    auto self = shared_from_this();
    resp_.end(
        [self, frame_shared, offset, trail_hdrs = std::move(trail_hdrs)]
        (uint8_t* buf, std::size_t len, uint32_t* data_flags) mutable
        -> ssize_t {
            const std::size_t remaining = frame_shared->size() - offset;
            if (remaining == 0) {
                *data_flags =
                    NGHTTP2_DATA_FLAG_EOF | NGHTTP2_DATA_FLAG_NO_END_STREAM;
                self->resp_.write_trailer(trail_hdrs);
                return 0;
            }
            const std::size_t n = std::min(remaining, len);
            std::memcpy(buf, frame_shared->data() + offset, n);
            offset += n;
            return static_cast<ssize_t>(n);
        });
}

} // namespace rpcpio::internal
