#include "call_state.h"

#include <algorithm>
#include <cstring>
#include <nghttp2/nghttp2.h>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/post.hpp>
#include "src/protocol/compression.h"
#include "src/protocol/framing.h"
#include "src/protocol/metadata_codec.h"
#include "src/protocol/status_map.h"
#include "src/protocol/timeout.h"

namespace asio_grpc::internal {

ServerCallState::ServerCallState(
    boost::asio::io_context&                        ioc,
    const nghttp2::asio_http2::server::request&     req,
    const nghttp2::asio_http2::server::response&    resp,
    RawHandler                                       handler,
    std::size_t                                      max_request_size,
    std::size_t                                      max_metadata_size)
    : ioc_(ioc)
    , req_(req)
    , resp_(resp)
    , handler_(std::move(handler))
    , max_request_size_(max_request_size)
    , max_metadata_size_(max_metadata_size)
    , decoder_(max_request_size)
    , timer_(ioc)
{}

void ServerCallState::Start() {
    // Collect client metadata (skip HTTP pseudo-headers and gRPC reserved ones).
    MetadataMap client_meta;
    Status meta_status = protocol::Nghttp2HeadersToMetadata(
        req_.header(), client_meta, max_metadata_size_);
    if (!meta_status.ok()) {
        SendError(meta_status);
        return;
    }
    ctx_.set_client_metadata(std::move(client_meta));

    // Parse grpc-encoding; reject unsupported encodings immediately.
    {
        auto enc_it = req_.header().find("grpc-encoding");
        if (enc_it != req_.header().end() &&
            enc_it->second.value != "identity") {
            auto enc = protocol::ParseEncoding(enc_it->second.value);
            if (!enc) {
                SendError(Status{StatusCode::UNIMPLEMENTED,
                    "unsupported grpc-encoding: " + enc_it->second.value});
                return;
            }
            request_encoding_ = *enc;
        }
    }

    // Derive peer string from URI authority or a placeholder.
    // nghttp2-asio exposes the remote address differently per build; fall back
    // to the :authority pseudo-header.
    {
        auto it = req_.header().find(":authority");
        std::string peer = (it != req_.header().end()) ? it->second.value : "unknown";
        ctx_.set_peer(std::move(peer));
    }

    // Parse grpc-timeout and arm the deadline timer.
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
                    self->SendError(Status{StatusCode::DEADLINE_EXCEEDED,
                                          "deadline exceeded"});
                }
            });
        }
    }

    // Register the DATA callback to accumulate the request body.
    auto self = shared_from_this();
    req_.on_data([self](const uint8_t* data, std::size_t len) {
        self->OnData(data, len);
    });
}

void ServerCallState::OnData(const uint8_t* data, std::size_t len) {
    if (responded_) return;

    if (len == 0) {
        // EOS — request body is complete.
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

void ServerCallState::OnRequestEnd() {
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

    std::string req_bytes;
    if (request_encoding_ &&
        *request_encoding_ != protocol::Encoding::kIdentity &&
        decoder_.compress_flag() != 0) {
        if (!protocol::Decompress(*request_encoding_,
                                  decoder_.payload(), req_bytes,
                                  max_request_size_)) {
            SendError(Status{StatusCode::INTERNAL, "request decompression failed"});
            return;
        }
    } else {
        req_bytes = std::string{decoder_.payload()};
    }

    auto self = shared_from_this();

    // Run the handler as a C++20 coroutine on the server's executor.
    boost::asio::co_spawn(
        ioc_,
        [self, req_bytes = std::move(req_bytes)]()
                -> boost::asio::awaitable<void> {
            std::string resp_bytes;
            Status status;
            try {
                status = co_await self->handler_(self->ctx_, req_bytes, resp_bytes);
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

// ── Sending ───────────────────────────────────────────────────────────────────

// Shared helper: add headers common to every gRPC response.
static void AddCommonHeaders(nghttp2::asio_http2::header_map& hdrs) {
    hdrs.emplace("content-type",
        nghttp2::asio_http2::header_value{"application/grpc+proto", false});
    hdrs.emplace("grpc-accept-encoding",
        nghttp2::asio_http2::header_value{protocol::AcceptEncodingValue(), false});
}

// Send a trailers-only response (no DATA frame):
//   HTTP 200 + {grpc-status, optional grpc-message} in the initial HEADERS.
// This is the gRPC-compliant way to signal an error before any data is sent.
void ServerCallState::SendError(Status status) {
    if (responded_) return;
    responded_ = true;
    timer_.cancel();

    // Build the combined headers + trailers in the initial HEADERS block
    // (trailers-only response per gRPC spec §4).
    nghttp2::asio_http2::header_map hdrs;
    AddCommonHeaders(hdrs);
    protocol::BuildTrailers(status, {}, hdrs);

    resp_.write_head(200, std::move(hdrs));
    resp_.end();  // END_STREAM with no DATA
}

// Send a full response: initial metadata, one DATA frame, then trailers.
// gRPC wire:
//   HEADERS (200 + initial metadata, no END_STREAM)
//   DATA    (LPM-encoded response message, no END_STREAM via NO_END_STREAM flag)
//   HEADERS (grpc-status + trailing metadata, END_STREAM)
void ServerCallState::SendResponse(const Status&      status,
                                    std::string_view   resp_bytes,
                                    const MetadataMap& initial_meta,
                                    const MetadataMap& trailing_meta) {
    if (responded_) return;
    responded_ = true;

    if (!status.ok() || resp_bytes.empty()) {
        // Trailers-only path: no DATA to send.
        nghttp2::asio_http2::header_map hdrs;
        AddCommonHeaders(hdrs);
        protocol::MetadataToNghttp2Headers(initial_meta, hdrs);
        protocol::BuildTrailers(status, trailing_meta, hdrs);
        resp_.write_head(200, std::move(hdrs));
        resp_.end();
        return;
    }

    // Encode the response as a gRPC LPM frame.
    std::string frame;
    protocol::EncodeFrame(0, resp_bytes, frame);

    // Initial response headers (no END_STREAM implied by write_head).
    nghttp2::asio_http2::header_map init_hdrs;
    AddCommonHeaders(init_hdrs);
    protocol::MetadataToNghttp2Headers(initial_meta, init_hdrs);
    resp_.write_head(200, std::move(init_hdrs));

    // Build trailing headers for the HEADERS frame with END_STREAM.
    nghttp2::asio_http2::header_map trail_hdrs;
    protocol::BuildTrailers(status, trailing_meta, trail_hdrs);

    // Use a generator callback to:
    //   1. Emit the framed DATA with NGHTTP2_DATA_FLAG_NO_END_STREAM.
    //   2. After the DATA generator completes, call write_trailer to send the
    //      trailing HEADERS frame with END_STREAM.
    //
    // The generator is called repeatedly by nghttp2 until it sets DATA_FLAG_EOF.
    // Setting NO_END_STREAM alongside EOF tells nghttp2 to keep the stream open
    // for the subsequent write_trailer call.
    auto frame_shared = std::make_shared<std::string>(std::move(frame));
    std::size_t offset = 0;

    auto self = shared_from_this();

    resp_.end([self, frame_shared, offset,
               trail_hdrs = std::move(trail_hdrs)]
              (uint8_t* buf, std::size_t len, uint32_t* data_flags) mutable
              -> ssize_t {
        const std::size_t remaining = frame_shared->size() - offset;
        if (remaining == 0) {
            // Mark end of DATA; keep stream open for trailers.
            *data_flags = NGHTTP2_DATA_FLAG_EOF | NGHTTP2_DATA_FLAG_NO_END_STREAM;
            // Emit the trailing HEADERS frame (END_STREAM).
            self->resp_.write_trailer(trail_hdrs);
            return 0;
        }
        const std::size_t n = std::min(remaining, len);
        std::memcpy(buf, frame_shared->data() + offset, n);
        offset += n;
        return static_cast<ssize_t>(n);
    });
}

} // namespace asio_grpc::internal
