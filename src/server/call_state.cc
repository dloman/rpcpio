#include "call_state.h"

#include <algorithm>
#include <cstring>
#include <nghttp2/nghttp2.h>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/post.hpp>
#include "server_impl.h"
#include "src/protocol/compression.h"
#include "src/protocol/framing.h"
#include "src/protocol/metadata_codec.h"
#include "src/protocol/status_map.h"
#include "src/protocol/timeout.h"

namespace rpcpio::internal {

namespace {

void AddCommonHeaders(nghttp2::asio_http2::header_map& hdrs) {
    hdrs.emplace("content-type",
        nghttp2::asio_http2::header_value{"application/grpc+proto", false});
    hdrs.emplace("grpc-accept-encoding",
        nghttp2::asio_http2::header_value{protocol::AcceptEncodingValue(), false});
}

} // namespace

ServerCallState::ServerCallState(
    boost::asio::io_context&                        ioc,
    ServerImpl*                                       server,
    const nghttp2::asio_http2::server::request&     req,
    const nghttp2::asio_http2::server::response&    resp,
    RawHandler                                         coroutine_handler,
    std::size_t                                      max_request_size,
    std::size_t                                      max_metadata_size)
    : ioc_(ioc)
    , server_(server)
    , req_(req)
    , resp_(resp)
    , coroutine_handler_(std::move(coroutine_handler))
    , max_request_size_(max_request_size)
    , max_metadata_size_(max_metadata_size)
    , decoder_(max_request_size)
    , timer_(ioc)
{}

ServerCallState::ServerCallState(
    boost::asio::io_context&                        ioc,
    ServerImpl*                                       server,
    const nghttp2::asio_http2::server::request&     req,
    const nghttp2::asio_http2::server::response&    resp,
    UnaryCallbackHandler                             callback_handler,
    std::size_t                                      max_request_size,
    std::size_t                                      max_metadata_size)
    : ioc_(ioc)
    , server_(server)
    , req_(req)
    , resp_(resp)
    , callback_handler_(std::move(callback_handler))
    , max_request_size_(max_request_size)
    , max_metadata_size_(max_metadata_size)
    , decoder_(max_request_size)
    , timer_(ioc)
{}

void ServerCallState::Start() {
    if (server_) server_->TrackCall(shared_from_this());

    MetadataMap client_meta;
    Status meta_status = protocol::Nghttp2HeadersToMetadata(
        req_.header(), client_meta, max_metadata_size_);
    if (!meta_status.ok()) {
        SendError(meta_status);
        return;
    }
    ctx_.set_client_metadata(std::move(client_meta));

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

    {
        auto it = req_.header().find(":authority");
        std::string peer = (it != req_.header().end()) ? it->second.value : "unknown";
        ctx_.set_peer(std::move(peer));
    }

    auto tmo_it = req_.header().find("grpc-timeout");
    if (tmo_it != req_.header().end()) {
        auto dur = protocol::ParseTimeout(tmo_it->second.value);
        if (dur && dur->count() > 0) {
            ctx_.set_deadline(std::chrono::system_clock::now() + *dur);
            ctx_.set_has_deadline(true);

            timer_.expires_after(*dur);
            auto self = shared_from_this();
            timer_.async_wait([self](boost::system::error_code ec) {
                if (ec == boost::asio::error::operation_aborted) return;
                self->ctx_.trigger_cancel();
                if (!self->responded_.load(std::memory_order_acquire)) {
                    self->SendError(Status{StatusCode::DEADLINE_EXCEEDED,
                                          "deadline exceeded"});
                }
            });
        }
    }

    auto self = shared_from_this();
    req_.on_data([self](const uint8_t* data, std::size_t len) {
        self->OnData(data, len);
    });
    resp_.on_close([self](uint32_t /*error_code*/) {
        self->closed_.store(true, std::memory_order_release);
        if (!self->responded_.load(std::memory_order_acquire)) {
            self->CancelDueToPeerClose();
        }
        if (self->server_) self->server_->UntrackCall(self.get());
    });
}

bool ServerCallState::TryMarkResponded() {
    return !responded_.exchange(true, std::memory_order_acq_rel);
}

void ServerCallState::FinishFromReply(Status              status,
                                       std::string         response_bytes,
                                       MetadataMap         initial_metadata,
                                       MetadataMap         trailing_metadata) {
    auto self = shared_from_this();
    boost::asio::dispatch(ioc_, [self, status = std::move(status),
                                 response_bytes = std::move(response_bytes),
                                 initial_metadata = std::move(initial_metadata),
                                 trailing_metadata = std::move(trailing_metadata)]() mutable {
        if (!self->TryMarkResponded()) return;
        self->timer_.cancel();
        self->SendResponse(status, response_bytes,
                           initial_metadata, trailing_metadata);
    });
}

void ServerCallState::CancelDueToShutdown(Status status) {
    boost::asio::dispatch(ioc_, [self = shared_from_this(),
                                 status = std::move(status)]() mutable {
        if (!self->TryMarkResponded()) return;
        self->timer_.cancel();
        self->ctx_.trigger_cancel();
        self->SendError(std::move(status));
    });
}

void ServerCallState::CancelDueToPeerClose() {
    boost::asio::dispatch(ioc_, [self = shared_from_this()]() {
        if (!self->TryMarkResponded()) return;
        self->timer_.cancel();
        self->ctx_.trigger_cancel();
    });
}

void ServerCallState::OnData(const uint8_t* data, std::size_t len) {
    if (responded_.load(std::memory_order_acquire)) return;

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

void ServerCallState::OnRequestEnd() {
    if (responded_.load(std::memory_order_acquire)) return;

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

    if (callback_handler_) {
        DispatchCallbackHandler(std::move(req_bytes));
    } else {
        DispatchCoroutineHandler(std::move(req_bytes));
    }
}

void ServerCallState::DispatchCallbackHandler(std::string req_bytes) {
    auto reply = std::shared_ptr<UnaryServerReply>(
        new UnaryServerReply(shared_from_this()));
    callback_handler_(ctx_, req_bytes, std::move(reply));
}

void ServerCallState::DispatchCoroutineHandler(std::string req_bytes) {
    auto self = shared_from_this();
    boost::asio::co_spawn(
        ioc_,
        [self, req_bytes = std::move(req_bytes)]()
                -> boost::asio::awaitable<void> {
            std::string resp_bytes;
            Status status;
            try {
                status = co_await self->coroutine_handler_(
                    self->ctx_, req_bytes, resp_bytes);
            } catch (...) {
                status = Status{StatusCode::INTERNAL, "handler threw exception"};
            }

            if (!self->responded_.load(std::memory_order_acquire)) {
                self->timer_.cancel();
                self->FinishFromReply(status, std::move(resp_bytes),
                                      self->ctx_.initial_metadata(),
                                      self->ctx_.trailing_metadata());
            }
        },
        boost::asio::detached);
}

void ServerCallState::SendError(Status status) {
    if (!TryMarkResponded()) return;
    timer_.cancel();

    nghttp2::asio_http2::header_map hdrs;
    AddCommonHeaders(hdrs);
    protocol::BuildTrailers(status, {}, hdrs);

    resp_.write_head(200, std::move(hdrs));
    resp_.end();
}

void ServerCallState::SendResponse(const Status&      status,
                                    std::string_view   resp_bytes,
                                    const MetadataMap& initial_meta,
                                    const MetadataMap& trailing_meta) {
    if (closed_.load(std::memory_order_acquire)) return;

    if (!status.ok()) {
        nghttp2::asio_http2::header_map hdrs;
        AddCommonHeaders(hdrs);
        protocol::MetadataToNghttp2Headers(initial_meta, hdrs);
        protocol::BuildTrailers(status, trailing_meta, hdrs);
        resp_.write_head(200, std::move(hdrs));
        resp_.end();
        return;
    }

    std::string frame;
    protocol::EncodeFrame(0, resp_bytes, frame);

    nghttp2::asio_http2::header_map init_hdrs;
    AddCommonHeaders(init_hdrs);
    protocol::MetadataToNghttp2Headers(initial_meta, init_hdrs);
    resp_.write_head(200, std::move(init_hdrs));

    nghttp2::asio_http2::header_map trail_hdrs;
    protocol::BuildTrailers(status, trailing_meta, trail_hdrs);

    auto frame_shared = std::make_shared<std::string>(std::move(frame));
    std::size_t offset = 0;

    auto self = shared_from_this();

    resp_.end([self, frame_shared, offset,
               trail_hdrs = std::move(trail_hdrs)]
              (uint8_t* buf, std::size_t len, uint32_t* data_flags) mutable
              -> ssize_t {
        const std::size_t remaining = frame_shared->size() - offset;
        if (remaining == 0) {
            *data_flags = NGHTTP2_DATA_FLAG_EOF | NGHTTP2_DATA_FLAG_NO_END_STREAM;
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

// ── UnaryServerReply ─────────────────────────────────────────────────────────

#include "rpcpio/unary_server_reply.h"

namespace rpcpio {

UnaryServerReply::UnaryServerReply(
        std::shared_ptr<internal::ServerCallState> state)
    : state_(std::move(state))
{}

void UnaryServerReply::Finish(Status              status,
                               std::string         response_bytes,
                               MetadataMap         initial_metadata,
                               MetadataMap         trailing_metadata) {
    if (auto state = state_) {
        state->FinishFromReply(std::move(status), std::move(response_bytes),
                               std::move(initial_metadata),
                               std::move(trailing_metadata));
    }
}

bool UnaryServerReply::finished() const noexcept {
    return !state_ || state_->finished();
}

} // namespace rpcpio
