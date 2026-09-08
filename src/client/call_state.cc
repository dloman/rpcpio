#include "call_state.h"

#include <boost/asio/post.hpp>
#include "src/protocol/compression.h"
#include "src/protocol/metadata_codec.h"
#include "src/protocol/status_map.h"

namespace asio_grpc::internal {

ClientCallState::ClientCallState(boost::asio::io_context& ioc,
                                  ClientContext*           ctx,
                                  CompletionCb             cb)
    : ioc_(ioc)
    , ctx_(ctx)
    , completion_(std::move(cb))
    , timer_(ioc)
    , decoder_(ctx ? ctx->max_receive_message_size() : 4 * 1024 * 1024)
{}

void ClientCallState::ArmTimer() {
    if (!ctx_ || !ctx_->has_deadline()) return;

    auto dl = ctx_->deadline();
    if (!dl) return;

    timer_.expires_at(*dl);
    auto self = shared_from_this();
    timer_.async_wait([self](const boost::system::error_code& ec) {
        if (ec == boost::asio::error::operation_aborted) return;
        self->Complete(UnaryResultRaw{
            Status{StatusCode::DEADLINE_EXCEEDED, "deadline exceeded"}});
    });
}

void ClientCallState::Complete(UnaryResultRaw result) {
    if (completed_.exchange(true)) return;
    timer_.cancel();
    auto cb = std::move(completion_);
    boost::asio::post(ioc_, [cb = std::move(cb), r = std::move(result)]() mutable {
        cb(std::move(r));
    });
}

void ClientCallState::Fail(Status status) {
    Complete(UnaryResultRaw{std::move(status)});
}

void ClientCallState::Cancel() {
    Complete(UnaryResultRaw{Status{StatusCode::CANCELLED, "call cancelled"}});
}

void ClientCallState::OnStreamClose(uint32_t error_code) {
    if (completed_) return;
    if (!trailers_done_) {
        if (error_code == 0) {
            Complete(UnaryResultRaw{Status{StatusCode::UNKNOWN, "missing grpc-status"}});
        } else {
            Complete(UnaryResultRaw{
                Status{protocol::Http2ErrorCodeToStatusCode(error_code),
                       "RST_STREAM error_code=" + std::to_string(error_code)}});
        }
    }
}

void ClientCallState::Attach(const nghttp2::asio_http2::client::response& resp) {
    int http_status = resp.status_code();

    // Extract initial metadata (application headers only).
    protocol::Nghttp2HeadersToMetadata(resp.header(), result_.initial_metadata);

    // Check content-type.
    auto ct_it = resp.header().find("content-type");
    bool is_grpc = ct_it != resp.header().end() &&
                   ct_it->second.value.find("application/grpc") != std::string::npos;

    if (!is_grpc) {
        Complete(UnaryResultRaw{Status::FromHttpStatus(http_status),
                                {}, std::move(result_.initial_metadata)});
        return;
    }

    if (http_status != 200) {
        Complete(UnaryResultRaw{Status::FromHttpStatus(http_status),
                                {}, std::move(result_.initial_metadata)});
        return;
    }

    // Parse content-encoding for the response body.  Unknown encoding causes
    // UNIMPLEMENTED; identity (or absent header) means no decompression needed.
    {
        auto enc_it = resp.header().find("grpc-encoding");
        if (enc_it != resp.header().end() &&
            enc_it->second.value != "identity") {
            auto enc = protocol::ParseEncoding(enc_it->second.value);
            if (!enc) {
                Complete(UnaryResultRaw{Status{
                    StatusCode::UNIMPLEMENTED,
                    "unsupported grpc-encoding: " + enc_it->second.value}});
                return;
            }
            response_encoding_ = *enc;
        }
    }

    // Trailers-only: grpc-status in the initial HEADERS block.
    if (resp.header().count("grpc-status")) {
        result_.status = protocol::ExtractTrailerStatus(resp.header());
        protocol::Nghttp2HeadersToMetadata(resp.header(), result_.trailing_metadata);
        Complete(std::move(result_));
        return;
    }

    initial_meta_done_ = true;

    auto self = shared_from_this();

    resp.on_data([self](const uint8_t* data, std::size_t len) {
        if (self->completed_) return;
        if (len == 0) {
            self->decoder_.MarkEos();
            if (self->decoder_.error()) {
                self->Complete(UnaryResultRaw{
                    Status{StatusCode::INTERNAL,
                           "frame error: " + self->decoder_.error_message()}});
            }
            return;
        }
        self->decoder_.Feed({reinterpret_cast<const char*>(data), len});
        if (self->decoder_.error()) {
            self->Complete(UnaryResultRaw{
                Status{StatusCode::INTERNAL,
                       "frame error: " + self->decoder_.error_message()}});
        }
    });

    // on_trailers is the Phase-1 extension to the CESNET nghttp2-asio API.
    resp.on_trailers([self](const nghttp2::asio_http2::header_map& trailers) {
        if (self->completed_) return;
        self->trailers_done_ = true;

        self->result_.status = protocol::ExtractTrailerStatus(trailers);
        protocol::Nghttp2HeadersToMetadata(trailers, self->result_.trailing_metadata);

        if (!self->result_.status.ok()) {
            self->Complete(std::move(self->result_));
            return;
        }

        // Collect the raw (possibly compressed) payload from the decoder.
        std::string_view raw_payload;
        if (self->decoder_.done()) {
            raw_payload = self->decoder_.payload();
        }

        // Decompress if the server signalled a non-identity encoding.
        if (self->response_encoding_ &&
            *self->response_encoding_ != protocol::Encoding::kIdentity &&
            self->decoder_.compress_flag() != 0) {
            const std::size_t max_size = self->ctx_
                ? self->ctx_->max_receive_message_size()
                : 4 * 1024 * 1024;
            std::string decompressed;
            if (!protocol::Decompress(*self->response_encoding_,
                                      raw_payload, decompressed, max_size)) {
                self->Complete(UnaryResultRaw{Status{
                    StatusCode::INTERNAL, "response decompression failed"}});
                return;
            }
            self->result_.response_bytes = std::move(decompressed);
        } else {
            self->result_.response_bytes = std::string(raw_payload);
        }

        self->Complete(std::move(self->result_));
    });
}

} // namespace asio_grpc::internal
