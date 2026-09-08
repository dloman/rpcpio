#include "call_state.h"

#include "src/protocol/compression.h"
#include "src/protocol/metadata_codec.h"
#include "src/protocol/status_map.h"
#include "rpcpio/status.h"

namespace rpcpio::internal {

ClientCallState::ClientCallState(
        boost::asio::io_context&                    ioc,
        std::shared_ptr<UnaryCallSubmission>        submission,
        std::size_t                                 max_receive_message_size)
    : ioc_(ioc)
    , submission_(std::move(submission))
    , max_receive_message_size_(max_receive_message_size)
    , decoder_(max_receive_message_size)
{}

void ClientCallState::BindRequest(
        const nghttp2::asio_http2::client::request* req) {
    request_ = req;
}

void ClientCallState::ResetStream() {
    if (request_) request_->cancel(NGHTTP2_CANCEL);
}

void ClientCallState::Complete(UnaryResultRaw result) {
    ResetStream();
    if (submission_) {
        submission_->Complete(std::move(result));
        submission_.reset();
    }
}

void ClientCallState::Fail(Status status) {
    Complete(UnaryResultRaw{std::move(status)});
}

void ClientCallState::OnStreamClose(uint32_t error_code) {
    if (submission_ && submission_->completed()) return;
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
    if (submission_ && submission_->completed()) return;

    int http_status = resp.status_code();

    protocol::Nghttp2HeadersToMetadata(resp.header(), result_.initial_metadata);

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

    if (resp.header().count("grpc-status")) {
        result_.status = protocol::ExtractTrailerStatus(resp.header());
        protocol::Nghttp2HeadersToMetadata(resp.header(), result_.trailing_metadata);
        Complete(std::move(result_));
        return;
    }

    initial_meta_done_ = true;

    auto self = shared_from_this();

    resp.on_data([self](const uint8_t* data, std::size_t len) {
        if (self->submission_ && self->submission_->completed()) return;
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

    resp.on_trailers([self](const nghttp2::asio_http2::header_map& trailers) {
        if (self->submission_ && self->submission_->completed()) return;
        self->trailers_done_ = true;

        self->result_.status = protocol::ExtractTrailerStatus(trailers);
        protocol::Nghttp2HeadersToMetadata(trailers, self->result_.trailing_metadata);

        if (!self->result_.status.ok()) {
            self->Complete(std::move(self->result_));
            return;
        }

        if (!self->decoder_.done()) {
            self->Complete(UnaryResultRaw{
                Status{StatusCode::INTERNAL, "missing unary response message"}});
            return;
        }
        self->result_.has_response = true;
        const std::string_view raw_payload = self->decoder_.payload();

        if (self->response_encoding_ &&
            *self->response_encoding_ != protocol::Encoding::kIdentity &&
            self->decoder_.compress_flag() != 0) {
            std::string decompressed;
            if (!protocol::Decompress(*self->response_encoding_,
                                      raw_payload, decompressed,
                                      self->max_receive_message_size_)) {
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

} // namespace rpcpio::internal
