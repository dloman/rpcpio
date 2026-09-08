#include "client_streaming_call_state.h"

#include <algorithm>
#include <cstring>
#include <boost/asio/post.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/use_awaitable.hpp>
#include "src/protocol/metadata_codec.h"
#include "src/protocol/status_map.h"

namespace rpcpio::internal {

// ── ClientStreamingClientCallState ────────────────────────────────────────────

ClientStreamingClientCallState::ClientStreamingClientCallState(
        boost::asio::io_context& ioc,
        ClientContext*           ctx)
    : writer_impl_(std::make_shared<RawClientWriterImpl>(ioc))
    , ioc_(ioc)
    , ctx_(ctx)
    , timer_(ioc)
{}

void ClientStreamingClientCallState::ArmTimer() {
    if (!ctx_ || !ctx_->has_deadline()) return;
    const std::chrono::nanoseconds remaining = ctx_->deadline_from_now();
    timer_.expires_after(
        remaining > std::chrono::nanoseconds::zero()
            ? remaining
            : std::chrono::nanoseconds::zero());
    auto self = shared_from_this();
    timer_.async_wait([self](const boost::system::error_code& ec) {
        if (ec == boost::asio::error::operation_aborted) return;
        self->Fail(Status{StatusCode::DEADLINE_EXCEEDED, "deadline exceeded"});
    });
}

void ClientStreamingClientCallState::Cancel() {
    Fail(Status{StatusCode::CANCELLED, "call cancelled"});
}

void ClientStreamingClientCallState::Fail(Status s) {
    if (closed_.exchange(true)) return;
    timer_.cancel();
    boost::asio::post(ioc_, [impl = writer_impl_, st = std::move(s)]() mutable {
        impl->SetFinalStatus(std::move(st));
    });
}

void ClientStreamingClientCallState::OnStreamClose(uint32_t error_code) {
    if (closed_.exchange(true)) return;
    timer_.cancel();

    boost::asio::post(ioc_, [impl = writer_impl_, error_code,
                              trailers_done = trailers_done_]() mutable {
        if (!trailers_done) {
            Status s;
            if (error_code == 0) {
                s = Status{StatusCode::UNKNOWN, "missing grpc-status"};
            } else {
                s = Status{protocol::Http2ErrorCodeToStatusCode(error_code),
                           "RST_STREAM error_code=" + std::to_string(error_code)};
            }
            impl->SetFinalStatus(std::move(s));
        }
    });
}

void ClientStreamingClientCallState::MaybeDeliverStatus(Status s) {
    trailers_done_ = true;
    writer_impl_->SetFinalStatus(std::move(s));
}

void ClientStreamingClientCallState::Attach(
        const nghttp2::asio_http2::client::request* req,
        const nghttp2::asio_http2::client::response&          resp)
{
    // Store the request handle so Write()/WritesDone() can call resume().
    writer_impl_->req_ = req;

    // Validate content-type.
    auto ct_it = resp.header().find("content-type");
    bool is_grpc = ct_it != resp.header().end() &&
                   ct_it->second.value.find("application/grpc") != std::string::npos;
    if (!is_grpc) {
        Fail(Status::FromHttpStatus(resp.status_code()));
        return;
    }
    if (resp.status_code() != 200) {
        Fail(Status::FromHttpStatus(resp.status_code()));
        return;
    }

    // Trailers-only (error, no response body).
    if (resp.header().count("grpc-status")) {
        Status s = protocol::ExtractTrailerStatus(resp.header());
        auto self = shared_from_this();
        boost::asio::post(ioc_, [self, s = std::move(s)]() mutable {
            if (self->closed_.exchange(true)) return;
            self->timer_.cancel();
            self->MaybeDeliverStatus(std::move(s));
        });
        return;
    }

    auto self = shared_from_this();
    const std::size_t max_msg_size = ctx_
        ? ctx_->max_receive_message_size()
        : 4 * 1024 * 1024;

    // on_data: accumulate the single response message.
    resp.on_data([self, max_msg_size](const uint8_t* data, std::size_t len) {
        if (self->closed_) return;
        if (len == 0) return;  // EOS — handled by on_trailers

        std::size_t pos = 0;
        while (pos < len) {
            if (!self->hdr_done_) {
                const std::size_t need = 5 - self->hdr_bytes_;
                const std::size_t take = std::min(need, len - pos);
                std::memcpy(self->hdr_buf_.data() + self->hdr_bytes_,
                            data + pos, take);
                self->hdr_bytes_ += take;
                pos += take;

                if (self->hdr_bytes_ == 5) {
                    self->hdr_done_ = true;
                    self->payload_len_ =
                        (uint32_t(self->hdr_buf_[1]) << 24) |
                        (uint32_t(self->hdr_buf_[2]) << 16) |
                        (uint32_t(self->hdr_buf_[3]) <<  8) |
                         uint32_t(self->hdr_buf_[4]);
                    if (self->payload_len_ > max_msg_size) {
                        self->Fail(Status{StatusCode::RESOURCE_EXHAUSTED,
                                          "received message too large"});
                        return;
                    }
                    self->payload_buf_.clear();
                    self->payload_buf_.reserve(self->payload_len_);
                }
            } else {
                const std::size_t need = self->payload_len_ - self->payload_buf_.size();
                const std::size_t take = std::min(need, len - pos);
                self->payload_buf_.append(
                    reinterpret_cast<const char*>(data) + pos, take);
                pos += take;

                if (self->payload_buf_.size() == self->payload_len_) {
                    // Store the single server response.
                    self->writer_impl_->response_bytes_ = self->payload_buf_;
                    // Reset parser (in case there is any extra data, which would
                    // be a protocol error, but we don't reject it here).
                    self->hdr_done_    = false;
                    self->hdr_bytes_   = 0;
                    self->payload_len_ = 0;
                    self->payload_buf_.clear();
                }
            }
        }
    });

    resp.on_trailers([self](const nghttp2::asio_http2::header_map& trailers) {
        if (self->closed_) return;
        Status s = protocol::ExtractTrailerStatus(trailers);
        self->MaybeDeliverStatus(std::move(s));
    });
}

RawClientWriter ClientStreamingClientCallState::TakeWriter() {
    return RawClientWriter(writer_impl_);
}

// ── RawClientWriter pImpl methods ─────────────────────────────────────────────

RawClientWriter::RawClientWriter(
        std::shared_ptr<RawClientWriterImpl> impl) noexcept
    : impl_(std::move(impl))
{}

boost::asio::awaitable<Status>
RawClientWriter::Write(std::string_view proto_bytes) {
    co_return impl_->Enqueue(proto_bytes);
}

boost::asio::awaitable<void>
RawClientWriter::WritesDone() {
    impl_->writes_done_ = true;
    if (impl_->req_) impl_->req_->resume();
    co_return;
}

boost::asio::awaitable<Status>
RawClientWriter::Finish() {
    using namespace std::chrono;
    while (!impl_->status_ready_) {
        impl_->status_timer_.expires_at(steady_clock::time_point::max());
        boost::system::error_code ec;
        co_await impl_->status_timer_.async_wait(
            boost::asio::redirect_error(boost::asio::use_awaitable, ec));
    }
    co_return impl_->final_status_;
}

// response_bytes accessor (used by Channel::ClientStreamingCall template).
const std::string& RawClientWriter::response_bytes() const noexcept {
    return impl_->response_bytes_;
}

} // namespace rpcpio::internal
