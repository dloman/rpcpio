#include "channel_impl.h"
#include "call_state.h"
#include "server_streaming_call_state.h"
#include "client_streaming_call_state.h"
#include "bidi_streaming_call_state.h"

#include <algorithm>
#include <cstring>
#include <boost/asio/post.hpp>
#include <nghttp2/asio_http2_client.h>
#include <nghttp2/nghttp2.h>

#include "src/protocol/framing.h"
#include "src/protocol/metadata_codec.h"
#include "src/protocol/status_map.h"
#include "src/protocol/timeout.h"

namespace rpcpio::internal {

ChannelImpl::ChannelImpl(boost::asio::io_context& ioc,
                         std::string              host,
                         std::uint16_t            port,
                         ChannelOptions           opts)
    : ioc_(ioc)
    , host_(std::move(host))
    , port_(port)
    , opts_(std::move(opts))
{}

ChannelImpl::~ChannelImpl() = default;

void ChannelImpl::Connect(std::function<void(boost::system::error_code)> cb) {
    if (state_ == ConnState::kReady) { cb({}); return; }
    if (state_ == ConnState::kFailed) {
        cb(boost::system::error_code{
            boost::asio::error::connection_refused,
            boost::asio::error::get_system_category()});
        return;
    }
    connect_waiters_.push_back(std::move(cb));
    if (state_ == ConnState::kConnecting) return;
    state_ = ConnState::kConnecting;
    DoConnect();
}

void ChannelImpl::DoConnect() {
    auto self = shared_from_this();

    auto wire_session = [self](
            std::shared_ptr<nghttp2::asio_http2::client::session> sess) {
        sess->on_connect([self, sess](boost::asio::ip::tcp::endpoint) {
            self->session_ = sess;
            self->OnConnected({});
        });
        sess->on_error([self](const boost::system::error_code& ec) {
            if (self->state_ != ConnState::kReady)
                self->OnConnected(ec);
            else
                self->FailAll(Status{
                    protocol::AsioErrorToStatusCode(ec.value(), true),
                    "connection error: " + ec.message()});
        });
        sess->on_goaway([self](std::uint32_t ec, std::int32_t last_stream_id) {
            self->OnGoaway(ec, last_stream_id);
        });
    };

    if (opts_.use_tls && !opts_.use_h2c) {
        // Share the SSL context so it outlives the session.
        auto ssl_ctx = std::make_shared<boost::asio::ssl::context>(
            boost::asio::ssl::context::tls_client);
        ssl_ctx->set_options(boost::asio::ssl::context::default_workarounds |
                             boost::asio::ssl::context::no_sslv2 |
                             boost::asio::ssl::context::no_sslv3);

        if (!opts_.ca_cert_file.empty())
            ssl_ctx->load_verify_file(opts_.ca_cert_file);
        else
            ssl_ctx->set_default_verify_paths();

        ssl_ctx->set_verify_mode(opts_.verify_peer
            ? boost::asio::ssl::verify_peer
            : boost::asio::ssl::verify_none);

        if (!opts_.client_cert_file.empty()) {
            ssl_ctx->use_certificate_chain_file(opts_.client_cert_file);
            ssl_ctx->use_private_key_file(opts_.client_key_file,
                                           boost::asio::ssl::context::pem);
        }

        // Advertise h2 via ALPN.
        static const unsigned char kH2Alpn[] = "\x02h2";
        SSL_CTX_set_alpn_protos(ssl_ctx->native_handle(), kH2Alpn,
                                sizeof(kH2Alpn) - 1);

        // Capture ssl_ctx in the on_connect lambda to keep it alive.
        auto sess = std::make_shared<nghttp2::asio_http2::client::session>(
            ioc_, *ssl_ctx, host_, std::to_string(port_));
        sess->on_connect([self, sess, ssl_ctx](
                boost::asio::ip::tcp::endpoint) {
            self->session_ = sess;
            self->OnConnected({});
        });
        sess->on_error([self](const boost::system::error_code& ec) {
            if (self->state_ != ConnState::kReady)
                self->OnConnected(ec);
            else
                self->FailAll(Status{
                    protocol::AsioErrorToStatusCode(ec.value(), true),
                    "connection error: " + ec.message()});
        });
        sess->on_goaway([self](std::uint32_t ec, std::int32_t last_stream_id) {
            self->OnGoaway(ec, last_stream_id);
        });
    } else {
        // h2c prior-knowledge (plaintext)
        auto sess = std::make_shared<nghttp2::asio_http2::client::session>(
            ioc_, host_, std::to_string(port_));
        wire_session(sess);
    }
}

void ChannelImpl::OnGoaway(std::uint32_t /*error_code*/,
                            std::int32_t  last_stream_id) {
    // No new streams can be opened on this connection.
    state_ = ConnState::kFailed;

    // Fail calls that the peer did not see (stream_id above last_stream_id).
    // Calls at or below last_stream_id were accepted and should complete via
    // their own on_close / on_trailers path.
    std::string detail =
        "server sent GOAWAY (last_stream_id=" +
        std::to_string(last_stream_id) + ")";
    for (auto it = active_calls_.begin(); it != active_calls_.end(); ) {
        if (it->first > last_stream_id) {
            it->second->Fail(Status{StatusCode::UNAVAILABLE, detail});
            it = active_calls_.erase(it);
        } else {
            ++it;
        }
    }

    // Fail streaming calls the peer did not process.
    Status goaway_status{StatusCode::UNAVAILABLE, detail};
    for (auto it = active_streaming_calls_.begin();
         it != active_streaming_calls_.end(); ) {
        if (it->first > last_stream_id) {
            it->second(goaway_status);
            it = active_streaming_calls_.erase(it);
        } else {
            ++it;
        }
    }

    // Fail any calls that were still queued and never submitted.
    FailAll(Status{StatusCode::UNAVAILABLE, detail});
}

void ChannelImpl::RemoveActiveCall(std::int32_t stream_id) {
    active_calls_.erase(stream_id);
}

void ChannelImpl::OnConnected(boost::system::error_code ec) {
    state_ = ec ? ConnState::kFailed : ConnState::kReady;
    DrainQueue(ec);
}

void ChannelImpl::DrainQueue(boost::system::error_code ec) {
    auto waiters = std::move(connect_waiters_);
    for (auto& w : waiters) w(ec);

    if (ec) {
        FailAll(Status{protocol::AsioErrorToStatusCode(ec.value(), true),
                       "connection failed: " + ec.message()});
        return;
    }
    auto pending = std::move(pending_calls_);
    for (auto& p : pending)
        SubmitCall(std::move(p.path), p.ctx,
                   std::move(p.request_bytes), std::move(p.completion));

    auto generic_pending = std::move(pending_generic_calls_);
    for (auto& fn : generic_pending) fn();
}

void ChannelImpl::FailAll(Status status) {
    auto pending = std::move(pending_calls_);
    for (auto& p : pending)
        p.completion(UnaryResultRaw{status});

    auto generic_pending = std::move(pending_generic_calls_);
    for (auto& fn : generic_pending) fn();
}

void ChannelImpl::SubmitCall(std::string                         path,
                              ClientContext*                      ctx,
                              std::string                         req_bytes,
                              std::function<void(UnaryResultRaw)> completion) {
    auto self = shared_from_this();
    if (state_ == ConnState::kIdle || state_ == ConnState::kConnecting) {
        pending_calls_.push_back({std::move(path), ctx,
                                   std::move(req_bytes), std::move(completion)});
        if (state_ == ConnState::kIdle) {
            state_ = ConnState::kConnecting;
            DoConnect();
        }
        return;
    }
    if (state_ == ConnState::kFailed) {
        completion(UnaryResultRaw{
            Status{StatusCode::UNAVAILABLE, "channel failed"}});
        return;
    }

    // Build request headers.
    nghttp2::asio_http2::header_map hdrs;
    hdrs.emplace("content-type",
        nghttp2::asio_http2::header_value{"application/grpc+proto", false});
    hdrs.emplace("te",
        nghttp2::asio_http2::header_value{"trailers", false});
    hdrs.emplace(":authority",
        nghttp2::asio_http2::header_value{host_, false});
    hdrs.emplace("user-agent",
        nghttp2::asio_http2::header_value{opts_.user_agent, false});

    if (ctx && ctx->has_deadline()) {
        auto d = ctx->deadline_from_now();
        if (d.count() <= 0) {
            completion(UnaryResultRaw{
                Status{StatusCode::DEADLINE_EXCEEDED, "deadline already passed"}});
            return;
        }
        auto ts = protocol::FormatTimeout(d);
        if (!ts.empty())
            hdrs.emplace("grpc-timeout",
                nghttp2::asio_http2::header_value{ts, false});
    }

    if (ctx && ctx->compression_algorithm() != "identity") {
        hdrs.emplace("grpc-encoding",
            nghttp2::asio_http2::header_value{ctx->compression_algorithm(), false});
    }

    if (ctx)
        protocol::MetadataToNghttp2Headers(ctx->send_metadata(), hdrs);

    // Encode gRPC LPM frame.
    std::string frame;
    if (!protocol::EncodeFrame(0, req_bytes, frame)) {
        completion(UnaryResultRaw{
            Status{StatusCode::RESOURCE_EXHAUSTED, "request too large"}});
        return;
    }

    // Create the per-call state before submit to avoid a race.
    auto call = std::make_shared<ClientCallState>(ioc_, ctx, std::move(completion));

    boost::system::error_code ec;
    auto req = session_->submit(ec, "POST", std::string{path}, frame, hdrs);
    if (ec) {
        call->Fail(Status{protocol::AsioErrorToStatusCode(ec.value(), false),
                          "stream submit failed: " + ec.message()});
        return;
    }

    // Track this call so OnGoaway can fail it if the peer did not accept it.
    const auto stream_id = static_cast<std::int32_t>(req->stream_id());
    active_calls_.emplace(stream_id, call);

    req->on_response([call](const nghttp2::asio_http2::client::response& resp) {
        call->Attach(resp);
    });
    req->on_close([self, call, stream_id](uint32_t error_code) {
        self->RemoveActiveCall(stream_id);
        call->OnStreamClose(error_code);
    });

    call->ArmTimer();
}

// ── Shared header-builder helper ──────────────────────────────────────────────
// Returns true on success, false if the deadline has already passed.
// On false, out_err is set to the deadline-exceeded status.
static bool BuildStreamingGrpcHeaders(
        const std::string&               host,
        const ChannelOptions&            opts,
        ClientContext*                   ctx,
        nghttp2::asio_http2::header_map& hdrs,
        Status&                          out_err)
{
    hdrs.emplace("content-type",
        nghttp2::asio_http2::header_value{"application/grpc+proto", false});
    hdrs.emplace("te",
        nghttp2::asio_http2::header_value{"trailers", false});
    hdrs.emplace(":authority",
        nghttp2::asio_http2::header_value{host, false});
    hdrs.emplace("user-agent",
        nghttp2::asio_http2::header_value{opts.user_agent, false});

    if (ctx && ctx->has_deadline()) {
        auto d = ctx->deadline_from_now();
        if (d.count() <= 0) {
            out_err = Status{StatusCode::DEADLINE_EXCEEDED, "deadline already passed"};
            return false;
        }
        auto ts = protocol::FormatTimeout(d);
        if (!ts.empty())
            hdrs.emplace("grpc-timeout",
                nghttp2::asio_http2::header_value{ts, false});
    }

    if (ctx && ctx->compression_algorithm() != "identity") {
        hdrs.emplace("grpc-encoding",
            nghttp2::asio_http2::header_value{ctx->compression_algorithm(), false});
    }

    if (ctx)
        protocol::MetadataToNghttp2Headers(ctx->send_metadata(), hdrs);

    return true;
}

// ── SubmitServerStreamingCall ─────────────────────────────────────────────────

void ChannelImpl::SubmitServerStreamingCall(
        std::string                             path,
        ClientContext*                          ctx,
        std::string                             request_bytes,
        std::function<void(RawClientReader)>    completion)
{
    auto self = shared_from_this();

    if (state_ == ConnState::kIdle || state_ == ConnState::kConnecting) {
        pending_generic_calls_.push_back(
            [self, path, ctx, req_bytes = std::move(request_bytes),
             comp = std::move(completion)]() mutable {
                self->SubmitServerStreamingCall(
                    std::move(path), ctx, std::move(req_bytes), std::move(comp));
            });
        if (state_ == ConnState::kIdle) {
            state_ = ConnState::kConnecting;
            DoConnect();
        }
        return;
    }
    if (state_ == ConnState::kFailed) {
        auto call = std::make_shared<ServerStreamingClientCallState>(ioc_, ctx);
        call->Fail(Status{StatusCode::UNAVAILABLE, "channel failed"});
        completion(call->TakeReader());
        return;
    }

    // Build request headers.
    nghttp2::asio_http2::header_map hdrs;
    {
        Status err;
        if (!BuildStreamingGrpcHeaders(host_, opts_, ctx, hdrs, err)) {
            auto call = std::make_shared<ServerStreamingClientCallState>(ioc_, ctx);
            call->Fail(std::move(err));
            completion(call->TakeReader());
            return;
        }
    }

    // Encode the single request message.
    std::string frame;
    if (!protocol::EncodeFrame(0, request_bytes, frame)) {
        auto call = std::make_shared<ServerStreamingClientCallState>(ioc_, ctx);
        call->Fail(Status{StatusCode::RESOURCE_EXHAUSTED, "request too large"});
        completion(call->TakeReader());
        return;
    }

    auto call = std::make_shared<ServerStreamingClientCallState>(ioc_, ctx);

    boost::system::error_code ec;
    auto req = session_->submit(ec, "POST", std::string{path}, frame, hdrs);
    if (ec) {
        call->Fail(Status{protocol::AsioErrorToStatusCode(ec.value(), false),
                          "stream submit failed: " + ec.message()});
        completion(call->TakeReader());
        return;
    }

    const auto stream_id = static_cast<std::int32_t>(req->stream_id());
    active_streaming_calls_.emplace(stream_id,
        [call](Status s) { call->Fail(std::move(s)); });

    req->on_response([call](const nghttp2::asio_http2::client::response& resp) {
        call->Attach(resp);
    });
    req->on_close([self, call, stream_id](uint32_t error_code) {
        self->active_streaming_calls_.erase(stream_id);
        call->OnStreamClose(error_code);
    });

    call->ArmTimer();

    // Deliver the reader handle to the caller immediately; the caller then
    // co_awaits Read() which suspends until messages arrive.
    completion(call->TakeReader());
}

// ── SubmitClientStreamingCall ─────────────────────────────────────────────────

// Generator callback type used by nghttp2-asio for deferred send.
static ssize_t ClientWriterGenerator(
        RawClientWriterImpl* impl,
        uint8_t* buf, std::size_t len, uint32_t* flags)
{
    if (impl->pending_.empty()) {
        if (impl->writes_done_) {
            *flags = NGHTTP2_DATA_FLAG_EOF;
            return 0;
        }
        *flags = NGHTTP2_DATA_FLAG_DEFERRED;
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
}

void ChannelImpl::SubmitClientStreamingCall(
        std::string                             path,
        ClientContext*                          ctx,
        std::function<void(RawClientWriter)>    completion)
{
    auto self = shared_from_this();

    if (state_ == ConnState::kIdle || state_ == ConnState::kConnecting) {
        pending_generic_calls_.push_back(
            [self, path, ctx, comp = std::move(completion)]() mutable {
                self->SubmitClientStreamingCall(
                    std::move(path), ctx, std::move(comp));
            });
        if (state_ == ConnState::kIdle) {
            state_ = ConnState::kConnecting;
            DoConnect();
        }
        return;
    }
    if (state_ == ConnState::kFailed) {
        auto call = std::make_shared<ClientStreamingClientCallState>(ioc_, ctx);
        call->Fail(Status{StatusCode::UNAVAILABLE, "channel failed"});
        completion(call->TakeWriter());
        return;
    }

    // Build request headers.
    nghttp2::asio_http2::header_map hdrs;
    {
        Status err;
        if (!BuildStreamingGrpcHeaders(host_, opts_, ctx, hdrs, err)) {
            auto call = std::make_shared<ClientStreamingClientCallState>(ioc_, ctx);
            call->Fail(std::move(err));
            completion(call->TakeWriter());
            return;
        }
    }

    auto call = std::make_shared<ClientStreamingClientCallState>(ioc_, ctx);
    auto writer_impl = call->writer_impl_;

    // Submit with a generator callback that drains writer_impl->pending_.
    boost::system::error_code ec;
    auto req = session_->submit(
        ec, "POST", std::string{path},
        [writer_impl](uint8_t* buf, std::size_t len, uint32_t* flags) mutable
                -> ssize_t {
            return ClientWriterGenerator(writer_impl.get(), buf, len, flags);
        },
        hdrs);

    if (ec) {
        call->Fail(Status{protocol::AsioErrorToStatusCode(ec.value(), false),
                          "stream submit failed: " + ec.message()});
        completion(call->TakeWriter());
        return;
    }

    const auto stream_id = static_cast<std::int32_t>(req->stream_id());
    active_streaming_calls_.emplace(stream_id,
        [call](Status s) { call->Fail(std::move(s)); });

    // Capture req in on_response so it stays alive until Attach() is called,
    // which sets writer_impl_->req_ so Write()/WritesDone() can call resume().
    req->on_response(
        [call, req](const nghttp2::asio_http2::client::response& resp) {
            call->Attach(req, resp);
        });
    req->on_close([self, call, stream_id](uint32_t error_code) {
        self->active_streaming_calls_.erase(stream_id);
        call->OnStreamClose(error_code);
    });

    call->ArmTimer();
    completion(call->TakeWriter());
}

// ── SubmitBidiStreamingCall ───────────────────────────────────────────────────

void ChannelImpl::SubmitBidiStreamingCall(
        std::string                             path,
        ClientContext*                          ctx,
        std::function<void(BidiHandles)>        completion)
{
    auto self = shared_from_this();

    if (state_ == ConnState::kIdle || state_ == ConnState::kConnecting) {
        pending_generic_calls_.push_back(
            [self, path, ctx, comp = std::move(completion)]() mutable {
                self->SubmitBidiStreamingCall(
                    std::move(path), ctx, std::move(comp));
            });
        if (state_ == ConnState::kIdle) {
            state_ = ConnState::kConnecting;
            DoConnect();
        }
        return;
    }
    if (state_ == ConnState::kFailed) {
        auto call = std::make_shared<BidiStreamingClientCallState>(ioc_, ctx);
        call->Fail(Status{StatusCode::UNAVAILABLE, "channel failed"});
        completion(BidiHandles{call->TakeReader(), call->TakeWriter()});
        return;
    }

    // Build request headers.
    nghttp2::asio_http2::header_map hdrs;
    {
        Status err;
        if (!BuildStreamingGrpcHeaders(host_, opts_, ctx, hdrs, err)) {
            auto call = std::make_shared<BidiStreamingClientCallState>(ioc_, ctx);
            call->Fail(std::move(err));
            completion(BidiHandles{call->TakeReader(), call->TakeWriter()});
            return;
        }
    }

    auto call = std::make_shared<BidiStreamingClientCallState>(ioc_, ctx);
    auto writer_impl = call->writer_impl_;

    boost::system::error_code ec;
    auto req = session_->submit(
        ec, "POST", std::string{path},
        [writer_impl](uint8_t* buf, std::size_t len, uint32_t* flags) mutable
                -> ssize_t {
            return ClientWriterGenerator(writer_impl.get(), buf, len, flags);
        },
        hdrs);

    if (ec) {
        call->Fail(Status{protocol::AsioErrorToStatusCode(ec.value(), false),
                          "stream submit failed: " + ec.message()});
        completion(BidiHandles{call->TakeReader(), call->TakeWriter()});
        return;
    }

    const auto stream_id = static_cast<std::int32_t>(req->stream_id());
    active_streaming_calls_.emplace(stream_id,
        [call](Status s) { call->Fail(std::move(s)); });

    req->on_response(
        [call, req](const nghttp2::asio_http2::client::response& resp) {
            call->Attach(req, resp);
        });
    req->on_close([self, call, stream_id](uint32_t error_code) {
        self->active_streaming_calls_.erase(stream_id);
        call->OnStreamClose(error_code);
    });

    call->ArmTimer();
    completion(BidiHandles{call->TakeReader(), call->TakeWriter()});
}

} // namespace rpcpio::internal

// ── Channel public API ───────────────────────────────────────────────────────

#include "rpcpio/channel.h"
#include <boost/asio/async_result.hpp>

namespace rpcpio {

Channel::Channel(boost::asio::io_context& ioc,
                 std::string              host,
                 std::uint16_t            port,
                 ChannelOptions           opts)
    : impl_(std::make_shared<internal::ChannelImpl>(
          ioc, std::move(host), port, std::move(opts)))
{}

Channel::~Channel() = default;

boost::asio::awaitable<void> Channel::Connect() {
    auto impl = impl_;
    co_await boost::asio::async_initiate<
        boost::asio::use_awaitable_t<>,
        void(boost::system::error_code)>(
        [impl](auto handler) {
            impl->Connect([h = std::move(handler)](
                    boost::system::error_code ec) mutable {
                std::move(h)(ec);
            });
        },
        boost::asio::use_awaitable);
}

boost::asio::awaitable<internal::UnaryResultRaw>
Channel::UnaryCallRaw(std::string_view path,
                      ClientContext&   ctx,
                      std::string_view req_bytes)
{
    auto impl   = impl_;
    std::string path_str{path};
    std::string req_str{req_bytes};

    co_return co_await boost::asio::async_initiate<
        boost::asio::use_awaitable_t<>,
        void(internal::UnaryResultRaw)>(
        [impl, path_str, req_str, &ctx](auto handler) mutable {
            impl->SubmitCall(
                std::move(path_str),
                &ctx,
                std::move(req_str),
                [h = std::move(handler)](internal::UnaryResultRaw r) mutable {
                    std::move(h)(std::move(r));
                });
        },
        boost::asio::use_awaitable);
}

boost::asio::awaitable<internal::RawClientReader>
Channel::ServerStreamingCallRaw(std::string_view path,
                                 ClientContext&   ctx,
                                 std::string_view req_bytes)
{
    auto impl     = impl_;
    std::string path_str{path};
    std::string req_str{req_bytes};

    co_return co_await boost::asio::async_initiate<
        boost::asio::use_awaitable_t<>,
        void(internal::RawClientReader)>(
        [impl, path_str, req_str, &ctx](auto handler) mutable {
            impl->SubmitServerStreamingCall(
                std::move(path_str),
                &ctx,
                std::move(req_str),
                [h = std::move(handler)](internal::RawClientReader r) mutable {
                    std::move(h)(std::move(r));
                });
        },
        boost::asio::use_awaitable);
}

boost::asio::awaitable<internal::RawClientWriter>
Channel::ClientStreamingCallRaw(std::string_view path,
                                 ClientContext&   ctx)
{
    auto impl     = impl_;
    std::string path_str{path};

    co_return co_await boost::asio::async_initiate<
        boost::asio::use_awaitable_t<>,
        void(internal::RawClientWriter)>(
        [impl, path_str, &ctx](auto handler) mutable {
            impl->SubmitClientStreamingCall(
                std::move(path_str),
                &ctx,
                [h = std::move(handler)](internal::RawClientWriter w) mutable {
                    std::move(h)(std::move(w));
                });
        },
        boost::asio::use_awaitable);
}

boost::asio::awaitable<Channel::RawBidiHandles>
Channel::BidiStreamingCallRaw(std::string_view path,
                               ClientContext&   ctx)
{
    auto impl     = impl_;
    std::string path_str{path};

    co_return co_await boost::asio::async_initiate<
        boost::asio::use_awaitable_t<>,
        void(Channel::RawBidiHandles)>(
        [impl, path_str, &ctx](auto handler) mutable {
            impl->SubmitBidiStreamingCall(
                std::move(path_str),
                &ctx,
                [h = std::move(handler)](
                        internal::ChannelImpl::BidiHandles bh) mutable {
                    std::move(h)(Channel::RawBidiHandles{
                        std::move(bh.reader), std::move(bh.writer)});
                });
        },
        boost::asio::use_awaitable);
}

} // namespace rpcpio
