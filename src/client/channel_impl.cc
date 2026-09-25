#include "channel_impl.h"
#include "call_state.h"
#include "server_streaming_call_state.h"
#include "client_streaming_call_state.h"
#include "bidi_streaming_call_state.h"

#include <algorithm>
#include <array>
#include <cstring>
#include <exception>
#include <boost/asio/post.hpp>
#include <boost/asio/associated_cancellation_slot.hpp>
#include <boost/asio/associated_executor.hpp>
#include <boost/asio/any_io_executor.hpp>
#include <nghttp2/asio_http2_client.h>
#include <nghttp2/nghttp2.h>
#include <openssl/err.h>

#include "src/protocol/framing.h"
#include "src/protocol/metadata_codec.h"
#include "src/protocol/status_map.h"
#include "src/protocol/timeout.h"

namespace {

rpcpio::Status TlsFileError(
        std::string_view kind,
        const std::string& path,
        const boost::system::error_code& error) {
    return rpcpio::Status{
        rpcpio::StatusCode::INVALID_ARGUMENT,
        std::string(kind) + " '" + path + "': " + error.message()};
}

rpcpio::Status ConfigureTlsContext(
        const rpcpio::ChannelOptions& options,
        boost::asio::ssl::context& context) {
    if (options.client_cert_file.empty() !=
        options.client_key_file.empty()) {
        return rpcpio::Status{
            rpcpio::StatusCode::INVALID_ARGUMENT,
            "client certificate and private key files must be set together"};
    }

    boost::system::error_code error;
    context.set_options(
        boost::asio::ssl::context::default_workarounds |
            boost::asio::ssl::context::no_sslv2 |
            boost::asio::ssl::context::no_sslv3,
        error);
    if (error) {
        return rpcpio::Status{
            rpcpio::StatusCode::INVALID_ARGUMENT,
            "TLS context options: " + error.message()};
    }

    if (!options.ca_cert_file.empty()) {
        context.load_verify_file(options.ca_cert_file, error);
        if (error) {
            return TlsFileError(
                "CA certificate file", options.ca_cert_file, error);
        }
    } else {
        context.set_default_verify_paths(error);
        error.clear();
    }

    if (!options.client_cert_file.empty()) {
        context.use_certificate_chain_file(
            options.client_cert_file, error);
        if (error) {
            return TlsFileError(
                "client certificate file",
                options.client_cert_file,
                error);
        }
        context.use_private_key_file(
            options.client_key_file,
            boost::asio::ssl::context::pem,
            error);
        if (error) {
            return TlsFileError(
                "client private key file",
                options.client_key_file,
                error);
        }
        if (SSL_CTX_check_private_key(context.native_handle()) != 1) {
            std::array<char, 256> message{};
            ERR_error_string_n(
                ERR_get_error(), message.data(), message.size());
            return rpcpio::Status{
                rpcpio::StatusCode::INVALID_ARGUMENT,
                "client certificate and private key do not match: " +
                    std::string(message.data())};
        }
    }

    context.set_verify_mode(
        options.verify_peer
            ? boost::asio::ssl::verify_peer
            : boost::asio::ssl::verify_none,
        error);
    if (error) {
        return rpcpio::Status{
            rpcpio::StatusCode::INVALID_ARGUMENT,
            "TLS verification mode: " + error.message()};
    }

    static const unsigned char kH2Alpn[] = "\x02h2";
    if (SSL_CTX_set_alpn_protos(
            context.native_handle(),
            kH2Alpn,
            sizeof(kH2Alpn) - 1) != 0) {
        return rpcpio::Status{
            rpcpio::StatusCode::INVALID_ARGUMENT,
            "failed to configure TLS ALPN"};
    }
    return {};
}

rpcpio::Status BuildTlsContext(
        const rpcpio::ChannelOptions& options,
        std::shared_ptr<boost::asio::ssl::context>& context) noexcept {
    try {
        context = std::make_shared<boost::asio::ssl::context>(
            boost::asio::ssl::context::tls_client);
        return ConfigureTlsContext(options, *context);
    } catch (const std::exception& error) {
        return rpcpio::Status{
            rpcpio::StatusCode::INVALID_ARGUMENT,
            "TLS configuration: " + std::string(error.what())};
    }
}

} // namespace

namespace rpcpio {

Status ValidateChannelOptions(const ChannelOptions& options) {
    if (!options.use_tls || options.use_h2c) {
        return {};
    }
    std::shared_ptr<boost::asio::ssl::context> context;
    return BuildTlsContext(options, context);
}

} // namespace rpcpio

namespace rpcpio::internal {

class UnaryCallControl {
public:
    void Cancel() {
        cancelled_.store(true, std::memory_order_release);
        std::function<void()> cancel_stream;
        {
            std::lock_guard lock(mutex_);
            cancel_stream = cancel_stream_;
        }
        if (cancel_stream) {
            cancel_stream();
        }
    }

    bool cancelled() const noexcept {
        return cancelled_.load(std::memory_order_acquire);
    }

    void SetCancelStream(std::function<void()> cancel_stream) {
        bool cancel_now = false;
        {
            std::lock_guard lock(mutex_);
            cancel_stream_ = std::move(cancel_stream);
            cancel_now = cancelled();
        }
        if (cancel_now) {
            Cancel();
        }
    }

    void ClearCancelStream() {
        std::lock_guard lock(mutex_);
        cancel_stream_ = {};
    }

private:
    std::atomic<bool> cancelled_{false};
    std::mutex mutex_;
    std::function<void()> cancel_stream_;
};

template<typename Handler>
class UnaryCallCompletion
    : public std::enable_shared_from_this<UnaryCallCompletion<Handler>> {
public:
    UnaryCallCompletion(
            boost::asio::any_io_executor executor,
            Handler handler)
        : executor_(std::move(executor))
        , handler_(std::move(handler))
        , control_(std::make_shared<UnaryCallControl>())
    {}

    void Complete(UnaryResultRaw result) {
        if (completed_.exchange(true)) {
            return;
        }
        control_->ClearCancelStream();
        boost::asio::post(
            executor_,
            [self = this->shared_from_this(),
             result = std::move(result)]() mutable {
                Handler handler = std::move(*self->handler_);
                self->handler_.reset();
                std::move(handler)(std::move(result));
            });
    }

    void Cancel() {
        control_->Cancel();
        Complete(UnaryResultRaw{
            Status{StatusCode::CANCELLED, "call cancelled"}});
    }

    const std::shared_ptr<UnaryCallControl>& control() const {
        return control_;
    }

private:
    boost::asio::any_io_executor executor_;
    std::optional<Handler> handler_;
    std::shared_ptr<UnaryCallControl> control_;
    std::atomic<bool> completed_{false};
};

ChannelImpl::ChannelImpl(boost::asio::io_context& ioc,
                         std::string              host,
                         std::uint16_t            port,
                         ChannelOptions           opts)
    : ioc_(ioc)
    , strand_(ioc.get_executor())
    , host_(std::move(host))
    , port_(port)
    , opts_(std::move(opts))
{}

ChannelImpl::~ChannelImpl() = default;

void ChannelImpl::Connect(std::function<void(Status)> cb) {
    boost::asio::dispatch(strand_,
        [self = shared_from_this(), cb = std::move(cb)]() mutable {
            if (self->state_ == ConnState::kShutdown) {
                cb(self->ShutdownStatus());
                return;
            }
            if (self->state_ == ConnState::kReady) {
                cb({});
                return;
            }
            if (self->state_ == ConnState::kFailed) {
                cb(Status{
                    StatusCode::UNAVAILABLE, "channel failed"});
                return;
            }
            self->connect_waiters_.push_back(std::move(cb));
            if (self->state_ == ConnState::kConnecting) return;
            self->state_ = ConnState::kConnecting;
            self->DoConnect();
        });
}

void ChannelImpl::DoConnect() {
    // Must be called from strand_.
    auto self = shared_from_this();

    if (opts_.use_tls && !opts_.use_h2c) {
        std::shared_ptr<boost::asio::ssl::context> ssl_ctx;
        Status configuration_status =
            BuildTlsContext(opts_, ssl_ctx);
        if (!configuration_status.ok()) {
            state_ = ConnState::kShutdown;
            terminal_status_ = configuration_status;
            DrainWithStatus(std::move(configuration_status));
            return;
        }

        // Create session and store in connecting_session_ before callbacks.
        auto sess = std::make_shared<nghttp2::asio_http2::client::session>(
            strand_, *ssl_ctx, host_, std::to_string(port_));
        connecting_session_ = sess;

        sess->on_connect([self, sess, ssl_ctx](
                boost::asio::ip::tcp::endpoint) mutable {
            boost::asio::post(self->strand_, [self, sess = std::move(sess)]() mutable {
                if (self->state_ == ConnState::kShutdown) {
                    self->connecting_session_.reset();
                    return;
                }
                self->connecting_session_.reset();
                self->session_ = std::move(sess);
                self->OnConnected({});
            });
        });
        sess->on_error([self](const boost::system::error_code& ec) {
            boost::asio::post(self->strand_, [self, ec]() {
                if (self->state_ == ConnState::kShutdown) return;
                if (self->state_ != ConnState::kReady)
                    self->OnConnected(ec);
                else
                    self->FailAll(Status{
                        protocol::AsioErrorToStatusCode(ec.value(), true),
                        "connection error: " + ec.message()});
            });
        });
        sess->on_goaway([self](std::uint32_t ec, std::int32_t last_stream_id) {
            boost::asio::post(self->strand_, [self, ec, last_stream_id]() {
                if (self->state_ == ConnState::kShutdown) return;
                self->OnGoaway(ec, last_stream_id);
            });
        });
    } else {
        // h2c prior-knowledge (plaintext)
        auto sess = std::make_shared<nghttp2::asio_http2::client::session>(
            strand_, host_, std::to_string(port_));
        connecting_session_ = sess;

        sess->on_connect([self, sess](boost::asio::ip::tcp::endpoint) mutable {
            boost::asio::post(self->strand_, [self, sess = std::move(sess)]() mutable {
                if (self->state_ == ConnState::kShutdown) {
                    self->connecting_session_.reset();
                    return;
                }
                self->connecting_session_.reset();
                self->session_ = std::move(sess);
                self->OnConnected({});
            });
        });
        sess->on_error([self](const boost::system::error_code& ec) {
            boost::asio::post(self->strand_, [self, ec]() {
                if (self->state_ == ConnState::kShutdown) return;
                if (self->state_ != ConnState::kReady)
                    self->OnConnected(ec);
                else
                    self->FailAll(Status{
                        protocol::AsioErrorToStatusCode(ec.value(), true),
                        "connection error: " + ec.message()});
            });
        });
        sess->on_goaway([self](std::uint32_t ec, std::int32_t last_stream_id) {
            boost::asio::post(self->strand_, [self, ec, last_stream_id]() {
                if (self->state_ == ConnState::kShutdown) return;
                self->OnGoaway(ec, last_stream_id);
            });
        });
    }
}

void ChannelImpl::DoShutdown() {
    if (state_ == ConnState::kShutdown) return;
    state_ = ConnState::kShutdown;

    const auto clear_session_callbacks = [](
            const std::shared_ptr<
                nghttp2::asio_http2::client::session>& session) {
        session->on_connect({});
        session->on_error({});
        session->on_goaway({});
    };

    // Close any session being established.
    if (connecting_session_) {
        clear_session_callbacks(connecting_session_);
        connecting_session_->shutdown();
        connecting_session_.reset();
    }
    // Close any established session.
    if (session_) {
        clear_session_callbacks(session_);
        session_->shutdown();
        session_.reset();
    }

    Status cancelled{StatusCode::CANCELLED, "channel shut down"};

    // Drain connect waiters.
    auto waiters = std::move(connect_waiters_);
    for (auto& w : waiters)
        w(cancelled);

    // Fail active unary calls.
    for (auto& [id, call] : active_calls_)
        call->Fail(cancelled);
    active_calls_.clear();

    // Fail active streaming calls.
    for (auto& [id, cb] : active_streaming_calls_)
        cb(cancelled);
    active_streaming_calls_.clear();

    FailAll(cancelled);
}

std::string ChannelImpl::RequestUri(std::string_view path) const {
    const bool tls = opts_.use_tls && !opts_.use_h2c;
    return std::string(tls ? "https://" : "http://") + host_ + ":" +
           std::to_string(port_) + std::string(path);
}

void ChannelImpl::Shutdown() {
    if (ioc_.stopped()) {
        // io_context is stopped — no handlers are running, safe to execute directly.
        DoShutdown();
    } else {
        boost::asio::dispatch(strand_,
            [self = shared_from_this()]() { self->DoShutdown(); });
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
    connecting_session_.reset();  // always clear — no-op on success path (already cleared in on_connect)
    state_ = ec ? ConnState::kFailed : ConnState::kReady;
    DrainQueue(ec);
}

void ChannelImpl::DrainQueue(boost::system::error_code ec) {
    auto waiters = std::move(connect_waiters_);
    Status connection_status;
    if (ec) {
        connection_status = Status{
            protocol::AsioErrorToStatusCode(ec.value(), true),
            "connection failed: " + ec.message()};
    }
    for (auto& w : waiters) {
        w(connection_status);
    }

    if (ec) {
        FailAll(connection_status);
        return;
    }
    auto pending = std::move(pending_calls_);
    for (auto& p : pending)
        SubmitCall(std::move(p.path), p.ctx,
                   std::move(p.request_bytes), std::move(p.completion),
                   std::move(p.control));

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

void ChannelImpl::DrainWithStatus(Status st) {
    auto waiters = std::move(connect_waiters_);
    for (auto& w : waiters)
        w(st);
    FailAll(st);
}

Status ChannelImpl::ShutdownStatus() const {
    return terminal_status_.value_or(
        Status{StatusCode::CANCELLED, "channel shut down"});
}

void ChannelImpl::SubmitCall(std::string                         path,
                              ClientContext*                      ctx,
                              std::string                         req_bytes,
                              std::function<void(UnaryResultRaw)> completion,
                              std::shared_ptr<UnaryCallControl>   control) {
    boost::asio::dispatch(strand_,
        [self = shared_from_this(),
         path = std::move(path),
         ctx,
         req_bytes  = std::move(req_bytes),
         completion = std::move(completion),
         control = std::move(control)]() mutable {
            if (control && control->cancelled()) {
                completion(UnaryResultRaw{
                    Status{StatusCode::CANCELLED, "call cancelled"}});
                return;
            }
            if (self->state_ == ConnState::kShutdown) {
                completion(UnaryResultRaw{self->ShutdownStatus()});
                return;
            }
            if (self->state_ == ConnState::kIdle ||
                self->state_ == ConnState::kConnecting) {
                self->pending_calls_.push_back({
                    std::move(path),
                    ctx,
                    std::move(req_bytes),
                    std::move(completion),
                    std::move(control)});
                if (self->state_ == ConnState::kIdle) {
                    self->state_ = ConnState::kConnecting;
                    self->DoConnect();
                }
                return;
            }
            if (self->state_ == ConnState::kFailed) {
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
            hdrs.emplace("user-agent",
                nghttp2::asio_http2::header_value{self->opts_.user_agent, false});

            if (ctx && ctx->has_deadline()) {
                auto d = ctx->deadline_from_now();
                if (d.count() <= 0) {
                    completion(UnaryResultRaw{
                        Status{StatusCode::DEADLINE_EXCEEDED,
                               "deadline already passed"}});
                    return;
                }
                auto ts = protocol::FormatTimeout(d);
                if (!ts.empty())
                    hdrs.emplace("grpc-timeout",
                        nghttp2::asio_http2::header_value{ts, false});
            }

            if (ctx && ctx->compression_algorithm() != "identity") {
                hdrs.emplace("grpc-encoding",
                    nghttp2::asio_http2::header_value{
                        ctx->compression_algorithm(), false});
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
            auto call = std::make_shared<ClientCallState>(
                self->ioc_, self->strand_, ctx, std::move(completion));

            boost::system::error_code ec;
            auto req = self->session_->submit(
                ec, "POST", self->RequestUri(path), frame, hdrs);
            if (ec) {
                call->Fail(Status{
                    protocol::AsioErrorToStatusCode(ec.value(), false),
                    "stream submit failed: " + ec.message()});
                return;
            }

            // Track this call so OnGoaway can fail it if the peer did not accept it.
            const auto stream_id =
                static_cast<std::int32_t>(req->stream_id());
            self->active_calls_.emplace(stream_id, call);
            if (control) {
                control->SetCancelStream(
                    [weak_self = std::weak_ptr<ChannelImpl>(self),
                     call,
                     req] {
                        if (auto self = weak_self.lock()) {
                            boost::asio::dispatch(
                                self->strand_,
                                [call, req] {
                                    req->cancel(NGHTTP2_CANCEL);
                                    call->Cancel();
                                });
                        }
                    });
            }

            // on_response: register sub-callbacks on the response immediately.
            // The response object is valid for the duration of this callback.
            req->on_response([call](
                    const nghttp2::asio_http2::client::response& resp) {
                call->Attach(resp);
            });
            req->on_close([self, call, stream_id](uint32_t error_code) {
                boost::asio::post(self->strand_,
                    [self, call, stream_id, error_code]() {
                        self->RemoveActiveCall(stream_id);
                        call->OnStreamClose(error_code);
                    });
            });

            call->ArmTimer();
        });
}

// ── Shared header-builder helper ──────────────────────────────────────────────
// Returns true on success, false if the deadline has already passed.
// On false, out_err is set to the deadline-exceeded status.
static bool BuildStreamingGrpcHeaders(
        const ChannelOptions&            opts,
        ClientContext*                   ctx,
        nghttp2::asio_http2::header_map& hdrs,
        Status&                          out_err)
{
    hdrs.emplace("content-type",
        nghttp2::asio_http2::header_value{"application/grpc+proto", false});
    hdrs.emplace("te",
        nghttp2::asio_http2::header_value{"trailers", false});
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
    boost::asio::dispatch(strand_,
        [self = shared_from_this(),
         path = std::move(path),
         ctx,
         request_bytes = std::move(request_bytes),
         completion = std::move(completion)]() mutable {
            if (self->state_ == ConnState::kShutdown) {
                auto call = std::make_shared<ServerStreamingClientCallState>(
                    self->ioc_, self->strand_, ctx);
                call->Fail(self->ShutdownStatus());
                completion(call->TakeReader());
                return;
            }
            if (self->state_ == ConnState::kIdle ||
                self->state_ == ConnState::kConnecting) {
                self->pending_generic_calls_.push_back(
                    [self, path, ctx,
                     req_bytes = std::move(request_bytes),
                     comp = std::move(completion)]() mutable {
                        self->SubmitServerStreamingCall(
                            std::move(path), ctx,
                            std::move(req_bytes), std::move(comp));
                    });
                if (self->state_ == ConnState::kIdle) {
                    self->state_ = ConnState::kConnecting;
                    self->DoConnect();
                }
                return;
            }
            if (self->state_ == ConnState::kFailed) {
                auto call = std::make_shared<ServerStreamingClientCallState>(
                    self->ioc_, self->strand_, ctx);
                call->Fail(Status{StatusCode::UNAVAILABLE, "channel failed"});
                completion(call->TakeReader());
                return;
            }

            // Build request headers.
            nghttp2::asio_http2::header_map hdrs;
            {
                Status err;
                if (!BuildStreamingGrpcHeaders(
                        self->opts_, ctx, hdrs, err)) {
                    auto call = std::make_shared<ServerStreamingClientCallState>(
                        self->ioc_, self->strand_, ctx);
                    call->Fail(std::move(err));
                    completion(call->TakeReader());
                    return;
                }
            }

            // Encode the single request message.
            std::string frame;
            if (!protocol::EncodeFrame(0, request_bytes, frame)) {
                auto call = std::make_shared<ServerStreamingClientCallState>(
                    self->ioc_, self->strand_, ctx);
                call->Fail(Status{StatusCode::RESOURCE_EXHAUSTED,
                                  "request too large"});
                completion(call->TakeReader());
                return;
            }

            auto call = std::make_shared<ServerStreamingClientCallState>(
                self->ioc_, self->strand_, ctx);

            boost::system::error_code ec;
            auto req = self->session_->submit(
                ec, "POST", self->RequestUri(path), frame, hdrs);
            if (ec) {
                call->Fail(Status{
                    protocol::AsioErrorToStatusCode(ec.value(), false),
                    "stream submit failed: " + ec.message()});
                completion(call->TakeReader());
                return;
            }

            const auto stream_id =
                static_cast<std::int32_t>(req->stream_id());
            self->active_streaming_calls_.emplace(stream_id,
                [call](Status s) { call->Fail(std::move(s)); });

            req->on_response([call](
                    const nghttp2::asio_http2::client::response& resp) {
                call->Attach(resp);
            });
            req->on_close([self, call, stream_id](uint32_t error_code) {
                boost::asio::post(self->strand_,
                    [self, call, stream_id, error_code]() {
                        self->active_streaming_calls_.erase(stream_id);
                        call->OnStreamClose(error_code);
                    });
            });

            call->ArmTimer();

            // Deliver the reader handle to the caller immediately.
            completion(call->TakeReader());
        });
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
        return NGHTTP2_ERR_DEFERRED;
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
    boost::asio::dispatch(strand_,
        [self = shared_from_this(),
         path = std::move(path),
         ctx,
         completion = std::move(completion)]() mutable {
            if (self->state_ == ConnState::kShutdown) {
                auto call = std::make_shared<ClientStreamingClientCallState>(
                    self->ioc_, self->strand_, ctx);
                call->Fail(self->ShutdownStatus());
                completion(call->TakeWriter());
                return;
            }
            if (self->state_ == ConnState::kIdle ||
                self->state_ == ConnState::kConnecting) {
                self->pending_generic_calls_.push_back(
                    [self, path, ctx,
                     comp = std::move(completion)]() mutable {
                        self->SubmitClientStreamingCall(
                            std::move(path), ctx, std::move(comp));
                    });
                if (self->state_ == ConnState::kIdle) {
                    self->state_ = ConnState::kConnecting;
                    self->DoConnect();
                }
                return;
            }
            if (self->state_ == ConnState::kFailed) {
                auto call = std::make_shared<ClientStreamingClientCallState>(
                    self->ioc_, self->strand_, ctx);
                call->Fail(Status{StatusCode::UNAVAILABLE, "channel failed"});
                completion(call->TakeWriter());
                return;
            }

            // Build request headers.
            nghttp2::asio_http2::header_map hdrs;
            {
                Status err;
                if (!BuildStreamingGrpcHeaders(
                        self->opts_, ctx, hdrs, err)) {
                    auto call = std::make_shared<ClientStreamingClientCallState>(
                        self->ioc_, self->strand_, ctx);
                    call->Fail(std::move(err));
                    completion(call->TakeWriter());
                    return;
                }
            }

            auto call = std::make_shared<ClientStreamingClientCallState>(
                self->ioc_, self->strand_, ctx);
            auto writer_impl = call->writer_impl_;

            // Submit with a generator callback that drains writer_impl->pending_.
            boost::system::error_code ec;
            auto req = self->session_->submit(
                ec, "POST", self->RequestUri(path),
                [writer_impl](uint8_t* buf, std::size_t len,
                              uint32_t* flags) mutable -> ssize_t {
                    return ClientWriterGenerator(writer_impl.get(), buf, len, flags);
                },
                hdrs);

            if (ec) {
                call->Fail(Status{
                    protocol::AsioErrorToStatusCode(ec.value(), false),
                    "stream submit failed: " + ec.message()});
                completion(call->TakeWriter());
                return;
            }

            writer_impl->req_ = req;

            const auto stream_id =
                static_cast<std::int32_t>(req->stream_id());
            self->active_streaming_calls_.emplace(stream_id,
                [call](Status s) { call->Fail(std::move(s)); });

            req->on_response(
                [call](const nghttp2::asio_http2::client::response& resp) {
                    call->Attach(resp);
                });
            req->on_close([self, call, stream_id](uint32_t error_code) {
                boost::asio::post(self->strand_,
                    [self, call, stream_id, error_code]() {
                        self->active_streaming_calls_.erase(stream_id);
                        call->OnStreamClose(error_code);
                    });
            });

            call->ArmTimer();
            completion(call->TakeWriter());
        });
}

// ── SubmitBidiStreamingCall ───────────────────────────────────────────────────

void ChannelImpl::SubmitBidiStreamingCall(
        std::string                             path,
        ClientContext*                          ctx,
        std::function<void(BidiHandles)>        completion)
{
    boost::asio::dispatch(strand_,
        [self = shared_from_this(),
         path = std::move(path),
         ctx,
         completion = std::move(completion)]() mutable {
            if (self->state_ == ConnState::kShutdown) {
                auto call = std::make_shared<BidiStreamingClientCallState>(
                    self->ioc_, self->strand_, ctx);
                call->Fail(self->ShutdownStatus());
                completion(BidiHandles{call->TakeReader(), call->TakeWriter()});
                return;
            }
            if (self->state_ == ConnState::kIdle ||
                self->state_ == ConnState::kConnecting) {
                self->pending_generic_calls_.push_back(
                    [self, path, ctx,
                     comp = std::move(completion)]() mutable {
                        self->SubmitBidiStreamingCall(
                            std::move(path), ctx, std::move(comp));
                    });
                if (self->state_ == ConnState::kIdle) {
                    self->state_ = ConnState::kConnecting;
                    self->DoConnect();
                }
                return;
            }
            if (self->state_ == ConnState::kFailed) {
                auto call = std::make_shared<BidiStreamingClientCallState>(
                    self->ioc_, self->strand_, ctx);
                call->Fail(Status{StatusCode::UNAVAILABLE, "channel failed"});
                completion(BidiHandles{call->TakeReader(), call->TakeWriter()});
                return;
            }

            // Build request headers.
            nghttp2::asio_http2::header_map hdrs;
            {
                Status err;
                if (!BuildStreamingGrpcHeaders(
                        self->opts_, ctx, hdrs, err)) {
                    auto call = std::make_shared<BidiStreamingClientCallState>(
                        self->ioc_, self->strand_, ctx);
                    call->Fail(std::move(err));
                    completion(BidiHandles{
                        call->TakeReader(), call->TakeWriter()});
                    return;
                }
            }

            auto call = std::make_shared<BidiStreamingClientCallState>(
                self->ioc_, self->strand_, ctx);
            auto writer_impl = call->writer_impl_;

            boost::system::error_code ec;
            auto req = self->session_->submit(
                ec, "POST", self->RequestUri(path),
                [writer_impl](uint8_t* buf, std::size_t len,
                              uint32_t* flags) mutable -> ssize_t {
                    return ClientWriterGenerator(writer_impl.get(), buf, len, flags);
                },
                hdrs);

            if (ec) {
                call->Fail(Status{
                    protocol::AsioErrorToStatusCode(ec.value(), false),
                    "stream submit failed: " + ec.message()});
                completion(BidiHandles{call->TakeReader(), call->TakeWriter()});
                return;
            }

            writer_impl->req_ = req;

            const auto stream_id =
                static_cast<std::int32_t>(req->stream_id());
            self->active_streaming_calls_.emplace(stream_id,
                [call](Status s) { call->Fail(std::move(s)); });

            req->on_response(
                [call](const nghttp2::asio_http2::client::response& resp) {
                    call->Attach(resp);
                });
            req->on_close([self, call, stream_id](uint32_t error_code) {
                boost::asio::post(self->strand_,
                    [self, call, stream_id, error_code]() {
                        self->active_streaming_calls_.erase(stream_id);
                        call->OnStreamClose(error_code);
                    });
            });

            call->ArmTimer();
            completion(BidiHandles{call->TakeReader(), call->TakeWriter()});
        });
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

// The session callbacks keep ChannelImpl alive, so without an explicit shutdown
// the connection would outlive the last Channel and keep its io_context busy.
Channel::~Channel() {
    if (impl_) impl_->Shutdown();
}

void Channel::Shutdown() {
    impl_->Shutdown();
}

boost::asio::awaitable<Status> Channel::Connect() {
    return boost::asio::async_initiate<
        const boost::asio::use_awaitable_t<>&,
        void(Status)>(
        [impl = impl_](auto handler) {
            impl->Connect([h = std::make_shared<decltype(handler)>(std::move(handler))](
                    Status status) mutable {
                std::move(*h)(std::move(status));
            });
        },
        boost::asio::use_awaitable);
}

boost::asio::awaitable<UnaryResultRaw>
Channel::UnaryCallRaw(std::string_view path,
                      ClientContext&   ctx,
                      std::string_view req_bytes)
{
    return boost::asio::async_initiate<
        const boost::asio::use_awaitable_t<>&,
        void(UnaryResultRaw)>(
        [impl = impl_, path_str = std::string(path),
         req_str = std::string(req_bytes), &ctx](auto handler) mutable {
            auto executor =
                boost::asio::get_associated_executor(handler);
            auto coroutine_slot =
                boost::asio::get_associated_cancellation_slot(handler);
            auto completion = std::make_shared<
                internal::UnaryCallCompletion<decltype(handler)>>(
                    std::move(executor), std::move(handler));
            std::weak_ptr weak_completion = completion;
            const auto cancel = [weak_completion](
                    boost::asio::cancellation_type) {
                if (auto completion = weak_completion.lock()) {
                    completion->Cancel();
                }
            };
            ctx.cancellation_slot().assign(cancel);
            if (coroutine_slot.is_connected()) {
                coroutine_slot.assign(cancel);
            }
            impl->SubmitCall(
                std::move(path_str),
                &ctx,
                std::move(req_str),
                [completion](UnaryResultRaw result) mutable {
                    completion->Complete(std::move(result));
                },
                completion->control());
        },
        boost::asio::use_awaitable);
}

boost::asio::awaitable<internal::RawClientReader>
Channel::ServerStreamingCallRaw(std::string_view path,
                                 ClientContext&   ctx,
                                 std::string_view req_bytes)
{
    return boost::asio::async_initiate<
        const boost::asio::use_awaitable_t<>&,
        void(internal::RawClientReader)>(
        [impl = impl_, path_str = std::string(path),
         req_str = std::string(req_bytes), &ctx](auto handler) mutable {
            impl->SubmitServerStreamingCall(
                std::move(path_str),
                &ctx,
                std::move(req_str),
                [h = std::make_shared<decltype(handler)>(std::move(handler))](internal::RawClientReader r) mutable {
                    std::move(*h)(std::move(r));
                });
        },
        boost::asio::use_awaitable);
}

boost::asio::awaitable<internal::RawClientWriter>
Channel::ClientStreamingCallRaw(std::string_view path,
                                 ClientContext&   ctx)
{
    return boost::asio::async_initiate<
        const boost::asio::use_awaitable_t<>&,
        void(internal::RawClientWriter)>(
        [impl = impl_, path_str = std::string(path), &ctx](auto handler) mutable {
            impl->SubmitClientStreamingCall(
                std::move(path_str),
                &ctx,
                [h = std::make_shared<decltype(handler)>(std::move(handler))](internal::RawClientWriter w) mutable {
                    std::move(*h)(std::move(w));
                });
        },
        boost::asio::use_awaitable);
}

boost::asio::awaitable<Channel::RawBidiHandles>
Channel::BidiStreamingCallRaw(std::string_view path,
                               ClientContext&   ctx)
{
    return boost::asio::async_initiate<
        const boost::asio::use_awaitable_t<>&,
        void(Channel::RawBidiHandles)>(
        [impl = impl_, path_str = std::string(path), &ctx](auto handler) mutable {
            impl->SubmitBidiStreamingCall(
                std::move(path_str),
                &ctx,
                [h = std::make_shared<decltype(handler)>(std::move(handler))](
                        internal::ChannelImpl::BidiHandles bh) mutable {
                    std::move(*h)(Channel::RawBidiHandles{
                        std::move(bh.reader), std::move(bh.writer)});
                });
        },
        boost::asio::use_awaitable);
}

} // namespace rpcpio
