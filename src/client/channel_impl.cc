#include "channel_impl.h"
#include "call_state.h"

#include <boost/asio/post.hpp>
#include <nghttp2/asio_http2_client.h>

#include "src/protocol/metadata_codec.h"
#include "src/protocol/status_map.h"
#include "src/protocol/timeout.h"

namespace asio_grpc::internal {

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
        sess->on_connect([self, sess](boost::asio::ip::tcp::resolver::iterator) {
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
                boost::asio::ip::tcp::resolver::iterator) {
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
}

void ChannelImpl::FailAll(Status status) {
    auto pending = std::move(pending_calls_);
    for (auto& p : pending)
        p.completion(UnaryResultRaw{status});
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

} // namespace asio_grpc::internal

// ── Channel public API ───────────────────────────────────────────────────────

#include "asio_grpc/channel.h"
#include <boost/asio/async_result.hpp>

namespace asio_grpc {

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

} // namespace asio_grpc
