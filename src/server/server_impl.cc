#include "server_impl.h"
#include "call_state.h"
#include "server_streaming_call_state.h"
#include "client_streaming_call_state.h"
#include "bidi_streaming_call_state.h"

#include <algorithm>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/ssl/context.hpp>
#include <nghttp2/asio_http2_server.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>
#include "src/protocol/metadata_codec.h"
#include "src/protocol/status_map.h"

namespace {

std::string PeerIdentity(X509* certificate) {
    if (certificate == nullptr) {
        return {};
    }

    GENERAL_NAMES* names = static_cast<GENERAL_NAMES*>(
        X509_get_ext_d2i(certificate, NID_subject_alt_name, nullptr, nullptr));
    if (names == nullptr) {
        return {};
    }

    std::string first_dns_name;
    const int count = sk_GENERAL_NAME_num(names);
    for (int i = 0; i < count; ++i) {
        const GENERAL_NAME* name = sk_GENERAL_NAME_value(names, i);
        const ASN1_IA5STRING* value = nullptr;
        if (name->type == GEN_URI) {
            value = name->d.uniformResourceIdentifier;
        } else if (name->type == GEN_DNS && first_dns_name.empty()) {
            value = name->d.dNSName;
        }
        if (value == nullptr) {
            continue;
        }

        const auto* data = ASN1_STRING_get0_data(value);
        const int length = ASN1_STRING_length(value);
        std::string identity{
            reinterpret_cast<const char*>(data),
            static_cast<std::size_t>(length)};
        if (name->type == GEN_URI) {
            GENERAL_NAMES_free(names);
            return identity;
        }
        first_dns_name = std::move(identity);
    }

    GENERAL_NAMES_free(names);
    return first_dns_name;
}

} // namespace

namespace rpcpio::internal {

ServerImpl::ServerImpl(boost::asio::io_context& ioc, ServerOptions opts)
    : owned_ioc_(opts.num_threads == 0
          ? nullptr
          : std::make_unique<boost::asio::io_context>())
    , ioc_(owned_ioc_ ? *owned_ioc_ : ioc)
    , opts_(std::move(opts))
    , http2_(ioc_)
    , shutdown_timer_(ioc_)
{
    if (owned_ioc_) {
        work_guard_.emplace(ioc_.get_executor());
    }
}

ServerImpl::~ServerImpl() {
    Shutdown();
    Wait();
}

void ServerImpl::RegisterUnaryRaw(std::string_view path, RawHandler handler) {
    handlers_[std::string(path)] = std::move(handler);
}

void ServerImpl::RegisterServerStreamingRaw(std::string_view path,
                                             RawServerStreamingHandler handler) {
    server_streaming_handlers_[std::string(path)] = std::move(handler);
}

void ServerImpl::RegisterClientStreamingRaw(std::string_view path,
                                             RawClientStreamingHandler handler) {
    client_streaming_handlers_[std::string(path)] = std::move(handler);
}

void ServerImpl::RegisterBidiRaw(std::string_view path,
                                  RawBidiStreamingHandler handler) {
    bidi_handlers_[std::string(path)] = std::move(handler);
}

std::uint16_t ServerImpl::ResolvePort(std::uint16_t port) {
    if (port != 0) return port;
    // Briefly bind an acceptor on port 0 so the OS picks an ephemeral port.
    // There is a small TOCTOU window between close() and listen_and_serve();
    // this is acceptable for test environments.
    boost::asio::ip::tcp::acceptor tmp(ioc_);
    tmp.open(boost::asio::ip::tcp::v4());
    tmp.set_option(boost::asio::ip::tcp::acceptor::reuse_address(true));
    tmp.bind(boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(), 0));
    const std::uint16_t chosen = tmp.local_endpoint().port();
    tmp.close();
    return chosen;
}

rpcpio::Status ServerImpl::Start(std::string host, std::uint16_t port) {
    if (started_.exchange(true))
        return Status{StatusCode::FAILED_PRECONDITION, "Server::Start called more than once"};
    port = ResolvePort(port);
    bound_port_ = port;
    auto self = this;

    // Register a catch-all handler; dispatch per-path inside.
    http2_.handle("/", [self](
            const nghttp2::asio_http2::server::request&  req,
            const nghttp2::asio_http2::server::response& resp) {
        self->HandleRequest(req, resp);
    });

    boost::system::error_code ec;

    if (!opts_.server_cert_file.empty() && !opts_.use_h2c) {
        ssl_context_ = std::make_shared<boost::asio::ssl::context>(
            boost::asio::ssl::context::tls_server);
        auto& ssl_ctx = *ssl_context_;

        ssl_ctx.use_certificate_chain_file(opts_.server_cert_file, ec);
        if (ec) return Status{StatusCode::INVALID_ARGUMENT,
                              "server cert file: " + ec.message()};

        ssl_ctx.use_private_key_file(opts_.server_key_file,
                                      boost::asio::ssl::context::pem, ec);
        if (ec) return Status{StatusCode::INVALID_ARGUMENT,
                              "server key file: " + ec.message()};

        if (!opts_.ca_cert_file.empty()) {
            ssl_ctx.load_verify_file(opts_.ca_cert_file, ec);
            if (ec) return Status{StatusCode::INVALID_ARGUMENT,
                                  "CA cert file: " + ec.message()};
            ssl_ctx.set_verify_mode(boost::asio::ssl::verify_peer |
                                    boost::asio::ssl::verify_fail_if_no_peer_cert);
        }

        // ALPN h2
        static const unsigned char kH2Alpn[] = "\x02h2";
        SSL_CTX_set_alpn_select_cb(ssl_ctx.native_handle(),
            [](SSL*, const unsigned char** out, unsigned char* outlen,
               const unsigned char* in, unsigned int inlen, void*) -> int {
                if (SSL_select_next_proto(
                        const_cast<unsigned char**>(out), outlen,
                        kH2Alpn, sizeof(kH2Alpn)-1,
                        in, inlen) != OPENSSL_NPN_NEGOTIATED)
                    return SSL_TLSEXT_ERR_NOACK;
                return SSL_TLSEXT_ERR_OK;
            },
            nullptr);

        http2_.listen_and_serve(ec, ssl_ctx, host, std::to_string(port), true);
    } else {
        http2_.listen_and_serve(ec, host, std::to_string(port), true);
    }

    if (ec) return Status{StatusCode::UNAVAILABLE,
                          "server listen failed: " + ec.message()};
    listening_ = true;

    // Spin up worker threads.
    for (std::uint32_t i = 0; i < opts_.num_threads; ++i) {
        threads_.emplace_back([this]() { ioc_.run(); });
    }

    return Status{};
}

void ServerImpl::Shutdown() {
    if (shutdown_.exchange(true)) return;

    if (listening_) {
        http2_.stop_listening();
    }

    std::vector<std::shared_ptr<ServerCallStateBase>> calls;
    {
        std::lock_guard lock(active_calls_mutex_);
        for (const auto& weak_call : active_calls_) {
            if (auto call = weak_call.lock()) {
                calls.push_back(std::move(call));
            }
        }
        active_calls_.clear();
    }
    for (const auto& call : calls) {
        call->Cancel(
            Status{StatusCode::UNAVAILABLE, "server shutting down"});
    }

    if (!listening_) {
        work_guard_.reset();
        return;
    }
    if (ioc_.stopped()) {
        StopTransport();
        return;
    }

    boost::asio::post(ioc_, [self = shared_from_this()] {
        self->FinishShutdown(
            std::chrono::steady_clock::now() + self->opts_.grace_period);
    });
}

void ServerImpl::Wait() {
    std::vector<std::thread> threads;
    {
        std::lock_guard lock(threads_mutex_);
        threads = std::move(threads_);
    }
    for (auto& thread : threads) {
        if (!thread.joinable()) {
            continue;
        }
        if (thread.get_id() == std::this_thread::get_id()) {
            thread.detach();
        } else {
            thread.join();
        }
    }
}

void ServerImpl::StopTransport() {
    shutdown_timer_.cancel();
    if (listening_.exchange(false)) {
        http2_.stop();
    }
    work_guard_.reset();
}

void ServerImpl::FinishShutdown(
        std::chrono::steady_clock::time_point deadline) {
    bool has_active_calls = false;
    {
        std::lock_guard lock(active_calls_mutex_);
        std::erase_if(active_calls_, [](const auto& weak_call) {
            auto call = weak_call.lock();
            return !call || call->IsTerminal();
        });
        has_active_calls = !active_calls_.empty();
    }

    if (!has_active_calls || std::chrono::steady_clock::now() >= deadline) {
        StopTransport();
        return;
    }

    shutdown_timer_.expires_after(std::chrono::milliseconds(1));
    shutdown_timer_.async_wait(
        [self = shared_from_this(), deadline](
                const boost::system::error_code& error) {
            if (!error) {
                self->FinishShutdown(deadline);
            }
        });
}

void ServerImpl::TrackCall(
        const std::shared_ptr<ServerCallStateBase>& call) {
    std::lock_guard lock(active_calls_mutex_);
    std::erase_if(active_calls_, [](const auto& weak_call) {
        return weak_call.expired();
    });
    active_calls_.push_back(call);
}

void ServerImpl::HandleRequest(
    const nghttp2::asio_http2::server::request&  req,
    const nghttp2::asio_http2::server::response& resp)
{
    const std::string& path   = req.uri().path;
    const std::string& method = req.method();

    if (shutdown_) {
        nghttp2::asio_http2::header_map trail;
        protocol::BuildTrailers(
            Status{StatusCode::UNAVAILABLE, "server shutting down"}, {}, trail);
        resp.write_head(200, trail);
        resp.end();
        return;
    }

    // Validate HTTP method.
    if (method != "POST") {
        resp.write_head(405);
        resp.end();
        return;
    }

    // Validate content-type.
    auto ct_it = req.header().find("content-type");
    if (ct_it == req.header().end() ||
        ct_it->second.value.find("application/grpc") == std::string::npos) {
        resp.write_head(415);
        resp.end();
        return;
    }

    // Validate te: trailers.
    auto te_it = req.header().find("te");
    if (te_it == req.header().end() ||
        te_it->second.value.find("trailers") == std::string::npos) {
        // Technically required; reject as UNIMPLEMENTED via trailer.
        nghttp2::asio_http2::header_map trail;
        protocol::BuildTrailers(
            Status{StatusCode::UNIMPLEMENTED, "missing te: trailers"}, {}, trail);
        resp.write_head(200, trail);
        resp.end();
        return;
    }

    // The request borrows the certificate owned by its connection.
    std::string peer_identity = PeerIdentity(req.tls_peer_certificate());

    // Look up handler: check all four RPC kind maps in order.
    {
        auto it = handlers_.find(path);
        if (it != handlers_.end()) {
            auto state = std::make_shared<ServerCallState>(
                ioc_, req, resp, it->second,
                opts_.max_request_message_size,
                opts_.max_metadata_size,
                opts_.max_response_message_size,
                peer_identity);
            TrackCall(state);
            state->Start();
            return;
        }
    }
    {
        auto it = server_streaming_handlers_.find(path);
        if (it != server_streaming_handlers_.end()) {
            auto state = std::make_shared<ServerStreamingCallState>(
                ioc_, req, resp, it->second,
                opts_.max_request_message_size,
                opts_.max_metadata_size,
                opts_.max_response_message_size,
                peer_identity);
            TrackCall(state);
            state->Start();
            return;
        }
    }
    {
        auto it = client_streaming_handlers_.find(path);
        if (it != client_streaming_handlers_.end()) {
            auto state = std::make_shared<ClientStreamingCallState>(
                ioc_, req, resp, it->second,
                opts_.max_request_message_size,
                opts_.max_metadata_size,
                opts_.max_response_message_size,
                peer_identity);
            TrackCall(state);
            state->Start();
            return;
        }
    }
    {
        auto it = bidi_handlers_.find(path);
        if (it != bidi_handlers_.end()) {
            auto state = std::make_shared<BidiStreamingCallState>(
                ioc_, req, resp, it->second,
                opts_.max_request_message_size,
                opts_.max_metadata_size,
                opts_.max_response_message_size,
                peer_identity);
            TrackCall(state);
            state->Start();
            return;
        }
    }

    // No handler found.
    nghttp2::asio_http2::header_map trail;
    protocol::BuildTrailers(
        Status{StatusCode::UNIMPLEMENTED,
               "unknown method: " + path}, {}, trail);
    resp.write_head(200, trail);
    resp.end();
}

} // namespace rpcpio::internal

// ── Server public API ────────────────────────────────────────────────────────

#include "rpcpio/server.h"

namespace rpcpio {

Server::Server(boost::asio::io_context& ioc, ServerOptions opts)
    : impl_(std::make_shared<internal::ServerImpl>(ioc, std::move(opts)))
{}

Server::~Server() {
    impl_->Shutdown();
    impl_->Wait();
}

void Server::RegisterUnaryRaw(std::string_view path, RawHandler handler) {
    impl_->RegisterUnaryRaw(path, std::move(handler));
}

void Server::RegisterServerStreamingRaw(std::string_view path,
                                         RawServerStreamingHandler handler) {
    impl_->RegisterServerStreamingRaw(path, std::move(handler));
}

void Server::RegisterClientStreamingRaw(std::string_view path,
                                         RawClientStreamingHandler handler) {
    impl_->RegisterClientStreamingRaw(path, std::move(handler));
}

void Server::RegisterBidiRaw(std::string_view path,
                               RawBidiStreamingHandler handler) {
    impl_->RegisterBidiRaw(path, std::move(handler));
}

rpcpio::Status Server::Start(std::string host, std::uint16_t port) {
    return impl_->Start(std::move(host), port);
}

std::uint16_t Server::bound_port() const noexcept {
    return impl_->bound_port();
}

void Server::Shutdown() {
    impl_->Shutdown();
}

void Server::Wait() {
    impl_->Wait();
}

} // namespace rpcpio
