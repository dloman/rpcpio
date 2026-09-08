#include "server_impl.h"

#include <algorithm>
#include "call_state.h"
#include "server_streaming_call_state.h"
#include "client_streaming_call_state.h"
#include "bidi_streaming_call_state.h"

#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl/context.hpp>
#include <nghttp2/asio_http2_server.h>
#include "src/protocol/metadata_codec.h"
#include "src/protocol/status_map.h"

namespace rpcpio::internal {

ServerImpl::ServerImpl(boost::asio::io_context& ioc, ServerOptions opts)
    : ioc_(ioc)
    , opts_(std::move(opts))
    , http2_(ioc)
{}

ServerImpl::~ServerImpl() {
    Shutdown();
}

void ServerImpl::RegisterUnaryRaw(std::string_view path, RawHandler handler) {
    handlers_[std::string(path)] = std::move(handler);
}

void ServerImpl::RegisterUnaryCallback(std::string_view path,
                                        UnaryCallbackHandler handler) {
    callback_handlers_[std::string(path)] = std::move(handler);
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

void ServerImpl::TrackCall(const std::shared_ptr<ServerCallState>& call) {
    std::lock_guard lock(calls_mutex_);
    active_calls_.push_back(call);
}

void ServerImpl::UntrackCall(const ServerCallState* call) {
    std::lock_guard lock(calls_mutex_);
    active_calls_.erase(
        std::remove_if(active_calls_.begin(), active_calls_.end(),
            [call](const std::weak_ptr<ServerCallState>& weak) {
                auto locked = weak.lock();
                return !locked || locked.get() == call;
            }),
        active_calls_.end());
}

void ServerImpl::CancelActiveCalls() {
    std::vector<std::shared_ptr<ServerCallState>> calls;
    {
        std::lock_guard lock(calls_mutex_);
        for (auto& weak : active_calls_) {
            if (auto call = weak.lock()) calls.push_back(std::move(call));
        }
        active_calls_.clear();
    }
    for (auto& call : calls) {
        call->CancelDueToShutdown(
            Status{StatusCode::UNAVAILABLE, "server shutting down"});
    }
}

std::uint16_t ServerImpl::ResolvePort(std::uint16_t port) {
    if (port != 0) return port;
    boost::asio::ip::tcp::acceptor tmp(ioc_);
    tmp.open(boost::asio::ip::tcp::v4());
    tmp.set_option(boost::asio::ip::tcp::acceptor::reuse_address(true));
    tmp.bind(boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(), 0));
    const std::uint16_t chosen = tmp.local_endpoint().port();
    tmp.close();
    return chosen;
}

void ServerImpl::Start(std::string host, std::uint16_t port) {
    port = ResolvePort(port);
    bound_port_ = port;
    auto self = this;

    http2_.handle("/", [self](
            const nghttp2::asio_http2::server::request&  req,
            const nghttp2::asio_http2::server::response& resp) {
        self->HandleRequest(req, resp);
    });

    boost::system::error_code ec;

    if (!opts_.server_cert_file.empty() && !opts_.use_h2c) {
        ssl_context_ = std::make_unique<boost::asio::ssl::context>(
            boost::asio::ssl::context::tls_server);
        ssl_context_->use_certificate_chain_file(opts_.server_cert_file);
        ssl_context_->use_private_key_file(
            opts_.server_key_file, boost::asio::ssl::context::pem);
        if (!opts_.ca_cert_file.empty()) {
            ssl_context_->load_verify_file(opts_.ca_cert_file);
            ssl_context_->set_verify_mode(
                boost::asio::ssl::verify_peer |
                boost::asio::ssl::verify_fail_if_no_peer_cert);
        }
        static const unsigned char kH2Alpn[] = "\x02h2";
        SSL_CTX_set_alpn_select_cb(ssl_context_->native_handle(),
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

        http2_.listen_and_serve(
            ec, *ssl_context_, host, std::to_string(port), true);
    } else {
        http2_.listen_and_serve(ec, host, std::to_string(port), true);
    }

    if (ec) throw boost::system::system_error(ec, "server listen failed");
}

void ServerImpl::Shutdown() {
    if (shutdown_.exchange(true)) return;
    CancelActiveCalls();
    http2_.stop();
}

void ServerImpl::Wait() {
    // Embedded Server does not own worker threads; caller drains ioc_.
}

void ServerImpl::HandleRequest(
    const nghttp2::asio_http2::server::request&  req,
    const nghttp2::asio_http2::server::response& resp)
{
    if (shutdown_.load(std::memory_order_acquire)) {
        nghttp2::asio_http2::header_map trail;
        protocol::BuildTrailers(
            Status{StatusCode::UNAVAILABLE, "server shutting down"}, {}, trail);
        resp.write_head(200, trail);
        resp.end();
        return;
    }

    const std::string& path   = req.uri().path;
    const std::string& method = req.method();

    if (method != "POST") {
        resp.write_head(405);
        resp.end();
        return;
    }

    auto ct_it = req.header().find("content-type");
    if (ct_it == req.header().end() ||
        ct_it->second.value.find("application/grpc") == std::string::npos) {
        resp.write_head(415);
        resp.end();
        return;
    }

    auto te_it = req.header().find("te");
    if (te_it == req.header().end() ||
        te_it->second.value.find("trailers") == std::string::npos) {
        nghttp2::asio_http2::header_map trail;
        protocol::BuildTrailers(
            Status{StatusCode::UNIMPLEMENTED, "missing te: trailers"}, {}, trail);
        resp.write_head(200, trail);
        resp.end();
        return;
    }

    {
        auto it = callback_handlers_.find(path);
        if (it != callback_handlers_.end()) {
            auto state = std::make_shared<ServerCallState>(
                ioc_, this, req, resp, it->second,
                opts_.max_request_message_size,
                opts_.max_metadata_size);
            state->Start();
            return;
        }
    }
    {
        auto it = handlers_.find(path);
        if (it != handlers_.end()) {
            auto state = std::make_shared<ServerCallState>(
                ioc_, this, req, resp, it->second,
                opts_.max_request_message_size,
                opts_.max_metadata_size);
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
                opts_.max_metadata_size);
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
                opts_.max_metadata_size);
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
                opts_.max_metadata_size);
            state->Start();
            return;
        }
    }

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

Server::~Server() = default;

void Server::RegisterUnaryRaw(std::string_view path, RawHandler handler) {
    impl_->RegisterUnaryRaw(path, std::move(handler));
}

void Server::RegisterUnaryCallback(std::string_view path,
                                    UnaryCallbackHandler handler) {
    impl_->RegisterUnaryCallback(path, std::move(handler));
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

void Server::Start(std::string host, std::uint16_t port) {
    impl_->Start(std::move(host), port);
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

// ── StandaloneServer ─────────────────────────────────────────────────────────

#include "rpcpio/standalone_server.h"

namespace rpcpio {

StandaloneServer::StandaloneServer(ServerOptions opts)
    : opts_(std::move(opts))
    , server_(std::make_unique<Server>(ioc_, opts_))
{}

StandaloneServer::~StandaloneServer() {
    Shutdown();
}

void StandaloneServer::Start(std::string host, std::uint16_t port) {
    server_->Start(std::move(host), port);
    started_ = true;
    threads_.reserve(opts_.num_threads);
    for (std::uint32_t i = 0; i < opts_.num_threads; ++i) {
        threads_.emplace_back([this]() { ioc_.run(); });
    }
}

std::uint16_t StandaloneServer::bound_port() const noexcept {
    return server_->bound_port();
}

void StandaloneServer::Shutdown() {
    if (!started_) return;
    server_->Shutdown();
    ioc_.stop();
    for (auto& t : threads_) {
        if (t.joinable()) t.join();
    }
    threads_.clear();
    started_ = false;
}

void StandaloneServer::Wait() {
    for (auto& t : threads_) {
        if (t.joinable()) t.join();
    }
}

} // namespace rpcpio
