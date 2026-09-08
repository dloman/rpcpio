#include "server_impl.h"
#include "call_state.h"

#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl/context.hpp>
#include <nghttp2/asio_http2_server.h>
#include "src/protocol/metadata_codec.h"
#include "src/protocol/status_map.h"

namespace rpcpio::internal {

ServerImpl::ServerImpl(boost::asio::io_context& ioc, ServerOptions opts)
    : ioc_(ioc)
    , opts_(std::move(opts))
    , http2_(ioc)    // share caller's io_context for both I/O and handler coroutines
{}

ServerImpl::~ServerImpl() {
    Shutdown();
}

void ServerImpl::RegisterUnaryRaw(std::string_view path, RawHandler handler) {
    handlers_[std::string(path)] = std::move(handler);
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

void ServerImpl::Start(std::string host, std::uint16_t port) {
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
        boost::asio::ssl::context ssl_ctx(boost::asio::ssl::context::tls_server);
        ssl_ctx.use_certificate_chain_file(opts_.server_cert_file);
        ssl_ctx.use_private_key_file(opts_.server_key_file,
                                      boost::asio::ssl::context::pem);
        if (!opts_.ca_cert_file.empty()) {
            ssl_ctx.load_verify_file(opts_.ca_cert_file);
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

    if (ec) throw boost::system::system_error(ec, "server listen failed");

    // Spin up worker threads.
    for (std::uint32_t i = 0; i < opts_.num_threads; ++i) {
        threads_.emplace_back([this]() { ioc_.run(); });
    }
}

void ServerImpl::Shutdown() {
    if (shutdown_.exchange(true)) return;
    http2_.stop();
    ioc_.stop();
    for (auto& t : threads_) {
        if (t.joinable()) t.join();
    }
    threads_.clear();
}

void ServerImpl::Wait() {
    for (auto& t : threads_) {
        if (t.joinable()) t.join();
    }
}

void ServerImpl::HandleRequest(
    const nghttp2::asio_http2::server::request&  req,
    const nghttp2::asio_http2::server::response& resp)
{
    const std::string& path   = req.uri().path;
    const std::string& method = req.method();

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

    // Look up handler.
    auto it = handlers_.find(path);
    if (it == handlers_.end()) {
        nghttp2::asio_http2::header_map trail;
        protocol::BuildTrailers(
            Status{StatusCode::UNIMPLEMENTED,
                   "unknown method: " + path}, {}, trail);
        resp.write_head(200, trail);
        resp.end();
        return;
    }

    // Create and start per-call state.
    auto state = std::make_shared<ServerCallState>(
        ioc_, req, resp, it->second,
        opts_.max_request_message_size,
        opts_.max_metadata_size);
    state->Start();
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
