#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>
#include <boost/asio/io_context.hpp>
#include <nghttp2/asio_http2_server.h>
#include "asio_grpc/server.h"
#include "src/server/call_state.h"

namespace asio_grpc::internal {

class ServerImpl {
public:
    ServerImpl(boost::asio::io_context& ioc, ServerOptions opts);
    ~ServerImpl();

    // Register a handler for an exact RPC path.  Not thread-safe after Start().
    void RegisterUnaryRaw(std::string_view path, RawHandler handler);

    // Bind and start accepting connections.  If port is 0 the OS picks an
    // ephemeral port; call bound_port() afterward to discover it.
    void Start(std::string host, std::uint16_t port);

    // Returns the port the server is actually listening on.
    // Valid only after a successful Start() call.
    std::uint16_t bound_port() const noexcept { return bound_port_; }

    // Signal graceful shutdown.
    void Shutdown();

    // Block until all worker threads exit.
    void Wait();

private:
    void HandleRequest(const nghttp2::asio_http2::server::request&  req,
                       const nghttp2::asio_http2::server::response& resp);

    // Resolve an ephemeral port (port == 0) by briefly binding a TCP acceptor.
    std::uint16_t ResolvePort(std::uint16_t port);

    boost::asio::io_context&  ioc_;
    ServerOptions             opts_;
    nghttp2::asio_http2::server::http2 http2_;
    std::unordered_map<std::string, RawHandler> handlers_;
    std::vector<std::thread>  threads_;
    std::atomic<bool>         shutdown_{false};
    std::uint16_t             bound_port_{0};
};

} // namespace asio_grpc::internal
