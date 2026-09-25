#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/ssl/context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <nghttp2/asio_http2_server.h>
#include "rpcpio/server.h"
#include "rpcpio/server_context.h"
#include "rpcpio/status.h"
#include "rpcpio/internal/raw_server_reader.h"
#include "rpcpio/internal/raw_server_writer.h"
#include "src/server/call_state.h"
#include "src/server/server_streaming_call_state.h"
#include "src/server/client_streaming_call_state.h"
#include "src/server/bidi_streaming_call_state.h"
#include "src/server/server_call_state_base.h"

namespace rpcpio::internal {

class ServerImpl : public std::enable_shared_from_this<ServerImpl> {
public:
    ServerImpl(boost::asio::io_context& ioc, ServerOptions opts);
    ~ServerImpl();

    // Register a handler for an exact RPC path.  Not thread-safe after Start().
    void RegisterUnaryRaw(std::string_view path, RawHandler handler);
    void RegisterServerStreamingRaw(std::string_view path,
                                     RawServerStreamingHandler handler);
    void RegisterClientStreamingRaw(std::string_view path,
                                     RawClientStreamingHandler handler);
    void RegisterBidiRaw(std::string_view path,
                          RawBidiStreamingHandler handler);

    // Bind and start accepting connections. Returns non-OK on port conflict or
    // TLS configuration errors; never throws. Pass port=0 for ephemeral.
    rpcpio::Status Start(std::string host, std::uint16_t port);

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
    void TrackCall(const std::shared_ptr<ServerCallStateBase>& call);
    void FinishShutdown(std::chrono::steady_clock::time_point deadline);
    void StopTransport();

    // Resolve an ephemeral port (port == 0) by briefly binding a TCP acceptor.
    std::uint16_t ResolvePort(std::uint16_t port);

    std::unique_ptr<boost::asio::io_context> owned_ioc_;
    boost::asio::io_context&  ioc_;
    std::optional<boost::asio::executor_work_guard<
        boost::asio::io_context::executor_type>> work_guard_;
    ServerOptions             opts_;
    nghttp2::asio_http2::server::http2 http2_;
    boost::asio::steady_timer shutdown_timer_;
    // nghttp2-asio retains a reference for future accepted connections.
    std::shared_ptr<boost::asio::ssl::context> ssl_context_;

    // One map per RPC kind.
    std::unordered_map<std::string, RawHandler>                handlers_;
    std::unordered_map<std::string, RawServerStreamingHandler> server_streaming_handlers_;
    std::unordered_map<std::string, RawClientStreamingHandler> client_streaming_handlers_;
    std::unordered_map<std::string, RawBidiStreamingHandler>   bidi_handlers_;

    std::vector<std::thread>  threads_;
    std::mutex                threads_mutex_;
    std::mutex                active_calls_mutex_;
    std::vector<std::weak_ptr<ServerCallStateBase>> active_calls_;
    std::atomic<bool>         shutdown_{false};
    std::atomic<bool>         started_{false};
    // nghttp2's server::stop() dereferences state that only listen_and_serve creates.
    std::atomic<bool>         listening_{false};
    std::uint16_t             bound_port_{0};
};

} // namespace rpcpio::internal
