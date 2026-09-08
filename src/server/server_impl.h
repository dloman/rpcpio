#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ssl/context.hpp>
#include <nghttp2/asio_http2_server.h>
#include "rpcpio/server.h"
#include "rpcpio/server_context.h"
#include "rpcpio/status.h"
#include "rpcpio/unary_server_reply.h"
#include "rpcpio/internal/raw_server_reader.h"
#include "rpcpio/internal/raw_server_writer.h"
#include "src/server/call_state.h"
#include "src/server/server_streaming_call_state.h"
#include "src/server/client_streaming_call_state.h"
#include "src/server/bidi_streaming_call_state.h"

namespace rpcpio::internal {

class ServerImpl {
public:
    ServerImpl(boost::asio::io_context& ioc, ServerOptions opts);
    ~ServerImpl();

    void RegisterUnaryRaw(std::string_view path, RawHandler handler);
    void RegisterUnaryCallback(std::string_view path, UnaryCallbackHandler handler);
    void RegisterServerStreamingRaw(std::string_view path,
                                     RawServerStreamingHandler handler);
    void RegisterClientStreamingRaw(std::string_view path,
                                     RawClientStreamingHandler handler);
    void RegisterBidiRaw(std::string_view path,
                          RawBidiStreamingHandler handler);

    void Start(std::string host, std::uint16_t port);
    std::uint16_t bound_port() const noexcept { return bound_port_; }

    void Shutdown();
    void Wait();

    void TrackCall(const std::shared_ptr<ServerCallState>& call);
    void UntrackCall(const ServerCallState* call);

private:
    void HandleRequest(const nghttp2::asio_http2::server::request&  req,
                       const nghttp2::asio_http2::server::response& resp);

    std::uint16_t ResolvePort(std::uint16_t port);
    void CancelActiveCalls();

    boost::asio::io_context&  ioc_;
    ServerOptions             opts_;
    std::unique_ptr<boost::asio::ssl::context> ssl_context_;
    nghttp2::asio_http2::server::http2 http2_;

    std::unordered_map<std::string, RawHandler>                handlers_;
    std::unordered_map<std::string, UnaryCallbackHandler>      callback_handlers_;
    std::unordered_map<std::string, RawServerStreamingHandler> server_streaming_handlers_;
    std::unordered_map<std::string, RawClientStreamingHandler> client_streaming_handlers_;
    std::unordered_map<std::string, RawBidiStreamingHandler>   bidi_handlers_;

    std::mutex                calls_mutex_;
    std::vector<std::weak_ptr<ServerCallState>> active_calls_;

    std::atomic<bool>         shutdown_{false};
    std::uint16_t             bound_port_{0};
};

} // namespace rpcpio::internal
