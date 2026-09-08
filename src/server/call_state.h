#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <nghttp2/asio_http2_server.h>
#include "rpcpio/server.h"
#include "rpcpio/server_context.h"
#include "rpcpio/status.h"
#include "rpcpio/unary_server_reply.h"
#include "src/protocol/compression.h"
#include "src/protocol/framing.h"

namespace rpcpio::internal {

using RawHandler = std::function<
    boost::asio::awaitable<Status>(
        ServerContext&, std::string_view, std::string&)>;

class ServerCallState : public std::enable_shared_from_this<ServerCallState> {
public:
    ServerCallState(boost::asio::io_context&              ioc,
                    ServerImpl*                           server,
                    const nghttp2::asio_http2::server::request&  req,
                    const nghttp2::asio_http2::server::response& resp,
                    RawHandler                            coroutine_handler,
                    std::size_t                           max_request_size,
                    std::size_t                           max_metadata_size);

    ServerCallState(boost::asio::io_context&              ioc,
                    ServerImpl*                           server,
                    const nghttp2::asio_http2::server::request&  req,
                    const nghttp2::asio_http2::server::response& resp,
                    UnaryCallbackHandler                  callback_handler,
                    std::size_t                           max_request_size,
                    std::size_t                           max_metadata_size);

    void Start();

    // Exactly-once reply entry point (any executor).
    void FinishFromReply(Status              status,
                         std::string         response_bytes,
                         MetadataMap         initial_metadata,
                         MetadataMap         trailing_metadata);

    [[nodiscard]] bool finished() const noexcept {
        return responded_.load(std::memory_order_acquire);
    }

    // Server shutdown or peer closure before a reply was sent.
    void CancelDueToShutdown(Status status);
    void CancelDueToPeerClose();

private:
    void OnData(const uint8_t* data, std::size_t len);
    void OnRequestEnd();
    void DispatchCoroutineHandler(std::string req_bytes);
    void DispatchCallbackHandler(std::string req_bytes);

    bool TryMarkResponded();
    void SendError(Status status);
    void SendResponse(const Status& status, std::string_view resp_bytes,
                      const MetadataMap& initial_meta,
                      const MetadataMap& trailing_meta);

    boost::asio::io_context&                       ioc_;
    ServerImpl*                                    server_;
    const nghttp2::asio_http2::server::request&    req_;
    const nghttp2::asio_http2::server::response&   resp_;
    RawHandler                                     coroutine_handler_;
    UnaryCallbackHandler                           callback_handler_;
    std::size_t                                    max_request_size_;
    std::size_t                                    max_metadata_size_;

    protocol::FrameDecoder           decoder_;
    std::optional<protocol::Encoding> request_encoding_;
    ServerContext                    ctx_;
    boost::asio::steady_timer        timer_;
    std::atomic<bool>                responded_{false};
    std::atomic<bool>                closed_{false};
};

} // namespace rpcpio::internal
