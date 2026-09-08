#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <nghttp2/asio_http2_server.h>
#include "rpcpio/server_context.h"
#include "rpcpio/status.h"
#include "rpcpio/internal/raw_server_writer.h"
#include "src/protocol/framing.h"
#include "src/server/raw_server_impls.h"

namespace rpcpio::internal {

// Type-erased handler for a server-streaming RPC.
// Receives: context, serialized request bytes, write handle.
// Returns: final Status (used only when the handler forgets to call Finish).
using RawServerStreamingHandler = std::function<
    boost::asio::awaitable<Status>(
        ServerContext&, std::string_view, RawServerWriter&)>;

// ── ServerStreamingCallState ─────────────────────────────────────────────────
//
// Manages one inbound server-streaming call:
//   1. Accumulates the single request message.
//   2. Sends HTTP 200 + initial metadata.
//   3. Arms an nghttp2 generator callback that drains the LPM frame queue.
//   4. Co-spawns the handler coroutine with a RawServerWriter handle.
//   5. On handler return (or exception), ensures Finish() is called.
class ServerStreamingCallState
    : public std::enable_shared_from_this<ServerStreamingCallState>
{
public:
    ServerStreamingCallState(
        boost::asio::io_context&                        ioc,
        const nghttp2::asio_http2::server::request&     req,
        const nghttp2::asio_http2::server::response&    resp,
        RawServerStreamingHandler                       handler,
        std::size_t                                     max_request_size,
        std::size_t                                     max_metadata_size);

    // Register on_data callback and begin receiving the request body.
    void Start();

private:
    void OnData(const uint8_t* data, std::size_t len);
    void OnRequestEnd();
    void SendError(Status status);

    // Send HTTP 200 + initial headers and arm the generator callback.
    void SendHeaders();

    boost::asio::io_context&                       ioc_;
    const nghttp2::asio_http2::server::request&    req_;
    const nghttp2::asio_http2::server::response&   resp_;
    RawServerStreamingHandler                      handler_;
    std::size_t                                    max_request_size_;
    std::size_t                                    max_metadata_size_;

    protocol::FrameDecoder    decoder_;
    ServerContext             ctx_;
    boost::asio::steady_timer timer_;
    bool                      responded_{false};

    // Shared with RawServerWriter handed to the handler coroutine.
    std::shared_ptr<RawServerWriterImpl> writer_impl_;
};

} // namespace rpcpio::internal
