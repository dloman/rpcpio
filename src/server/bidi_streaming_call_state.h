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
#include "rpcpio/internal/raw_server_reader.h"
#include "rpcpio/internal/raw_server_writer.h"
#include "src/server/client_streaming_call_state.h"  // StreamingFrameParser
#include "src/server/raw_server_impls.h"

namespace rpcpio::internal {

// Type-erased handler for a bidi-streaming RPC.
// Receives: context, reader handle, writer handle.
// Returns: final Status.
using RawBidiStreamingHandler = std::function<
    boost::asio::awaitable<Status>(
        ServerContext&, RawServerReader&, RawServerWriter&)>;

// ── BidiStreamingCallState ───────────────────────────────────────────────────
//
// Manages one inbound bidirectional-streaming call:
//   1. Parses metadata and starts the handler coroutine immediately.
//   2. As DATA chunks arrive (via on_data), decodes LPM messages and pushes
//      them into the reader queue for the handler to consume.
//   3. The handler interleaves Read() and Write() calls freely.
//   4. When the handler returns, ensures Finish() is called on the writer
//      to flush trailing headers.
class BidiStreamingCallState
    : public std::enable_shared_from_this<BidiStreamingCallState>
{
public:
    BidiStreamingCallState(
        boost::asio::io_context&                        ioc,
        const nghttp2::asio_http2::server::request&     req,
        const nghttp2::asio_http2::server::response&    resp,
        RawBidiStreamingHandler                         handler,
        std::size_t                                     max_message_size,
        std::size_t                                     max_metadata_size);

    // Register on_data callback, send initial HTTP 200, arm generator, and
    // start the handler coroutine.
    void Start();

private:
    void OnData(const uint8_t* data, std::size_t len);
    void SendError(Status status);
    void SendHeaders();

    boost::asio::io_context&                       ioc_;
    const nghttp2::asio_http2::server::request&    req_;
    const nghttp2::asio_http2::server::response&   resp_;
    RawBidiStreamingHandler                        handler_;
    std::size_t                                    max_message_size_;
    std::size_t                                    max_metadata_size_;

    StreamingFrameParser      parser_;   // multi-message LPM frame parser
    ServerContext             ctx_;
    boost::asio::steady_timer timer_;
    bool                      responded_{false};

    std::shared_ptr<RawServerReaderImpl> reader_impl_;
    std::shared_ptr<RawServerWriterImpl> writer_impl_;
};

} // namespace rpcpio::internal
