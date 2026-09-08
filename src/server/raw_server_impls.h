#pragma once

// Internal header that provides the concrete definitions of RawServerReaderImpl
// and RawServerWriterImpl.  These structs are forward-declared in the public
// raw_server_reader.h / raw_server_writer.h headers; the call state TUs
// include this file to access their members.
//
// Keeping the definitions here (rather than in the individual call_state.h
// files) avoids circular include chains when bidi_streaming_call_state.h needs
// both impl types.

#include <cstddef>
#include <deque>
#include <string>
#include <string_view>
#include <boost/asio/io_context.hpp>
#include <nghttp2/asio_http2_server.h>
#include "rpcpio/status.h"
#include "src/protocol/stream_queue.h"

namespace rpcpio::internal {

// ── RawServerReaderImpl ──────────────────────────────────────────────────────
// Used by client-streaming and bidi-streaming call states.
// Wraps a StreamMessageQueue fed by the nghttp2 DATA callbacks.
struct RawServerReaderImpl {
    explicit RawServerReaderImpl(boost::asio::io_context& ioc) : queue_(ioc) {}
    StreamMessageQueue queue_;
};

// ── RawServerWriterImpl ──────────────────────────────────────────────────────
// Used by server-streaming and bidi-streaming call states.
// Holds the send queue and trailing headers; the nghttp2 generator drains it.
struct RawServerWriterImpl {
    explicit RawServerWriterImpl(
        const nghttp2::asio_http2::server::response& resp)
        : resp_(resp)
    {}

    const nghttp2::asio_http2::server::response& resp_;

    // LPM-encoded frames waiting to be consumed by the generator callback.
    std::deque<std::string> pending_;

    // Byte offset into pending_.front() already consumed by the generator.
    std::size_t offset_{0};

    // Set to true once Finish() is called; no more Write() calls allowed after.
    bool finished_{false};

    // Trailing headers built by Finish().
    nghttp2::asio_http2::header_map trail_hdrs_;

    // Encode proto_bytes as an LPM frame, enqueue it, and wake the generator.
    Status Write(std::string_view proto_bytes);

    // Build trailing headers from status, mark finished_, and wake the generator.
    void Finish(Status status);
};

} // namespace rpcpio::internal
