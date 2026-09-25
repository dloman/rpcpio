#pragma once

#include <atomic>
#include <deque>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <nghttp2/asio_http2_client.h>
#include "rpcpio/status.h"
#include "src/protocol/stream_queue.h"

namespace rpcpio::internal {

// ── RawClientReaderImpl ──────────────────────────────────────────────────────
// Used by server-streaming and bidi client call states.
// msg_queue_ receives decoded proto bytes from on_data callbacks.
// final_status_ is populated by the on_trailers callback.
struct RawClientReaderImpl {
    explicit RawClientReaderImpl(boost::asio::io_context& ioc)
        : msg_queue_(ioc)
        , status_timer_(ioc, std::chrono::steady_clock::time_point::max())
    {}

    StreamMessageQueue        msg_queue_;
    Status                    final_status_;
    std::atomic<bool>         status_ready_{false};
    boost::asio::steady_timer status_timer_;  // woken when on_trailers fires

    void SetFinalStatus(Status s) {
        final_status_  = std::move(s);
        status_ready_.store(true, std::memory_order_release);
    }
};

// ── RawClientWriterImpl ──────────────────────────────────────────────────────
// Used by client-streaming and bidi client call states.
// Holds the LPM-frame queue; a generator callback drains it.
//
// The generator and request::resume() run inside the nghttp2 session, so
// req_, pending_, offset_ and writes_done_ are only touched on strand_.
struct RawClientWriterImpl
    : std::enable_shared_from_this<RawClientWriterImpl> {
    RawClientWriterImpl(
            boost::asio::io_context& ioc,
            boost::asio::any_io_executor strand)
        : strand_(std::move(strand))
        , status_timer_(ioc, std::chrono::steady_clock::time_point::max())
    {}

    boost::asio::any_io_executor strand_;

    // Set once submit() succeeds and cleared when the stream closes, because
    // nghttp2-asio frees the request at close.
    const nghttp2::asio_http2::client::request* req_{nullptr};

    std::deque<std::string> pending_;   // LPM-encoded frames to send
    std::size_t             offset_{0}; // byte offset into pending_.front()
    bool                    writes_done_{false};

    // Single response message from the server (client-streaming RPC).
    std::string response_bytes_;
    bool        has_response_ = false;

    // Final grpc-status from server trailing headers.
    Status                    final_status_;
    std::atomic<bool>         status_ready_{false};
    boost::asio::steady_timer status_timer_;

    void SetFinalStatus(Status s) {
        final_status_  = std::move(s);
        status_ready_.store(true, std::memory_order_release);
    }

    // Encode proto_bytes as an LPM frame, enqueue it, and wake the generator.
    // Returns non-OK if the message is too large to frame.
    Status Enqueue(std::string_view proto_bytes);

    // Mark the request stream half-closed once queued frames are sent.
    void MarkWritesDone();
};

} // namespace rpcpio::internal
