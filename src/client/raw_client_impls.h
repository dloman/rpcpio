#pragma once

#include <deque>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
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
    bool                      status_ready_{false};
    boost::asio::steady_timer status_timer_;  // woken when on_trailers fires

    void SetFinalStatus(Status s) {
        final_status_  = std::move(s);
        status_ready_  = true;
        status_timer_.cancel();
    }
};

// ── RawClientWriterImpl ──────────────────────────────────────────────────────
// Used by client-streaming and bidi client call states.
// Holds the LPM-frame queue; a generator callback drains it.
// req_ is set by the call state after session_->submit() succeeds.
struct RawClientWriterImpl {
    explicit RawClientWriterImpl(boost::asio::io_context& ioc)
        : status_timer_(ioc, std::chrono::steady_clock::time_point::max())
    {}

    // Set by the call state after submit() returns.
    std::shared_ptr<nghttp2::asio_http2::client::request> req_;

    std::deque<std::string> pending_;   // LPM-encoded frames to send
    std::size_t             offset_{0}; // byte offset into pending_.front()
    bool                    writes_done_{false};

    // Single response message from the server (client-streaming RPC).
    std::string response_bytes_;

    // Final grpc-status from server trailing headers.
    Status                    final_status_;
    bool                      status_ready_{false};
    boost::asio::steady_timer status_timer_;

    void SetFinalStatus(Status s) {
        final_status_  = std::move(s);
        status_ready_  = true;
        status_timer_.cancel();
    }

    // Encode proto_bytes as an LPM frame, enqueue it, and wake the generator.
    // Returns non-OK if the message is too large to frame.
    Status Enqueue(std::string_view proto_bytes);
};

} // namespace rpcpio::internal
