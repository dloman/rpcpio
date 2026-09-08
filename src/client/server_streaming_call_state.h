#pragma once

#include <atomic>
#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <nghttp2/asio_http2_client.h>
#include "rpcpio/client_context.h"
#include "rpcpio/status.h"
#include "rpcpio/internal/raw_client_reader.h"
#include "src/client/raw_client_impls.h"

namespace rpcpio::internal {

// Manages the client side of a server-streaming RPC:
//   - Submits a single-message request (same as unary).
//   - Feeds incoming DATA frames into a StreamMessageQueue via incremental LPM
//     decoding (supports multi-message streams).
//   - Populates the final status from the on_trailers callback.
class ServerStreamingClientCallState
    : public std::enable_shared_from_this<ServerStreamingClientCallState>
{
public:
    ServerStreamingClientCallState(boost::asio::io_context& ioc,
                                   ClientContext*           ctx);

    // Called by ChannelImpl after submit() returns, with the server response.
    // Registers on_data and on_trailers callbacks.
    void Attach(const nghttp2::asio_http2::client::response& resp);

    // Arm the deadline timer (call once, after Attach).
    void ArmTimer();

    // Externally-triggered cancellation.
    void Cancel();

    // Called by ChannelImpl's on_close lambda when the stream closes.
    void OnStreamClose(uint32_t error_code);

    // Complete with an error status (called on submit failure, GOAWAY, etc.).
    void Fail(Status s);

    // Returns the RawClientReader handle backed by reader_impl_.
    // May only be called once (after Attach).
    RawClientReader TakeReader();

    std::shared_ptr<RawClientReaderImpl> reader_impl_;

private:
    void MaybeDeliverStatus(Status s);

    boost::asio::io_context&  ioc_;
    ClientContext*             ctx_;
    boost::asio::steady_timer  timer_;

    // Incremental LPM frame parser state (shared with on_data callback).
    std::array<uint8_t, 5>    hdr_buf_{};
    std::size_t                hdr_bytes_{0};
    uint32_t                   payload_len_{0};
    std::string                payload_buf_;
    bool                       hdr_done_{false};  // true once 5-byte header is read

    std::atomic<bool>          closed_{false};
    bool                       trailers_done_{false};
};

} // namespace rpcpio::internal
