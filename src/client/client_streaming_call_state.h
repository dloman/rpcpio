#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <nghttp2/asio_http2_client.h>
#include "rpcpio/client_context.h"
#include "rpcpio/status.h"
#include "rpcpio/internal/raw_client_writer.h"
#include "src/client/raw_client_impls.h"

namespace rpcpio::internal {

// Manages the client side of a client-streaming RPC:
//   - Submits the HTTP/2 request with a generator callback (no body initially).
//   - Write() LPM-encodes messages, enqueues them, and wakes the generator.
//   - WritesDone() sets END_STREAM on the generator and wakes it.
//   - The server sends one response message + trailing status.
//   - Finish() waits for the final grpc-status from on_trailers.
class ClientStreamingClientCallState
    : public std::enable_shared_from_this<ClientStreamingClientCallState>
{
public:
    ClientStreamingClientCallState(boost::asio::io_context& ioc,
                                   ClientContext*           ctx);

    // Called by ChannelImpl after session_->submit() returns the request and
    // response objects.  Sets req_ on writer_impl_ and registers callbacks.
    void Attach(const nghttp2::asio_http2::client::request* req,
                const nghttp2::asio_http2::client::response&          resp);

    // Arm the deadline timer (call once, after Attach).
    void ArmTimer();

    // Externally-triggered cancellation.
    void Cancel();

    // Called by ChannelImpl's on_close lambda when the stream closes.
    void OnStreamClose(uint32_t error_code);

    // Complete with an error status (GOAWAY, submit failure, etc.).
    void Fail(Status s);

    // Returns the RawClientWriter handle backed by writer_impl_.
    // May only be called once.
    RawClientWriter TakeWriter();

    std::shared_ptr<RawClientWriterImpl> writer_impl_;

private:
    void MaybeDeliverStatus(Status s);

    boost::asio::io_context&  ioc_;
    ClientContext*             ctx_;
    boost::asio::steady_timer  timer_;

    // Incremental single-message LPM parser for the server's response.
    std::array<uint8_t, 5>    hdr_buf_{};
    std::size_t                hdr_bytes_{0};
    uint32_t                   payload_len_{0};
    std::string                payload_buf_;
    bool                       hdr_done_{false};

    std::atomic<bool>          closed_{false};
    bool                       trailers_done_{false};
};

} // namespace rpcpio::internal
