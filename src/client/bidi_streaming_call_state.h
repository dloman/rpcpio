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
#include "rpcpio/internal/raw_client_writer.h"
#include "src/client/raw_client_impls.h"

namespace rpcpio::internal {

// Manages the client side of a bidirectional-streaming RPC:
//   - Submits the HTTP/2 request with a generator callback for sends.
//   - Write() LPM-encodes messages, enqueues them, and wakes the generator.
//   - WritesDone() half-closes the send side.
//   - Receives multiple DATA frames decoded into msg_queue_ for Read().
//   - Populates the final status from the on_trailers callback for Finish().
class BidiStreamingClientCallState
    : public std::enable_shared_from_this<BidiStreamingClientCallState>
{
public:
    BidiStreamingClientCallState(boost::asio::io_context& ioc,
                                 ClientContext*           ctx);

    // Called by ChannelImpl after submit() returns the request and response.
    // Sets req_ on writer_impl_, registers on_data and on_trailers.
    void Attach(std::shared_ptr<nghttp2::asio_http2::client::request> req,
                const nghttp2::asio_http2::client::response&          resp);

    // Arm the deadline timer (call once, after Attach).
    void ArmTimer();

    // Externally-triggered cancellation.
    void Cancel();

    // Called by ChannelImpl's on_close lambda when the stream closes.
    void OnStreamClose(uint32_t error_code);

    // Complete with an error status.
    void Fail(Status s);

    // Returns the read handle backed by reader_impl_.
    RawClientReader TakeReader();

    // Returns the write handle backed by writer_impl_.
    RawClientWriter TakeWriter();

    std::shared_ptr<RawClientReaderImpl> reader_impl_;
    std::shared_ptr<RawClientWriterImpl> writer_impl_;

private:
    void MaybeDeliverStatus(Status s);

    boost::asio::io_context&  ioc_;
    ClientContext*             ctx_;
    boost::asio::steady_timer  timer_;

    // Incremental LPM parser for incoming server messages.
    std::array<uint8_t, 5>    hdr_buf_{};
    std::size_t                hdr_bytes_{0};
    uint32_t                   payload_len_{0};
    std::string                payload_buf_;
    bool                       hdr_done_{false};

    std::atomic<bool>          closed_{false};
    bool                       trailers_done_{false};
};

} // namespace rpcpio::internal
