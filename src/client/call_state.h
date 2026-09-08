#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <nghttp2/asio_http2_client.h>
#include "rpcpio/client_context.h"
#include "rpcpio/internal/raw_result.h"
#include "src/protocol/compression.h"
#include "src/protocol/framing.h"
#include "src/protocol/metadata_codec.h"

namespace rpcpio::internal {

// Manages the complete lifecycle of one unary RPC from the client side.
// Created when a stream is submitted; destroyed once the completion fires.
// All methods must be called from the session's io_context executor.
class ClientCallState : public std::enable_shared_from_this<ClientCallState> {
public:
    using CompletionCb = std::function<void(UnaryResultRaw)>;

    ClientCallState(boost::asio::io_context& ioc,
                    ClientContext*           ctx,
                    CompletionCb             cb);

    // Called by ChannelImpl with the response object returned by session.submit().
    void Attach(const nghttp2::asio_http2::client::response& resp);

    // Arm the deadline timer (call once, after Attach).
    void ArmTimer();

    // Externally-triggered cancellation (e.g. ClientContext::Cancel()).
    void Cancel();

    // Called by ChannelImpl's on_close lambda when the stream closes.
    void OnStreamClose(uint32_t error_code);

    // Complete with an error status (called externally, e.g. on submit failure).
    void Fail(Status status);

private:
    // Internal single-fire completion gate; posts cb to ioc_.
    void Complete(UnaryResultRaw result);

    boost::asio::io_context&    ioc_;
    ClientContext*              ctx_;
    CompletionCb                completion_;
    boost::asio::steady_timer   timer_;

    protocol::FrameDecoder           decoder_;
    UnaryResultRaw                   result_;
    std::optional<protocol::Encoding> response_encoding_;  // from grpc-encoding header

    std::atomic<bool>           completed_{false};
    bool                        initial_meta_done_{false};
    bool                        trailers_done_{false};
};

} // namespace rpcpio::internal
