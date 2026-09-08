#pragma once

#include <chrono>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl/context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <nghttp2/asio_http2_client.h>
#include "rpcpio/channel.h"
#include "rpcpio/metadata.h"
#include "rpcpio/client_context.h"
#include "rpcpio/internal/raw_result.h"
#include "rpcpio/internal/raw_client_reader.h"
#include "rpcpio/internal/raw_client_writer.h"

namespace rpcpio::internal {

class ClientCallState;
class UnaryCallSubmission;
class ServerStreamingClientCallState;
class ClientStreamingClientCallState;
class BidiStreamingClientCallState;

// State of the shared connection.
enum class ConnState { kIdle, kConnecting, kReady, kFailed };

// ChannelImpl owns the nghttp2 client session and connection lifecycle.
// All public methods must be called from the associated io_context strand.
class ChannelImpl : public std::enable_shared_from_this<ChannelImpl> {
public:
    ChannelImpl(boost::asio::io_context& ioc,
                std::string              host,
                std::uint16_t            port,
                ChannelOptions           opts);

    ~ChannelImpl();

    // Initiate connection (idempotent; safe to call while already connecting).
    void Connect(std::function<void(boost::system::error_code)> cb);

    // Submit a unary call.  If the connection is not yet ready, queues the
    // call until connection completes (or fails).
    void SubmitCall(std::string                              path,
                    ClientContext*                           ctx,
                    std::string                              request_bytes,
                    std::function<void(UnaryResultRaw)>      completion,
                    boost::asio::cancellation_slot           handler_slot = {});

    // Submit a server-streaming call (single request, many responses).
    void SubmitServerStreamingCall(
        std::string                             path,
        ClientContext*                          ctx,
        std::string                             request_bytes,
        std::function<void(RawClientReader)>    completion);

    // Handles for a bidi (or client-streaming) call: both send and receive ends.
    struct BidiHandles {
        RawClientReader reader;
        RawClientWriter writer;
    };

    // Submit a client-streaming call (many requests, one response).
    void SubmitClientStreamingCall(
        std::string                             path,
        ClientContext*                          ctx,
        std::function<void(RawClientWriter)>    completion);

    // Submit a bidi-streaming call (many requests, many responses).
    void SubmitBidiStreamingCall(
        std::string                             path,
        ClientContext*                          ctx,
        std::function<void(BidiHandles)>        completion);

    boost::asio::io_context& ioc() noexcept { return ioc_; }
    const ChannelOptions&    opts() const noexcept { return opts_; }

private:
    void DoConnect();
    void OnConnected(boost::system::error_code ec);
    void DrainQueue(boost::system::error_code ec);
    void FailAll(Status status);

    // Called when peer sends GOAWAY.  Fails unaccepted streams
    // (stream_id > last_stream_id) immediately; accepted streams are left to
    // complete normally or be closed by the subsequent connection error.
    void OnGoaway(std::uint32_t error_code, std::int32_t last_stream_id);

    // Remove a stream from the active-call index when it closes.
    void RemoveActiveCall(std::int32_t stream_id);

    struct PendingCall {
        std::string                        path;
        std::string                        request_bytes;
        MetadataMap                        send_metadata;
        std::string                        compression_algorithm;
        std::optional<std::chrono::system_clock::time_point> deadline;
        std::shared_ptr<UnaryCallSubmission> submission;
    };

    void ErasePendingSubmission(const std::shared_ptr<UnaryCallSubmission>& submission);
    void SubmitCallReady(PendingCall pending);

    boost::asio::io_context&              ioc_;
    std::string                           host_;
    std::uint16_t                         port_;
    ChannelOptions                        opts_;

    ConnState     state_{ConnState::kIdle};
    std::deque<PendingCall> pending_calls_;

    // Generic pending lambdas for streaming calls (queued while connecting).
    std::deque<std::function<void()>> pending_generic_calls_;

    // nghttp2-asio client session (created on demand; null until connected)
    std::shared_ptr<nghttp2::asio_http2::client::session> session_;

    // Callbacks waiting for connection to be established.
    std::vector<std::function<void(boost::system::error_code)>> connect_waiters_;

    // Tracks submitted but not yet closed streams: stream_id → call state.
    // Used by OnGoaway to fail calls the peer did not process.
    std::map<std::int32_t, std::shared_ptr<ClientCallState>> active_calls_;

    // Fail callbacks for streaming calls (keyed by stream_id).
    // OnGoaway calls these for streams above last_stream_id.
    std::map<std::int32_t, std::function<void(Status)>> active_streaming_calls_;
};

} // namespace rpcpio::internal
