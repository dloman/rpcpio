#pragma once

#include <atomic>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl/context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <nghttp2/asio_http2_client.h>
#include "asio_grpc/channel.h"
#include "asio_grpc/client_context.h"
#include "asio_grpc/internal/raw_result.h"

namespace asio_grpc::internal {

class ClientCallState;

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
                    std::function<void(UnaryResultRaw)>      completion);

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
        ClientContext*                     ctx;
        std::string                        request_bytes;
        std::function<void(UnaryResultRaw)> completion;
    };

    boost::asio::io_context&              ioc_;
    std::string                           host_;
    std::uint16_t                         port_;
    ChannelOptions                        opts_;

    ConnState     state_{ConnState::kIdle};
    std::deque<PendingCall> pending_calls_;

    // nghttp2-asio client session (created on demand; null until connected)
    std::shared_ptr<nghttp2::asio_http2::client::session> session_;

    // Callbacks waiting for connection to be established.
    std::vector<std::function<void(boost::system::error_code)>> connect_waiters_;

    // Tracks submitted but not yet closed streams: stream_id → call state.
    // Used by OnGoaway to fail calls the peer did not process.
    std::map<std::int32_t, std::shared_ptr<ClientCallState>> active_calls_;
};

} // namespace asio_grpc::internal
