#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <google/protobuf/message.h>
#include "asio_grpc/client_context.h"
#include "asio_grpc/internal/raw_result.h"
#include "asio_grpc/status.h"
#include "asio_grpc/unary_method.h"

namespace asio_grpc {

namespace internal { class ChannelImpl; }

struct ChannelOptions {
    // TLS (default: verify peer certificate + hostname)
    bool        use_tls{true};
    bool        verify_peer{true};     // false only in tests
    std::string ca_cert_file;          // PEM bundle; empty = system store
    std::string client_cert_file;      // optional mTLS
    std::string client_key_file;

    // Plaintext HTTP/2 prior-knowledge (requires use_tls = false)
    bool use_h2c{false};

    // Timeouts and identification
    std::chrono::milliseconds connect_timeout{5000};
    std::string user_agent{"asio-grpc/0.1"};

    // Per-call size limits (bytes)
    std::size_t max_receive_message_size{4 * 1024 * 1024};
    std::size_t max_send_message_size{4 * 1024 * 1024};
};

class Channel {
public:
    // host: hostname or IP; port: 443 default for TLS, 80 for h2c
    Channel(boost::asio::io_context& ioc,
            std::string              host,
            std::uint16_t            port,
            ChannelOptions           opts = {});
    ~Channel();

    // Proactively establish the connection (optional; first UnaryCall connects lazily).
    boost::asio::awaitable<void> Connect();

    // Execute one unary RPC.
    // Req and Resp must derive from google::protobuf::Message.
    template<typename Req, typename Resp>
    boost::asio::awaitable<UnaryResult<Resp>>
    UnaryCall(const UnaryMethod<Req, Resp>& method,
              ClientContext&                ctx,
              const Req&                    req) {
        // Serialize request.
        std::string req_bytes;
        if (!req.SerializeToString(&req_bytes)) {
            co_return UnaryResult<Resp>{
                Status{StatusCode::INTERNAL, "request serialization failed"}};
        }

        // Type-erased wire call.
        internal::UnaryResultRaw raw =
            co_await UnaryCallRaw(method.path, ctx, req_bytes);

        // Assemble typed result.
        UnaryResult<Resp> result;
        result.status            = raw.status;
        result.initial_metadata  = std::move(raw.initial_metadata);
        result.trailing_metadata = std::move(raw.trailing_metadata);

        if (raw.status.ok() && !raw.response_bytes.empty()) {
            result.response.emplace();
            if (!result.response->ParseFromString(raw.response_bytes)) {
                result.status = Status{StatusCode::INTERNAL,
                                       "response deserialization failed"};
                result.response.reset();
            }
        } else if (raw.status.ok() && raw.response_bytes.empty()) {
            // gRPC requires exactly one response message for OK unary calls.
            result.status = Status{StatusCode::INTERNAL, "missing response message"};
        }
        co_return result;
    }

    Channel(const Channel&)            = delete;
    Channel& operator=(const Channel&) = delete;

private:
    // Type-erased network call; implemented in channel_impl.cc.
    boost::asio::awaitable<internal::UnaryResultRaw>
    UnaryCallRaw(std::string_view path,
                 ClientContext&   ctx,
                 std::string_view request_bytes);

    std::shared_ptr<internal::ChannelImpl> impl_;
};

} // namespace asio_grpc
