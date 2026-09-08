#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <google/protobuf/message.h>
#include "rpcpio/client_context.h"
#include "rpcpio/internal/raw_result.h"
#include "rpcpio/internal/raw_client_reader.h"
#include "rpcpio/internal/raw_client_writer.h"
#include "rpcpio/status.h"
#include "rpcpio/unary_method.h"
#include "rpcpio/streaming_method.h"
#include "rpcpio/client_reader.h"
#include "rpcpio/client_writer.h"
#include "rpcpio/bidi_stream.h"

namespace rpcpio {

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
    std::string user_agent{"rpcpio/0.1"};

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

    // Execute a server-streaming RPC (one request, many responses).
    template<typename Req, typename Resp>
    boost::asio::awaitable<ClientReader<Resp>>
    ServerStreamingCall(const ServerStreamingMethod<Req, Resp>& method,
                        ClientContext&                           ctx,
                        const Req&                               req) {
        std::string req_bytes;
        if (!req.SerializeToString(&req_bytes)) {
            // Serialization error: send an empty body — the server will reject it.
            // An empty req_bytes still produces a valid 5-byte LPM frame (length=0).
            req_bytes = "";
        }
        internal::RawClientReader raw =
            co_await ServerStreamingCallRaw(method.path, ctx, req_bytes);
        co_return ClientReader<Resp>(std::move(raw));
    }

    // Execute a client-streaming RPC (many requests, one response).
    template<typename Req, typename Resp>
    boost::asio::awaitable<ClientWriter<Req, Resp>>
    ClientStreamingCall(const ClientStreamingMethod<Req, Resp>& method,
                        ClientContext&                           ctx) {
        internal::RawClientWriter raw =
            co_await ClientStreamingCallRaw(method.path, ctx);
        co_return ClientWriter<Req, Resp>(std::move(raw));
    }

    // Execute a bidi-streaming RPC (many requests, many responses).
    template<typename Req, typename Resp>
    boost::asio::awaitable<BidiStream<Req, Resp>>
    BidiStreamingCall(const BidiStreamingMethod<Req, Resp>& method,
                      ClientContext&                         ctx) {
        RawBidiHandles bh = co_await BidiStreamingCallRaw(method.path, ctx);
        co_return BidiStream<Req, Resp>(std::move(bh.reader), std::move(bh.writer));
    }

    Channel(const Channel&)            = delete;
    Channel& operator=(const Channel&) = delete;

private:
    // Pair of raw handles returned by BidiStreamingCallRaw.
    struct RawBidiHandles {
        internal::RawClientReader reader;
        internal::RawClientWriter writer;
    };

    // Type-erased network calls; implemented in channel_impl.cc.
    boost::asio::awaitable<internal::UnaryResultRaw>
    UnaryCallRaw(std::string_view path,
                 ClientContext&   ctx,
                 std::string_view request_bytes);

    boost::asio::awaitable<internal::RawClientReader>
    ServerStreamingCallRaw(std::string_view path,
                           ClientContext&   ctx,
                           std::string_view request_bytes);

    boost::asio::awaitable<internal::RawClientWriter>
    ClientStreamingCallRaw(std::string_view path,
                           ClientContext&   ctx);

    boost::asio::awaitable<RawBidiHandles>
    BidiStreamingCallRaw(std::string_view path,
                         ClientContext&   ctx);

    std::shared_ptr<internal::ChannelImpl> impl_;
};

} // namespace rpcpio
