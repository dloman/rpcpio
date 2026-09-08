#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <boost/asio/async_result.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <google/protobuf/message.h>
#include "rpcpio/client_context.h"
#include "rpcpio/unary_result_raw.h"
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
    bool        use_tls{true};
    bool        verify_peer{true};
    std::string ca_cert_file;
    std::string client_cert_file;
    std::string client_key_file;

    bool use_h2c{false};

    std::chrono::milliseconds connect_timeout{5000};
    std::string user_agent{"rpcpio/0.1"};

    std::size_t max_receive_message_size{4 * 1024 * 1024};
    std::size_t max_send_message_size{4 * 1024 * 1024};
};

class Channel {
public:
    Channel(boost::asio::io_context& ioc,
            std::string              host,
            std::uint16_t            port,
            ChannelOptions           opts = {});
    ~Channel();

    boost::asio::awaitable<void> Connect();

    // Completion-token unary call on raw protobuf bytes (Atlas / yield_context).
    // |ctx| must outlive the operation until the completion handler runs.
    // Request bytes are copied; honors ClientContext deadline/cancellation and
    // associated completion-token cancellation slots (including while connecting).
    template<typename CompletionToken>
    auto AsyncUnaryCallRaw(std::string_view path,
                           ClientContext&   ctx,
                           std::string_view request_bytes,
                           CompletionToken&& token);

    template<typename Req, typename Resp>
    boost::asio::awaitable<UnaryResult<Resp>>
    UnaryCall(const UnaryMethod<Req, Resp>& method,
              ClientContext&                ctx,
              const Req&                    req) {
        std::string req_bytes;
        if (!req.SerializeToString(&req_bytes)) {
            co_return UnaryResult<Resp>{
                Status{StatusCode::INTERNAL, "request serialization failed"}};
        }

        UnaryResultRaw raw =
            co_await AsyncUnaryCallRaw(method.path, ctx, req_bytes,
                                       boost::asio::use_awaitable);

        UnaryResult<Resp> result;
        result.status            = raw.status;
        result.initial_metadata  = std::move(raw.initial_metadata);
        result.trailing_metadata = std::move(raw.trailing_metadata);

        if (raw.status.ok() && raw.has_response) {
            result.response.emplace();
            if (!result.response->ParseFromString(raw.response_bytes)) {
                result.status = Status{StatusCode::INTERNAL,
                                       "response deserialization failed"};
                result.response.reset();
            }
        } else if (raw.status.ok()) {
            result.status = Status{StatusCode::INTERNAL, "missing response message"};
        }
        co_return result;
    }

    template<typename Req, typename Resp>
    boost::asio::awaitable<ClientReader<Resp>>
    ServerStreamingCall(const ServerStreamingMethod<Req, Resp>& method,
                        ClientContext&                           ctx,
                        const Req&                               req) {
        std::string req_bytes;
        if (!req.SerializeToString(&req_bytes)) {
            req_bytes = "";
        }
        internal::RawClientReader raw =
            co_await ServerStreamingCallRaw(method.path, ctx, req_bytes);
        co_return ClientReader<Resp>(std::move(raw));
    }

    template<typename Req, typename Resp>
    boost::asio::awaitable<ClientWriter<Req, Resp>>
    ClientStreamingCall(const ClientStreamingMethod<Req, Resp>& method,
                        ClientContext&                           ctx) {
        internal::RawClientWriter raw =
            co_await ClientStreamingCallRaw(method.path, ctx);
        co_return ClientWriter<Req, Resp>(std::move(raw));
    }

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
    void AsyncUnaryCallRawImpl(
        std::string path,
        ClientContext* ctx,
        std::string request_bytes,
        std::function<void(UnaryResultRaw)> completion,
        boost::asio::cancellation_slot cancellation_slot);

    struct RawBidiHandles {
        internal::RawClientReader reader;
        internal::RawClientWriter writer;
    };

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

template<typename CompletionToken>
auto Channel::AsyncUnaryCallRaw(std::string_view path,
                                ClientContext&   ctx,
                                std::string_view request_bytes,
                                CompletionToken&& token) {
    return boost::asio::async_initiate<
        CompletionToken,
        void(UnaryResultRaw)>(
        [this, path, &ctx, request_bytes](auto handler) mutable {
            const boost::asio::cancellation_slot cancellation_slot =
                boost::asio::get_associated_cancellation_slot(handler);
            auto shared_handler =
                std::make_shared<std::decay_t<decltype(handler)>>(
                    std::move(handler));
            AsyncUnaryCallRawImpl(
                std::string(path),
                &ctx,
                std::string(request_bytes),
                [shared_handler](UnaryResultRaw result) mutable {
                    std::move(*shared_handler)(std::move(result));
                },
                cancellation_slot);
        },
        token);
}

} // namespace rpcpio
