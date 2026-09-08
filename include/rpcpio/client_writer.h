#pragma once

#include <optional>
#include <string>
#include <boost/asio/awaitable.hpp>
#include <google/protobuf/message.h>
#include "rpcpio/status.h"
#include "rpcpio/internal/raw_client_writer.h"

namespace rpcpio {

// Typed, coroutine-friendly write handle for client-streaming RPCs.
//
// Call Write() any number of times to send request messages, then call
// WritesDone() to half-close the request stream, then call FinishAndGetResponse()
// to retrieve the single server response and final grpc-status.
template<typename Req, typename Resp>
class ClientWriter {
public:
    explicit ClientWriter(internal::RawClientWriter raw) noexcept
        : raw_(std::move(raw)) {}

    // Serialize msg and send it as the next gRPC LPM request frame.
    boost::asio::awaitable<Status> Write(const Req& msg) {
        std::string bytes;
        if (!msg.SerializeToString(&bytes)) {
            co_return Status{StatusCode::INTERNAL,
                             "ClientWriter: serialization failed"};
        }
        co_return co_await raw_.Write(bytes);
    }

    // Half-close the request stream.  The server may still send its response.
    boost::asio::awaitable<void> WritesDone() {
        co_await raw_.WritesDone();
    }

    // Wait for the final grpc-status and deserialize the server response.
    // Must be called exactly once, after WritesDone().
    boost::asio::awaitable<UnaryResult<Resp>> FinishAndGetResponse() {
        Status s = co_await raw_.Finish();
        UnaryResult<Resp> result;
        result.status = s;
        if (s.ok()) {
            const std::string& rb = raw_.response_bytes();
            if (rb.empty()) {
                result.status = Status{StatusCode::INTERNAL,
                                       "client-streaming: missing server response"};
            } else {
                result.response.emplace();
                if (!result.response->ParseFromString(rb)) {
                    result.status = Status{StatusCode::INTERNAL,
                                           "client-streaming: response parse failed"};
                    result.response.reset();
                }
            }
        }
        co_return result;
    }

    ClientWriter(ClientWriter&&)                 = default;
    ClientWriter& operator=(ClientWriter&&)      = default;
    ClientWriter(const ClientWriter&)            = delete;
    ClientWriter& operator=(const ClientWriter&) = delete;

private:
    internal::RawClientWriter raw_;
};

} // namespace rpcpio
