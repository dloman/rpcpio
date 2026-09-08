#pragma once

#include <optional>
#include <string>
#include <boost/asio/awaitable.hpp>
#include <google/protobuf/message.h>
#include "rpcpio/status.h"
#include "rpcpio/internal/raw_client_reader.h"
#include "rpcpio/internal/raw_client_writer.h"

namespace rpcpio {

// Typed, coroutine-friendly bidirectional handle for bidi-streaming RPCs.
//
// The caller may interleave Read() and Write() calls freely on the same
// coroutine, or co_spawn separate coroutines for send and receive.
// Call WritesDone() to half-close the request stream and Finish() to
// retrieve the final grpc-status.
template<typename Req, typename Resp>
class BidiStream {
public:
    BidiStream(internal::RawClientReader reader,
               internal::RawClientWriter writer) noexcept
        : reader_(std::move(reader))
        , writer_(std::move(writer))
    {}

    // Returns:
    //   ok() && has_value()  → next response message
    //   ok() && !has_value() → clean server half-close (EOS)
    //   !ok()                → stream error
    boost::asio::awaitable<StatusOr<std::optional<Resp>>> Read() {
        auto r = co_await reader_.Read();
        if (!r.status.ok()) {
            co_return StatusOr<std::optional<Resp>>(r.status);
        }
        if (!r.data.has_value()) {
            co_return std::optional<Resp>(std::nullopt);
        }
        Resp msg;
        if (!msg.ParseFromString(*r.data)) {
            co_return StatusOr<std::optional<Resp>>(
                Status{StatusCode::INTERNAL, "BidiStream::Read parse failed"});
        }
        co_return std::optional<Resp>(std::move(msg));
    }

    // Serialize msg and send it as the next gRPC LPM request frame.
    boost::asio::awaitable<Status> Write(const Req& msg) {
        std::string bytes;
        if (!msg.SerializeToString(&bytes)) {
            co_return Status{StatusCode::INTERNAL,
                             "BidiStream::Write serialization failed"};
        }
        co_return co_await writer_.Write(bytes);
    }

    // Half-close the request stream.  The server may still send responses.
    boost::asio::awaitable<void> WritesDone() {
        co_await writer_.WritesDone();
    }

    // Wait for the final grpc-status from the server's trailing HEADERS.
    // Must be called exactly once after all Read()s and WritesDone().
    boost::asio::awaitable<Status> Finish() {
        co_return co_await reader_.Finish();
    }

    BidiStream(BidiStream&&)                 = default;
    BidiStream& operator=(BidiStream&&)      = default;
    BidiStream(const BidiStream&)            = delete;
    BidiStream& operator=(const BidiStream&) = delete;

private:
    internal::RawClientReader reader_;
    internal::RawClientWriter writer_;
};

} // namespace rpcpio
