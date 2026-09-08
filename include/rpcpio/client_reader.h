#pragma once

#include <optional>
#include <boost/asio/awaitable.hpp>
#include <google/protobuf/message.h>
#include "rpcpio/status.h"
#include "rpcpio/internal/raw_client_reader.h"

namespace rpcpio {

// Typed, coroutine-friendly read handle for server-streaming and bidi clients.
//
// Each Read() call returns one deserialized response message, or signals
// clean end-of-stream (nullopt with OK status), or an error.
// Call Finish() exactly once after all Read()s to retrieve the final
// grpc-status from the server's trailing HEADERS.
template<typename Resp>
class ClientReader {
public:
    explicit ClientReader(internal::RawClientReader raw) noexcept
        : raw_(std::move(raw)) {}

    // Returns:
    //   ok() && has_value()  → next response message
    //   ok() && !has_value() → clean server half-close (EOS)
    //   !ok()                → stream error
    boost::asio::awaitable<StatusOr<std::optional<Resp>>> Read() {
        auto r = co_await raw_.Read();
        if (!r.status.ok()) {
            co_return StatusOr<std::optional<Resp>>(r.status);
        }
        if (!r.data.has_value()) {
            // Clean EOS.
            co_return std::optional<Resp>(std::nullopt);
        }
        Resp msg;
        if (!msg.ParseFromString(*r.data)) {
            co_return StatusOr<std::optional<Resp>>(
                Status{StatusCode::INTERNAL, "ClientReader: parse failed"});
        }
        co_return std::optional<Resp>(std::move(msg));
    }

    // Must be called exactly once after all Read()s are done.
    boost::asio::awaitable<Status> Finish() {
        co_return co_await raw_.Finish();
    }

    ClientReader(ClientReader&&)                 = default;
    ClientReader& operator=(ClientReader&&)      = default;
    ClientReader(const ClientReader&)            = delete;
    ClientReader& operator=(const ClientReader&) = delete;

private:
    internal::RawClientReader raw_;
};

} // namespace rpcpio
