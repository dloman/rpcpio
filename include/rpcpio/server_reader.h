#pragma once

#include <optional>
#include <string>
#include <boost/asio/awaitable.hpp>
#include <google/protobuf/message.h>
#include "rpcpio/status.h"
#include "rpcpio/internal/raw_server_reader.h"

namespace rpcpio {

// Typed, coroutine-friendly read handle for client-streaming and bidi handlers.
//
// Instances are created by the streaming call state and passed by reference to
// the handler coroutine. The handler MUST NOT retain a reference past the end
// of the coroutine.
template<typename Req>
class ServerReader {
public:
    explicit ServerReader(internal::RawServerReader* raw) noexcept : raw_(raw) {}

    // Returns the next deserialized message.
    //
    // Return values:
    //   ok() == true,  has_value() == true   → next message
    //   ok() == true,  has_value() == false  → clean client half-close (EOS)
    //   ok() == false                         → stream error
    boost::asio::awaitable<StatusOr<std::optional<Req>>> Read() {
        auto r = co_await raw_->Read();
        if (!r.status.ok()) {
            co_return StatusOr<std::optional<Req>>(r.status);
        }
        if (!r.data.has_value()) {
            co_return std::optional<Req>(std::nullopt);
        }
        Req msg;
        if (!msg.ParseFromString(*r.data)) {
            co_return StatusOr<std::optional<Req>>(
                Status{StatusCode::INTERNAL, "ServerReader: parse failed"});
        }
        co_return std::optional<Req>(std::move(msg));
    }

    ServerReader(const ServerReader&)            = delete;
    ServerReader& operator=(const ServerReader&) = delete;

private:
    internal::RawServerReader* raw_;  // non-owning; lifetime tied to handler scope
};

} // namespace rpcpio
