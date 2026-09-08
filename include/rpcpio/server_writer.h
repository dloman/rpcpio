#pragma once

#include <string>
#include <boost/asio/awaitable.hpp>
#include <google/protobuf/message.h>
#include "rpcpio/status.h"
#include "rpcpio/internal/raw_server_writer.h"

namespace rpcpio {

// Typed, coroutine-friendly write handle for server-streaming and bidi handlers.
//
// Instances are created by the streaming call state and passed by reference to
// the handler coroutine. The handler MUST NOT retain a reference past the end
// of the coroutine — the underlying RawServerWriter may be invalidated when the
// call state tears down.
template<typename Resp>
class ServerWriter {
public:
    explicit ServerWriter(internal::RawServerWriter* raw) noexcept : raw_(raw) {}

    // Serialize msg and send it as the next gRPC LPM frame.
    // Returns a non-OK Status on serialization failure or write error.
    boost::asio::awaitable<Status> Write(const Resp& msg) {
        std::string bytes;
        if (!msg.SerializeToString(&bytes)) {
            co_return Status{StatusCode::INTERNAL,
                             "ServerWriter: serialization failed"};
        }
        co_return raw_->Write(bytes);
    }

    ServerWriter(const ServerWriter&)            = delete;
    ServerWriter& operator=(const ServerWriter&) = delete;

private:
    internal::RawServerWriter* raw_;  // non-owning; lifetime tied to handler scope
};

} // namespace rpcpio
