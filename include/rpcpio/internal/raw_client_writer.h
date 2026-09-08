#pragma once

#include <memory>
#include <string_view>
#include <boost/asio/awaitable.hpp>
#include "rpcpio/status.h"

namespace rpcpio::internal {

struct RawClientWriterImpl;

// Opaque handle to the client-side request write channel (client-streaming / bidi).
// ClientWriter<T> wraps this to provide typed, serializing Write().
class RawClientWriter {
public:
    explicit RawClientWriter(std::shared_ptr<RawClientWriterImpl> impl) noexcept;

    boost::asio::awaitable<Status> Write(std::string_view proto_bytes);

    // Half-close the request stream. Server may still be sending.
    boost::asio::awaitable<void> WritesDone();

    // Returns the final grpc-status after server has finished.
    boost::asio::awaitable<Status> Finish();

    // Returns the single serialized response message from the server.
    // Valid only after Finish() returns with a status of OK.
    // Used by the typed ClientStreamingCall template to deserialize the response.
    const std::string& response_bytes() const noexcept;

private:
    std::shared_ptr<RawClientWriterImpl> impl_;
};

} // namespace rpcpio::internal
