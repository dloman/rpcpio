#pragma once

#include <memory>
#include <optional>
#include <string>
#include <boost/asio/awaitable.hpp>
#include "rpcpio/status.h"

namespace rpcpio::internal {

struct RawClientReaderImpl;

// Opaque handle to the client-side response read channel (server-streaming / bidi).
// ClientReader<T> wraps this to provide typed, deserializing Read() and Finish().
class RawClientReader {
public:
    explicit RawClientReader(std::shared_ptr<RawClientReaderImpl> impl) noexcept;

    struct ReadResult {
        Status                     status;
        std::optional<std::string> data;
        bool eos() const noexcept { return status.ok() && !data.has_value(); }
    };
    boost::asio::awaitable<ReadResult> Read();

    // Must be called exactly once after all Read()s.
    // Returns the final grpc-status from the server's trailing HEADERS.
    boost::asio::awaitable<Status> Finish();

private:
    std::shared_ptr<RawClientReaderImpl> impl_;
};

} // namespace rpcpio::internal
