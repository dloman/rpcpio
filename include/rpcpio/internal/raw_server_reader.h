#pragma once

#include <memory>
#include <optional>
#include <string>
#include <boost/asio/awaitable.hpp>
#include "rpcpio/status.h"

namespace rpcpio::internal {

struct RawServerReaderImpl;

// Opaque handle to the server-side read channel (client-streaming / bidi).
// ServerReader<T> wraps this to provide typed, deserializing Read().
class RawServerReader {
public:
    explicit RawServerReader(std::shared_ptr<RawServerReaderImpl> impl) noexcept;

    // Returns next proto bytes, or nullopt on clean client half-close.
    // status field is non-OK on stream error.
    struct ReadResult {
        Status                     status;
        std::optional<std::string> data;    // nullopt = EOS when status.ok()
        bool eos() const noexcept { return status.ok() && !data.has_value(); }
    };
    boost::asio::awaitable<ReadResult> Read();

private:
    std::shared_ptr<RawServerReaderImpl> impl_;
};

} // namespace rpcpio::internal
