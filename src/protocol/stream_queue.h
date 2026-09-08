#pragma once

#include <deque>
#include <optional>
#include <string>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include "rpcpio/status.h"

namespace rpcpio::internal {

// Single-threaded async queue (all access on the same io_context executor).
// Bridges nghttp2 DATA callbacks → coroutine consumers/producers.
class StreamMessageQueue {
public:
    explicit StreamMessageQueue(boost::asio::io_context& ioc);

    // Push an assembled proto-bytes payload. Wakes a suspended Pop().
    void Push(std::string proto_bytes);

    // Signal clean end-of-stream. Wakes a suspended Pop().
    void SetEos();

    // Signal an error. Next Pop() returns a non-OK status inside the result.
    void SetError(Status error);

    bool eos()       const noexcept { return eos_; }
    bool has_error() const noexcept { return error_.has_value(); }
    const Status& error() const noexcept { return *error_; }

    // Async read: returns next proto bytes, or nullopt on clean EOS.
    // Returns an error-carrying result if SetError() was called.
    // Return type carries either data (ok, has_value), EOS (ok, nullopt),
    // or error (non-ok).
    struct PopResult {
        Status                     status;  // non-ok = error
        std::optional<std::string> data;    // nullopt = EOS (when status.ok())

        bool ok()  const noexcept { return status.ok(); }
        bool eos() const noexcept { return status.ok() && !data.has_value(); }
    };
    boost::asio::awaitable<PopResult> Pop();

private:
    void Wake();

    boost::asio::steady_timer    wake_;
    std::deque<std::string>      buffer_;
    bool                         eos_{false};
    std::optional<Status>        error_;
};

} // namespace rpcpio::internal
