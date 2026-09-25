#pragma once

#include <atomic>

#include "rpcpio/status.h"

namespace rpcpio::internal {

class ServerCallStateBase {
public:
    virtual ~ServerCallStateBase() = default;

    // May be called from any thread. The implementation serializes
    // cancellation with the connection's request callbacks.
    virtual void Cancel(Status status) = 0;

    bool IsTerminal() const noexcept {
        return terminal_.load(std::memory_order_acquire);
    }

protected:
    void MarkTerminal() noexcept {
        terminal_.store(true, std::memory_order_release);
    }

private:
    std::atomic<bool> terminal_{false};
};

} // namespace rpcpio::internal
