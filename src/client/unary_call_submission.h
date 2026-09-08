#pragma once

#include <atomic>
#include <chrono>
#include <functional>
#include <optional>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include "rpcpio/unary_result_raw.h"

namespace rpcpio::internal {

class UnaryCallSubmission : public std::enable_shared_from_this<UnaryCallSubmission> {
public:
    UnaryCallSubmission(boost::asio::io_context&              ioc,
                        std::function<void(UnaryResultRaw)>   completion);

    void ArmDeadline(std::optional<std::chrono::system_clock::time_point> deadline);
    void WireCancellation(boost::asio::cancellation_slot slot);

    void SetPendingCleanup(std::function<void()> cleanup);

    bool Complete(UnaryResultRaw result);

    [[nodiscard]] bool completed() const noexcept {
        return completed_.load(std::memory_order_acquire);
    }

private:
    void Cancel();
    void FailDeadline();

    boost::asio::io_context&              ioc_;
    std::function<void(UnaryResultRaw)>   completion_;
    boost::asio::steady_timer             timer_;
    std::function<void()>                 pending_cleanup_;
    std::atomic<bool>                     completed_{false};
};

} // namespace rpcpio::internal
