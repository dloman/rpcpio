#include "stream_queue.h"

#include <boost/asio/redirect_error.hpp>
#include <boost/asio/use_awaitable.hpp>

namespace rpcpio::internal {

StreamMessageQueue::StreamMessageQueue(boost::asio::io_context& ioc)
    : wake_(ioc, std::chrono::steady_clock::time_point::max())
{}

void StreamMessageQueue::Push(std::string proto_bytes) {
    buffer_.push_back(std::move(proto_bytes));
    Wake();
}

void StreamMessageQueue::SetEos() {
    eos_ = true;
    Wake();
}

void StreamMessageQueue::SetError(Status error) {
    error_ = std::move(error);
    Wake();
}

void StreamMessageQueue::Wake() {
    wake_.cancel();
}

boost::asio::awaitable<StreamMessageQueue::PopResult>
StreamMessageQueue::Pop() {
    while (true) {
        if (!buffer_.empty()) {
            auto msg = std::move(buffer_.front());
            buffer_.pop_front();
            co_return PopResult{Status{}, std::move(msg)};
        }
        if (error_) {
            co_return PopResult{*error_, std::nullopt};
        }
        if (eos_) {
            co_return PopResult{Status{}, std::nullopt};
        }
        // Arm timer in the "never fires" position, then wait.
        // Wake() cancels the timer, producing operation_aborted — we loop.
        wake_.expires_at(std::chrono::steady_clock::time_point::max());
        boost::system::error_code ec;
        co_await wake_.async_wait(
            boost::asio::redirect_error(boost::asio::use_awaitable, ec));
        // ec == operation_aborted → data arrived; loop to check buffer
    }
}

} // namespace rpcpio::internal
