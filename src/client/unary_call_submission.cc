#include "unary_call_submission.h"

#include <boost/asio/post.hpp>
#include "rpcpio/status.h"

namespace rpcpio::internal {

UnaryCallSubmission::UnaryCallSubmission(
        boost::asio::io_context&              ioc,
        std::function<void(UnaryResultRaw)>   completion)
    : ioc_(ioc)
    , completion_(std::move(completion))
    , timer_(ioc)
{}

void UnaryCallSubmission::SetPendingCleanup(std::function<void()> cleanup) {
    pending_cleanup_ = std::move(cleanup);
}

void UnaryCallSubmission::ArmDeadline(
        std::optional<std::chrono::system_clock::time_point> deadline) {
    if (!deadline) return;

    const auto remaining = std::chrono::duration_cast<std::chrono::nanoseconds>(
        *deadline - std::chrono::system_clock::now());
    if (remaining <= std::chrono::nanoseconds::zero()) {
        FailDeadline();
        return;
    }

    timer_.expires_after(remaining);
    auto self = shared_from_this();
    timer_.async_wait([self](const boost::system::error_code& ec) {
        if (ec == boost::asio::error::operation_aborted) return;
        self->FailDeadline();
    });
}

void UnaryCallSubmission::WireCancellation(boost::asio::cancellation_slot slot) {
    if (!slot.is_connected()) return;
    auto self = shared_from_this();
    slot.assign([self](boost::asio::cancellation_type_t type) {
        if ((type & boost::asio::cancellation_type::all)
            != boost::asio::cancellation_type::none) {
            self->Cancel();
        }
    });
}

void UnaryCallSubmission::Cancel() {
    Complete(UnaryResultRaw{Status{StatusCode::CANCELLED, "call cancelled"}});
}

void UnaryCallSubmission::FailDeadline() {
    Complete(UnaryResultRaw{
        Status{StatusCode::DEADLINE_EXCEEDED, "deadline exceeded"}});
}

bool UnaryCallSubmission::Complete(UnaryResultRaw result) {
    if (completed_.exchange(true, std::memory_order_acq_rel)) return false;
    timer_.cancel();
    if (pending_cleanup_) {
        auto cleanup = std::move(pending_cleanup_);
        cleanup();
    }
    auto cb = std::move(completion_);
    boost::asio::post(ioc_, [cb = std::move(cb),
                             r = std::move(result)]() mutable {
        cb(std::move(r));
    });
    return true;
}

} // namespace rpcpio::internal
