#pragma once

#include <chrono>
#include <string>
#include <boost/asio/cancellation_signal.hpp>
#include "rpcpio/metadata.h"
#include "rpcpio/status.h"

namespace rpcpio {

// Forward declaration for the friend relationship.
namespace internal { class ServerCallState; }

class ServerContext {
public:
    ServerContext() = default;

    // ── Incoming call info (read-only to handler) ─────────────────────────────

    const MetadataMap& client_metadata() const noexcept { return client_metadata_; }
    const std::string& peer()            const noexcept { return peer_; }

    bool has_deadline() const noexcept { return has_deadline_; }
    std::chrono::system_clock::time_point deadline() const noexcept { return deadline_; }
    std::chrono::nanoseconds deadline_from_now() const {
        if (!has_deadline_) return std::chrono::nanoseconds::max();
        return std::chrono::duration_cast<std::chrono::nanoseconds>(
            deadline_ - std::chrono::system_clock::now());
    }

    // ── Outgoing metadata (set before returning from handler) ─────────────────

    void AddInitialMetadata(std::string key, std::string value) {
        metadata::ValidateAndAdd(initial_metadata_, key, value);
    }
    void AddTrailingMetadata(std::string key, std::string value) {
        metadata::ValidateAndAdd(trailing_metadata_, key, value);
    }
    const MetadataMap& initial_metadata()  const noexcept { return initial_metadata_; }
    const MetadataMap& trailing_metadata() const noexcept { return trailing_metadata_; }

    // ── Cancellation (handler may co_await this) ──────────────────────────────

    boost::asio::cancellation_slot cancellation_slot() {
        return cancel_signal_.slot();
    }

    ServerContext(const ServerContext&)            = delete;
    ServerContext& operator=(const ServerContext&) = delete;

private:
    friend class internal::ServerCallState;

    void set_peer(std::string peer)                                       { peer_ = std::move(peer); }
    void set_deadline(std::chrono::system_clock::time_point d)            { deadline_ = d; }
    void set_has_deadline(bool v)                                         { has_deadline_ = v; }
    void set_client_metadata(MetadataMap meta)                            { client_metadata_ = std::move(meta); }
    void trigger_cancel()                                                  {
        cancel_signal_.emit(boost::asio::cancellation_type::all);
    }

    MetadataMap client_metadata_;
    std::string peer_;
    bool        has_deadline_{false};
    std::chrono::system_clock::time_point deadline_{};
    MetadataMap initial_metadata_;
    MetadataMap trailing_metadata_;
    boost::asio::cancellation_signal cancel_signal_;
};

} // namespace rpcpio
