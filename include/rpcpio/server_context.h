#pragma once

#include <chrono>
#include <optional>
#include <string>
#include <boost/asio/cancellation_signal.hpp>
#include "rpcpio/metadata.h"
#include "rpcpio/status.h"

namespace rpcpio {

// Forward declarations for the friend relationships.
namespace internal {
class ServerCallState;
class ServerStreamingCallState;
class ClientStreamingCallState;
class BidiStreamingCallState;
}

class ServerContext {
public:
    ServerContext() = default;

    // ── Incoming call info (read-only to handler) ─────────────────────────────

    const MetadataMap& client_metadata() const noexcept { return client_metadata_; }

    // Untrusted HTTP/2 :authority pseudo-header (routing info, NOT authenticated identity).
    const std::string& authority() const noexcept { return authority_; }

    // Authenticated peer identity from this connection's verified client
    // certificate. This is the first URI SAN, otherwise the first DNS SAN.
    // It is absent for h2c, unverified TLS, and certificates without either
    // SAN type. The subject name and :authority are never identities.
    const std::optional<std::string>& peer_identity() const noexcept { return peer_identity_; }

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
    friend class internal::ServerStreamingCallState;
    friend class internal::ClientStreamingCallState;
    friend class internal::BidiStreamingCallState;

    void set_authority(std::string a)            { authority_ = std::move(a); }
    void set_peer_identity(std::string identity) { peer_identity_ = std::move(identity); }
    void set_deadline(std::chrono::system_clock::time_point d)            { deadline_ = d; }
    void set_has_deadline(bool v)                                         { has_deadline_ = v; }
    void set_client_metadata(MetadataMap meta)                            { client_metadata_ = std::move(meta); }
    void trigger_cancel()                                                  {
        cancel_signal_.emit(boost::asio::cancellation_type::all);
    }

    MetadataMap client_metadata_;
    std::string authority_;
    std::optional<std::string> peer_identity_;
    bool        has_deadline_{false};
    std::chrono::system_clock::time_point deadline_{};
    MetadataMap initial_metadata_;
    MetadataMap trailing_metadata_;
    boost::asio::cancellation_signal cancel_signal_;
};

} // namespace rpcpio
