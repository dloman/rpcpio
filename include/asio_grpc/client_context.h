#pragma once

#include <chrono>
#include <optional>
#include <string>
#include <boost/asio/cancellation_signal.hpp>
#include "asio_grpc/metadata.h"
#include "asio_grpc/status.h"

namespace asio_grpc {

class ClientContext {
public:
    ClientContext() = default;
    ~ClientContext() = default;

    // ── Deadline ─────────────────────────────────────────────────────────────

    void set_deadline(std::chrono::system_clock::time_point deadline) {
        deadline_ = deadline;
    }
    std::optional<std::chrono::system_clock::time_point> deadline() const {
        return deadline_;
    }
    bool has_deadline() const noexcept { return deadline_.has_value(); }
    std::chrono::nanoseconds deadline_from_now() const {
        if (!deadline_) return std::chrono::nanoseconds::max();
        return std::chrono::duration_cast<std::chrono::nanoseconds>(
            *deadline_ - std::chrono::system_clock::now());
    }

    // ── Outgoing metadata ─────────────────────────────────────────────────────

    void AddMetadata(std::string key, std::string value) {
        metadata::ValidateAndAdd(send_metadata_, key, value);
    }
    const MetadataMap& send_metadata() const noexcept { return send_metadata_; }

    // ── Message size limits ───────────────────────────────────────────────────

    void        set_max_receive_message_size(std::size_t bytes) noexcept { max_recv_size_ = bytes; }
    std::size_t max_receive_message_size() const noexcept                { return max_recv_size_; }
    void        set_max_send_message_size(std::size_t bytes) noexcept    { max_send_size_ = bytes; }
    std::size_t max_send_message_size() const noexcept                   { return max_send_size_; }

    // ── Compression preference ────────────────────────────────────────────────

    // Hint to the transport; "identity" or "gzip".  The server makes the
    // final choice; the client will at minimum handle identity encoding.
    void               set_compression_algorithm(std::string algo) { compression_algorithm_ = std::move(algo); }
    const std::string& compression_algorithm() const noexcept      { return compression_algorithm_; }

    // ── Cancellation ──────────────────────────────────────────────────────────

    boost::asio::cancellation_slot cancellation_slot() {
        return cancel_signal_.slot();
    }
    void Cancel() {
        cancel_signal_.emit(boost::asio::cancellation_type::all);
    }

    ClientContext(const ClientContext&)            = delete;
    ClientContext& operator=(const ClientContext&) = delete;

private:
    std::optional<std::chrono::system_clock::time_point> deadline_;
    MetadataMap send_metadata_;
    std::size_t max_recv_size_{4 * 1024 * 1024};
    std::size_t max_send_size_{4 * 1024 * 1024};
    std::string compression_algorithm_{"identity"};
    boost::asio::cancellation_signal cancel_signal_;
};

} // namespace asio_grpc
