#pragma once

#include <cassert>
#include <cstdint>
#include <map>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>

namespace asio_grpc {

// ── StatusCode ────────────────────────────────────────────────────────────────

enum class StatusCode : std::int32_t {
    OK                  =  0,
    CANCELLED           =  1,
    UNKNOWN             =  2,
    INVALID_ARGUMENT    =  3,
    DEADLINE_EXCEEDED   =  4,
    NOT_FOUND           =  5,
    ALREADY_EXISTS      =  6,
    PERMISSION_DENIED   =  7,
    RESOURCE_EXHAUSTED  =  8,
    FAILED_PRECONDITION =  9,
    ABORTED             = 10,
    OUT_OF_RANGE        = 11,
    UNIMPLEMENTED       = 12,
    INTERNAL            = 13,
    UNAVAILABLE         = 14,
    DATA_LOSS           = 15,
    UNAUTHENTICATED     = 16,
};

inline std::string_view StatusCodeName(StatusCode code) noexcept {
    switch (code) {
        case StatusCode::OK:                   return "OK";
        case StatusCode::CANCELLED:            return "CANCELLED";
        case StatusCode::UNKNOWN:              return "UNKNOWN";
        case StatusCode::INVALID_ARGUMENT:     return "INVALID_ARGUMENT";
        case StatusCode::DEADLINE_EXCEEDED:    return "DEADLINE_EXCEEDED";
        case StatusCode::NOT_FOUND:            return "NOT_FOUND";
        case StatusCode::ALREADY_EXISTS:       return "ALREADY_EXISTS";
        case StatusCode::PERMISSION_DENIED:    return "PERMISSION_DENIED";
        case StatusCode::RESOURCE_EXHAUSTED:   return "RESOURCE_EXHAUSTED";
        case StatusCode::FAILED_PRECONDITION:  return "FAILED_PRECONDITION";
        case StatusCode::ABORTED:              return "ABORTED";
        case StatusCode::OUT_OF_RANGE:         return "OUT_OF_RANGE";
        case StatusCode::UNIMPLEMENTED:        return "UNIMPLEMENTED";
        case StatusCode::INTERNAL:             return "INTERNAL";
        case StatusCode::UNAVAILABLE:          return "UNAVAILABLE";
        case StatusCode::DATA_LOSS:            return "DATA_LOSS";
        case StatusCode::UNAUTHENTICATED:      return "UNAUTHENTICATED";
        default:                               return "UNKNOWN";
    }
}

// Whether a numeric value is a known status code.
inline bool IsKnownStatusCode(std::int32_t v) noexcept {
    return v >= 0 && v <= 16;
}

inline StatusCode StatusCodeFromInt(std::int32_t v) noexcept {
    return IsKnownStatusCode(v) ? static_cast<StatusCode>(v) : StatusCode::UNKNOWN;
}

// ── Status ────────────────────────────────────────────────────────────────────

class Status {
public:
    Status() : code_(StatusCode::OK) {}

    Status(StatusCode code, std::string message, std::string details = {})
        : code_(code)
        , message_(std::move(message))
        , details_(std::move(details))
    {}

    [[nodiscard]] bool ok() const noexcept { return code_ == StatusCode::OK; }
    [[nodiscard]] StatusCode code() const noexcept { return code_; }
    [[nodiscard]] const std::string& message() const noexcept { return message_; }
    // Opaque bytes; may encode google.rpc.Status proto.
    [[nodiscard]] const std::string& details() const noexcept { return details_; }

    bool operator==(const Status& o) const noexcept {
        return code_ == o.code_ && message_ == o.message_ && details_ == o.details_;
    }
    bool operator!=(const Status& o) const noexcept { return !(*this == o); }

    [[nodiscard]] std::string DebugString() const {
        if (ok()) return "OK";
        std::string s = "StatusCode::";
        s += StatusCodeName(code_);
        if (!message_.empty()) { s += " message:\""; s += message_; s += '"'; }
        if (!details_.empty()) {
            s += " details:<"; s += std::to_string(details_.size()); s += " bytes>";
        }
        return s;
    }

    // HTTP-status → gRPC-status fallback mapping (gRPC spec §A4).
    // Used only when grpc-status trailer is absent.
    [[nodiscard]] static Status FromHttpStatus(int http) {
        StatusCode code;
        switch (http) {
            case 400: code = StatusCode::INTERNAL;            break;
            case 401: code = StatusCode::UNAUTHENTICATED;     break;
            case 403: code = StatusCode::PERMISSION_DENIED;   break;
            case 404: code = StatusCode::UNIMPLEMENTED;       break;
            case 429: code = StatusCode::UNAVAILABLE;         break;
            case 502: code = StatusCode::UNAVAILABLE;         break;
            case 503: code = StatusCode::UNAVAILABLE;         break;
            case 504: code = StatusCode::UNAVAILABLE;         break;
            default:  code = StatusCode::UNKNOWN;             break;
        }
        return Status{code, "HTTP " + std::to_string(http)};
    }

private:
    StatusCode  code_{StatusCode::OK};
    std::string message_;
    std::string details_;
};

// ── StatusOr<T> ───────────────────────────────────────────────────────────────

template<typename T>
class StatusOr {
public:
    // Implicit construction from a value implies OK.
    StatusOr(T value)  // NOLINT(google-explicit-constructor)
        : value_(std::move(value)), status_() {}

    // Construction from a non-OK Status.
    explicit StatusOr(Status status) : status_(std::move(status)) {
        if (status_.ok())
            throw std::invalid_argument("StatusOr constructed from OK Status");
    }

    [[nodiscard]] bool ok() const noexcept { return status_.ok(); }

    [[nodiscard]] Status  status() const&  { return status_; }
    [[nodiscard]] Status  status() &&      { return std::move(status_); }

    [[nodiscard]] T&       value() &       { RequireOk(); return *value_; }
    [[nodiscard]] const T& value() const&  { RequireOk(); return *value_; }
    [[nodiscard]] T        value() &&      { RequireOk(); return std::move(*value_); }

    [[nodiscard]] T&       operator*() &       noexcept { return *value_; }
    [[nodiscard]] const T& operator*() const&  noexcept { return *value_; }
    [[nodiscard]] T*       operator->()        noexcept { return value_ ? &*value_ : nullptr; }
    [[nodiscard]] const T* operator->() const  noexcept { return value_ ? &*value_ : nullptr; }

private:
    void RequireOk() const {
        if (!status_.ok()) throw std::runtime_error(status_.DebugString());
    }

    std::optional<T> value_;
    Status           status_;
};

// ── UnaryResult<T> ────────────────────────────────────────────────────────────

// Complete result of a single unary RPC call (client side).
template<typename T>
struct UnaryResult {
    Status                                  status;
    std::optional<T>                        response;
    std::multimap<std::string, std::string> initial_metadata;
    std::multimap<std::string, std::string> trailing_metadata;
};

} // namespace asio_grpc
