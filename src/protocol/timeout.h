#pragma once

#include <chrono>
#include <optional>
#include <string>
#include <string_view>

namespace rpcpio::protocol {

// ── grpc-timeout header encoding/decoding ────────────────────────────────────
//
// Format:  <TimeoutValue> <TimeoutUnit>
// where TimeoutValue is at most 8 ASCII decimal digits and TimeoutUnit is one of:
//   H  hours
//   M  minutes
//   S  seconds
//   m  milliseconds
//   u  microseconds
//   n  nanoseconds
//
// Encoding rounds OUTWARD (ceiling) so that the formatted timeout is never
// shorter than the caller's actual deadline.

// Encode a duration as a grpc-timeout header value.
// Returns empty string if duration is zero or negative (no timeout).
std::string FormatTimeout(std::chrono::nanoseconds duration);

// Parse a grpc-timeout header value.
// Returns nullopt on parse error or overflow.
std::optional<std::chrono::nanoseconds> ParseTimeout(std::string_view s);

} // namespace rpcpio::protocol
