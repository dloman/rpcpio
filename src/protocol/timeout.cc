#include "timeout.h"

#include <cstdint>
#include <limits>

namespace rpcpio::protocol {

// Maximum digit count per spec.
static constexpr int kMaxDigits = 8;
static constexpr std::uint64_t kMaxTimeoutValue = 99999999ULL; // 8 nines

// Choose the coarsest unit that fits in 8 digits and rounds ceiling.
// Order: H > M > S > m > u > n  (coarsest first).
std::string FormatTimeout(std::chrono::nanoseconds duration) {
    if (duration.count() <= 0) return {};

    auto ns = static_cast<std::uint64_t>(duration.count());

    struct Unit { const char* sym; std::uint64_t ns_per; };
    static constexpr Unit kUnits[] = {
        { "H", 3'600'000'000'000ULL },
        { "M",    60'000'000'000ULL },
        { "S",     1'000'000'000ULL },
        { "m",         1'000'000ULL },
        { "u",             1'000ULL },
        { "n",                 1ULL },
    };

    for (auto& u : kUnits) {
        // Ceiling division: val = ceil(ns / ns_per)
        std::uint64_t val = (ns + u.ns_per - 1) / u.ns_per;
        if (val <= kMaxTimeoutValue) {
            return std::to_string(val) + u.sym;
        }
    }
    // Fallback: emit max nanoseconds (shouldn't be reached with valid input).
    return std::to_string(kMaxTimeoutValue) + "n";
}

std::optional<std::chrono::nanoseconds> ParseTimeout(std::string_view s) {
    if (s.empty()) return std::nullopt;

    char unit = s.back();
    s.remove_suffix(1);

    if (s.empty() || s.size() > kMaxDigits) return std::nullopt;

    // Parse digits only.
    std::uint64_t val = 0;
    for (char c : s) {
        if (c < '0' || c > '9') return std::nullopt;
        val = val * 10 + static_cast<std::uint64_t>(c - '0');
    }

    std::uint64_t ns_per = 0;
    switch (unit) {
        case 'H': ns_per = 3'600'000'000'000ULL; break;
        case 'M': ns_per =    60'000'000'000ULL; break;
        case 'S': ns_per =     1'000'000'000ULL; break;
        case 'm': ns_per =         1'000'000ULL; break;
        case 'u': ns_per =             1'000ULL; break;
        case 'n': ns_per =                 1ULL; break;
        default:  return std::nullopt;
    }

    // Overflow check before multiplication.
    constexpr std::uint64_t kMaxNs =
        static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max());
    if (ns_per != 0 && val > kMaxNs / ns_per) return std::nullopt;

    return std::chrono::nanoseconds{static_cast<std::int64_t>(val * ns_per)};
}

} // namespace rpcpio::protocol
