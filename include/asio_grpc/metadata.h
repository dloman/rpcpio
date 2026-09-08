#pragma once

#include <array>
#include <cstddef>
#include <map>
#include <stdexcept>
#include <string>
#include <string_view>

namespace asio_grpc {

using MetadataValue = std::string;

// Ordered multimap with lowercase string keys.  Duplicate entries are allowed
// (gRPC permits multiple values for the same header name).
using MetadataMap = std::multimap<std::string, MetadataValue>;

// Maximum cumulative byte size (all keys + values combined).
inline constexpr std::size_t kMaxMetadataSize = 8192;
inline constexpr std::size_t kMaxTrailerSize  = 8192;

namespace metadata {

// Key must be all lowercase alphanumeric, hyphen, or underscore.
// Binary metadata keys additionally end with "-bin".
inline bool IsValidKey(std::string_view key) noexcept {
    if (key.empty()) return false;
    for (unsigned char c : key) {
        bool ok = (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')
               || c == '-' || c == '_';
        if (!ok) return false;
    }
    return true;
}

inline bool IsBinaryKey(std::string_view key) noexcept {
    return key.size() > 4 && key.substr(key.size() - 4) == "-bin";
}

// Reserved headers must not be set by application code.
inline bool IsReservedKey(std::string_view key) noexcept {
    auto sw = [&](std::string_view prefix) {
        return key.size() >= prefix.size() && key.substr(0, prefix.size()) == prefix;
    };
    if (sw("grpc-")) return true;
    if (key == "content-type") return true;
    if (key == "te")           return true;
    if (sw(":"))               return true;  // :status, :path, etc.
    return false;
}

// Unpadded standard Base64 (RFC 4648 §5 with + and /).
inline std::string EncodeBase64(std::string_view data) {
    static constexpr std::string_view kAlpha =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string out;
    out.reserve((data.size() + 2) / 3 * 4);
    const auto* p = reinterpret_cast<const unsigned char*>(data.data());
    std::size_t i = 0;
    while (i + 2 < data.size()) {
        std::uint32_t v = (std::uint32_t(p[i]) << 16)
                        | (std::uint32_t(p[i+1]) << 8)
                        |  std::uint32_t(p[i+2]);
        out += kAlpha[(v >> 18) & 63];
        out += kAlpha[(v >> 12) & 63];
        out += kAlpha[(v >>  6) & 63];
        out += kAlpha[(v      ) & 63];
        i += 3;
    }
    if (i < data.size()) {
        std::uint32_t v = std::uint32_t(p[i]) << 16;
        if (i + 1 < data.size()) v |= std::uint32_t(p[i+1]) << 8;
        out += kAlpha[(v >> 18) & 63];
        out += kAlpha[(v >> 12) & 63];
        if (i + 1 < data.size()) out += kAlpha[(v >> 6) & 63];
        // No padding '=' characters (unpadded).
    }
    return out;
}

// Accepts padded or unpadded base64; unknown characters are skipped.
inline std::string DecodeBase64(std::string_view encoded) {
    // Build lookup table at function-local static scope (C++17 compatible).
    static const auto kTable = []() {
        std::array<int, 256> t{};
        t.fill(-1);
        for (int i = 0; i < 26; ++i) t[static_cast<unsigned char>('A')+i] = i;
        for (int i = 0; i < 26; ++i) t[static_cast<unsigned char>('a')+i] = 26+i;
        for (int i = 0; i < 10; ++i) t[static_cast<unsigned char>('0')+i] = 52+i;
        t[static_cast<unsigned char>('+')] = 62;
        t[static_cast<unsigned char>('/')] = 63;
        t[static_cast<unsigned char>('=')] = 0;
        return t;
    }();

    std::string   out;
    out.reserve(encoded.size() / 4 * 3 + 3);
    std::uint32_t buf  = 0;
    int           bits = 0;
    for (char c : encoded) {
        if (c == '=') break;
        int v = kTable[static_cast<unsigned char>(c)];
        if (v < 0) continue;
        buf   = (buf << 6) | static_cast<std::uint32_t>(v);
        bits += 6;
        if (bits >= 8) {
            bits -= 8;
            out += static_cast<char>((buf >> bits) & 0xFF);
        }
    }
    return out;
}

// Percent-encode for grpc-message: encode everything outside printable ASCII
// (0x20–0x7E), plus space (0x20) and percent (0x25) themselves.
inline std::string PercentEncode(std::string_view utf8) {
    static constexpr std::string_view kHex = "0123456789ABCDEF";
    std::string out;
    out.reserve(utf8.size());
    for (unsigned char c : utf8) {
        // Safe: printable ASCII except space and %
        if (c >= 0x21 && c <= 0x7E && c != '%') {
            out += static_cast<char>(c);
        } else {
            out += '%';
            out += kHex[(c >> 4) & 0xF];
            out += kHex[ c       & 0xF];
        }
    }
    return out;
}

// Percent-decode; malformed escape sequences are passed through unchanged.
inline std::string PercentDecode(std::string_view encoded) {
    std::string out;
    out.reserve(encoded.size());
    for (std::size_t i = 0; i < encoded.size(); ++i) {
        if (encoded[i] == '%' && i + 2 < encoded.size()) {
            auto hexval = [](char c) -> int {
                if (c >= '0' && c <= '9') return c - '0';
                if (c >= 'a' && c <= 'f') return c - 'a' + 10;
                if (c >= 'A' && c <= 'F') return c - 'A' + 10;
                return -1;
            };
            int hi = hexval(encoded[i+1]);
            int lo = hexval(encoded[i+2]);
            if (hi >= 0 && lo >= 0) {
                out += static_cast<char>((hi << 4) | lo);
                i += 2;
                continue;
            }
        }
        out += encoded[i];
    }
    return out;
}

// Validate key and value (binary keys must have base64-decodable values),
// then insert into the map.  Throws on invalid or reserved keys.
inline void ValidateAndAdd(MetadataMap&    map,
                            std::string_view key,
                            std::string_view value) {
    if (!IsValidKey(key))
        throw std::invalid_argument("Invalid metadata key: " + std::string(key));
    if (IsReservedKey(key))
        throw std::invalid_argument("Reserved metadata key: " + std::string(key));
    map.emplace(std::string(key), std::string(value));
}

// Total cumulative byte size of all entries (key + value lengths).
inline std::size_t TotalSize(const MetadataMap& m) noexcept {
    std::size_t n = 0;
    for (auto& [k, v] : m) n += k.size() + v.size();
    return n;
}

} // namespace metadata
} // namespace asio_grpc
