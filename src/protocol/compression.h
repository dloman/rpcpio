#pragma once

#include <cstddef>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace asio_grpc::protocol {

// ── Message compression registry ─────────────────────────────────────────────
//
// Mandatory: identity (pass-through, always available).
// Optional:  gzip (requires zlib linkage; controlled by ASIO_GRPC_ENABLE_GZIP).
//
// Design rules:
//   - Never advertise an encoding that is not compiled in.
//   - Reject a compressed flag without a prior negotiated encoding.
//   - Reset compression state for every message (stateless per-message codec).

enum class Encoding { kIdentity, kGzip };

// True if gzip support was compiled in.
bool GzipAvailable() noexcept;

// Parse "grpc-encoding" or "grpc-accept-encoding" header value.
// Returns nullopt for unknown/unsupported encodings.
std::optional<Encoding> ParseEncoding(std::string_view s) noexcept;

// Returns the canonical header string for an encoding.
std::string_view EncodingName(Encoding enc) noexcept;

// Build the "grpc-accept-encoding" response header value listing all available
// encodings (always includes "identity").
std::string AcceptEncodingValue();

// Compress data using the given encoding.
// Returns false and leaves out unchanged on error.
// For kIdentity, simply copies src to out.
bool Compress(Encoding enc, std::string_view src, std::string& out);

// Decompress data.  max_output_size enforces decompression-bomb protection.
// Returns false on error (bad stream, output exceeds limit, etc.).
bool Decompress(Encoding enc,
                std::string_view src,
                std::string& out,
                std::size_t  max_output_size = 4 * 1024 * 1024);

} // namespace asio_grpc::protocol
