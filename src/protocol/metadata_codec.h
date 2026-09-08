#pragma once

#include <string>
#include <string_view>
#include <nghttp2/asio_http2.h>
#include "rpcpio/metadata.h"
#include "rpcpio/status.h"

namespace rpcpio::protocol {

// ── nghttp2 header_map ↔ rpcpio MetadataMap conversion ────────────────────

// Convert nghttp2 response/request headers to a MetadataMap.
// - Skips HTTP pseudo-headers (:status, :path, :method, etc.)
// - Skips gRPC reserved headers (content-type, te, grpc-*)
// - For "-bin" keys, base64-decodes the value.
// - Enforces kMaxMetadataSize; returns RESOURCE_EXHAUSTED if exceeded.
Status Nghttp2HeadersToMetadata(
    const nghttp2::asio_http2::header_map& headers,
    MetadataMap& out,
    std::size_t  max_size = kMaxMetadataSize);

// Append a MetadataMap's entries to an nghttp2 header_map.
// - For "-bin" keys, base64-encodes the value.
// - Skips invalid or reserved keys (silently).
void MetadataToNghttp2Headers(
    const MetadataMap& metadata,
    nghttp2::asio_http2::header_map& out);

// Parse a single "grpc-status" value string.  Returns UNKNOWN for unknown codes.
StatusCode ParseGrpcStatus(std::string_view value) noexcept;

// Parse the "grpc-message" header (percent-decoded).
std::string ParseGrpcMessage(std::string_view value);

// Parse the "grpc-status-details-bin" header (base64-decoded).
std::string ParseGrpcStatusDetails(std::string_view value);

// Extract gRPC status from a trailer map.  Returns UNKNOWN with message
// "missing grpc-status" if the key is absent.
Status ExtractTrailerStatus(const nghttp2::asio_http2::header_map& trailers);

// Same but from our MetadataMap (post-conversion).
Status ExtractTrailerStatus(const MetadataMap& trailers);

// Append grpc-status (and optionally grpc-message / grpc-status-details-bin)
// to an nghttp2 header_map for use as trailers.
void BuildTrailers(const Status&                    status,
                   const MetadataMap&               trailing_metadata,
                   nghttp2::asio_http2::header_map& out);

} // namespace rpcpio::protocol
