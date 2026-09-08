#include "metadata_codec.h"

#include <cstdint>
#include <string>
#include "asio_grpc/metadata.h"

namespace asio_grpc::protocol {

namespace {

// Keys that must never appear in application metadata.
bool IsHttp2Pseudo(std::string_view k) noexcept {
    return !k.empty() && k[0] == ':';
}

bool IsGrpcInternal(std::string_view k) noexcept {
    // These are handled by the protocol layer, not application code.
    static constexpr std::string_view kSkip[] = {
        "grpc-status", "grpc-message", "grpc-status-details-bin",
        "grpc-encoding", "grpc-accept-encoding", "grpc-timeout",
        "content-type", "te",
    };
    for (auto s : kSkip)
        if (k == s) return true;
    return false;
}

} // namespace

Status Nghttp2HeadersToMetadata(
    const nghttp2::asio_http2::header_map& headers,
    MetadataMap& out,
    std::size_t  max_size)
{
    std::size_t total = metadata::TotalSize(out);

    for (auto& [name, hv] : headers) {
        if (IsHttp2Pseudo(name)) continue;
        if (IsGrpcInternal(name)) continue;
        if (!metadata::IsValidKey(name)) continue;

        std::string value = hv.value;
        if (metadata::IsBinaryKey(name)) {
            value = metadata::DecodeBase64(value);
        }

        total += name.size() + value.size();
        if (total > max_size) {
            return Status{StatusCode::RESOURCE_EXHAUSTED,
                          "metadata size exceeds limit"};
        }
        out.emplace(name, std::move(value));
    }
    return Status{};
}

void MetadataToNghttp2Headers(
    const MetadataMap& metadata,
    nghttp2::asio_http2::header_map& out)
{
    for (auto& [key, value] : metadata) {
        if (!metadata::IsValidKey(key)) continue;
        if (metadata::IsReservedKey(key)) continue;

        std::string encoded_value = metadata::IsBinaryKey(key)
            ? metadata::EncodeBase64(value)
            : value;
        out.emplace(key, nghttp2::asio_http2::header_value{
            std::move(encoded_value), false});
    }
}

StatusCode ParseGrpcStatus(std::string_view value) noexcept {
    std::int32_t code = 0;
    bool negative = false;
    if (!value.empty() && value[0] == '-') { negative = true; value.remove_prefix(1); }
    for (char c : value) {
        if (c < '0' || c > '9') return StatusCode::UNKNOWN;
        code = code * 10 + (c - '0');
        if (code > 16) return StatusCode::UNKNOWN;
    }
    if (negative) return StatusCode::UNKNOWN;
    return StatusCodeFromInt(code);
}

std::string ParseGrpcMessage(std::string_view value) {
    return metadata::PercentDecode(value);
}

std::string ParseGrpcStatusDetails(std::string_view value) {
    return metadata::DecodeBase64(value);
}

Status ExtractTrailerStatus(const nghttp2::asio_http2::header_map& trailers) {
    auto it = trailers.find("grpc-status");
    if (it == trailers.end()) {
        return Status{StatusCode::UNKNOWN, "missing grpc-status trailer"};
    }

    StatusCode code = ParseGrpcStatus(it->second.value);
    std::string message;
    std::string details;

    auto msg_it = trailers.find("grpc-message");
    if (msg_it != trailers.end()) {
        message = ParseGrpcMessage(msg_it->second.value);
    }

    auto det_it = trailers.find("grpc-status-details-bin");
    if (det_it != trailers.end()) {
        details = ParseGrpcStatusDetails(det_it->second.value);
    }

    return Status{code, std::move(message), std::move(details)};
}

Status ExtractTrailerStatus(const MetadataMap& trailers) {
    // Build a temporary nghttp2 header_map to reuse ExtractTrailerStatus.
    // For the three grpc-status keys, values are plain strings.
    auto find = [&](std::string_view key) -> std::string_view {
        auto it = trailers.find(std::string(key));
        if (it == trailers.end()) return {};
        return it->second;
    };

    std::string_view status_str = find("grpc-status");
    if (status_str.empty()) {
        return Status{StatusCode::UNKNOWN, "missing grpc-status trailer"};
    }

    StatusCode code = ParseGrpcStatus(status_str);
    std::string message = std::string(ParseGrpcMessage(find("grpc-message")));
    std::string details = std::string(ParseGrpcStatusDetails(find("grpc-status-details-bin")));
    return Status{code, std::move(message), std::move(details)};
}

void BuildTrailers(const Status&                    status,
                   const MetadataMap&               trailing_metadata,
                   nghttp2::asio_http2::header_map& out)
{
    // grpc-status is always first.
    out.emplace("grpc-status",
        nghttp2::asio_http2::header_value{
            std::to_string(static_cast<int>(status.code())), false});

    if (!status.message().empty()) {
        out.emplace("grpc-message",
            nghttp2::asio_http2::header_value{
                metadata::PercentEncode(status.message()), false});
    }

    if (!status.details().empty()) {
        out.emplace("grpc-status-details-bin",
            nghttp2::asio_http2::header_value{
                metadata::EncodeBase64(status.details()), false});
    }

    // Application trailing metadata.
    MetadataToNghttp2Headers(trailing_metadata, out);
}

} // namespace asio_grpc::protocol
