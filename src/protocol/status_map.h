#pragma once

#include <string_view>
#include "asio_grpc/status.h"

namespace asio_grpc::protocol {

// Map an nghttp2 library error code (NGHTTP2_ERR_*) to a gRPC StatusCode.
StatusCode NgHttp2ErrToStatusCode(int nghttp2_error) noexcept;

// Map an HTTP/2 RST_STREAM error code to a gRPC StatusCode.
StatusCode Http2ErrorCodeToStatusCode(std::uint32_t error_code) noexcept;

// Map a TLS/SSL handshake error (described as a string) to a StatusCode.
StatusCode TlsErrorToStatusCode(std::string_view description) noexcept;

// Map a Boost.Asio/system error to a StatusCode.
// Handles common cases: connection_refused, timed_out, eof, etc.
StatusCode AsioErrorToStatusCode(int ec_value, bool is_connection) noexcept;

} // namespace asio_grpc::protocol
