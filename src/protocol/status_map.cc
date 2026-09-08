#include "status_map.h"

#include <nghttp2/nghttp2.h>
#include <boost/asio/error.hpp>
#include <cerrno>

namespace asio_grpc::protocol {

StatusCode NgHttp2ErrToStatusCode(int err) noexcept {
    switch (err) {
        case NGHTTP2_ERR_REFUSED_STREAM:
        case NGHTTP2_ERR_STREAM_CLOSED:
            return StatusCode::UNAVAILABLE;
        case NGHTTP2_ERR_FLOW_CONTROL:
            return StatusCode::RESOURCE_EXHAUSTED;
        case NGHTTP2_ERR_FRAME_SIZE_ERROR:
            return StatusCode::RESOURCE_EXHAUSTED;
        case NGHTTP2_ERR_PROTO:
        case NGHTTP2_ERR_HTTP_HEADER:
        case NGHTTP2_ERR_HTTP_MESSAGING:
            return StatusCode::INTERNAL;
        default:
            return StatusCode::INTERNAL;
    }
}

StatusCode Http2ErrorCodeToStatusCode(std::uint32_t ec) noexcept {
    // HTTP/2 error codes (RFC 7540 §7).
    switch (ec) {
        case 0x0: // NO_ERROR
            return StatusCode::INTERNAL;    // RST_STREAM with NO_ERROR is odd
        case 0x1: // PROTOCOL_ERROR
        case 0x2: // INTERNAL_ERROR
            return StatusCode::INTERNAL;
        case 0x3: // FLOW_CONTROL_ERROR
        case 0x4: // SETTINGS_TIMEOUT
            return StatusCode::INTERNAL;
        case 0x5: // STREAM_CLOSED
            return StatusCode::INTERNAL;
        case 0x6: // FRAME_SIZE_ERROR
            return StatusCode::RESOURCE_EXHAUSTED;
        case 0x7: // REFUSED_STREAM — safe to retry
            return StatusCode::UNAVAILABLE;
        case 0x8: // CANCEL
            return StatusCode::CANCELLED;
        case 0x9: // COMPRESSION_ERROR
        case 0xA: // CONNECT_ERROR
        case 0xB: // ENHANCE_YOUR_CALM
            return StatusCode::RESOURCE_EXHAUSTED;
        case 0xD: // HTTP_1_1_REQUIRED
            return StatusCode::UNIMPLEMENTED;
        default:
            return StatusCode::INTERNAL;
    }
}

StatusCode TlsErrorToStatusCode(std::string_view) noexcept {
    return StatusCode::UNAVAILABLE;
}

StatusCode AsioErrorToStatusCode(int ec_value, bool is_connection) noexcept {
    if (ec_value == boost::asio::error::connection_refused ||
        ec_value == boost::asio::error::connection_reset  ||
        ec_value == boost::asio::error::broken_pipe       ||
        ec_value == boost::asio::error::network_down      ||
        ec_value == boost::asio::error::network_reset     ||
        ec_value == boost::asio::error::network_unreachable) {
        return StatusCode::UNAVAILABLE;
    }
    if (ec_value == boost::asio::error::timed_out) {
        return StatusCode::DEADLINE_EXCEEDED;
    }
    if (ec_value == boost::asio::error::operation_aborted) {
        return StatusCode::CANCELLED;
    }
    if (ec_value == boost::asio::error::eof) {
        return is_connection ? StatusCode::UNAVAILABLE : StatusCode::INTERNAL;
    }
    if (ec_value == boost::asio::error::host_not_found ||
        ec_value == boost::asio::error::host_not_found_try_again) {
        return StatusCode::UNAVAILABLE;
    }
    return StatusCode::INTERNAL;
}

} // namespace asio_grpc::protocol
