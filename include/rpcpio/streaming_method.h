#pragma once

#include <string_view>

namespace rpcpio {

// Descriptor for a server-streaming RPC: one request, many responses.
template<typename Request, typename Response>
struct ServerStreamingMethod {
    using request_type  = Request;
    using response_type = Response;
    constexpr explicit ServerStreamingMethod(std::string_view path) noexcept
        : path(path) {}
    const std::string_view path;
};

// Descriptor for a client-streaming RPC: many requests, one response.
template<typename Request, typename Response>
struct ClientStreamingMethod {
    using request_type  = Request;
    using response_type = Response;
    constexpr explicit ClientStreamingMethod(std::string_view path) noexcept
        : path(path) {}
    const std::string_view path;
};

// Descriptor for a bidirectional-streaming RPC: many requests, many responses.
template<typename Request, typename Response>
struct BidiStreamingMethod {
    using request_type  = Request;
    using response_type = Response;
    constexpr explicit BidiStreamingMethod(std::string_view path) noexcept
        : path(path) {}
    const std::string_view path;
};

} // namespace rpcpio
