#pragma once

#include <string_view>

namespace rpcpio {

// Compile-time descriptor for one unary RPC method.
// Instances are typically constexpr static members of a generated Stub class.
//
// Example:
//   static constexpr UnaryMethod<HelloRequest, HelloReply>
//       SayHello{"/helloworld.Greeter/SayHello"};
template<typename Request, typename Response>
struct UnaryMethod {
    using request_type  = Request;
    using response_type = Response;

    // path must be a stable string (e.g. a string literal or a constexpr string).
    constexpr explicit UnaryMethod(std::string_view path) noexcept
        : path(path) {}

    const std::string_view path;  // "/fully.qualified.Service/MethodName"
};

} // namespace rpcpio
