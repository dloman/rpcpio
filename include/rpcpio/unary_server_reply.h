#pragma once

#include <memory>
#include <string>
#include "rpcpio/metadata.h"
#include "rpcpio/status.h"

namespace rpcpio {

namespace internal { class ServerCallState; }

// Exactly-once reply handle for callback-based unary server handlers.
// Finish() is safe to call from any executor; work is dispatched to the
// server's io_context before touching nghttp2 resources.
class UnaryServerReply {
public:
    UnaryServerReply() = default;

    // Send the gRPC response (or trailers-only error).  Ignored after the
    // first successful invocation.
    void Finish(Status              status,
                std::string         response_bytes = {},
                MetadataMap         initial_metadata = {},
                MetadataMap         trailing_metadata = {});

    [[nodiscard]] bool finished() const noexcept;

private:
    friend class internal::ServerCallState;

    explicit UnaryServerReply(std::shared_ptr<internal::ServerCallState> state);

    std::shared_ptr<internal::ServerCallState> state_;
};

} // namespace rpcpio
