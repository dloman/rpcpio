#pragma once

#include <memory>
#include <string_view>
#include "rpcpio/status.h"

namespace rpcpio::internal {

struct RawServerWriterImpl;

// Opaque handle to the server-side write channel.
// ServerWriter<T> wraps this to provide typed, serializing Write().
class RawServerWriter {
public:
    explicit RawServerWriter(std::shared_ptr<RawServerWriterImpl> impl) noexcept;

    // Write serialized proto bytes as a gRPC LPM frame to the response stream.
    Status Write(std::string_view proto_bytes);

    // Internal: called by the streaming call state to signal the handler finished.
    // After Finish(), no more Write() calls are allowed.
    void Finish(Status status);

    bool valid() const noexcept { return impl_ != nullptr; }

private:
    std::shared_ptr<RawServerWriterImpl> impl_;
};

} // namespace rpcpio::internal
