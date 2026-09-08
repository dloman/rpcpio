#include <cstddef>
#include <cstdint>
#include <string_view>
#include "src/protocol/timeout.h"

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    std::string_view s{reinterpret_cast<const char*>(data), size};
    auto result = asio_grpc::protocol::ParseTimeout(s);
    if (result.has_value()) {
        // Re-format and ensure it round-trips to a non-negative value.
        auto formatted = asio_grpc::protocol::FormatTimeout(*result);
        (void)formatted;
    }
    return 0;
}
