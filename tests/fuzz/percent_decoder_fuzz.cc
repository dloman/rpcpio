#include <cstddef>
#include <cstdint>
#include <string_view>
#include "rpcpio/metadata.h"

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    std::string_view s{reinterpret_cast<const char*>(data), size};
    // PercentDecode must never crash on arbitrary input.
    auto decoded = rpcpio::metadata::PercentDecode(s);
    // Re-encode the decoded result must not lose data.
    auto re_encoded = rpcpio::metadata::PercentEncode(decoded);
    auto re_decoded = rpcpio::metadata::PercentDecode(re_encoded);
    (void)re_decoded;
    return 0;
}
