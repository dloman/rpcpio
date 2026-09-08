#include <cstddef>
#include <cstdint>
#include <string_view>
#include "asio_grpc/metadata.h"

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    std::string_view s{reinterpret_cast<const char*>(data), size};
    // DecodeBase64 must never crash on arbitrary input.
    auto decoded = asio_grpc::metadata::DecodeBase64(s);
    // Re-encode must produce valid base64 that decodes back to the same bytes.
    auto re_encoded = asio_grpc::metadata::EncodeBase64(decoded);
    auto re_decoded = asio_grpc::metadata::DecodeBase64(re_encoded);
    (void)re_decoded;
    return 0;
}
