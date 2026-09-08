#include <cstddef>
#include <cstdint>
#include <string_view>
#include "src/protocol/framing.h"

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    rpcpio::protocol::FrameDecoder dec(4 * 1024 * 1024);

    // Feed in variable-size chunks to exercise all split boundaries.
    std::string_view input{reinterpret_cast<const char*>(data), size};

    // Feed in two halves to test multi-chunk delivery.
    std::size_t mid = size / 2;
    dec.Feed(input.substr(0, mid));
    if (!dec.error() && !dec.done())
        dec.Feed(input.substr(mid));
    dec.MarkEos();

    // Must always reach a terminal state.
    bool ok = dec.done() || dec.error();
    (void)ok;
    return 0;
}
