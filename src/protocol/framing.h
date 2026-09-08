#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace rpcpio::protocol {

// ── gRPC length-prefixed message framing ─────────────────────────────────────
//
// Wire format (per gRPC HTTP/2 specification):
//   Byte 0:     compression flag (0 = identity, 1 = compressed)
//   Bytes 1-4:  big-endian uint32 message length
//   Bytes 5+:   message payload
//
// The 5-byte header is called a "length-prefixed message prefix" (LPM prefix).

static constexpr std::size_t kLpmHeaderSize    = 5;
static constexpr std::uint8_t kFlagUncompressed = 0;
static constexpr std::uint8_t kFlagCompressed   = 1;

// Encode a single message into a gRPC LPM frame.
// compress_flag: 0 for identity, 1 if payload is already compressed.
// Returns false if payload exceeds the 32-bit frame-size limit (~4 GiB).
bool EncodeFrame(std::uint8_t     compress_flag,
                 std::string_view payload,
                 std::string&     out);

// ── Incremental decoder ───────────────────────────────────────────────────────

// Stateful, bounded incremental decoder for a single unary gRPC message.
// Accepts arbitrary HTTP/2 DATA chunk boundaries.  Call Feed() with each
// chunk; inspect state after each call.
//
// For unary calls:
//   - Exactly one message is allowed.
//   - After the message is complete, subsequent non-empty chunks → error.
class FrameDecoder {
public:
    enum class State {
        kAwaitingHeader,   // collecting the 5-byte prefix
        kAwaitingPayload,  // collecting payload bytes
        kDone,             // message complete; payload_ holds the data
        kError,            // irrecoverable decode error
    };

    explicit FrameDecoder(std::size_t max_message_size = 4 * 1024 * 1024);

    // Feed a chunk of DATA frame bytes into the decoder.
    // May be called zero or more times.  A zero-length chunk signals EOS
    // (End Of Stream); if no complete message was received it is an error
    // only if the caller explicitly marks it as such via MarkEos().
    void Feed(std::string_view chunk);

    // Signal that no more data will arrive.  If the message was not fully
    // received, transitions to kError.
    void MarkEos();

    State state() const noexcept { return state_; }
    bool  done()  const noexcept { return state_ == State::kDone; }
    bool  error() const noexcept { return state_ == State::kError; }

    // Valid only when state() == kDone.
    const std::string& payload()       const noexcept { return payload_; }
    std::uint8_t       compress_flag() const noexcept { return compress_flag_; }

    // Description of the error (valid when state() == kError).
    const std::string& error_message() const noexcept { return error_message_; }

    // Reset to kAwaitingHeader; reuses max_message_size_.
    // Call between successive messages in a streaming context.
    void Reset();

    // True once one complete message has been seen (even before payload is consumed).
    bool has_message() const noexcept { return has_message_; }

private:
    void SetError(std::string msg);
    void TryParseHeader();

    State         state_{State::kAwaitingHeader};
    std::size_t   max_message_size_;
    bool          has_message_{false};

    // Header accumulation (5 bytes)
    std::uint8_t  header_buf_[kLpmHeaderSize]{};
    std::size_t   header_bytes_{0};

    // Payload accumulation
    std::uint8_t  compress_flag_{0};
    std::uint32_t payload_len_{0};
    std::string   payload_;

    std::string   error_message_;
};

} // namespace rpcpio::protocol
