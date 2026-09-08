#include "framing.h"

#include <algorithm>
#include <cstring>
#include <limits>

namespace rpcpio::protocol {

// ── Encoder ──────────────────────────────────────────────────────────────────

bool EncodeFrame(std::uint8_t     compress_flag,
                 std::string_view payload,
                 std::string&     out) {
    if (payload.size() > std::numeric_limits<std::uint32_t>::max())
        return false;

    auto len = static_cast<std::uint32_t>(payload.size());
    out.clear();
    out.reserve(kLpmHeaderSize + payload.size());

    out += static_cast<char>(compress_flag);
    out += static_cast<char>((len >> 24) & 0xFF);
    out += static_cast<char>((len >> 16) & 0xFF);
    out += static_cast<char>((len >>  8) & 0xFF);
    out += static_cast<char>( len        & 0xFF);
    out += payload;
    return true;
}

// ── FrameDecoder ─────────────────────────────────────────────────────────────

FrameDecoder::FrameDecoder(std::size_t max_message_size)
    : max_message_size_(max_message_size)
{}

void FrameDecoder::SetError(std::string msg) {
    state_         = State::kError;
    error_message_ = std::move(msg);
}

void FrameDecoder::TryParseHeader() {
    if (header_bytes_ < kLpmHeaderSize) return;

    compress_flag_ = header_buf_[0];

    // Reject reserved compression flags (only 0 and 1 are defined).
    if (compress_flag_ != kFlagUncompressed && compress_flag_ != kFlagCompressed) {
        SetError("reserved compression flag: " +
                 std::to_string(static_cast<int>(compress_flag_)));
        return;
    }

    payload_len_ =
        (std::uint32_t(header_buf_[1]) << 24) |
        (std::uint32_t(header_buf_[2]) << 16) |
        (std::uint32_t(header_buf_[3]) <<  8) |
         std::uint32_t(header_buf_[4]);

    // Reject before allocating.
    if (payload_len_ > max_message_size_) {
        SetError("message length " + std::to_string(payload_len_) +
                 " exceeds limit " + std::to_string(max_message_size_));
        return;
    }

    payload_.clear();
    payload_.reserve(payload_len_);
    state_ = State::kAwaitingPayload;
}

void FrameDecoder::Feed(std::string_view chunk) {
    if (state_ == State::kError) return;

    while (!chunk.empty()) {
        switch (state_) {
            case State::kAwaitingHeader: {
                // Fill the 5-byte header buffer.
                std::size_t need = kLpmHeaderSize - header_bytes_;
                std::size_t take = std::min(need, chunk.size());
                std::memcpy(header_buf_ + header_bytes_, chunk.data(), take);
                header_bytes_ += take;
                chunk.remove_prefix(take);
                TryParseHeader();
                if (state_ == State::kError) return;
                break;
            }
            case State::kAwaitingPayload: {
                std::size_t need = payload_len_ - payload_.size();
                std::size_t take = std::min(need, chunk.size());
                payload_.append(chunk.data(), take);
                chunk.remove_prefix(take);
                if (payload_.size() == payload_len_) {
                    has_message_ = true;
                    state_       = State::kDone;
                    // Fall through: check for spurious trailing bytes.
                    if (!chunk.empty()) {
                        SetError("unexpected bytes after single unary message");
                        return;
                    }
                }
                break;
            }
            case State::kDone:
                // A second message in a unary call is an error.
                SetError("unexpected second message in unary call");
                return;
            case State::kError:
                return;
        }
    }
}

void FrameDecoder::MarkEos() {
    if (state_ == State::kError || state_ == State::kDone) return;
    // Incomplete header or payload at EOS is an error.
    SetError("stream ended with incomplete message");
}

} // namespace rpcpio::protocol
