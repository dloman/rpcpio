#pragma once

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <nghttp2/asio_http2_server.h>
#include "rpcpio/server_context.h"
#include "rpcpio/status.h"
#include "rpcpio/internal/raw_server_reader.h"
#include "src/server/raw_server_impls.h"

namespace rpcpio::internal {

// Type-erased handler for a client-streaming RPC.
// Receives: context, reader handle, output reference for response bytes.
// Returns: final Status.
using RawClientStreamingHandler = std::function<
    boost::asio::awaitable<Status>(
        ServerContext&, RawServerReader&, std::string& resp_bytes)>;

// ── StreamingFrameParser ─────────────────────────────────────────────────────
//
// Lightweight stateful parser for a sequence of gRPC LPM frames.
// Unlike FrameDecoder (which is designed for exactly-one-message unary calls),
// this class correctly handles multi-message streams by resetting after each
// complete frame.  Call Feed() with each DATA chunk; it invokes the provided
// callback for every complete proto-bytes payload extracted.
//
// Returns a non-OK Status on framing errors.
class StreamingFrameParser {
public:
    explicit StreamingFrameParser(std::size_t max_message_size)
        : max_message_size_(max_message_size)
    {}

    // Feed chunk bytes into the parser.
    // For each complete LPM frame, calls callback(proto_bytes).
    // Returns non-OK on any framing error.
    template<typename Callback>
    Status Feed(const uint8_t* data, std::size_t len, Callback callback) {
        std::string_view chunk{reinterpret_cast<const char*>(data), len};

        while (!chunk.empty()) {
            if (header_bytes_ < 5) {
                // Accumulate the 5-byte header.
                const std::size_t need = 5 - header_bytes_;
                const std::size_t take = std::min(need, chunk.size());
                std::memcpy(header_buf_.data() + header_bytes_,
                            chunk.data(), take);
                header_bytes_ += take;
                chunk.remove_prefix(take);

                if (header_bytes_ == 5) {
                    // Parse header: byte[0]=compress_flag, bytes[1-4]=length BE.
                    const uint8_t compress = header_buf_[0];
                    if (compress != 0 && compress != 1) {
                        return Status{StatusCode::INVALID_ARGUMENT,
                                      "invalid LPM compression flag"};
                    }
                    payload_len_ =
                        (std::uint32_t(header_buf_[1]) << 24) |
                        (std::uint32_t(header_buf_[2]) << 16) |
                        (std::uint32_t(header_buf_[3]) <<  8) |
                         std::uint32_t(header_buf_[4]);
                    if (payload_len_ > max_message_size_) {
                        return Status{StatusCode::RESOURCE_EXHAUSTED,
                                      "message length exceeds limit"};
                    }
                    payload_.clear();
                    payload_.reserve(payload_len_);
                }
            } else {
                // Accumulate payload bytes.
                const std::size_t need = payload_len_ - payload_.size();
                const std::size_t take = std::min(need, chunk.size());
                payload_.append(chunk.data(), take);
                chunk.remove_prefix(take);

                if (payload_.size() == payload_len_) {
                    // Message complete — invoke callback and reset for next frame.
                    callback(payload_);
                    header_bytes_ = 0;
                    payload_len_  = 0;
                    payload_.clear();
                }
            }
        }
        return Status{};
    }

    // Returns true if the parser is between messages (no partial frame buffered).
    bool between_frames() const noexcept {
        return header_bytes_ == 0 && payload_.empty();
    }

private:
    std::size_t                max_message_size_;
    std::array<uint8_t, 5>    header_buf_{};
    std::size_t                header_bytes_{0};
    std::uint32_t              payload_len_{0};
    std::string                payload_;
};

// ── ClientStreamingCallState ─────────────────────────────────────────────────
//
// Manages one inbound client-streaming call:
//   1. Starts the handler coroutine immediately after metadata is parsed.
//   2. As DATA chunks arrive, feeds decoded LPM messages into a StreamMessageQueue.
//   3. On client half-close (EOS), signals end-of-stream to the queue.
//   4. When the handler returns, sends the response exactly once (same path as
//      the unary ServerCallState).
class ClientStreamingCallState
    : public std::enable_shared_from_this<ClientStreamingCallState>
{
public:
    ClientStreamingCallState(
        boost::asio::io_context&                        ioc,
        const nghttp2::asio_http2::server::request&     req,
        const nghttp2::asio_http2::server::response&    resp,
        RawClientStreamingHandler                       handler,
        std::size_t                                     max_message_size,
        std::size_t                                     max_metadata_size);

    // Register on_data callback and start the handler coroutine.
    void Start();

private:
    void OnData(const uint8_t* data, std::size_t len);
    void SendError(Status status);
    void SendResponse(const Status&      status,
                      std::string_view   resp_bytes,
                      const MetadataMap& initial_meta,
                      const MetadataMap& trailing_meta);

    boost::asio::io_context&                       ioc_;
    const nghttp2::asio_http2::server::request&    req_;
    const nghttp2::asio_http2::server::response&   resp_;
    RawClientStreamingHandler                      handler_;
    std::size_t                                    max_message_size_;
    std::size_t                                    max_metadata_size_;

    StreamingFrameParser      parser_;   // multi-message LPM frame parser
    ServerContext             ctx_;
    boost::asio::steady_timer timer_;
    bool                      responded_{false};

    // Shared with the RawServerReader handed to the handler coroutine.
    std::shared_ptr<RawServerReaderImpl> reader_impl_;
};

} // namespace rpcpio::internal
