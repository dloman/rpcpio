#include <gtest/gtest.h>
#include "src/protocol/framing.h"

using namespace rpcpio::protocol;

// ── EncodeFrame ───────────────────────────────────────────────────────────────

TEST(EncodeFrame, EmptyPayload) {
    std::string out;
    ASSERT_TRUE(EncodeFrame(0, "", out));
    ASSERT_EQ(out.size(), 5u);
    EXPECT_EQ(static_cast<uint8_t>(out[0]), 0u);   // compression flag
    EXPECT_EQ(static_cast<uint8_t>(out[1]), 0u);   // length MSB
    EXPECT_EQ(static_cast<uint8_t>(out[4]), 0u);   // length LSB
}

TEST(EncodeFrame, SmallPayload) {
    std::string out;
    std::string payload = "hello";
    ASSERT_TRUE(EncodeFrame(0, payload, out));
    ASSERT_EQ(out.size(), 5u + payload.size());
    EXPECT_EQ(static_cast<uint8_t>(out[0]), 0u);
    // Length = 5 (big-endian)
    EXPECT_EQ(static_cast<uint8_t>(out[4]), 5u);
    EXPECT_EQ(out.substr(5), "hello");
}

TEST(EncodeFrame, CompressedFlag) {
    std::string out;
    ASSERT_TRUE(EncodeFrame(1, "data", out));
    EXPECT_EQ(static_cast<uint8_t>(out[0]), 1u);
}

// ── FrameDecoder — basic ──────────────────────────────────────────────────────

TEST(FrameDecoder, DecodeRoundtrip) {
    std::string frame;
    EncodeFrame(0, "hello world", frame);

    FrameDecoder dec;
    dec.Feed(frame);
    dec.MarkEos();

    ASSERT_EQ(dec.state(), FrameDecoder::State::kDone);
    EXPECT_EQ(dec.payload(), "hello world");
    EXPECT_EQ(dec.compress_flag(), 0u);
}

TEST(FrameDecoder, EmptyMessage) {
    std::string frame;
    EncodeFrame(0, "", frame);

    FrameDecoder dec;
    dec.Feed(frame);
    dec.MarkEos();

    ASSERT_EQ(dec.state(), FrameDecoder::State::kDone);
    EXPECT_EQ(dec.payload(), "");
}

TEST(FrameDecoder, ByteByByteFeeding) {
    std::string payload = "byte-by-byte test payload";
    std::string frame;
    EncodeFrame(0, payload, frame);

    FrameDecoder dec;
    for (char c : frame) {
        dec.Feed({&c, 1});
        if (dec.error()) break;
    }
    dec.MarkEos();

    ASSERT_EQ(dec.state(), FrameDecoder::State::kDone);
    EXPECT_EQ(dec.payload(), payload);
}

TEST(FrameDecoder, CoalescedChunks) {
    std::string p1 = "first ", p2 = "second";
    std::string frame1, frame2;
    EncodeFrame(0, p1, frame1);
    EncodeFrame(0, p2, frame2);

    // FrameDecoder for unary: only one message allowed.
    FrameDecoder dec;
    dec.Feed(frame1 + frame2);  // two frames coalesced
    // Should decode first message, then error on second.
    EXPECT_TRUE(dec.error());
}

TEST(FrameDecoder, TruncatedHeader) {
    std::string frame;
    EncodeFrame(0, "payload", frame);

    FrameDecoder dec;
    dec.Feed(frame.substr(0, 3));  // only 3 of 5 header bytes
    dec.MarkEos();
    EXPECT_TRUE(dec.error());
}

TEST(FrameDecoder, TruncatedPayload) {
    std::string frame;
    EncodeFrame(0, "payload", frame);

    FrameDecoder dec;
    dec.Feed(frame.substr(0, frame.size() - 2));  // missing last 2 bytes
    dec.MarkEos();
    EXPECT_TRUE(dec.error());
}

TEST(FrameDecoder, ReservedCompressionFlag) {
    std::string frame;
    EncodeFrame(0, "data", frame);
    frame[0] = 0x42;  // reserved flag value

    FrameDecoder dec;
    dec.Feed(frame);
    EXPECT_TRUE(dec.error());
    EXPECT_FALSE(dec.error_message().empty());
}

TEST(FrameDecoder, MessageTooLarge) {
    // Decoder with 10-byte limit.
    FrameDecoder dec(10);

    // Craft a frame claiming 11 bytes.
    std::string frame;
    frame += '\x00';  // no compression
    frame += '\x00'; frame += '\x00'; frame += '\x00'; frame += '\x0B';  // length = 11
    frame += "hello world"; // 11 bytes

    dec.Feed(frame);
    EXPECT_TRUE(dec.error());
}

TEST(FrameDecoder, MultipleChunksForOneMessage) {
    std::string payload = "a slightly longer test payload for chunking";
    std::string frame;
    EncodeFrame(0, payload, frame);

    // Split the frame into 3 roughly equal parts.
    std::size_t n = frame.size();
    FrameDecoder dec;
    dec.Feed(frame.substr(0, n / 3));
    dec.Feed(frame.substr(n / 3, n / 3));
    dec.Feed(frame.substr(2 * (n / 3)));
    dec.MarkEos();

    ASSERT_EQ(dec.state(), FrameDecoder::State::kDone);
    EXPECT_EQ(dec.payload(), payload);
}
