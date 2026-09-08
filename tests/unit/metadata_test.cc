#include <gtest/gtest.h>
#include "rpcpio/metadata.h"

using namespace rpcpio;
using namespace rpcpio::metadata;

// ── Key validation ────────────────────────────────────────────────────────────

TEST(MetadataKey, ValidLowercase) {
    EXPECT_TRUE(IsValidKey("foo"));
    EXPECT_TRUE(IsValidKey("foo-bar"));
    EXPECT_TRUE(IsValidKey("foo_bar"));
    EXPECT_TRUE(IsValidKey("foo123"));
    EXPECT_TRUE(IsValidKey("x-custom-header"));
}

TEST(MetadataKey, InvalidUppercase) {
    EXPECT_FALSE(IsValidKey("Foo"));
    EXPECT_FALSE(IsValidKey("FOO"));
}

TEST(MetadataKey, InvalidChars) {
    EXPECT_FALSE(IsValidKey("foo bar"));
    EXPECT_FALSE(IsValidKey("foo:bar"));
    EXPECT_FALSE(IsValidKey("foo.bar"));
    EXPECT_FALSE(IsValidKey(""));
}

TEST(MetadataKey, BinaryKey) {
    EXPECT_TRUE(IsBinaryKey("foo-bin"));
    EXPECT_TRUE(IsBinaryKey("x-custom-bin"));
    EXPECT_FALSE(IsBinaryKey("foo"));
    EXPECT_FALSE(IsBinaryKey("bin"));  // no prefix before "-bin"
}

TEST(MetadataKey, ReservedGrpc) {
    EXPECT_TRUE(IsReservedKey("grpc-status"));
    EXPECT_TRUE(IsReservedKey("grpc-message"));
    EXPECT_TRUE(IsReservedKey("grpc-timeout"));
    EXPECT_TRUE(IsReservedKey("grpc-encoding"));
}

TEST(MetadataKey, ReservedHttp) {
    EXPECT_TRUE(IsReservedKey("content-type"));
    EXPECT_TRUE(IsReservedKey("te"));
    EXPECT_TRUE(IsReservedKey(":status"));
    EXPECT_TRUE(IsReservedKey(":path"));
}

TEST(MetadataKey, NotReserved) {
    EXPECT_FALSE(IsReservedKey("x-custom"));
    EXPECT_FALSE(IsReservedKey("authorization"));
}

// ── Base64 ────────────────────────────────────────────────────────────────────

TEST(Base64, EmptyRoundtrip) {
    EXPECT_EQ(DecodeBase64(EncodeBase64("")), "");
}

TEST(Base64, SingleByteRoundtrip) {
    for (int i = 0; i < 256; ++i) {
        std::string b{static_cast<char>(i)};
        EXPECT_EQ(DecodeBase64(EncodeBase64(b)), b) << "byte=" << i;
    }
}

TEST(Base64, PaddedInput) {
    // Standard padded base64 for "hello"
    std::string padded = "aGVsbG8=";
    EXPECT_EQ(DecodeBase64(padded), "hello");
}

TEST(Base64, UnpaddedOutput) {
    // Our encoder produces unpadded output.
    auto enc = EncodeBase64("hello");
    EXPECT_EQ(enc, "aGVsbG8");  // no trailing '='
}

TEST(Base64, BinaryData) {
    std::string data;
    for (int i = 0; i < 256; ++i) data += static_cast<char>(i);
    EXPECT_EQ(DecodeBase64(EncodeBase64(data)), data);
}

// ── PercentEncode/Decode ──────────────────────────────────────────────────────

TEST(PercentEncode, PrintableAsciiPassthrough) {
    EXPECT_EQ(PercentEncode("hello"), "hello");
    EXPECT_EQ(PercentEncode("Hello World!"), "Hello%20World!");
}

TEST(PercentEncode, PercentSign) {
    EXPECT_EQ(PercentEncode("%"), "%25");
}

TEST(PercentEncode, NonAscii) {
    // UTF-8 bytes above 0x7E are encoded.
    std::string s = "\xC3\xA9";  // é in UTF-8
    EXPECT_EQ(PercentEncode(s), "%C3%A9");
}

TEST(PercentDecode, Simple) {
    EXPECT_EQ(PercentDecode("hello%20world"), "hello world");
}

TEST(PercentDecode, MalformedEscape) {
    // Malformed: pass through the '%' unchanged.
    EXPECT_EQ(PercentDecode("%GG"), "%GG");
}

TEST(PercentDecode, TrailingPercent) {
    EXPECT_EQ(PercentDecode("foo%"), "foo%");
}

TEST(PercentDecode, Roundtrip) {
    std::string original = "Hello, World! \xC3\xA9 100%";
    EXPECT_EQ(PercentDecode(PercentEncode(original)), original);
}

// ── ValidateAndAdd ────────────────────────────────────────────────────────────

TEST(ValidateAndAdd, Valid) {
    MetadataMap m;
    EXPECT_NO_THROW(ValidateAndAdd(m, "x-custom", "value"));
    ASSERT_EQ(m.size(), 1u);
}

TEST(ValidateAndAdd, InvalidKey) {
    MetadataMap m;
    EXPECT_THROW(ValidateAndAdd(m, "INVALID", "value"), std::invalid_argument);
    EXPECT_THROW(ValidateAndAdd(m, "has space", "value"), std::invalid_argument);
}

TEST(ValidateAndAdd, ReservedKey) {
    MetadataMap m;
    EXPECT_THROW(ValidateAndAdd(m, "grpc-status", "0"), std::invalid_argument);
    EXPECT_THROW(ValidateAndAdd(m, "content-type", "application/grpc"), std::invalid_argument);
}

TEST(ValidateAndAdd, DuplicateKeys) {
    MetadataMap m;
    ValidateAndAdd(m, "x-custom", "v1");
    ValidateAndAdd(m, "x-custom", "v2");
    EXPECT_EQ(m.count("x-custom"), 2u);
}
