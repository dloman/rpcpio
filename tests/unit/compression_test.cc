#include <gtest/gtest.h>
#include "src/protocol/compression.h"

using namespace asio_grpc::protocol;

TEST(Compression, IdentityRoundtrip) {
    std::string src = "hello compression world";
    std::string out;
    ASSERT_TRUE(Compress(Encoding::kIdentity, src, out));
    EXPECT_EQ(out, src);

    std::string dec;
    ASSERT_TRUE(Decompress(Encoding::kIdentity, out, dec));
    EXPECT_EQ(dec, src);
}

TEST(Compression, IdentityEmptyInput) {
    std::string out;
    ASSERT_TRUE(Compress(Encoding::kIdentity, "", out));
    EXPECT_TRUE(out.empty());
}

TEST(Compression, IdentityDecompressionBomb) {
    // Large input should be rejected when it exceeds max_output_size.
    std::string big(100, 'x');
    std::string out;
    EXPECT_FALSE(Decompress(Encoding::kIdentity, big, out, 50));
}

TEST(Compression, ParseEncoding_Identity) {
    auto enc = ParseEncoding("identity");
    ASSERT_TRUE(enc.has_value());
    EXPECT_EQ(*enc, Encoding::kIdentity);
}

TEST(Compression, ParseEncoding_Unknown) {
    EXPECT_FALSE(ParseEncoding("deflate").has_value());
    EXPECT_FALSE(ParseEncoding("br").has_value());
    EXPECT_FALSE(ParseEncoding("").has_value());
}

TEST(Compression, EncodingName) {
    EXPECT_EQ(EncodingName(Encoding::kIdentity), "identity");
}

TEST(Compression, AcceptEncodingIncludesIdentity) {
    auto val = AcceptEncodingValue();
    EXPECT_NE(val.find("identity"), std::string::npos);
}

#ifdef ASIO_GRPC_ENABLE_GZIP
TEST(Compression, GzipRoundtrip) {
    std::string src(1000, 'a');  // highly compressible
    std::string compressed;
    ASSERT_TRUE(Compress(Encoding::kGzip, src, compressed));
    EXPECT_LT(compressed.size(), src.size());  // should be smaller

    std::string decompressed;
    ASSERT_TRUE(Decompress(Encoding::kGzip, compressed, decompressed));
    EXPECT_EQ(decompressed, src);
}

TEST(Compression, GzipDecompressionBomb) {
    // Compressed form of a repetitive string should expand; clamp at limit.
    std::string src(1024 * 1024, 'z');
    std::string compressed;
    ASSERT_TRUE(Compress(Encoding::kGzip, src, compressed));

    std::string out;
    // Decompress with a tiny limit should fail.
    EXPECT_FALSE(Decompress(Encoding::kGzip, compressed, out, 1024));
}
#endif
