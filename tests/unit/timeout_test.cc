#include <gtest/gtest.h>
#include "src/protocol/timeout.h"

using namespace asio_grpc::protocol;
using namespace std::chrono_literals;

// ── FormatTimeout ─────────────────────────────────────────────────────────────

TEST(FormatTimeout, Zero) {
    EXPECT_EQ(FormatTimeout(0ns), "");
}

TEST(FormatTimeout, Negative) {
    EXPECT_EQ(FormatTimeout(-1ns), "");
}

TEST(FormatTimeout, Nanoseconds) {
    EXPECT_EQ(FormatTimeout(500ns), "500n");
}

TEST(FormatTimeout, Microseconds) {
    EXPECT_EQ(FormatTimeout(1000ns), "1u");
}

TEST(FormatTimeout, Milliseconds) {
    EXPECT_EQ(FormatTimeout(1'000'000ns), "1m");
}

TEST(FormatTimeout, Seconds) {
    EXPECT_EQ(FormatTimeout(1'000'000'000ns), "1S");
}

TEST(FormatTimeout, Minutes) {
    EXPECT_EQ(FormatTimeout(60'000'000'000ns), "1M");
}

TEST(FormatTimeout, Hours) {
    EXPECT_EQ(FormatTimeout(3'600'000'000'000ns), "1H");
}

TEST(FormatTimeout, RoundsOutward) {
    // 1001 nanoseconds should not be shortened to 1 microsecond.
    auto result = FormatTimeout(1001ns);
    // Should be 1001n (fits in 8 digits as nanoseconds) or 2u (ceiling microseconds).
    // 1001 ns ceiling to microseconds = 2 us.  Our code picks coarsest unit.
    // ceil(1001/1000) = 2, 2 <= 99999999 → "2u"
    EXPECT_EQ(result, "2u");
}

TEST(FormatTimeout, MaxDigitsNanoseconds) {
    // 99999999 nanoseconds is the largest value in nanoseconds unit.
    EXPECT_EQ(FormatTimeout(std::chrono::nanoseconds(99999999)), "99999999n");
}

// ── ParseTimeout ─────────────────────────────────────────────────────────────

TEST(ParseTimeout, Nanoseconds) {
    auto r = ParseTimeout("100n");
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(*r, 100ns);
}

TEST(ParseTimeout, Microseconds) {
    auto r = ParseTimeout("500u");
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(*r, 500'000ns);
}

TEST(ParseTimeout, Milliseconds) {
    auto r = ParseTimeout("200m");
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(*r, 200'000'000ns);
}

TEST(ParseTimeout, Seconds) {
    auto r = ParseTimeout("30S");
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(*r, 30'000'000'000ns);
}

TEST(ParseTimeout, Minutes) {
    auto r = ParseTimeout("5M");
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(*r, 300'000'000'000ns);
}

TEST(ParseTimeout, Hours) {
    auto r = ParseTimeout("2H");
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(*r, std::chrono::nanoseconds(7'200'000'000'000LL));
}

TEST(ParseTimeout, EmptyString) {
    EXPECT_FALSE(ParseTimeout("").has_value());
}

TEST(ParseTimeout, UnknownUnit) {
    EXPECT_FALSE(ParseTimeout("100x").has_value());
}

TEST(ParseTimeout, TooManyDigits) {
    EXPECT_FALSE(ParseTimeout("100000000n").has_value());  // 9 digits > 8
}

TEST(ParseTimeout, NonDigitChars) {
    EXPECT_FALSE(ParseTimeout("1a0n").has_value());
}

TEST(ParseTimeout, Overflow) {
    // A value that would overflow int64_t when multiplied by ns_per.
    // 99999999H = 99999999 * 3600000000000 ns — overflows int64.
    EXPECT_FALSE(ParseTimeout("99999999H").has_value());
}

TEST(ParseTimeout, RoundtripSmall) {
    // Small values should survive format→parse.
    auto dur = std::chrono::nanoseconds(12345678);
    auto s = FormatTimeout(dur);
    auto r = ParseTimeout(s);
    ASSERT_TRUE(r.has_value());
    // After rounding outward, parse value >= original.
    EXPECT_GE(r->count(), dur.count());
}
