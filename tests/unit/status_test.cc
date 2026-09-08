#include <gtest/gtest.h>
#include "rpcpio/status.h"

using namespace rpcpio;

// ── StatusCode ────────────────────────────────────────────────────────────────

TEST(StatusCode, Names) {
    EXPECT_EQ(StatusCodeName(StatusCode::OK),                   "OK");
    EXPECT_EQ(StatusCodeName(StatusCode::CANCELLED),            "CANCELLED");
    EXPECT_EQ(StatusCodeName(StatusCode::UNKNOWN),              "UNKNOWN");
    EXPECT_EQ(StatusCodeName(StatusCode::INVALID_ARGUMENT),     "INVALID_ARGUMENT");
    EXPECT_EQ(StatusCodeName(StatusCode::DEADLINE_EXCEEDED),    "DEADLINE_EXCEEDED");
    EXPECT_EQ(StatusCodeName(StatusCode::NOT_FOUND),            "NOT_FOUND");
    EXPECT_EQ(StatusCodeName(StatusCode::ALREADY_EXISTS),       "ALREADY_EXISTS");
    EXPECT_EQ(StatusCodeName(StatusCode::PERMISSION_DENIED),    "PERMISSION_DENIED");
    EXPECT_EQ(StatusCodeName(StatusCode::RESOURCE_EXHAUSTED),   "RESOURCE_EXHAUSTED");
    EXPECT_EQ(StatusCodeName(StatusCode::FAILED_PRECONDITION),  "FAILED_PRECONDITION");
    EXPECT_EQ(StatusCodeName(StatusCode::ABORTED),              "ABORTED");
    EXPECT_EQ(StatusCodeName(StatusCode::OUT_OF_RANGE),         "OUT_OF_RANGE");
    EXPECT_EQ(StatusCodeName(StatusCode::UNIMPLEMENTED),        "UNIMPLEMENTED");
    EXPECT_EQ(StatusCodeName(StatusCode::INTERNAL),             "INTERNAL");
    EXPECT_EQ(StatusCodeName(StatusCode::UNAVAILABLE),          "UNAVAILABLE");
    EXPECT_EQ(StatusCodeName(StatusCode::DATA_LOSS),            "DATA_LOSS");
    EXPECT_EQ(StatusCodeName(StatusCode::UNAUTHENTICATED),      "UNAUTHENTICATED");
}

TEST(StatusCode, UnknownValue) {
    // Values outside the known range map to UNKNOWN.
    EXPECT_EQ(StatusCodeFromInt(999), StatusCode::UNKNOWN);
    EXPECT_EQ(StatusCodeFromInt(-1),  StatusCode::UNKNOWN);
    EXPECT_EQ(StatusCodeFromInt(17),  StatusCode::UNKNOWN);
}

// ── Status ────────────────────────────────────────────────────────────────────

TEST(Status, DefaultIsOK) {
    Status s;
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(s.code(), StatusCode::OK);
    EXPECT_TRUE(s.message().empty());
}

TEST(Status, NonOK) {
    Status s{StatusCode::NOT_FOUND, "item not found"};
    EXPECT_FALSE(s.ok());
    EXPECT_EQ(s.code(), StatusCode::NOT_FOUND);
    EXPECT_EQ(s.message(), "item not found");
}

TEST(Status, WithDetails) {
    std::string details = "\x01\x02\x03";
    Status s{StatusCode::INTERNAL, "oops", details};
    EXPECT_EQ(s.details(), details);
}

TEST(Status, Equality) {
    Status a{StatusCode::CANCELLED, "c"};
    Status b{StatusCode::CANCELLED, "c"};
    Status c{StatusCode::CANCELLED, "d"};
    EXPECT_EQ(a, b);
    EXPECT_NE(a, c);
}

TEST(Status, DebugString_OK) {
    EXPECT_EQ(Status{}.DebugString(), "OK");
}

TEST(Status, DebugString_Error) {
    Status s{StatusCode::INTERNAL, "boom"};
    auto d = s.DebugString();
    EXPECT_NE(d.find("INTERNAL"), std::string::npos);
    EXPECT_NE(d.find("boom"), std::string::npos);
}

TEST(Status, FromHttpStatus_400) {
    auto s = Status::FromHttpStatus(400);
    EXPECT_EQ(s.code(), StatusCode::INTERNAL);
}

TEST(Status, FromHttpStatus_404) {
    auto s = Status::FromHttpStatus(404);
    EXPECT_EQ(s.code(), StatusCode::UNIMPLEMENTED);
}

TEST(Status, FromHttpStatus_503) {
    auto s = Status::FromHttpStatus(503);
    EXPECT_EQ(s.code(), StatusCode::UNAVAILABLE);
}

TEST(Status, FromHttpStatus_Unknown) {
    auto s = Status::FromHttpStatus(599);
    EXPECT_EQ(s.code(), StatusCode::UNKNOWN);
}

// ── StatusOr<T> ───────────────────────────────────────────────────────────────

TEST(StatusOr, FromValue) {
    StatusOr<int> s = 42;
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(*s, 42);
    EXPECT_EQ(s.value(), 42);
}

TEST(StatusOr, FromError) {
    StatusOr<int> s{Status{StatusCode::NOT_FOUND, "gone"}};
    EXPECT_FALSE(s.ok());
    EXPECT_EQ(s.status().code(), StatusCode::NOT_FOUND);
}

TEST(StatusOr, FromOKStatusThrows) {
    EXPECT_THROW(StatusOr<int>{Status{}}, std::invalid_argument);
}

TEST(StatusOr, ValueOnErrorThrows) {
    StatusOr<int> s{Status{StatusCode::INTERNAL, "boom"}};
    EXPECT_THROW(s.value(), std::runtime_error);
}

TEST(StatusOr, Arrow) {
    StatusOr<std::string> s = std::string{"hello"};
    EXPECT_EQ(s->size(), 5u);
}

// ── UnaryResult<T> ────────────────────────────────────────────────────────────

TEST(UnaryResult, DefaultFields) {
    UnaryResult<int> r;
    // Default status is OK.
    EXPECT_TRUE(r.status.ok());
    EXPECT_FALSE(r.response.has_value());
    EXPECT_TRUE(r.initial_metadata.empty());
    EXPECT_TRUE(r.trailing_metadata.empty());
}
