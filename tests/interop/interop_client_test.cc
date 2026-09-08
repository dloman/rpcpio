// gRPC interoperability client test.
//
// Launches unary calls against a reference gRPC server
// (official C++, Go, or Python implementation) and verifies results.
//
// Environment variables:
//   TEST_SERVER_HOST  (default: "localhost")
//   TEST_SERVER_PORT  (default: 10000)
//   TEST_USE_TLS      (default: "false")

#include <cstdlib>
#include <string>
#include <thread>
#include <boost/asio/io_context.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/use_future.hpp>
#include <gtest/gtest.h>
#include "grpc_testing.rpcpio.pb.h"

static std::string GetEnv(const char* name, const char* def) {
    const char* v = std::getenv(name);
    return v ? std::string{v} : def;
}

class InteropClientTest : public ::testing::Test {
protected:
    void SetUp() override {
        host_  = GetEnv("TEST_SERVER_HOST", "localhost");
        port_  = static_cast<std::uint16_t>(
            std::stoi(GetEnv("TEST_SERVER_PORT", "10000")));
        use_tls_ = GetEnv("TEST_USE_TLS", "false") == "true";

        rpcpio::ChannelOptions opts;
        opts.use_tls    = use_tls_;
        opts.use_h2c    = !use_tls_;
        opts.verify_peer = false;  // test CA may not be in system store

        channel_ = std::make_shared<rpcpio::Channel>(ioc_, host_, port_, opts);
        stub_    = std::make_unique<grpc::testing::TestServiceStub>(channel_);
    }

    template<typename Coro>
    auto Run(Coro coro) {
        auto fut = boost::asio::co_spawn(ioc_, std::move(coro),
                                          boost::asio::use_future);
        ioc_.run();
        ioc_.restart();
        return fut.get();
    }

    boost::asio::io_context ioc_;
    std::string host_;
    std::uint16_t port_;
    bool use_tls_;
    std::shared_ptr<rpcpio::Channel> channel_;
    std::unique_ptr<grpc::testing::TestServiceStub> stub_;
};

// ── empty_unary ───────────────────────────────────────────────────────────────

TEST_F(InteropClientTest, EmptyUnary) {
    Run([this]() -> boost::asio::awaitable<void> {
        rpcpio::ClientContext ctx;
        grpc::testing::Empty request;
        auto result = co_await stub_->EmptyCall(ctx, request);
        EXPECT_TRUE(result.status.ok()) << result.status.DebugString();
    });
}

// ── large_unary ───────────────────────────────────────────────────────────────

TEST_F(InteropClientTest, LargeUnary) {
    Run([this]() -> boost::asio::awaitable<void> {
        rpcpio::ClientContext ctx;
        grpc::testing::SimpleRequest request;
        request.set_response_size(314159);
        request.mutable_payload()->set_body(std::string(271828, '\0'));
        auto result = co_await stub_->UnaryCall(ctx, request);
        ASSERT_TRUE(result.status.ok()) << result.status.DebugString();
        EXPECT_EQ(result.response->payload().body().size(), 314159u);
    });
}

// ── status_code_and_message ───────────────────────────────────────────────────

TEST_F(InteropClientTest, StatusCodeAndMessage) {
    Run([this]() -> boost::asio::awaitable<void> {
        rpcpio::ClientContext ctx;
        grpc::testing::SimpleRequest request;
        request.mutable_response_status()->set_code(2);  // UNKNOWN
        request.mutable_response_status()->set_message("test status message");
        auto result = co_await stub_->UnaryCall(ctx, request);
        EXPECT_EQ(result.status.code(), rpcpio::StatusCode::UNKNOWN);
        EXPECT_EQ(result.status.message(), "test status message");
    });
}

// ── custom_metadata ───────────────────────────────────────────────────────────

TEST_F(InteropClientTest, CustomMetadata) {
    Run([this]() -> boost::asio::awaitable<void> {
        rpcpio::ClientContext ctx;
        ctx.AddMetadata("x-grpc-test-echo-initial",   "test_initial_metadata_value");
        ctx.AddMetadata("x-grpc-test-echo-trailing-bin",
                        std::string("\xab\xab\xab\xab\xab", 5));

        grpc::testing::SimpleRequest request;
        request.set_response_size(1);
        auto result = co_await stub_->UnaryCall(ctx, request);
        ASSERT_TRUE(result.status.ok()) << result.status.DebugString();

        // Check that the server echoed the initial metadata.
        EXPECT_EQ(result.initial_metadata.count("x-grpc-test-echo-initial"), 1u);
    });
}

// ── timeout_on_sleeping_server ────────────────────────────────────────────────

TEST_F(InteropClientTest, TimeoutOnSleepingServer) {
    Run([this]() -> boost::asio::awaitable<void> {
        rpcpio::ClientContext ctx;
        ctx.set_deadline(std::chrono::system_clock::now() +
                         std::chrono::milliseconds(100));

        grpc::testing::SimpleRequest request;
        request.mutable_response_status()->set_code(0);
        // The test server should sleep long enough for the client to time out.
        auto result = co_await stub_->UnaryCall(ctx, request);
        EXPECT_EQ(result.status.code(), rpcpio::StatusCode::DEADLINE_EXCEEDED);
    });
}

// ── unimplemented_method ─────────────────────────────────────────────────────

TEST_F(InteropClientTest, UnimplementedMethod) {
    // Directly submit a call to an unknown method path.
    Run([this]() -> boost::asio::awaitable<void> {
        static constexpr rpcpio::UnaryMethod<
            grpc::testing::Empty, grpc::testing::Empty>
            kFake{"/grpc.testing.TestService/UnimplementedCall"};

        rpcpio::ClientContext ctx;
        grpc::testing::Empty req;
        auto result = co_await channel_->UnaryCall(kFake, ctx, req);
        EXPECT_EQ(result.status.code(), rpcpio::StatusCode::UNIMPLEMENTED);
    });
}
