// gRPC interoperability server test.
//
// Starts an rpcpio server on an OS-assigned ephemeral port and exercises
// it using the rpcpio client.  A full interop run would substitute the
// official grpc_interop_client binary.

#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <gtest/gtest.h>
#include "rpcpio/channel.h"
#include "rpcpio/client_context.h"
#include "rpcpio/server.h"
#include "grpc_testing.rpcpio.pb.h"

// ── Service implementation ────────────────────────────────────────────────────

class InteropServiceImpl final : public grpc::testing::TestServiceService {
public:
    boost::asio::awaitable<rpcpio::StatusOr<grpc::testing::Empty>>
    EmptyCall(rpcpio::ServerContext&,
              const grpc::testing::Empty&) override {
        co_return grpc::testing::Empty{};
    }

    boost::asio::awaitable<rpcpio::StatusOr<grpc::testing::SimpleResponse>>
    UnaryCall(rpcpio::ServerContext&,
              const grpc::testing::SimpleRequest& req) override {
        if (req.response_status().code() != 0) {
            co_return rpcpio::Status{
                rpcpio::StatusCodeFromInt(req.response_status().code()),
                req.response_status().message()};
        }
        grpc::testing::SimpleResponse resp;
        if (req.response_size() > 0)
            resp.mutable_payload()->set_body(
                std::string(static_cast<std::size_t>(req.response_size()), '\0'));
        co_return resp;
    }
};

// ── Test fixture ──────────────────────────────────────────────────────────────

class InteropServerTest : public ::testing::Test {
protected:
    void SetUp() override {
        rpcpio::ServerOptions opts;
        opts.use_h2c     = true;
        opts.num_threads = 2;

        server_ = std::make_unique<rpcpio::Server>(ioc_, opts);
        service_.Register(*server_);
        server_->Start("127.0.0.1", 0);   // OS picks ephemeral port
        port_ = server_->bound_port();

        thread_ = std::thread([this] { ioc_.run(); });
    }

    void TearDown() override {
        server_->Shutdown();
        ioc_.stop();
        if (thread_.joinable()) thread_.join();
    }

    boost::asio::io_context            ioc_;
    InteropServiceImpl                 service_;
    std::unique_ptr<rpcpio::Server> server_;
    std::thread                        thread_;
    std::uint16_t                      port_{0};
};

// ── Tests ─────────────────────────────────────────────────────────────────────

TEST_F(InteropServerTest, EmptyCallFromClient) {
    boost::asio::io_context client_ioc;

    rpcpio::ChannelOptions copts;
    copts.use_h2c = true;
    auto channel = std::make_shared<rpcpio::Channel>(
        client_ioc, "127.0.0.1", port_, copts);

    grpc::testing::TestServiceStub stub(channel);

    bool done = false;
    rpcpio::Status result_status;

    boost::asio::co_spawn(
        client_ioc,
        [&]() -> boost::asio::awaitable<void> {
            rpcpio::ClientContext ctx;
            grpc::testing::Empty request;
            auto result = co_await stub.EmptyCall(ctx, request);
            result_status = result.status;
            done = true;
        },
        boost::asio::detached);

    client_ioc.run();

    EXPECT_TRUE(done);
    EXPECT_TRUE(result_status.ok()) << result_status.message();
}

TEST_F(InteropServerTest, UnaryCallEchoStatus) {
    boost::asio::io_context client_ioc;

    rpcpio::ChannelOptions copts;
    copts.use_h2c = true;
    auto channel = std::make_shared<rpcpio::Channel>(
        client_ioc, "127.0.0.1", port_, copts);

    grpc::testing::TestServiceStub stub(channel);

    rpcpio::Status result_status;

    boost::asio::co_spawn(
        client_ioc,
        [&]() -> boost::asio::awaitable<void> {
            rpcpio::ClientContext ctx;
            grpc::testing::SimpleRequest req;
            req.mutable_response_status()->set_code(
                static_cast<int>(rpcpio::StatusCode::NOT_FOUND));
            req.mutable_response_status()->set_message("not here");
            auto result = co_await stub.UnaryCall(ctx, req);
            result_status = result.status;
        },
        boost::asio::detached);

    client_ioc.run();

    EXPECT_EQ(result_status.code(), rpcpio::StatusCode::NOT_FOUND);
    EXPECT_EQ(result_status.message(), "not here");
}
