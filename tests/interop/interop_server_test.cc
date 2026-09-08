// gRPC interoperability server test.
//
// Starts an rpcpio server on an OS-assigned ephemeral port and exercises
// it using the rpcpio client.  A full interop run would substitute the
// official grpc_interop_client binary.

#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <vector>
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

    // Server streaming: send response_size individual responses, each with a
    // payload of (i+1) bytes where i is the 0-based response index.
    boost::asio::awaitable<rpcpio::Status>
    StreamingOutputCall(
            rpcpio::ServerContext&,
            const grpc::testing::SimpleRequest& req,
            rpcpio::ServerWriter<grpc::testing::SimpleResponse>& writer) override {
        for (int i = 0; i < req.response_size(); ++i) {
            grpc::testing::SimpleResponse resp;
            resp.mutable_payload()->set_body(
                std::string(static_cast<std::size_t>(i + 1), 'x'));
            auto st = co_await writer.Write(resp);
            if (!st.ok()) co_return st;
        }
        co_return rpcpio::Status{};
    }

    // Client streaming: accumulate response_size from each request; return a
    // response whose payload.body is that many 'y' bytes.
    boost::asio::awaitable<rpcpio::StatusOr<grpc::testing::SimpleResponse>>
    StreamingInputCall(
            rpcpio::ServerContext&,
            rpcpio::ServerReader<grpc::testing::SimpleRequest>& reader) override {
        std::size_t total = 0;
        while (true) {
            auto r = co_await reader.Read();
            if (!r.ok())
                co_return rpcpio::StatusOr<grpc::testing::SimpleResponse>(r.status());
            if (!(*r).has_value()) break;
            total += static_cast<std::size_t>((**r).response_size());
        }
        grpc::testing::SimpleResponse resp;
        resp.mutable_payload()->set_body(std::string(total, 'y'));
        co_return resp;
    }

    // Bidi streaming: echo each request's payload.body back in a response.
    boost::asio::awaitable<rpcpio::Status>
    FullDuplexCall(
            rpcpio::ServerContext&,
            rpcpio::ServerReader<grpc::testing::SimpleRequest>& reader,
            rpcpio::ServerWriter<grpc::testing::SimpleResponse>& writer) override {
        while (true) {
            auto r = co_await reader.Read();
            if (!r.ok()) co_return r.status();
            if (!(*r).has_value()) break;
            grpc::testing::SimpleResponse resp;
            resp.mutable_payload()->set_body((**r).payload().body());
            auto st = co_await writer.Write(resp);
            if (!st.ok()) co_return st;
        }
        co_return rpcpio::Status{};
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

TEST_F(InteropServerTest, ServerStreamingCall) {
    boost::asio::io_context client_ioc;

    rpcpio::ChannelOptions copts;
    copts.use_h2c = true;
    auto channel = std::make_shared<rpcpio::Channel>(
        client_ioc, "127.0.0.1", port_, copts);

    grpc::testing::TestServiceStub stub(channel);

    rpcpio::Status final_status;
    int num_responses = 0;

    boost::asio::co_spawn(
        client_ioc,
        [&]() -> boost::asio::awaitable<void> {
            rpcpio::ClientContext ctx;
            grpc::testing::SimpleRequest req;
            req.set_response_size(3);

            auto reader = co_await stub.StreamingOutputCall(ctx, req);
            while (true) {
                auto r = co_await reader.Read();
                if (!r.ok()) { final_status = r.status(); co_return; }
                if (!(*r).has_value()) break;
                ++num_responses;
            }
            final_status = co_await reader.Finish();
        },
        boost::asio::detached);

    client_ioc.run();

    EXPECT_EQ(num_responses, 3);
    EXPECT_TRUE(final_status.ok()) << final_status.message();
}

TEST_F(InteropServerTest, ClientStreamingCall) {
    boost::asio::io_context client_ioc;

    rpcpio::ChannelOptions copts;
    copts.use_h2c = true;
    auto channel = std::make_shared<rpcpio::Channel>(
        client_ioc, "127.0.0.1", port_, copts);

    grpc::testing::TestServiceStub stub(channel);

    rpcpio::UnaryResult<grpc::testing::SimpleResponse> result;

    boost::asio::co_spawn(
        client_ioc,
        [&]() -> boost::asio::awaitable<void> {
            rpcpio::ClientContext ctx;
            auto writer = co_await stub.StreamingInputCall(ctx);

            for (int i = 1; i <= 3; ++i) {
                grpc::testing::SimpleRequest req;
                req.set_response_size(i * 10);  // 10 + 20 + 30 = 60
                co_await writer.Write(req);
            }
            co_await writer.WritesDone();
            result = co_await writer.FinishAndGetResponse();
        },
        boost::asio::detached);

    client_ioc.run();

    EXPECT_TRUE(result.status.ok()) << result.status.message();
    ASSERT_TRUE(result.response.has_value());
    EXPECT_EQ(result.response->payload().body().size(), 60u);
}

TEST_F(InteropServerTest, BidiStreamingCall) {
    boost::asio::io_context client_ioc;

    rpcpio::ChannelOptions copts;
    copts.use_h2c = true;
    auto channel = std::make_shared<rpcpio::Channel>(
        client_ioc, "127.0.0.1", port_, copts);

    grpc::testing::TestServiceStub stub(channel);

    rpcpio::Status final_status;
    std::vector<std::string> received;

    boost::asio::co_spawn(
        client_ioc,
        [&]() -> boost::asio::awaitable<void> {
            rpcpio::ClientContext ctx;
            auto stream = co_await stub.FullDuplexCall(ctx);

            for (std::string_view body : {"ping", "pong", "done"}) {
                grpc::testing::SimpleRequest req;
                req.mutable_payload()->set_body(std::string(body));
                co_await stream.Write(req);

                auto r = co_await stream.Read();
                if (!r.ok() || !(*r).has_value()) break;
                received.push_back((**r).payload().body());
            }
            co_await stream.WritesDone();
            final_status = co_await stream.Finish();
        },
        boost::asio::detached);

    client_ioc.run();

    EXPECT_TRUE(final_status.ok()) << final_status.message();
    ASSERT_EQ(received.size(), 3u);
    EXPECT_EQ(received[0], "ping");
    EXPECT_EQ(received[1], "pong");
    EXPECT_EQ(received[2], "done");
}
