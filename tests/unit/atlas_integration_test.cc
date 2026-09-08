// Focused Atlas integration surface tests: callback server registration,
// completion-token client calls, single-fire completion, shutdown, cancellation.

#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <gtest/gtest.h>
#include "rpcpio/channel.h"
#include "rpcpio/client_context.h"
#include "rpcpio/server.h"
#include "rpcpio/unary_result_raw.h"
#include "rpcpio/unary_server_reply.h"

namespace {

constexpr char kEchoPath[] = "/test.Echo/Echo";
constexpr char kSlowPath[] = "/test.Echo/Slow";
constexpr char kFailPath[] = "/test.Echo/Fail";
constexpr char kEmptyPath[] = "/test.Echo/Empty";

class AtlasIntegrationTest : public ::testing::Test {
protected:
    void SetUp() override {
        rpcpio::ServerOptions opts;
        opts.use_h2c = true;

        server_ = std::make_unique<rpcpio::Server>(ioc_, opts);

        server_->RegisterUnaryCallback(
            kEchoPath,
            [](rpcpio::ServerContext& ctx,
               std::string_view req_bytes,
               std::shared_ptr<rpcpio::UnaryServerReply> reply) {
                (void)ctx;
                reply->Finish(rpcpio::Status{}, std::string(req_bytes));
            });

        server_->RegisterUnaryCallback(
            kSlowPath,
            [this](rpcpio::ServerContext&,
                   std::string_view req_bytes,
                   std::shared_ptr<rpcpio::UnaryServerReply> reply) {
                auto timer = std::make_shared<boost::asio::steady_timer>(ioc_);
                timer->expires_after(std::chrono::milliseconds(50));
                timer->async_wait([timer, reply, payload = std::string(req_bytes)](
                        const boost::system::error_code& ec) {
                    if (ec) return;
                    reply->Finish(rpcpio::Status{}, payload);
                });
            });

        server_->RegisterUnaryCallback(
            kFailPath,
            [](rpcpio::ServerContext&,
               std::string_view,
               std::shared_ptr<rpcpio::UnaryServerReply> reply) {
                rpcpio::Status st{rpcpio::StatusCode::INTERNAL, "boom"};
                reply->Finish(st);
            });

        server_->RegisterUnaryCallback(
            kEmptyPath,
            [](rpcpio::ServerContext&,
               std::string_view,
               std::shared_ptr<rpcpio::UnaryServerReply> reply) {
                reply->Finish(rpcpio::Status{}, std::string{});
            });

        server_->Start("127.0.0.1", 0);
        port_ = server_->bound_port();

        server_thread_ = std::thread([this] { ioc_.run(); });
    }

    void TearDown() override {
        server_->Shutdown();
        ioc_.stop();
        if (server_thread_.joinable()) server_thread_.join();
    }

    boost::asio::io_context      ioc_;
    std::unique_ptr<rpcpio::Server> server_;
    std::thread                  server_thread_;
    std::uint16_t                port_{0};
};

} // namespace

TEST_F(AtlasIntegrationTest, AsyncUnaryCallRawEcho) {
    boost::asio::io_context client_ioc;
    rpcpio::ChannelOptions copts;
    copts.use_h2c = true;
    rpcpio::Channel channel(client_ioc, "127.0.0.1", port_, copts);

    rpcpio::UnaryResultRaw result;
    boost::asio::co_spawn(
        client_ioc,
        [&]() -> boost::asio::awaitable<void> {
            rpcpio::ClientContext ctx;
            result = co_await channel.AsyncUnaryCallRaw(
                kEchoPath, ctx, "hello-bytes", boost::asio::use_awaitable);
            client_ioc.stop();
        },
        boost::asio::detached);

    client_ioc.run();

    ASSERT_TRUE(result.status.ok()) << result.status.message();
    EXPECT_EQ(result.response_bytes, "hello-bytes");
}

TEST_F(AtlasIntegrationTest, DeferredCallbackReplyFromTimer) {
    boost::asio::io_context client_ioc;
    rpcpio::ChannelOptions copts;
    copts.use_h2c = true;
    rpcpio::Channel channel(client_ioc, "127.0.0.1", port_, copts);

    rpcpio::UnaryResultRaw result;
    boost::asio::co_spawn(
        client_ioc,
        [&]() -> boost::asio::awaitable<void> {
            rpcpio::ClientContext ctx;
            result = co_await channel.AsyncUnaryCallRaw(
                kSlowPath, ctx, "delayed", boost::asio::use_awaitable);
            client_ioc.stop();
        },
        boost::asio::detached);

    client_ioc.run();

    ASSERT_TRUE(result.status.ok()) << result.status.message();
    EXPECT_EQ(result.response_bytes, "delayed");
}

TEST_F(AtlasIntegrationTest, ServerErrorViaTrailersOnly) {
    boost::asio::io_context client_ioc;
    rpcpio::ChannelOptions copts;
    copts.use_h2c = true;
    rpcpio::Channel channel(client_ioc, "127.0.0.1", port_, copts);

    rpcpio::UnaryResultRaw result;
    boost::asio::co_spawn(
        client_ioc,
        [&]() -> boost::asio::awaitable<void> {
            rpcpio::ClientContext ctx;
            result = co_await channel.AsyncUnaryCallRaw(
                kFailPath, ctx, "", boost::asio::use_awaitable);
            client_ioc.stop();
        },
        boost::asio::detached);

    client_ioc.run();

    EXPECT_EQ(result.status.code(), rpcpio::StatusCode::INTERNAL);
    EXPECT_EQ(result.status.message(), "boom");
}

TEST_F(AtlasIntegrationTest, DeadlineExceeded) {
    boost::asio::io_context client_ioc;
    rpcpio::ChannelOptions copts;
    copts.use_h2c = true;
    rpcpio::Channel channel(client_ioc, "127.0.0.1", port_, copts);

    rpcpio::UnaryResultRaw result;
    boost::asio::co_spawn(
        client_ioc,
        [&]() -> boost::asio::awaitable<void> {
            rpcpio::ClientContext ctx;
            ctx.set_deadline(std::chrono::system_clock::now() +
                             std::chrono::milliseconds(1));
            result = co_await channel.AsyncUnaryCallRaw(
                kSlowPath, ctx, "too-late", boost::asio::use_awaitable);
            client_ioc.stop();
        },
        boost::asio::detached);

    client_ioc.run();

    EXPECT_EQ(result.status.code(), rpcpio::StatusCode::DEADLINE_EXCEEDED);
}

TEST_F(AtlasIntegrationTest, ShutdownIsNonBlocking) {
    auto t0 = std::chrono::steady_clock::now();
    server_->Shutdown();
    auto elapsed = std::chrono::steady_clock::now() - t0;
    EXPECT_LT(elapsed, std::chrono::milliseconds(100));
}

TEST_F(AtlasIntegrationTest, ReplyFinishIsSingleFire) {
    std::atomic<int> finish_calls{0};

    rpcpio::ServerOptions opts;
    opts.use_h2c = true;
    boost::asio::io_context ioc;
    rpcpio::Server server(ioc, opts);
    server.RegisterUnaryCallback(
        "/test.SingleFire/Twice",
        [&finish_calls](rpcpio::ServerContext&,
                        std::string_view,
                        std::shared_ptr<rpcpio::UnaryServerReply> reply) {
            ++finish_calls;
            reply->Finish(rpcpio::Status{}, "once");
            reply->Finish(rpcpio::Status{}, "twice");
        });
    server.Start("127.0.0.1", 0);
    const auto port = server.bound_port();
    std::thread thr([&] { ioc.run(); });

    boost::asio::io_context client_ioc;
    rpcpio::ChannelOptions copts;
    copts.use_h2c = true;
    rpcpio::Channel channel(client_ioc, "127.0.0.1", port, copts);

    rpcpio::UnaryResultRaw result;
    boost::asio::co_spawn(
        client_ioc,
        [&]() -> boost::asio::awaitable<void> {
            rpcpio::ClientContext ctx;
            result = co_await channel.AsyncUnaryCallRaw(
                "/test.SingleFire/Twice", ctx, "", boost::asio::use_awaitable);
            client_ioc.stop();
        },
        boost::asio::detached);
    client_ioc.run();

    EXPECT_EQ(finish_calls.load(), 1);
    ASSERT_TRUE(result.status.ok());
    EXPECT_EQ(result.response_bytes, "once");

    server.Shutdown();
    ioc.stop();
    thr.join();
}

TEST_F(AtlasIntegrationTest, ZeroLengthUnaryResponse) {
    boost::asio::io_context client_ioc;
    rpcpio::ChannelOptions copts;
    copts.use_h2c = true;
    rpcpio::Channel channel(client_ioc, "127.0.0.1", port_, copts);

    rpcpio::UnaryResultRaw result;
    boost::asio::co_spawn(
        client_ioc,
        [&]() -> boost::asio::awaitable<void> {
            rpcpio::ClientContext ctx;
            result = co_await channel.AsyncUnaryCallRaw(
                kEmptyPath, ctx, "", boost::asio::use_awaitable);
            client_ioc.stop();
        },
        boost::asio::detached);

    client_ioc.run();

    ASSERT_TRUE(result.status.ok()) << result.status.message();
    EXPECT_TRUE(result.has_response);
    EXPECT_TRUE(result.response_bytes.empty());
}

TEST_F(AtlasIntegrationTest, ClientContextCancelWhileConnecting) {
    boost::asio::io_context client_ioc;
    rpcpio::ChannelOptions copts;
    copts.use_h2c = true;
    rpcpio::Channel channel(client_ioc, "127.0.0.1", 9, copts);

    rpcpio::UnaryResultRaw result;
    boost::asio::co_spawn(
        client_ioc,
        [&]() -> boost::asio::awaitable<void> {
            rpcpio::ClientContext ctx;
            boost::asio::steady_timer cancel_timer(client_ioc);
            cancel_timer.expires_after(std::chrono::milliseconds(0));
            cancel_timer.async_wait([&ctx](const boost::system::error_code& ec) {
                if (!ec) ctx.Cancel();
            });
            result = co_await channel.AsyncUnaryCallRaw(
                kEchoPath, ctx, "cancel-me", boost::asio::use_awaitable);
            client_ioc.stop();
        },
        boost::asio::detached);

    client_ioc.run();

    EXPECT_EQ(result.status.code(), rpcpio::StatusCode::CANCELLED);
}

TEST_F(AtlasIntegrationTest, AssociatedCancellationWhileConnecting) {
    boost::asio::io_context client_ioc;
    rpcpio::ChannelOptions copts;
    copts.use_h2c = true;
    rpcpio::Channel channel(client_ioc, "127.0.0.1", 9, copts);

    boost::asio::cancellation_signal cancel_signal;
    rpcpio::UnaryResultRaw result;
    boost::asio::co_spawn(
        client_ioc,
        [&]() -> boost::asio::awaitable<void> {
            rpcpio::ClientContext ctx;
            boost::asio::steady_timer cancel_timer(client_ioc);
            cancel_timer.expires_after(std::chrono::milliseconds(0));
            cancel_timer.async_wait([&cancel_signal](const boost::system::error_code& ec) {
                if (!ec) {
                    cancel_signal.emit(boost::asio::cancellation_type::all);
                }
            });
            result = co_await channel.AsyncUnaryCallRaw(
                kEchoPath,
                ctx,
                "cancel-me",
                boost::asio::bind_cancellation_slot(
                    cancel_signal.slot(), boost::asio::use_awaitable));
            client_ioc.stop();
        },
        boost::asio::detached);

    client_ioc.run();

    EXPECT_EQ(result.status.code(), rpcpio::StatusCode::CANCELLED);
}

TEST_F(AtlasIntegrationTest, DeadlineWhileConnecting) {
    boost::asio::io_context client_ioc;
    rpcpio::ChannelOptions copts;
    copts.use_h2c = true;
    // TEST-NET-1 address; TCP should not complete before the short deadline.
    rpcpio::Channel channel(client_ioc, "192.0.2.1", 9, copts);

    rpcpio::UnaryResultRaw result;
    boost::asio::co_spawn(
        client_ioc,
        [&]() -> boost::asio::awaitable<void> {
            rpcpio::ClientContext ctx;
            ctx.set_deadline(std::chrono::system_clock::now() +
                             std::chrono::milliseconds(100));
            result = co_await channel.AsyncUnaryCallRaw(
                kEchoPath, ctx, "late", boost::asio::use_awaitable);
            client_ioc.stop();
        },
        boost::asio::detached);

    client_ioc.run();

    EXPECT_EQ(result.status.code(), rpcpio::StatusCode::DEADLINE_EXCEEDED);
}
