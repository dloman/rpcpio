#include <atomic>
#include <chrono>
#include <cstdint>
#include <exception>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <thread>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <gtest/gtest.h>

#include "rpcpio/channel.h"
#include "rpcpio/client_context.h"
#include "rpcpio/server.h"
#include "rpcpio/server_context.h"
#include "rpcpio/status.h"

namespace {

constexpr std::string_view kShutdownPath = "/test.ServerShutdown/Call";
constexpr std::string_view kUnaryCancelPath =
    "/test.ServerShutdown/UnaryCancel";
constexpr std::string_view kServerStreamingCancelPath =
    "/test.ServerShutdown/ServerStreamingCancel";
constexpr std::string_view kClientStreamingCancelPath =
    "/test.ServerShutdown/ClientStreamingCancel";
constexpr std::string_view kBidiCancelPath =
    "/test.ServerShutdown/BidiCancel";

enum class RpcKind {
    kUnary,
    kServerStreaming,
    kClientStreaming,
    kBidi,
};

std::string_view PathFor(RpcKind kind) {
    switch (kind) {
        case RpcKind::kUnary:
            return kUnaryCancelPath;
        case RpcKind::kServerStreaming:
            return kServerStreamingCancelPath;
        case RpcKind::kClientStreaming:
            return kClientStreamingCancelPath;
        case RpcKind::kBidi:
            return kBidiCancelPath;
    }
    return {};
}

boost::asio::awaitable<rpcpio::Status> WaitForCancellation(
        rpcpio::ServerContext& context,
        std::atomic<bool>& started,
        std::atomic<bool>& cancelled) {
    started = true;
    auto executor = co_await boost::asio::this_coro::executor;
    boost::asio::steady_timer timer(executor);
    timer.expires_after(std::chrono::seconds(5));
    boost::system::error_code error;
    co_await timer.async_wait(boost::asio::bind_cancellation_slot(
        context.cancellation_slot(),
        boost::asio::redirect_error(boost::asio::use_awaitable, error)));
    cancelled = error == boost::asio::error::operation_aborted;
    co_return rpcpio::Status{
        rpcpio::StatusCode::CANCELLED, "handler cancelled"};
}

void TestClientCloseCancelsHandler(RpcKind kind) {
    boost::asio::io_context server_ioc;
    rpcpio::ServerOptions options;
    options.use_h2c = true;
    options.num_threads = 2;
    options.grace_period = std::chrono::seconds(1);
    rpcpio::Server server(server_ioc, options);

    std::atomic<bool> started{false};
    std::atomic<bool> cancelled{false};
    server.RegisterUnaryRaw(
        kUnaryCancelPath,
        [&](rpcpio::ServerContext& context,
            std::string_view,
            std::string&) -> boost::asio::awaitable<rpcpio::Status> {
            co_return co_await WaitForCancellation(
                context, started, cancelled);
        });
    server.RegisterServerStreamingRaw(
        kServerStreamingCancelPath,
        [&](rpcpio::ServerContext& context,
            std::string_view,
            rpcpio::internal::RawServerWriter&)
                -> boost::asio::awaitable<rpcpio::Status> {
            co_return co_await WaitForCancellation(
                context, started, cancelled);
        });
    server.RegisterClientStreamingRaw(
        kClientStreamingCancelPath,
        [&](rpcpio::ServerContext& context,
            rpcpio::internal::RawServerReader&,
            std::string&) -> boost::asio::awaitable<rpcpio::Status> {
            co_return co_await WaitForCancellation(
                context, started, cancelled);
        });
    server.RegisterBidiRaw(
        kBidiCancelPath,
        [&](rpcpio::ServerContext& context,
            rpcpio::internal::RawServerReader&,
            rpcpio::internal::RawServerWriter&)
                -> boost::asio::awaitable<rpcpio::Status> {
            co_return co_await WaitForCancellation(
                context, started, cancelled);
        });
    ASSERT_TRUE(server.Start("127.0.0.1", 0).ok());

    boost::asio::io_context client_ioc;
    rpcpio::ChannelOptions client_options;
    client_options.use_h2c = true;
    auto channel = std::make_shared<rpcpio::Channel>(
        client_ioc,
        "127.0.0.1",
        server.bound_port(),
        client_options);

    boost::asio::co_spawn(
        client_ioc,
        [&, channel]() -> boost::asio::awaitable<void> {
            rpcpio::ClientContext context;
            const std::string_view path = PathFor(kind);
            switch (kind) {
                case RpcKind::kUnary:
                    co_await channel->UnaryCallRaw(
                        path, context, "");
                    break;
                case RpcKind::kServerStreaming: {
                    auto reader = co_await channel->ServerStreamingCallRaw(
                        path, context, "");
                    co_await reader.Finish();
                    break;
                }
                case RpcKind::kClientStreaming: {
                    auto writer = co_await channel->ClientStreamingCallRaw(
                        path, context);
                    co_await writer.Finish();
                    break;
                }
                case RpcKind::kBidi: {
                    auto handles = co_await channel->BidiStreamingCallRaw(
                        path, context);
                    co_await handles.reader.Finish();
                    break;
                }
            }
        },
        boost::asio::detached);
    std::thread client_thread([&] {
        client_ioc.run();
    });

    const auto start_deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(1);
    while (!started && std::chrono::steady_clock::now() < start_deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_TRUE(started);

    channel->Shutdown();
    client_thread.join();

    const auto cancel_deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(1);
    while (!cancelled && std::chrono::steady_clock::now() < cancel_deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    server.Shutdown();
    server.Wait();
    EXPECT_TRUE(cancelled);
}

} // namespace

TEST(ServerShutdown, DoesNotStopCallerIoContext) {
    boost::asio::io_context ioc;
    rpcpio::ServerOptions options;
    options.use_h2c = true;
    rpcpio::Server server(ioc, options);
    ASSERT_TRUE(server.Start("127.0.0.1", 0).ok());

    std::atomic<int> timer_ticks{0};
    boost::asio::steady_timer timer(ioc);
    std::function<void()> schedule_tick;
    schedule_tick = [&] {
        timer.expires_after(std::chrono::milliseconds(1));
        timer.async_wait([&](const boost::system::error_code& error) {
            if (!error) {
                ++timer_ticks;
                schedule_tick();
            }
        });
    };
    schedule_tick();

    std::thread io_thread([&] {
        ioc.run();
    });
    while (timer_ticks < 3) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    server.Shutdown();
    const int ticks_at_shutdown = timer_ticks;
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(100);
    while (timer_ticks == ticks_at_shutdown &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    EXPECT_GT(timer_ticks, ticks_at_shutdown);
    timer.cancel();
    ioc.stop();
    io_thread.join();
}

TEST(ServerShutdown, HandlerCanShutdownAndLateResultIsDropped) {
    boost::asio::io_context server_ioc;
    rpcpio::ServerOptions options;
    options.use_h2c = true;
    options.num_threads = 1;
    options.grace_period = std::chrono::seconds(1);
    rpcpio::Server server(server_ioc, options);

    std::atomic<bool> cancellation_seen{false};
    std::atomic<bool> shutdown_returned{false};
    std::atomic<bool> handler_returned{false};
    server.RegisterUnaryRaw(
        kShutdownPath,
        [&](rpcpio::ServerContext& context,
            std::string_view,
            std::string&) -> boost::asio::awaitable<rpcpio::Status> {
            context.cancellation_slot().assign(
                [&](boost::asio::cancellation_type) {
                    cancellation_seen = true;
                });

            server.Shutdown();
            shutdown_returned = true;

            auto executor = co_await boost::asio::this_coro::executor;
            boost::asio::steady_timer late_result_timer(executor);
            late_result_timer.expires_after(std::chrono::milliseconds(10));
            co_await late_result_timer.async_wait(boost::asio::use_awaitable);

            handler_returned = true;
            co_return rpcpio::Status{};
        });
    ASSERT_TRUE(server.Start("127.0.0.1", 0).ok());

    boost::asio::io_context client_ioc;
    rpcpio::ChannelOptions client_options;
    client_options.use_h2c = true;
    auto channel = std::make_shared<rpcpio::Channel>(
        client_ioc,
        "127.0.0.1",
        server.bound_port(),
        client_options);

    rpcpio::Status call_status{
        rpcpio::StatusCode::UNKNOWN, "call did not finish"};
    std::exception_ptr failure;
    boost::asio::co_spawn(
        client_ioc,
        [&]() -> boost::asio::awaitable<void> {
            rpcpio::ClientContext context;
            auto result =
                co_await channel->UnaryCallRaw(kShutdownPath, context, "");
            call_status = std::move(result.status);
        },
        [&](std::exception_ptr exception) {
            failure = exception;
            channel->Shutdown();
        });

    client_ioc.run();
    server.Wait();

    EXPECT_EQ(failure, nullptr);
    EXPECT_TRUE(cancellation_seen);
    EXPECT_TRUE(shutdown_returned);
    EXPECT_TRUE(handler_returned);
    EXPECT_EQ(call_status.code(), rpcpio::StatusCode::UNAVAILABLE)
        << call_status.DebugString();
}

TEST(ServerShutdown, ClientCloseCancelsEveryRpcKind) {
    for (RpcKind kind : {
             RpcKind::kUnary,
             RpcKind::kServerStreaming,
             RpcKind::kClientStreaming,
             RpcKind::kBidi,
         }) {
        TestClientCloseCancelsHandler(kind);
    }
}
