// response_size_test.cc
//
// Tests Fix 5: responses at the limit succeed; one byte over returns
// RESOURCE_EXHAUSTED.

#include <string>
#include <thread>
#include <cstdint>
#include <atomic>
#include <chrono>
#include <gtest/gtest.h>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include "rpcpio/channel.h"
#include "rpcpio/client_context.h"
#include "rpcpio/server.h"
#include "rpcpio/status.h"
#include "rpcpio/internal/raw_server_writer.h"

static constexpr std::string_view kSizePath = "/test.Size/Echo";

// Helper: spin up an h2c server with max_response_message_size = limit,
// register a raw unary handler that returns a response of `resp_size` bytes,
// and return the status observed by the client.
static rpcpio::Status RunOnce(std::size_t limit, std::size_t resp_size) {
    boost::asio::io_context server_ioc;
    rpcpio::ServerOptions opts;
    opts.use_h2c                    = true;
    opts.num_threads                = 1;
    opts.max_response_message_size  = limit;

    rpcpio::Server server(server_ioc, opts);
    server.RegisterUnaryRaw(kSizePath,
        [resp_size](rpcpio::ServerContext&,
                    std::string_view,
                    std::string& out) -> boost::asio::awaitable<rpcpio::Status> {
            out.assign(resp_size, 'x');
            co_return rpcpio::Status{};
        });

    if (!server.Start("127.0.0.1", 0).ok()) {
        return rpcpio::Status{rpcpio::StatusCode::INTERNAL, "server start failed"};
    }
    std::uint16_t port = server.bound_port();
    std::thread server_thread([&] { server_ioc.run(); });

    boost::asio::io_context client_ioc;
    rpcpio::ChannelOptions copts;
    copts.use_h2c = true;
    auto ch = std::make_shared<rpcpio::Channel>(client_ioc, "127.0.0.1", port, copts);

    rpcpio::Status result{rpcpio::StatusCode::INTERNAL, "not set"};
    boost::asio::co_spawn(client_ioc,
        [ch, &result]() -> boost::asio::awaitable<void> {
            rpcpio::ClientContext ctx;
            auto raw = co_await ch->UnaryCallRaw(kSizePath, ctx, "");
            result = raw.status;
            ch->Shutdown();
        },
        boost::asio::detached);

    client_ioc.run();
    server.Shutdown();
    server_thread.join();

    return result;
}

TEST(ResponseSize, AtLimitSucceeds) {
    constexpr std::size_t kLimit = 64;
    rpcpio::Status s = RunOnce(kLimit, kLimit);
    EXPECT_TRUE(s.ok()) << s.message();
}

TEST(ResponseSize, OneByteOverReturnsResourceExhausted) {
    constexpr std::size_t kLimit = 64;
    rpcpio::Status s = RunOnce(kLimit, kLimit + 1);
    EXPECT_FALSE(s.ok());
    EXPECT_EQ(s.code(), rpcpio::StatusCode::RESOURCE_EXHAUSTED) << s.message();
}

// ── Server-streaming size tests ───────────────────────────────────────────────

static constexpr std::string_view kStreamSizePath = "/test.Size/Stream";

// Helper: server-streaming handler writes two messages; second is second_size bytes.
static rpcpio::Status RunStreamingOnce(std::size_t limit,
                                        std::size_t first_size,
                                        std::size_t second_size) {
    boost::asio::io_context server_ioc;
    rpcpio::ServerOptions opts;
    opts.use_h2c                   = true;
    opts.num_threads               = 1;
    opts.max_response_message_size = limit;

    rpcpio::Server server(server_ioc, opts);
    server.RegisterServerStreamingRaw(kStreamSizePath,
        [first_size, second_size](
                rpcpio::ServerContext&,
                std::string_view,
                rpcpio::internal::RawServerWriter& writer)
                -> boost::asio::awaitable<rpcpio::Status> {
            std::string payload1(first_size, 'a');
            auto s1 = writer.Write(payload1);
            if (!s1.ok()) co_return s1;
            std::string payload2(second_size, 'b');
            auto s2 = writer.Write(payload2);
            if (!s2.ok()) {
                writer.Finish(s2);
                co_return s2;
            }
            writer.Finish(rpcpio::Status{});
            co_return rpcpio::Status{};
        });

    if (!server.Start("127.0.0.1", 0).ok())
        return rpcpio::Status{rpcpio::StatusCode::INTERNAL, "server start failed"};
    std::uint16_t port = server.bound_port();
    std::thread server_thread([&] { server_ioc.run(); });

    boost::asio::io_context client_ioc;
    rpcpio::ChannelOptions copts;
    copts.use_h2c = true;
    auto ch = std::make_shared<rpcpio::Channel>(client_ioc, "127.0.0.1", port, copts);

    rpcpio::Status final_status{rpcpio::StatusCode::INTERNAL, "not set"};
    boost::asio::co_spawn(client_ioc,
        [ch, &final_status]() -> boost::asio::awaitable<void> {
            rpcpio::ClientContext ctx;
            auto reader = co_await ch->ServerStreamingCallRaw(kStreamSizePath, ctx, "");
            while (true) {
                auto r = co_await reader.Read();
                if (!r.status.ok()) { final_status = r.status; break; }
                if (!r.data.has_value()) {
                    final_status = co_await reader.Finish();
                    break;
                }
            }
            ch->Shutdown();
        },
        boost::asio::detached);

    client_ioc.run();
    server.Shutdown();
    server_thread.join();
    return final_status;
}

TEST(ResponseSize, ServerStreamingAtLimitSucceeds) {
    constexpr std::size_t kLimit = 64;
    rpcpio::Status s = RunStreamingOnce(kLimit, kLimit / 2, kLimit);
    EXPECT_TRUE(s.ok()) << s.message();
}

TEST(ResponseSize, ServerStreamingOverLimitTerminatesStream) {
    constexpr std::size_t kLimit = 64;
    rpcpio::Status s = RunStreamingOnce(kLimit, kLimit / 2, kLimit + 1);
    EXPECT_FALSE(s.ok());
    EXPECT_EQ(s.code(), rpcpio::StatusCode::RESOURCE_EXHAUSTED) << s.message();
}

static constexpr std::string_view kClientStreamSizePath =
    "/test.Size/ClientStream";
static constexpr std::string_view kBidiSizePath = "/test.Size/Bidi";

static rpcpio::Status RunClientStreamingOnce(
        std::size_t limit,
        std::size_t response_size) {
    boost::asio::io_context server_ioc;
    rpcpio::ServerOptions options;
    options.use_h2c = true;
    options.num_threads = 1;
    options.max_response_message_size = limit;
    rpcpio::Server server(server_ioc, options);
    server.RegisterClientStreamingRaw(
        kClientStreamSizePath,
        [response_size](
                rpcpio::ServerContext&,
                rpcpio::internal::RawServerReader& reader,
                std::string& response)
                -> boost::asio::awaitable<rpcpio::Status> {
            while (true) {
                auto request = co_await reader.Read();
                if (!request.status.ok()) {
                    co_return request.status;
                }
                if (!request.data.has_value()) {
                    break;
                }
            }
            response.assign(response_size, 'x');
            co_return rpcpio::Status{};
        });
    if (!server.Start("127.0.0.1", 0).ok()) {
        return {rpcpio::StatusCode::INTERNAL, "server start failed"};
    }

    boost::asio::io_context client_ioc;
    rpcpio::ChannelOptions channel_options;
    channel_options.use_h2c = true;
    auto channel = std::make_shared<rpcpio::Channel>(
        client_ioc,
        "127.0.0.1",
        server.bound_port(),
        channel_options);
    rpcpio::Status result{
        rpcpio::StatusCode::INTERNAL, "not completed"};
    boost::asio::co_spawn(
        client_ioc,
        [&]() -> boost::asio::awaitable<void> {
            rpcpio::ClientContext context;
            auto writer = co_await channel->ClientStreamingCallRaw(
                kClientStreamSizePath, context);
            co_await writer.WritesDone();
            result = co_await writer.Finish();
        },
        [channel](std::exception_ptr) { channel->Shutdown(); });
    client_ioc.run();
    server.Shutdown();
    server.Wait();
    return result;
}

static rpcpio::Status RunBidiOnce(
        std::size_t limit,
        std::size_t response_size) {
    boost::asio::io_context server_ioc;
    rpcpio::ServerOptions options;
    options.use_h2c = true;
    options.num_threads = 1;
    options.max_response_message_size = limit;
    rpcpio::Server server(server_ioc, options);
    server.RegisterBidiRaw(
        kBidiSizePath,
        [response_size](
                rpcpio::ServerContext&,
                rpcpio::internal::RawServerReader& reader,
                rpcpio::internal::RawServerWriter& writer)
                -> boost::asio::awaitable<rpcpio::Status> {
            auto request = co_await reader.Read();
            if (!request.status.ok()) {
                co_return request.status;
            }
            if (!request.data.has_value()) {
                co_return rpcpio::Status{
                    rpcpio::StatusCode::INTERNAL, "missing request"};
            }
            co_return writer.Write(std::string(response_size, 'x'));
        });
    if (!server.Start("127.0.0.1", 0).ok()) {
        return {rpcpio::StatusCode::INTERNAL, "server start failed"};
    }

    boost::asio::io_context client_ioc;
    rpcpio::ChannelOptions channel_options;
    channel_options.use_h2c = true;
    auto channel = std::make_shared<rpcpio::Channel>(
        client_ioc,
        "127.0.0.1",
        server.bound_port(),
        channel_options);
    rpcpio::Status result{
        rpcpio::StatusCode::INTERNAL, "not completed"};
    boost::asio::co_spawn(
        client_ioc,
        [&]() -> boost::asio::awaitable<void> {
            rpcpio::ClientContext context;
            auto stream = co_await channel->BidiStreamingCallRaw(
                kBidiSizePath, context);
            co_await stream.writer.Write("");
            co_await stream.writer.WritesDone();
            while (true) {
                auto response = co_await stream.reader.Read();
                if (!response.status.ok()) {
                    result = response.status;
                    break;
                }
                if (!response.data.has_value()) {
                    result = co_await stream.reader.Finish();
                    break;
                }
            }
        },
        [channel](std::exception_ptr) { channel->Shutdown(); });
    client_ioc.run();
    server.Shutdown();
    server.Wait();
    return result;
}

TEST(ResponseSize, ClientStreamingAtLimitSucceeds) {
    EXPECT_TRUE(RunClientStreamingOnce(64, 64).ok());
}

TEST(ResponseSize, ClientStreamingOverLimitIsResourceExhausted) {
    const auto status = RunClientStreamingOnce(64, 65);
    EXPECT_EQ(status.code(), rpcpio::StatusCode::RESOURCE_EXHAUSTED)
        << status.DebugString();
}

TEST(ResponseSize, BidiAtLimitSucceeds) {
    EXPECT_TRUE(RunBidiOnce(64, 64).ok());
}

TEST(ResponseSize, BidiOverLimitIsResourceExhausted) {
    const auto status = RunBidiOnce(64, 65);
    EXPECT_EQ(status.code(), rpcpio::StatusCode::RESOURCE_EXHAUSTED)
        << status.DebugString();
}

enum class RaceRpcKind {
    kUnary,
    kServerStreaming,
    kClientStreaming,
    kBidi,
};

enum class RaceEvent {
    kDeadline,
    kClientCancel,
    kServerShutdown,
};

static rpcpio::Status RunResponseRace(
        RaceRpcKind kind,
        RaceEvent event,
        int& terminal_count) {
    constexpr std::size_t kLimit = 64;
    constexpr std::string_view kUnaryRacePath = "/test.SizeRace/Unary";
    constexpr std::string_view kServerStreamingRacePath =
        "/test.SizeRace/ServerStreaming";
    constexpr std::string_view kClientStreamingRacePath =
        "/test.SizeRace/ClientStreaming";
    constexpr std::string_view kBidiRacePath = "/test.SizeRace/Bidi";

    boost::asio::io_context server_ioc;
    rpcpio::ServerOptions options;
    options.use_h2c = true;
    options.num_threads = 2;
    options.max_response_message_size = kLimit;
    options.grace_period = std::chrono::seconds(1);
    rpcpio::Server server(server_ioc, options);
    std::atomic<bool> handler_started{false};
    const auto wait_to_respond = [&handler_started]()
            -> boost::asio::awaitable<void> {
        handler_started = true;
        auto executor = co_await boost::asio::this_coro::executor;
        boost::asio::steady_timer timer(executor);
        timer.expires_after(std::chrono::milliseconds(2));
        co_await timer.async_wait(boost::asio::use_awaitable);
    };
    server.RegisterUnaryRaw(
        kUnaryRacePath,
        [wait_to_respond](
                rpcpio::ServerContext&,
                std::string_view,
                std::string& response)
                -> boost::asio::awaitable<rpcpio::Status> {
            co_await wait_to_respond();
            response.assign(kLimit + 1, 'x');
            co_return rpcpio::Status{};
        });
    server.RegisterServerStreamingRaw(
        kServerStreamingRacePath,
        [wait_to_respond](
                rpcpio::ServerContext&,
                std::string_view,
                rpcpio::internal::RawServerWriter& writer)
                -> boost::asio::awaitable<rpcpio::Status> {
            co_await wait_to_respond();
            co_return writer.Write(std::string(kLimit + 1, 'x'));
        });
    server.RegisterClientStreamingRaw(
        kClientStreamingRacePath,
        [wait_to_respond](
                rpcpio::ServerContext&,
                rpcpio::internal::RawServerReader&,
                std::string& response)
                -> boost::asio::awaitable<rpcpio::Status> {
            co_await wait_to_respond();
            response.assign(kLimit + 1, 'x');
            co_return rpcpio::Status{};
        });
    server.RegisterBidiRaw(
        kBidiRacePath,
        [wait_to_respond](
                rpcpio::ServerContext&,
                rpcpio::internal::RawServerReader&,
                rpcpio::internal::RawServerWriter& writer)
                -> boost::asio::awaitable<rpcpio::Status> {
            co_await wait_to_respond();
            co_return writer.Write(std::string(kLimit + 1, 'x'));
        });
    if (!server.Start("127.0.0.1", 0).ok()) {
        return {rpcpio::StatusCode::INTERNAL, "server start failed"};
    }

    boost::asio::io_context client_ioc;
    rpcpio::ChannelOptions channel_options;
    channel_options.use_h2c = true;
    auto channel = std::make_shared<rpcpio::Channel>(
        client_ioc,
        "127.0.0.1",
        server.bound_port(),
        channel_options);
    auto context = std::make_shared<rpcpio::ClientContext>();
    if (event == RaceEvent::kDeadline) {
        context->set_deadline(
            std::chrono::system_clock::now() +
            std::chrono::milliseconds(1));
    } else if (event == RaceEvent::kServerShutdown) {
        context->set_deadline(
            std::chrono::system_clock::now() +
            std::chrono::milliseconds(50));
    }
    rpcpio::Status result{
        rpcpio::StatusCode::UNKNOWN, "not completed"};
    boost::asio::co_spawn(
        client_ioc,
        [&, context]() -> boost::asio::awaitable<void> {
            if (kind == RaceRpcKind::kUnary) {
                auto response = co_await channel->UnaryCallRaw(
                    kUnaryRacePath, *context, "");
                result = response.status;
            } else if (kind == RaceRpcKind::kServerStreaming) {
                auto reader = co_await channel->ServerStreamingCallRaw(
                    kServerStreamingRacePath, *context, "");
                while (true) {
                    auto response = co_await reader.Read();
                    if (!response.status.ok()) {
                        result = response.status;
                        break;
                    }
                    if (!response.data.has_value()) {
                        result = co_await reader.Finish();
                        break;
                    }
                }
            } else if (kind == RaceRpcKind::kClientStreaming) {
                auto writer = co_await channel->ClientStreamingCallRaw(
                    kClientStreamingRacePath, *context);
                co_await writer.WritesDone();
                result = co_await writer.Finish();
            } else {
                auto stream = co_await channel->BidiStreamingCallRaw(
                    kBidiRacePath, *context);
                co_await stream.writer.WritesDone();
                while (true) {
                    auto response = co_await stream.reader.Read();
                    if (!response.status.ok()) {
                        result = response.status;
                        break;
                    }
                    if (!response.data.has_value()) {
                        result = co_await stream.reader.Finish();
                        break;
                    }
                }
            }
            ++terminal_count;
        },
        [channel](std::exception_ptr) { channel->Shutdown(); });

    std::thread client_thread([&] { client_ioc.run(); });
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(1);
    while (!handler_started &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    if (event == RaceEvent::kClientCancel) {
        context->Cancel();
    } else if (event == RaceEvent::kServerShutdown) {
        server.Shutdown();
    }
    client_thread.join();
    server.Shutdown();
    server.Wait();
    return result;
}

TEST(ResponseSize, TerminalRacesCompleteExactlyOnceForEveryRpcKind) {
    for (RaceRpcKind kind : {
             RaceRpcKind::kUnary,
             RaceRpcKind::kServerStreaming,
             RaceRpcKind::kClientStreaming,
             RaceRpcKind::kBidi,
         }) {
        for (RaceEvent event : {
                 RaceEvent::kDeadline,
                 RaceEvent::kClientCancel,
                 RaceEvent::kServerShutdown,
             }) {
            int terminal_count = 0;
            const auto status =
                RunResponseRace(kind, event, terminal_count);
            EXPECT_EQ(terminal_count, 1);
            EXPECT_FALSE(status.ok()) << status.DebugString();
        }
    }
}
