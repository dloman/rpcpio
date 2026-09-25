#include <atomic>
#include <chrono>
#include <functional>
#include <string>
#include <thread>
#include <vector>
#include <cstdint>
#include <gtest/gtest.h>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include "rpcpio/channel.h"
#include "rpcpio/client_context.h"
#include "rpcpio/server.h"
#include "rpcpio/status.h"

static constexpr std::string_view kStrandPath = "/test.Strand/Echo";
static constexpr std::string_view kServerStreamPath =
    "/test.Strand/ServerStream";
static constexpr std::string_view kClientStreamPath =
    "/test.Strand/ClientStream";
static constexpr std::string_view kBidiPath = "/test.Strand/Bidi";

static bool WaitFor(const std::function<bool()>& predicate) {
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (!predicate() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return predicate();
}

TEST(Strand, ConcurrentCallsAllComplete) {
    constexpr int kCalls   = 20;
    constexpr int kWorkers = 4;

    // Server
    boost::asio::io_context server_ioc;
    rpcpio::ServerOptions sopts;
    sopts.use_h2c     = true;
    sopts.num_threads = 2;

    rpcpio::Server server(server_ioc, sopts);
    server.RegisterUnaryRaw(kStrandPath,
        [](rpcpio::ServerContext&,
           std::string_view req,
           std::string& resp) -> boost::asio::awaitable<rpcpio::Status> {
            resp = std::string(req);  // echo
            co_return rpcpio::Status{};
        });

    ASSERT_TRUE(server.Start("127.0.0.1", 0).ok());
    std::uint16_t port = server.bound_port();
    std::thread server_thread([&] { server_ioc.run(); });

    // Client
    boost::asio::io_context client_ioc;
    rpcpio::ChannelOptions copts;
    copts.use_h2c = true;
    auto ch = std::make_shared<rpcpio::Channel>(client_ioc, "127.0.0.1", port, copts);

    std::atomic<int> ok_count{0};
    std::atomic<int> done_count{0};

    for (int i = 0; i < kCalls; ++i) {
        boost::asio::co_spawn(client_ioc,
            [ch, i, &ok_count, &done_count]()
                    -> boost::asio::awaitable<void> {
                rpcpio::ClientContext ctx;
                std::string payload(8, static_cast<char>('a' + (i % 26)));
                auto raw = co_await ch->UnaryCallRaw(kStrandPath, ctx, payload);
                if (raw.status.ok()) ++ok_count;
                if (++done_count == kCalls) ch->Shutdown();
            },
            boost::asio::detached);
    }

    // Run with multiple threads.
    std::vector<std::thread> workers;
    workers.reserve(kWorkers);
    for (int i = 0; i < kWorkers; ++i)
        workers.emplace_back([&] { client_ioc.run(); });
    for (auto& t : workers) t.join();

    server.Shutdown();
    server_thread.join();

    EXPECT_EQ(done_count.load(), kCalls);
    EXPECT_EQ(ok_count.load(), kCalls);
}

TEST(Strand, ThousandMixedCallsCompleteOnceDuringShutdown) {
    constexpr int kCalls = 1000;
    constexpr int kWorkers = 4;

    boost::asio::io_context server_ioc;
    rpcpio::ServerOptions server_options;
    server_options.use_h2c = true;
    server_options.num_threads = 4;
    rpcpio::Server server(server_ioc, server_options);
    server.RegisterUnaryRaw(
        kStrandPath,
        [](rpcpio::ServerContext&,
           std::string_view request,
           std::string& response) -> boost::asio::awaitable<rpcpio::Status> {
            response = request;
            co_return rpcpio::Status{};
        });
    server.RegisterServerStreamingRaw(
        kServerStreamPath,
        [](rpcpio::ServerContext&,
           std::string_view,
           rpcpio::internal::RawServerWriter& writer)
                -> boost::asio::awaitable<rpcpio::Status> {
            co_return writer.Write("");
        });
    server.RegisterClientStreamingRaw(
        kClientStreamPath,
        [](rpcpio::ServerContext&,
           rpcpio::internal::RawServerReader& reader,
           std::string&) -> boost::asio::awaitable<rpcpio::Status> {
            while (true) {
                auto request = co_await reader.Read();
                if (!request.status.ok()) {
                    co_return request.status;
                }
                if (!request.data.has_value()) {
                    break;
                }
            }
            co_return rpcpio::Status{};
        });
    server.RegisterBidiRaw(
        kBidiPath,
        [](rpcpio::ServerContext&,
           rpcpio::internal::RawServerReader& reader,
           rpcpio::internal::RawServerWriter& writer)
                -> boost::asio::awaitable<rpcpio::Status> {
            while (true) {
                auto request = co_await reader.Read();
                if (!request.status.ok()) {
                    co_return request.status;
                }
                if (!request.data.has_value()) {
                    break;
                }
                auto status = writer.Write(*request.data);
                if (!status.ok()) {
                    co_return status;
                }
            }
            co_return rpcpio::Status{};
        });
    ASSERT_TRUE(server.Start("127.0.0.1", 0).ok());

    boost::asio::io_context client_ioc;
    rpcpio::ChannelOptions channel_options;
    channel_options.use_h2c = true;
    auto channel = std::make_shared<rpcpio::Channel>(
        client_ioc.get_executor(),
        "127.0.0.1",
        server.bound_port(),
        channel_options);
    std::atomic<int> started{0};
    std::atomic<int> completed{0};
    std::atomic<int> exceptions{0};

    for (int i = 0; i < kCalls; ++i) {
        boost::asio::co_spawn(
            client_ioc,
            [channel, i, &started]() -> boost::asio::awaitable<void> {
                rpcpio::ClientContext context;
                ++started;
                switch (i % 4) {
                    case 0:
                        co_await channel->UnaryCallRaw(
                            kStrandPath, context, "");
                        break;
                    case 1: {
                        auto reader =
                            co_await channel->ServerStreamingCallRaw(
                                kServerStreamPath, context, "");
                        co_await reader.Finish();
                        break;
                    }
                    case 2: {
                        auto writer =
                            co_await channel->ClientStreamingCallRaw(
                                kClientStreamPath, context);
                        co_await writer.Write("");
                        co_await writer.WritesDone();
                        co_await writer.Finish();
                        break;
                    }
                    case 3: {
                        auto stream =
                            co_await channel->BidiStreamingCallRaw(
                                kBidiPath, context);
                        co_await stream.writer.Write("");
                        co_await stream.writer.WritesDone();
                        co_await stream.reader.Finish();
                        break;
                    }
                }
            },
            [&](std::exception_ptr exception) {
                if (exception) {
                    ++exceptions;
                }
                ++completed;
            });
    }

    std::vector<std::thread> workers;
    for (int i = 0; i < kWorkers; ++i) {
        workers.emplace_back([&] { client_ioc.run(); });
    }
    ASSERT_TRUE(WaitFor([&] { return started.load() == kCalls; }));
    channel->Shutdown();
    const bool all_completed =
        WaitFor([&] { return completed.load() == kCalls; });
    client_ioc.stop();
    for (auto& worker : workers) {
        worker.join();
    }

    server.Shutdown();
    server.Wait();
    EXPECT_TRUE(all_completed);
    EXPECT_EQ(completed, kCalls);
    EXPECT_EQ(exceptions, 0);
}
