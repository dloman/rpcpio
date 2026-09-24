// strand_test.cc
//
// Tests Fix 1: multi-threaded channel usage.
// Many concurrent unary calls through a single Channel over an h2c connection,
// all completing with their expected statuses.  When run under ThreadSanitizer,
// this exercises the strand confinement.

#include <atomic>
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
                ++done_count;
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
