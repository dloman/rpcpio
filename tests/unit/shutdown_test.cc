// shutdown_test.cc
//
// Tests Fix 2: Channel::Shutdown() permanently stops the channel.
// Calls submitted after shutdown complete with CANCELLED without initiating
// a new connection attempt.

#include <string>
#include <thread>
#include <cstdint>
#include <gtest/gtest.h>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include "rpcpio/channel.h"
#include "rpcpio/client_context.h"
#include "rpcpio/status.h"

static constexpr std::string_view kPath = "/test.Shutdown/Call";

TEST(Shutdown, CallAfterShutdownReturnsCancelled) {
    boost::asio::io_context ioc;
    rpcpio::ChannelOptions copts;
    copts.use_h2c = true;
    // Point to a port that will never connect (loopback, high port likely closed).
    auto ch = std::make_shared<rpcpio::Channel>(ioc, "127.0.0.1", 19999, copts);

    ch->Shutdown();

    rpcpio::Status result{rpcpio::StatusCode::INTERNAL, "not set"};
    bool done = false;
    boost::asio::co_spawn(ioc,
        [ch, &result, &done]() -> boost::asio::awaitable<void> {
            rpcpio::ClientContext ctx;
            auto raw = co_await ch->UnaryCallRaw(kPath, ctx, "");
            result = raw.status;
            done   = true;
        },
        boost::asio::detached);

    ioc.run();

    ASSERT_TRUE(done);
    EXPECT_EQ(result.code(), rpcpio::StatusCode::CANCELLED) << result.message();
}

TEST(Shutdown, IsIdempotent) {
    boost::asio::io_context ioc;
    rpcpio::ChannelOptions copts;
    copts.use_h2c = true;
    auto ch = std::make_shared<rpcpio::Channel>(ioc, "127.0.0.1", 19999, copts);

    // Multiple calls to Shutdown() must not crash or throw.
    EXPECT_NO_THROW({
        ch->Shutdown();
        ch->Shutdown();
        ch->Shutdown();
    });

    ioc.run();
}

TEST(Shutdown, DuringConnect) {
    // Queue a call (triggers connect), shut down before io_context runs.
    boost::asio::io_context ioc;
    rpcpio::ChannelOptions copts;
    copts.use_h2c = true;
    auto ch = std::make_shared<rpcpio::Channel>(ioc, "127.0.0.1", 19999, copts);

    rpcpio::Status result{rpcpio::StatusCode::INTERNAL, "not set"};
    bool done = false;
    boost::asio::co_spawn(ioc,
        [ch, &result, &done]() -> boost::asio::awaitable<void> {
            rpcpio::ClientContext ctx;
            auto raw = co_await ch->UnaryCallRaw(kPath, ctx, "");
            result = raw.status;
            done = true;
        },
        boost::asio::detached);

    ch->Shutdown();
    ioc.run();

    ASSERT_TRUE(done);
    EXPECT_EQ(result.code(), rpcpio::StatusCode::CANCELLED) << result.message();
}

TEST(Shutdown, AfterIocStop) {
    boost::asio::io_context ioc;
    rpcpio::ChannelOptions copts;
    copts.use_h2c = true;
    auto ch = std::make_shared<rpcpio::Channel>(ioc, "127.0.0.1", 19999, copts);

    ioc.stop();
    // Shutdown() must run synchronously without hanging.
    EXPECT_NO_THROW(ch->Shutdown());
}
