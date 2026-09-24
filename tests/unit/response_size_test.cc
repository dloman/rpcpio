// response_size_test.cc
//
// Tests Fix 5: responses at the limit succeed; one byte over returns
// RESOURCE_EXHAUSTED.

#include <string>
#include <thread>
#include <cstdint>
#include <gtest/gtest.h>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include "rpcpio/channel.h"
#include "rpcpio/client_context.h"
#include "rpcpio/server.h"
#include "rpcpio/status.h"

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
