// peer_identity_test.cc
//
// Tests Fix 4: authority() returns the :authority header value and
// peer_identity() is nullopt for plain h2c connections (no mTLS).

#include <optional>
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
#include "rpcpio/server_context.h"
#include "rpcpio/status.h"

static constexpr std::string_view kEchoAuthPath = "/test.Peer/Echo";

TEST(PeerIdentity, H2cYieldsNulloptIdentityAndNonEmptyAuthority) {
    boost::asio::io_context server_ioc;
    rpcpio::ServerOptions opts;
    opts.use_h2c     = true;
    opts.num_threads = 1;

    rpcpio::Server server(server_ioc, opts);

    std::optional<std::string>              captured_authority;
    std::optional<std::optional<std::string>> captured_identity;

    server.RegisterUnaryRaw(kEchoAuthPath,
        [&](rpcpio::ServerContext& ctx,
            std::string_view,
            std::string&) -> boost::asio::awaitable<rpcpio::Status> {
            captured_authority = ctx.authority();
            captured_identity  = ctx.peer_identity();
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

    boost::asio::co_spawn(client_ioc,
        [ch]() -> boost::asio::awaitable<void> {
            rpcpio::ClientContext ctx;
            co_await ch->UnaryCallRaw(kEchoAuthPath, ctx, "");
        },
        boost::asio::detached);

    client_ioc.run();
    server.Shutdown();
    server_thread.join();

    ASSERT_TRUE(captured_identity.has_value())
        << "handler was never invoked";
    EXPECT_FALSE(captured_identity->has_value())
        << "h2c should yield nullopt peer identity";

    ASSERT_TRUE(captured_authority.has_value());
    EXPECT_FALSE(captured_authority->empty())
        << "authority should be set from :authority header";
}
