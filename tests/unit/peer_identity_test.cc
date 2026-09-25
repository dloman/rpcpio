// peer_identity_test.cc
//
// Tests Fix 4: authority() returns the :authority header value and
// peer_identity() is nullopt for plain h2c connections (no mTLS).

#include <fstream>
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
        [ch](std::exception_ptr) { ch->Shutdown(); });

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

TEST(PeerIdentity, MtlsPopulatesPeerIdentity) {
    // Requires pre-generated cert files. Skip if not present.
    // Generate with: openssl genrsa -out /tmp/rpcpio_test_ca.key 2048
    //   openssl req -new -x509 -days 365 -key /tmp/rpcpio_test_ca.key -out /tmp/rpcpio_test_ca.pem -subj "/CN=TestCA"
    //   openssl genrsa -out /tmp/rpcpio_test_server.key 2048
    //   openssl req -new -key /tmp/rpcpio_test_server.key -out /tmp/rpcpio_test_server.csr -subj "/CN=localhost"
    //   openssl x509 -req -days 365 -in /tmp/rpcpio_test_server.csr -CA /tmp/rpcpio_test_ca.pem -CAkey /tmp/rpcpio_test_ca.key -CAcreateserial -out /tmp/rpcpio_test_server.pem
    //   cp /tmp/rpcpio_test_ca.pem /tmp/rpcpio_test_client.pem; cp /tmp/rpcpio_test_ca.key /tmp/rpcpio_test_client.key
    const std::string ca_file     = "/tmp/rpcpio_test_ca.pem";
    const std::string server_cert = "/tmp/rpcpio_test_server.pem";
    const std::string server_key  = "/tmp/rpcpio_test_server.key";
    const std::string client_cert = "/tmp/rpcpio_test_client.pem";
    const std::string client_key  = "/tmp/rpcpio_test_client.key";

    // Check files exist.
    auto file_exists = [](const std::string& p) {
        std::ifstream f(p); return f.good();
    };
    if (!file_exists(ca_file) || !file_exists(server_cert) || !file_exists(server_key)) {
        GTEST_SKIP() << "TLS test cert files not found at /tmp/rpcpio_test_*.pem";
    }

    boost::asio::io_context server_ioc;
    rpcpio::ServerOptions opts;
    opts.num_threads      = 1;
    opts.server_cert_file = server_cert;
    opts.server_key_file  = server_key;
    opts.ca_cert_file     = ca_file;

    rpcpio::Server server(server_ioc, opts);

    std::optional<std::optional<std::string>> captured_identity;
    server.RegisterUnaryRaw(kEchoAuthPath,
        [&](rpcpio::ServerContext& ctx,
            std::string_view,
            std::string&) -> boost::asio::awaitable<rpcpio::Status> {
            captured_identity = ctx.peer_identity();
            co_return rpcpio::Status{};
        });

    ASSERT_TRUE(server.Start("127.0.0.1", 0).ok());
    std::uint16_t port = server.bound_port();
    std::thread server_thread([&] { server_ioc.run(); });

    boost::asio::io_context client_ioc;
    rpcpio::ChannelOptions copts;
    copts.use_tls          = true;
    copts.verify_peer      = true;
    copts.ca_cert_file     = ca_file;
    if (file_exists(client_cert) && file_exists(client_key)) {
        copts.client_cert_file = client_cert;
        copts.client_key_file  = client_key;
    }
    auto ch = std::make_shared<rpcpio::Channel>(client_ioc, "127.0.0.1", port, copts);

    boost::asio::co_spawn(client_ioc,
        [ch]() -> boost::asio::awaitable<void> {
            co_await ch->Connect();
            rpcpio::ClientContext ctx;
            co_await ch->UnaryCallRaw(kEchoAuthPath, ctx, "");
        },
        [ch](std::exception_ptr) { ch->Shutdown(); });

    client_ioc.run();
    server.Shutdown();
    server_thread.join();

    ASSERT_TRUE(captured_identity.has_value()) << "handler was never invoked";
    EXPECT_TRUE(captured_identity->has_value())
        << "mTLS connection should populate peer_identity";
}
