#include <atomic>
#include <chrono>
#include <cstdint>
#include <exception>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <gtest/gtest.h>

#include "rpcpio/channel.h"
#include "rpcpio/client_context.h"
#include "rpcpio/server.h"
#include "rpcpio/server_context.h"
#include "rpcpio/status.h"

namespace {

constexpr std::string_view kEchoAuthPath = "/test.Peer/Echo";
constexpr std::string_view kUriIdentity = "spiffe://rpcpio.test/client-uri";
constexpr std::string_view kDnsIdentity = "client.rpcpio.test";

std::string CertPath(std::string_view filename) {
    return "tests/certs/" + std::string(filename);
}

rpcpio::ServerOptions ServerTlsOptions(std::uint32_t num_threads) {
    rpcpio::ServerOptions options;
    options.num_threads = num_threads;
    options.server_cert_file = CertPath("server.pem");
    options.server_key_file = CertPath("server.key");
    options.ca_cert_file = CertPath("ca.pem");
    return options;
}

rpcpio::ChannelOptions ClientTlsOptions(std::string_view name) {
    rpcpio::ChannelOptions options;
    options.use_tls = true;
    options.verify_peer = true;
    options.ca_cert_file = CertPath("ca.pem");
    options.client_cert_file = CertPath(std::string(name) + ".pem");
    options.client_key_file = CertPath(std::string(name) + ".key");
    return options;
}

void RunInterleavedIdentityTest(std::uint32_t num_threads) {
    boost::asio::io_context server_ioc;
    rpcpio::Server server(server_ioc, ServerTlsOptions(num_threads));

    std::mutex observed_mutex;
    std::vector<std::optional<std::string>> observed;
    server.RegisterUnaryRaw(
        kEchoAuthPath,
        [&](rpcpio::ServerContext& context,
            std::string_view,
            std::string&) -> boost::asio::awaitable<rpcpio::Status> {
            std::lock_guard lock(observed_mutex);
            observed.push_back(context.peer_identity());
            co_return rpcpio::Status{};
        });

    ASSERT_TRUE(server.Start("127.0.0.1", 0).ok());

    boost::asio::io_context client_ioc;
    auto uri_channel = std::make_shared<rpcpio::Channel>(
        client_ioc,
        "127.0.0.1",
        server.bound_port(),
        ClientTlsOptions("client_uri"));
    auto dns_channel = std::make_shared<rpcpio::Channel>(
        client_ioc,
        "127.0.0.1",
        server.bound_port(),
        ClientTlsOptions("client_dns"));

    std::vector<rpcpio::Status> statuses;
    std::exception_ptr failure;
    boost::asio::co_spawn(
        client_ioc,
        [&]() -> boost::asio::awaitable<void> {
            for (int i = 0; i < 3; ++i) {
                for (const auto& channel : {uri_channel, dns_channel}) {
                    rpcpio::ClientContext context;
                    auto result =
                        co_await channel->UnaryCallRaw(kEchoAuthPath, context, "");
                    statuses.push_back(std::move(result.status));
                }
            }
        },
        [&](std::exception_ptr exception) {
            failure = exception;
            uri_channel->Shutdown();
            dns_channel->Shutdown();
        });

    client_ioc.run();
    server.Shutdown();

    ASSERT_EQ(failure, nullptr);
    ASSERT_EQ(statuses.size(), 6u);
    for (const auto& status : statuses) {
        EXPECT_TRUE(status.ok()) << status.DebugString();
    }

    const std::vector<std::optional<std::string>> expected = {
        std::string(kUriIdentity),
        std::string(kDnsIdentity),
        std::string(kUriIdentity),
        std::string(kDnsIdentity),
        std::string(kUriIdentity),
        std::string(kDnsIdentity),
    };
    EXPECT_EQ(observed, expected);
}

} // namespace

TEST(PeerIdentity, H2cHasAuthorityButNoIdentity) {
    boost::asio::io_context server_ioc;
    rpcpio::ServerOptions options;
    options.use_h2c = true;
    options.num_threads = 1;
    rpcpio::Server server(server_ioc, options);

    std::optional<std::string> captured_authority;
    std::optional<std::optional<std::string>> captured_identity;
    server.RegisterUnaryRaw(
        kEchoAuthPath,
        [&](rpcpio::ServerContext& context,
            std::string_view,
            std::string&) -> boost::asio::awaitable<rpcpio::Status> {
            captured_authority = context.authority();
            captured_identity = context.peer_identity();
            co_return rpcpio::Status{};
        });

    ASSERT_TRUE(server.Start("127.0.0.1", 0).ok());

    boost::asio::io_context client_ioc;
    rpcpio::ChannelOptions client_options;
    client_options.use_h2c = true;
    auto channel = std::make_shared<rpcpio::Channel>(
        client_ioc, "127.0.0.1", server.bound_port(), client_options);

    boost::asio::co_spawn(
        client_ioc,
        [channel]() -> boost::asio::awaitable<void> {
            rpcpio::ClientContext context;
            co_await channel->UnaryCallRaw(kEchoAuthPath, context, "");
        },
        [channel](std::exception_ptr) { channel->Shutdown(); });

    client_ioc.run();
    server.Shutdown();

    ASSERT_TRUE(captured_identity.has_value());
    EXPECT_FALSE(captured_identity->has_value());
    ASSERT_TRUE(captured_authority.has_value());
    EXPECT_FALSE(captured_authority->empty());
}

TEST(PeerIdentity, UriSanWinsAndIdentityPersistsSingleThreaded) {
    RunInterleavedIdentityTest(1);
}

TEST(PeerIdentity, UriSanWinsAndIdentityPersistsMultiThreaded) {
    RunInterleavedIdentityTest(4);
}

TEST(PeerIdentity, VerifiedCertificateWithoutSanHasNoIdentity) {
    boost::asio::io_context server_ioc;
    rpcpio::Server server(server_ioc, ServerTlsOptions(1));

    std::optional<std::optional<std::string>> captured_identity;
    server.RegisterUnaryRaw(
        kEchoAuthPath,
        [&](rpcpio::ServerContext& context,
            std::string_view,
            std::string&) -> boost::asio::awaitable<rpcpio::Status> {
            captured_identity = context.peer_identity();
            co_return rpcpio::Status{};
        });
    ASSERT_TRUE(server.Start("127.0.0.1", 0).ok());

    boost::asio::io_context client_ioc;
    auto channel = std::make_shared<rpcpio::Channel>(
        client_ioc,
        "127.0.0.1",
        server.bound_port(),
        ClientTlsOptions("client_no_san"));
    rpcpio::Status status{rpcpio::StatusCode::UNKNOWN, "call did not finish"};
    boost::asio::co_spawn(
        client_ioc,
        [&]() -> boost::asio::awaitable<void> {
            rpcpio::ClientContext context;
            auto result =
                co_await channel->UnaryCallRaw(kEchoAuthPath, context, "");
            status = std::move(result.status);
        },
        [channel](std::exception_ptr) { channel->Shutdown(); });

    client_ioc.run();
    server.Shutdown();

    EXPECT_TRUE(status.ok()) << status.DebugString();
    ASSERT_TRUE(captured_identity.has_value());
    EXPECT_FALSE(captured_identity->has_value());
}

TEST(PeerIdentity, UnverifiedClientNeverReachesHandler) {
    boost::asio::io_context server_ioc;
    rpcpio::Server server(server_ioc, ServerTlsOptions(1));

    std::atomic<int> handler_calls{0};
    server.RegisterUnaryRaw(
        kEchoAuthPath,
        [&](rpcpio::ServerContext&,
            std::string_view,
            std::string&) -> boost::asio::awaitable<rpcpio::Status> {
            ++handler_calls;
            co_return rpcpio::Status{};
        });
    ASSERT_TRUE(server.Start("127.0.0.1", 0).ok());

    boost::asio::io_context client_ioc;
    auto channel = std::make_shared<rpcpio::Channel>(
        client_ioc,
        "127.0.0.1",
        server.bound_port(),
        ClientTlsOptions("client_untrusted"));
    boost::asio::steady_timer timeout(client_ioc, std::chrono::milliseconds(200));
    timeout.async_wait([channel](const boost::system::error_code&) {
        channel->Shutdown();
    });
    rpcpio::Status status{rpcpio::StatusCode::UNKNOWN, "call did not finish"};
    boost::asio::co_spawn(
        client_ioc,
        [&]() -> boost::asio::awaitable<void> {
            rpcpio::ClientContext context;
            auto result =
                co_await channel->UnaryCallRaw(kEchoAuthPath, context, "");
            status = std::move(result.status);
        },
        [channel](std::exception_ptr) { channel->Shutdown(); });

    client_ioc.run();
    server.Shutdown();

    EXPECT_FALSE(status.ok());
    EXPECT_EQ(handler_calls, 0);
}
