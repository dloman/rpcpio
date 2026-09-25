#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <gtest/gtest.h>

#include "rpcpio/channel.h"
#include "rpcpio/client_context.h"
#include "rpcpio/status.h"

namespace {

constexpr std::string_view kPath = "/test.TlsConfig/Call";

std::string CertPath(std::string_view filename) {
    return "tests/certs/" + std::string(filename);
}

struct InvalidConfiguration {
    std::string name;
    rpcpio::ChannelOptions options;
    std::string message_fragment;
};

std::vector<InvalidConfiguration> InvalidConfigurations() {
    rpcpio::ChannelOptions missing_ca;
    missing_ca.ca_cert_file = CertPath("missing-ca.pem");

    rpcpio::ChannelOptions unreadable_ca;
    unreadable_ca.ca_cert_file = "tests/certs";

    rpcpio::ChannelOptions malformed_ca;
    malformed_ca.ca_cert_file = CertPath("malformed.pem");

    rpcpio::ChannelOptions missing_cert;
    missing_cert.ca_cert_file = CertPath("ca.pem");
    missing_cert.client_cert_file = CertPath("missing-client.pem");
    missing_cert.client_key_file = CertPath("client_uri.key");

    rpcpio::ChannelOptions malformed_cert;
    malformed_cert.ca_cert_file = CertPath("ca.pem");
    malformed_cert.client_cert_file = CertPath("malformed.pem");
    malformed_cert.client_key_file = CertPath("client_uri.key");

    rpcpio::ChannelOptions missing_key;
    missing_key.ca_cert_file = CertPath("ca.pem");
    missing_key.client_cert_file = CertPath("client_uri.pem");
    missing_key.client_key_file = CertPath("missing-client.key");

    rpcpio::ChannelOptions malformed_key;
    malformed_key.ca_cert_file = CertPath("ca.pem");
    malformed_key.client_cert_file = CertPath("client_uri.pem");
    malformed_key.client_key_file = CertPath("malformed.pem");

    rpcpio::ChannelOptions mismatched_pair;
    mismatched_pair.ca_cert_file = CertPath("ca.pem");
    mismatched_pair.client_cert_file = CertPath("client_uri.pem");
    mismatched_pair.client_key_file = CertPath("client_dns.key");

    rpcpio::ChannelOptions cert_without_key;
    cert_without_key.client_cert_file = CertPath("client_uri.pem");

    rpcpio::ChannelOptions key_without_cert;
    key_without_cert.client_key_file = CertPath("client_uri.key");

    return {
        {"missing CA", std::move(missing_ca), "CA certificate file"},
        {"unreadable CA", std::move(unreadable_ca), "CA certificate file"},
        {"malformed CA", std::move(malformed_ca), "CA certificate file"},
        {"missing certificate", std::move(missing_cert),
         "client certificate file"},
        {"malformed certificate", std::move(malformed_cert),
         "client certificate file"},
        {"missing key", std::move(missing_key), "client private key file"},
        {"malformed key", std::move(malformed_key), "client private key file"},
        {"mismatched pair", std::move(mismatched_pair), "mismatch"},
        {"certificate without key", std::move(cert_without_key),
         "must be set together"},
        {"key without certificate", std::move(key_without_cert),
         "must be set together"},
    };
}

} // namespace

TEST(ClientTlsConfiguration, ValidationAndCallsReturnStickyError) {
    for (const auto& test_case : InvalidConfigurations()) {
        SCOPED_TRACE(test_case.name);
        const rpcpio::Status validation =
            rpcpio::ValidateChannelOptions(test_case.options);
        ASSERT_EQ(
            validation.code(), rpcpio::StatusCode::INVALID_ARGUMENT);
        EXPECT_NE(
            validation.message().find(test_case.message_fragment),
            std::string::npos)
            << validation.DebugString();

        boost::asio::io_context ioc;
        auto channel = std::make_shared<rpcpio::Channel>(
            ioc, "127.0.0.1", 443, test_case.options);
        boost::asio::co_spawn(
            ioc,
            [channel, &validation]() -> boost::asio::awaitable<void> {
                const rpcpio::Status connect_status =
                    co_await channel->Connect();
                EXPECT_EQ(connect_status, validation);

                for (int i = 0; i < 2; ++i) {
                    rpcpio::ClientContext context;
                    auto result = co_await channel->UnaryCallRaw(
                        kPath, context, "");
                    EXPECT_EQ(result.status, validation);
                }
            },
            boost::asio::detached);
        ioc.run();
    }
}
