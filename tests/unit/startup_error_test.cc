// startup_error_test.cc
//
// Tests Fix 3: Start() returns a Status instead of throwing on error.

#include <gtest/gtest.h>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include "rpcpio/server.h"
#include "rpcpio/status.h"

TEST(StartupError, OccupiedPortReturnsUnavailableNoThrow) {
    boost::asio::io_context ioc;

    // Occupy the port.
    boost::asio::ip::tcp::acceptor holder(ioc);
    holder.open(boost::asio::ip::tcp::v4());
    holder.set_option(boost::asio::ip::tcp::acceptor::reuse_address(true));
    holder.bind(boost::asio::ip::tcp::endpoint(
        boost::asio::ip::tcp::v4(), 0));
    holder.listen();
    std::uint16_t occupied = holder.local_endpoint().port();

    rpcpio::ServerOptions opts;
    opts.use_h2c = true;
    opts.num_threads = 1;
    rpcpio::Server server(ioc, opts);
    rpcpio::Status s = server.Start("127.0.0.1", occupied);
    EXPECT_FALSE(s.ok());
    EXPECT_EQ(s.code(), rpcpio::StatusCode::UNAVAILABLE) << s.message();
}

TEST(StartupError, MissingCertFileReturnsInvalidArgNoThrow) {
    boost::asio::io_context ioc;
    rpcpio::ServerOptions opts;
    opts.num_threads         = 1;
    opts.server_cert_file    = "/nonexistent/server.pem";
    opts.server_key_file     = "/nonexistent/server.key";
    rpcpio::Server server(ioc, opts);
    rpcpio::Status s = server.Start("127.0.0.1", 0);
    EXPECT_FALSE(s.ok());
    EXPECT_EQ(s.code(), rpcpio::StatusCode::INVALID_ARGUMENT) << s.message();
}
