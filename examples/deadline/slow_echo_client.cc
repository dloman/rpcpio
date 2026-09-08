// Deadline example client — sets a 500 ms deadline on a call that takes 2 s.
// Expected output: RPC failed: StatusCode::DEADLINE_EXCEEDED ...
// No gRPC Core libraries are used.

#include <iostream>
#include <chrono>
#include <boost/asio/io_context.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include "slow_echo.rpcpio.pb.h"

static boost::asio::awaitable<void>
RunDeadlineDemo(boost::asio::io_context& ioc,
                const std::string&       host,
                std::uint16_t            port)
{
    rpcpio::ChannelOptions opts;
    opts.use_h2c     = true;
    opts.verify_peer = false;

    auto channel = std::make_shared<rpcpio::Channel>(ioc, host, port, opts);
    slowecho::SlowEchoStub stub(channel);

    // ── Call with a tight deadline (will expire before the server replies) ───
    {
        std::cout << "Sending request with 500 ms deadline...\n";
        slowecho::EchoRequest req;
        req.set_message("hello");

        rpcpio::ClientContext ctx;
        ctx.set_deadline(std::chrono::system_clock::now()
                         + std::chrono::milliseconds(500));

        auto result = co_await stub.SlowEcho(ctx, req);
        if (!result.status.ok()) {
            std::cout << "Expected failure: " << result.status.DebugString() << "\n";
        } else {
            // Would print this if the deadline were long enough.
            std::cout << "Reply: " << result.response->message() << "\n";
        }
    }

    // ── Same call without a deadline (succeeds after ~2 s) ───────────────────
    {
        std::cout << "\nSending request without deadline (waits ~2 s)...\n";
        slowecho::EchoRequest req;
        req.set_message("hello");

        rpcpio::ClientContext ctx;  // no deadline set

        auto result = co_await stub.SlowEcho(ctx, req);
        if (result.status.ok()) {
            std::cout << "Reply: " << result.response->message() << "\n";
        } else {
            std::cerr << "Unexpected error: " << result.status.DebugString() << "\n";
        }
    }
}

int main(int argc, char* argv[]) {
    std::string   host = "localhost";
    std::uint16_t port = 50053;
    if (argc >= 2) host = argv[1];
    if (argc >= 3) port = static_cast<std::uint16_t>(std::stoi(argv[2]));

    boost::asio::io_context ioc;
    boost::asio::co_spawn(
        ioc, RunDeadlineDemo(ioc, host, port), boost::asio::detached);
    ioc.run();
    return 0;
}
