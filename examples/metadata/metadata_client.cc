// Metadata example client — attaches custom metadata to a request and prints
// the initial and trailing metadata the server sends back.
// No gRPC Core libraries are used.

#include <iostream>
#include <boost/asio/io_context.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include "echo.rpcpio.pb.h"

static boost::asio::awaitable<void>
RunMetadataDemo(boost::asio::io_context& ioc,
                const std::string&       host,
                std::uint16_t            port)
{
    rpcpio::ChannelOptions opts;
    opts.use_h2c     = true;
    opts.verify_peer = false;

    auto channel = std::make_shared<rpcpio::Channel>(ioc, host, port, opts);
    echomd::EchoStub stub(channel);

    echomd::EchoRequest req;
    req.set_message("ping");

    rpcpio::ClientContext ctx;

    // ── Attach custom metadata to the outgoing request ────────────────────────
    // Keys must be lowercase alphanumeric with hyphens or underscores.
    ctx.AddMetadata("x-request-id", "abc-123");
    ctx.AddMetadata("x-client-version", "1.0");

    auto result = co_await stub.Echo(ctx, req);

    if (!result.status.ok()) {
        std::cerr << "RPC failed: " << result.status.DebugString() << "\n";
        co_return;
    }

    std::cout << "Reply: " << result.response->message() << "\n\n";

    // ── Print initial metadata (sent before the response body) ───────────────
    std::cout << "Initial metadata:\n";
    for (const auto& [key, val] : result.initial_metadata) {
        std::cout << "  " << key << ": " << val << "\n";
    }

    // ── Print trailing metadata (sent after the response body) ───────────────
    std::cout << "\nTrailing metadata:\n";
    for (const auto& [key, val] : result.trailing_metadata) {
        std::cout << "  " << key << ": " << val << "\n";
    }
}

int main(int argc, char* argv[]) {
    std::string   host = "localhost";
    std::uint16_t port = 50054;
    if (argc >= 2) host = argv[1];
    if (argc >= 3) port = static_cast<std::uint16_t>(std::stoi(argv[2]));

    boost::asio::io_context ioc;
    boost::asio::co_spawn(
        ioc, RunMetadataDemo(ioc, host, port), boost::asio::detached);
    ioc.run();
    return 0;
}
