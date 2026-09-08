// Greeter client — demonstrates rpcpio unary RPC client.
// No gRPC Core libraries or headers are used.

#include <iostream>
#include <memory>
#include <boost/asio/io_context.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include "helloworld.rpcpio.pb.h"

static boost::asio::awaitable<void>
RunGreeter(boost::asio::io_context& ioc,
           const std::string&       target_host,
           std::uint16_t            target_port,
           const std::string&       name)
{
    rpcpio::ChannelOptions opts;
    opts.use_h2c      = true;   // plaintext for the example
    opts.verify_peer  = false;

    auto channel = std::make_shared<rpcpio::Channel>(ioc, target_host, target_port, opts);

    helloworld::GreeterStub stub(channel);

    helloworld::HelloRequest request;
    request.set_name(name);

    rpcpio::ClientContext ctx;

    auto result = co_await stub.SayHello(ctx, request);

    if (!result.status.ok()) {
        std::cerr << "RPC failed: " << result.status.DebugString() << "\n";
        co_return;
    }

    std::cout << "Greeter received: " << result.response->message() << "\n";
}

int main(int argc, char* argv[]) {
    std::string host = "localhost";
    std::uint16_t port = 50051;
    std::string name = "World";

    if (argc >= 2) host = argv[1];
    if (argc >= 3) port = static_cast<std::uint16_t>(std::stoi(argv[2]));
    if (argc >= 4) name = argv[3];

    boost::asio::io_context ioc;

    boost::asio::co_spawn(
        ioc,
        RunGreeter(ioc, host, port, name),
        boost::asio::detached);

    ioc.run();
    return 0;
}
