// Greeter server — demonstrates rpcpio unary RPC server.
// No gRPC Core libraries or headers are used.

#include <iostream>
#include <memory>
#include <boost/asio/io_context.hpp>
#include <boost/asio/signal_set.hpp>
#include "helloworld.rpcpio.pb.h"

class GreeterServiceImpl final : public helloworld::GreeterService {
public:
    boost::asio::awaitable<rpcpio::StatusOr<helloworld::HelloReply>>
    SayHello(rpcpio::ServerContext&        ctx,
             const helloworld::HelloRequest&  request) override
    {
        (void)ctx;
        helloworld::HelloReply reply;
        reply.set_message("Hello, " + request.name() + "!");
        co_return reply;
    }
};

int main(int argc, char* argv[]) {
    std::string host = "0.0.0.0";
    std::uint16_t port = 50051;
    if (argc >= 2) port = static_cast<std::uint16_t>(std::stoi(argv[1]));

    boost::asio::io_context ioc;

    rpcpio::ServerOptions opts;
    opts.use_h2c      = true;   // plaintext for the example
    opts.num_threads  = 4;

    rpcpio::Server server(ioc, opts);

    GreeterServiceImpl service;
    service.Register(server);

    server.Start(host, port);
    std::cout << "Greeter server listening on " << host << ":" << port << "\n";

    // Wait for SIGINT/SIGTERM.
    boost::asio::signal_set signals(ioc, SIGINT, SIGTERM);
    signals.async_wait([&](const boost::system::error_code&, int) {
        server.Shutdown();
    });

    server.Wait();
    return 0;
}
