// Greeter server — demonstrates rpcpio unary RPC server.
// No gRPC Core libraries or headers are used.

#include <iostream>
#include <memory>
#include <boost/asio/signal_set.hpp>
#include "rpcpio/standalone_server.h"
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

    rpcpio::ServerOptions opts;
    opts.use_h2c     = true;
    opts.num_threads = 4;

    rpcpio::StandaloneServer standalone(opts);

    GreeterServiceImpl service;
    service.Register(standalone.server());

    standalone.Start(host, port);
    std::cout << "Greeter server listening on " << host << ":"
              << standalone.bound_port() << "\n";

    standalone.Wait();
    return 0;
}
