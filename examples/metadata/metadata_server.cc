// Metadata example server — reads client metadata and sends initial/trailing
// metadata back to the client.  No gRPC Core libraries are used.

#include <iostream>
#include <boost/asio/io_context.hpp>
#include <boost/asio/signal_set.hpp>
#include "echo.rpcpio.pb.h"

class EchoServiceImpl final : public echomd::EchoService {
public:
    boost::asio::awaitable<rpcpio::StatusOr<echomd::EchoResponse>>
    Echo(rpcpio::ServerContext&       ctx,
         const echomd::EchoRequest&   request) override
    {
        // ── Read metadata the client sent ────────────────────────────────────
        const rpcpio::MetadataMap& meta = ctx.client_metadata();
        auto it = meta.find("x-request-id");
        if (it != meta.end()) {
            std::cout << "Server received x-request-id: " << it->second << "\n";
        }
        std::cout << "Server received peer: " << ctx.peer() << "\n";

        // ── Attach metadata to the response ──────────────────────────────────
        // Initial metadata is sent before the response body.
        ctx.AddInitialMetadata("x-server-name", "metadata-example");

        // Trailing metadata is sent after the response body (in the trailers frame).
        ctx.AddTrailingMetadata("x-call-status", "processed");

        echomd::EchoResponse reply;
        reply.set_message(request.message());
        co_return reply;
    }
};

int main(int argc, char* argv[]) {
    std::uint16_t port = 50054;
    if (argc >= 2) port = static_cast<std::uint16_t>(std::stoi(argv[1]));

    boost::asio::io_context ioc;

    rpcpio::ServerOptions opts;
    opts.use_h2c     = true;
    opts.num_threads = 4;

    rpcpio::Server server(ioc, opts);

    EchoServiceImpl service;
    service.Register(server);

    server.Start("0.0.0.0", port);
    std::cout << "Metadata echo server listening on 0.0.0.0:" << port << "\n";

    boost::asio::signal_set signals(ioc, SIGINT, SIGTERM);
    signals.async_wait([&](const boost::system::error_code&, int) {
        server.Shutdown();
    });

    server.Wait();
    return 0;
}
