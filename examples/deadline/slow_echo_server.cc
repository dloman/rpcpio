// Deadline example server — handler sleeps for 2 seconds before replying.
// Run with the slow_echo_client to observe DEADLINE_EXCEEDED.
// No gRPC Core libraries are used.

#include <iostream>
#include <chrono>
#include <boost/asio/io_context.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include "slow_echo.rpcpio.pb.h"

class SlowEchoServiceImpl final : public slowecho::SlowEchoService {
public:
    boost::asio::awaitable<rpcpio::StatusOr<slowecho::EchoResponse>>
    SlowEcho(rpcpio::ServerContext&          ctx,
             const slowecho::EchoRequest&    request) override
    {
        (void)ctx;
        // Simulate slow work — sleep asynchronously for 2 seconds.
        // The cancellation_slot() on ctx fires when the client's deadline expires;
        // bind it to the timer wait to react promptly.
        auto executor = co_await boost::asio::this_coro::executor;
        boost::asio::steady_timer timer(executor);
        timer.expires_after(std::chrono::seconds(2));
        boost::system::error_code ec;
        co_await timer.async_wait(
            boost::asio::bind_cancellation_slot(
                ctx.cancellation_slot(),
                boost::asio::redirect_error(boost::asio::use_awaitable, ec)));

        if (ec == boost::asio::error::operation_aborted) {
            co_return rpcpio::StatusOr<slowecho::EchoResponse>(
                rpcpio::Status{rpcpio::StatusCode::CANCELLED, "deadline exceeded"});
        }

        slowecho::EchoResponse reply;
        reply.set_message(request.message());
        co_return reply;
    }
};

int main(int argc, char* argv[]) {
    std::uint16_t port = 50053;
    if (argc >= 2) port = static_cast<std::uint16_t>(std::stoi(argv[1]));

    boost::asio::io_context ioc;

    rpcpio::ServerOptions opts;
    opts.use_h2c     = true;
    opts.num_threads = 4;

    rpcpio::Server server(ioc, opts);

    SlowEchoServiceImpl service;
    service.Register(server);

    server.Start("0.0.0.0", port);
    std::cout << "SlowEcho server listening on 0.0.0.0:" << port
              << "  (sleeps 2 s per call)\n";

    boost::asio::signal_set signals(ioc, SIGINT, SIGTERM);
    signals.async_wait([&](const boost::system::error_code&, int) {
        server.Shutdown();
    });

    server.Wait();
    return 0;
}
