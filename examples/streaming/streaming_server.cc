// Streaming example server — demonstrates server-streaming, client-streaming,
// and bidi-streaming RPCs with rpcpio.  No gRPC Core libraries are used.

#include <iostream>
#include <boost/asio/io_context.hpp>
#include <boost/asio/signal_set.hpp>
#include "counter.rpcpio.pb.h"

class CounterServiceImpl final : public counter::CounterService {
public:
    // ── Server-streaming: emit start, start-1, ..., 1 ────────────────────────
    boost::asio::awaitable<rpcpio::Status>
    CountDown(rpcpio::ServerContext&                            ctx,
              const counter::CountRequest&                      req,
              rpcpio::ServerWriter<counter::CountResponse>&     writer) override
    {
        (void)ctx;
        for (int i = req.start(); i >= 1; --i) {
            counter::CountResponse resp;
            resp.set_value(i);
            rpcpio::Status s = co_await writer.Write(resp);
            if (!s.ok()) co_return s;   // client gone
        }
        co_return rpcpio::Status{};
    }

    // ── Client-streaming: accumulate integers and return their sum ────────────
    boost::asio::awaitable<rpcpio::StatusOr<counter::SumResponse>>
    Sum(rpcpio::ServerContext&                      ctx,
        rpcpio::ServerReader<counter::SumRequest>&  reader) override
    {
        (void)ctx;
        int32_t total = 0;
        while (true) {
            auto item = co_await reader.Read();
            if (!item.ok())
                co_return rpcpio::StatusOr<counter::SumResponse>(item.status());
            if (!item->has_value()) break;   // clean client half-close
            total += (*item)->value();
        }
        counter::SumResponse resp;
        resp.set_total(total);
        co_return resp;
    }

    // ── Bidi-streaming: echo each message back prefixed with "echo: " ────────
    boost::asio::awaitable<rpcpio::Status>
    Chat(rpcpio::ServerContext&                          ctx,
         rpcpio::ServerReader<counter::ChatMessage>&     reader,
         rpcpio::ServerWriter<counter::ChatMessage>&     writer) override
    {
        (void)ctx;
        while (true) {
            auto item = co_await reader.Read();
            if (!item.ok()) co_return item.status();
            if (!item->has_value()) break;   // client done writing

            counter::ChatMessage reply;
            reply.set_text("echo: " + (*item)->text());
            rpcpio::Status s = co_await writer.Write(reply);
            if (!s.ok()) co_return s;
        }
        co_return rpcpio::Status{};
    }
};

int main(int argc, char* argv[]) {
    std::uint16_t port = 50052;
    if (argc >= 2) port = static_cast<std::uint16_t>(std::stoi(argv[1]));

    boost::asio::io_context ioc;

    rpcpio::ServerOptions opts;
    opts.use_h2c     = true;
    opts.num_threads = 4;

    rpcpio::Server server(ioc, opts);

    CounterServiceImpl service;
    service.Register(server);

    server.Start("0.0.0.0", port);
    std::cout << "Streaming server listening on 0.0.0.0:" << port << "\n";

    boost::asio::signal_set signals(ioc, SIGINT, SIGTERM);
    signals.async_wait([&](const boost::system::error_code&, int) {
        server.Shutdown();
    });

    server.Wait();
    return 0;
}
