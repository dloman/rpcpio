// Streaming example client — exercises CountDown, Sum, and Chat RPCs.
// No gRPC Core libraries are used.

#include <iostream>
#include <vector>
#include <boost/asio/io_context.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include "counter.rpcpio.pb.h"

static boost::asio::awaitable<void>
RunExamples(boost::asio::io_context& ioc,
            const std::string&       host,
            std::uint16_t            port)
{
    rpcpio::ChannelOptions opts;
    opts.use_h2c     = true;
    opts.verify_peer = false;

    auto channel = std::make_shared<rpcpio::Channel>(ioc, host, port, opts);
    counter::CounterStub stub(channel);

    // ── Server-streaming: count down from 5 ──────────────────────────────────
    {
        std::cout << "\n=== CountDown (server-streaming) ===\n";
        counter::CountRequest req;
        req.set_start(5);

        rpcpio::ClientContext ctx;
        auto reader = co_await stub.CountDown(ctx, req);

        while (true) {
            auto item = co_await reader.Read();
            if (!item.ok()) {
                std::cerr << "Read error: " << item.status().DebugString() << "\n";
                break;
            }
            if (!item->has_value()) break;   // clean end-of-stream
            std::cout << "  " << (*item)->value() << "\n";
        }
        rpcpio::Status s = co_await reader.Finish();
        std::cout << "Finished: " << s.DebugString() << "\n";
    }

    // ── Client-streaming: send 1+2+3+4+5, expect sum=15 ─────────────────────
    {
        std::cout << "\n=== Sum (client-streaming) ===\n";
        rpcpio::ClientContext ctx;
        auto writer = co_await stub.Sum(ctx);

        for (int i = 1; i <= 5; ++i) {
            counter::SumRequest req;
            req.set_value(i);
            rpcpio::Status s = co_await writer.Write(req);
            if (!s.ok()) {
                std::cerr << "Write error: " << s.DebugString() << "\n";
                break;
            }
        }
        co_await writer.WritesDone();

        auto result = co_await writer.FinishAndGetResponse();
        if (result.status.ok()) {
            std::cout << "Sum = " << result.response->total() << "\n";  // 15
        } else {
            std::cerr << "Error: " << result.status.DebugString() << "\n";
        }
    }

    // ── Bidi-streaming: send messages, read echoes ────────────────────────────
    {
        std::cout << "\n=== Chat (bidi-streaming) ===\n";
        rpcpio::ClientContext ctx;
        auto stream = co_await stub.Chat(ctx);

        const std::vector<std::string> messages = {"hello", "world", "rpcpio"};
        for (const auto& text : messages) {
            counter::ChatMessage msg;
            msg.set_text(text);

            rpcpio::Status ws = co_await stream.Write(msg);
            if (!ws.ok()) {
                std::cerr << "Write error: " << ws.DebugString() << "\n";
                break;
            }

            // Read the server's echo for this message.
            auto reply = co_await stream.Read();
            if (!reply.ok()) {
                std::cerr << "Read error: " << reply.status().DebugString() << "\n";
                break;
            }
            if (!reply->has_value()) break;
            std::cout << "  " << (*reply)->text() << "\n";
        }

        co_await stream.WritesDone();
        rpcpio::Status s = co_await stream.Finish();
        std::cout << "Finished: " << s.DebugString() << "\n";
    }
}

int main(int argc, char* argv[]) {
    std::string   host = "localhost";
    std::uint16_t port = 50052;
    if (argc >= 2) host = argv[1];
    if (argc >= 3) port = static_cast<std::uint16_t>(std::stoi(argv[2]));

    boost::asio::io_context ioc;
    boost::asio::co_spawn(ioc, RunExamples(ioc, host, port), boost::asio::detached);
    ioc.run();
    return 0;
}
