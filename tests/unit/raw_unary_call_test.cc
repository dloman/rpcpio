#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <string_view>
#include <thread>
#include <vector>

#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <gtest/gtest.h>

#include "rpcpio/channel.h"
#include "rpcpio/client_context.h"
#include "rpcpio/server.h"
#include "rpcpio/server_context.h"
#include "rpcpio/status.h"

namespace {

using namespace std::chrono_literals;

constexpr std::string_view kPath = "/test.RawUnary/Call";

enum class CancelSource {
    kContext,
    kCoroutine,
};

rpcpio::ChannelOptions H2cOptions() {
    rpcpio::ChannelOptions options;
    options.use_h2c = true;
    return options;
}

bool WaitFor(const std::function<bool()>& predicate) {
    const auto deadline = std::chrono::steady_clock::now() + 2s;
    while (!predicate() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(1ms);
    }
    return predicate();
}

class CountingListener {
public:
    CountingListener()
        : acceptor_(ioc_, {
              boost::asio::ip::address_v4::loopback(), 0}) {
        Accept();
        thread_ = std::thread([this] { ioc_.run(); });
    }

    ~CountingListener() {
        ioc_.stop();
        thread_.join();
    }

    std::uint16_t port() const {
        return acceptor_.local_endpoint().port();
    }

    int connection_count() const {
        return connection_count_.load();
    }

private:
    void Accept() {
        auto socket =
            std::make_shared<boost::asio::ip::tcp::socket>(ioc_);
        acceptor_.async_accept(
            *socket,
            [this, socket](const boost::system::error_code& error) {
                if (error) {
                    return;
                }
                sockets_.push_back(socket);
                ++connection_count_;
                Accept();
            });
    }

    boost::asio::io_context ioc_;
    boost::asio::ip::tcp::acceptor acceptor_;
    std::vector<std::shared_ptr<boost::asio::ip::tcp::socket>> sockets_;
    std::atomic<int> connection_count_{0};
    std::thread thread_;
};

struct CallObservation {
    std::atomic<bool> started{false};
    std::atomic<int> completions{0};
    rpcpio::Status status{
        rpcpio::StatusCode::UNKNOWN, "not completed"};
    std::thread::id completion_thread;
};

void SpawnRawCall(
        boost::asio::io_context& caller_ioc,
        const std::shared_ptr<rpcpio::Channel>& channel,
        rpcpio::ClientContext& context,
        boost::asio::cancellation_signal& signal,
        CallObservation& observation) {
    boost::asio::co_spawn(
        caller_ioc,
        [&]() -> boost::asio::awaitable<void> {
            observation.started = true;
            auto result =
                co_await channel->UnaryCallRaw(kPath, context, "");
            observation.status = std::move(result.status);
            observation.completion_thread = std::this_thread::get_id();
            ++observation.completions;
        },
        boost::asio::bind_cancellation_slot(
            signal.slot(), boost::asio::detached));
}

void Cancel(
        CancelSource source,
        rpcpio::ClientContext& context,
        boost::asio::cancellation_signal& signal) {
    if (source == CancelSource::kContext) {
        context.Cancel();
    } else {
        signal.emit(boost::asio::cancellation_type::all);
    }
}

void RunBeforeConnect(CancelSource source) {
    boost::asio::io_context channel_ioc;
    boost::asio::io_context caller_ioc;
    auto caller_guard = boost::asio::make_work_guard(caller_ioc);
    auto channel = std::make_shared<rpcpio::Channel>(
        channel_ioc, "127.0.0.1", 1, H2cOptions());
    rpcpio::ClientContext context;
    boost::asio::cancellation_signal signal;
    CallObservation observation;

    SpawnRawCall(
        caller_ioc, channel, context, signal, observation);
    std::thread::id caller_thread_id;
    std::thread caller_thread([&] {
        caller_thread_id = std::this_thread::get_id();
        caller_ioc.run();
    });
    ASSERT_TRUE(WaitFor([&] { return observation.started.load(); }));

    Cancel(source, context, signal);
    ASSERT_TRUE(WaitFor([&] {
        return observation.completions.load() == 1;
    }));

    EXPECT_EQ(
        observation.status.code(), rpcpio::StatusCode::CANCELLED);
    EXPECT_EQ(observation.completion_thread, caller_thread_id);
    EXPECT_EQ(observation.completions, 1);

    channel_ioc.stop();
    channel->Shutdown();
    channel.reset();
    caller_guard.reset();
    caller_ioc.stop();
    caller_thread.join();
}

void RunDuringConnect(CancelSource source) {
    CountingListener listener;
    boost::asio::io_context channel_ioc;
    boost::asio::io_context caller_ioc;
    auto channel_guard = boost::asio::make_work_guard(channel_ioc);
    auto caller_guard = boost::asio::make_work_guard(caller_ioc);
    auto channel = std::make_shared<rpcpio::Channel>(
        channel_ioc,
        "127.0.0.1",
        listener.port(),
        H2cOptions());
    rpcpio::ClientContext context;
    boost::asio::cancellation_signal signal;
    CallObservation observation;

    SpawnRawCall(
        caller_ioc, channel, context, signal, observation);
    std::thread::id caller_thread_id;
    std::thread channel_thread([&] { channel_ioc.run(); });
    std::thread caller_thread([&] {
        caller_thread_id = std::this_thread::get_id();
        caller_ioc.run();
    });
    ASSERT_TRUE(WaitFor([&] {
        return listener.connection_count() == 1;
    }));

    Cancel(source, context, signal);
    ASSERT_TRUE(WaitFor([&] {
        return observation.completions.load() == 1;
    }));

    EXPECT_EQ(
        observation.status.code(), rpcpio::StatusCode::CANCELLED);
    EXPECT_EQ(observation.completion_thread, caller_thread_id);
    EXPECT_EQ(observation.completions, 1);

    channel->Shutdown();
    channel_guard.reset();
    caller_guard.reset();
    channel_ioc.stop();
    caller_ioc.stop();
    channel_thread.join();
    caller_thread.join();
}

void RunInFlight(CancelSource source) {
    boost::asio::io_context server_ioc;
    rpcpio::ServerOptions server_options;
    server_options.use_h2c = true;
    server_options.num_threads = 2;
    rpcpio::Server server(server_ioc, server_options);
    std::atomic<bool> handler_started{false};
    std::atomic<bool> handler_cancelled{false};
    server.RegisterUnaryRaw(
        kPath,
        [&](rpcpio::ServerContext& server_context,
            std::string_view,
            std::string&) -> boost::asio::awaitable<rpcpio::Status> {
            handler_started = true;
            auto executor = co_await boost::asio::this_coro::executor;
            boost::asio::steady_timer timer(executor);
            timer.expires_after(5s);
            boost::system::error_code error;
            co_await timer.async_wait(
                boost::asio::bind_cancellation_slot(
                    server_context.cancellation_slot(),
                    boost::asio::redirect_error(
                        boost::asio::use_awaitable, error)));
            handler_cancelled =
                error == boost::asio::error::operation_aborted;
            co_return rpcpio::Status{
                rpcpio::StatusCode::CANCELLED, "cancelled"};
        });
    ASSERT_TRUE(server.Start("127.0.0.1", 0).ok());

    boost::asio::io_context channel_ioc;
    boost::asio::io_context caller_ioc;
    auto channel_guard = boost::asio::make_work_guard(channel_ioc);
    auto caller_guard = boost::asio::make_work_guard(caller_ioc);
    auto channel = std::make_shared<rpcpio::Channel>(
        channel_ioc,
        "127.0.0.1",
        server.bound_port(),
        H2cOptions());
    rpcpio::ClientContext context;
    boost::asio::cancellation_signal signal;
    CallObservation observation;
    SpawnRawCall(
        caller_ioc, channel, context, signal, observation);

    std::thread::id caller_thread_id;
    std::thread channel_thread([&] { channel_ioc.run(); });
    std::thread caller_thread([&] {
        caller_thread_id = std::this_thread::get_id();
        caller_ioc.run();
    });
    ASSERT_TRUE(WaitFor([&] { return handler_started.load(); }));

    Cancel(source, context, signal);
    ASSERT_TRUE(WaitFor([&] {
        return observation.completions.load() == 1;
    }));
    ASSERT_TRUE(WaitFor([&] { return handler_cancelled.load(); }));

    EXPECT_EQ(
        observation.status.code(), rpcpio::StatusCode::CANCELLED);
    EXPECT_EQ(observation.completion_thread, caller_thread_id);
    EXPECT_EQ(observation.completions, 1);

    channel->Shutdown();
    server.Shutdown();
    server.Wait();
    channel_guard.reset();
    caller_guard.reset();
    channel_ioc.stop();
    caller_ioc.stop();
    channel_thread.join();
    caller_thread.join();
}

} // namespace

TEST(RawUnaryCall, ContextCancellationBeforeConnect) {
    RunBeforeConnect(CancelSource::kContext);
}

TEST(RawUnaryCall, CoroutineCancellationBeforeConnect) {
    RunBeforeConnect(CancelSource::kCoroutine);
}

TEST(RawUnaryCall, ContextCancellationDuringConnect) {
    RunDuringConnect(CancelSource::kContext);
}

TEST(RawUnaryCall, CoroutineCancellationDuringConnect) {
    RunDuringConnect(CancelSource::kCoroutine);
}

TEST(RawUnaryCall, ContextCancellationInFlight) {
    RunInFlight(CancelSource::kContext);
}

TEST(RawUnaryCall, CoroutineCancellationInFlight) {
    RunInFlight(CancelSource::kCoroutine);
}
