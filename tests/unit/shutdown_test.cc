#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>

#include "rpcpio/channel.h"
#include "rpcpio/client_context.h"
#include "rpcpio/status.h"

namespace {

constexpr std::string_view kPath = "/test.Shutdown/Call";

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

bool WaitForConnectionCount(
        const CountingListener& listener,
        int expected) {
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(1);
    while (listener.connection_count() != expected &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return listener.connection_count() == expected;
}

} // namespace

TEST(Shutdown, CallAfterShutdownReturnsCancelled) {
    boost::asio::io_context ioc;
    rpcpio::ChannelOptions copts;
    copts.use_h2c = true;
    CountingListener listener;
    auto ch = std::make_shared<rpcpio::Channel>(
        ioc, "127.0.0.1", listener.port(), copts);

    ch->Shutdown();

    rpcpio::Status result{rpcpio::StatusCode::INTERNAL, "not set"};
    bool done = false;
    boost::asio::co_spawn(ioc,
        [ch, &result, &done]() -> boost::asio::awaitable<void> {
            rpcpio::ClientContext ctx;
            auto raw = co_await ch->UnaryCallRaw(kPath, ctx, "");
            result = raw.status;
            done   = true;
        },
        boost::asio::detached);

    ioc.run();

    ASSERT_TRUE(done);
    EXPECT_EQ(result.code(), rpcpio::StatusCode::CANCELLED) << result.message();
    EXPECT_EQ(listener.connection_count(), 0);
}

TEST(Shutdown, IsIdempotent) {
    boost::asio::io_context ioc;
    rpcpio::ChannelOptions copts;
    copts.use_h2c = true;
    CountingListener listener;
    auto ch = std::make_shared<rpcpio::Channel>(
        ioc, "127.0.0.1", listener.port(), copts);

    // Multiple calls to Shutdown() must not crash or throw.
    EXPECT_NO_THROW({
        ch->Shutdown();
        ch->Shutdown();
        ch->Shutdown();
    });

    ioc.run();
}

TEST(Shutdown, DuringConnect) {
    CountingListener listener;
    boost::asio::io_context ioc;
    rpcpio::ChannelOptions copts;
    copts.use_h2c = true;
    auto ch = std::make_shared<rpcpio::Channel>(
        ioc, "127.0.0.1", listener.port(), copts);

    rpcpio::Status result{rpcpio::StatusCode::INTERNAL, "not set"};
    bool done = false;
    boost::asio::co_spawn(ioc,
        [ch, &result, &done]() -> boost::asio::awaitable<void> {
            rpcpio::ClientContext ctx;
            auto raw = co_await ch->UnaryCallRaw(kPath, ctx, "");
            result = raw.status;
            done = true;
        },
        boost::asio::detached);

    // Start the coroutine and its serialized connect operation, but leave the
    // client's TCP completion handler queued.
    ASSERT_EQ(ioc.run_one(), 1);
    ASSERT_EQ(ioc.run_one(), 1);
    ASSERT_TRUE(WaitForConnectionCount(listener, 1));

    ch->Shutdown();
    ioc.run();

    ASSERT_TRUE(done);
    EXPECT_EQ(result.code(), rpcpio::StatusCode::CANCELLED) << result.message();

    ioc.restart();
    bool later_done = false;
    boost::asio::co_spawn(
        ioc,
        [ch, &later_done]() -> boost::asio::awaitable<void> {
            rpcpio::ClientContext ctx;
            auto raw = co_await ch->UnaryCallRaw(kPath, ctx, "");
            EXPECT_EQ(raw.status.code(), rpcpio::StatusCode::CANCELLED);
            later_done = true;
        },
        boost::asio::detached);
    ioc.run();

    EXPECT_TRUE(later_done);
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    EXPECT_EQ(listener.connection_count(), 1);
    ch.reset();
}

TEST(Shutdown, AfterIocStop) {
    CountingListener listener;
    boost::asio::io_context ioc;
    rpcpio::ChannelOptions copts;
    copts.use_h2c = true;
    auto ch = std::make_shared<rpcpio::Channel>(
        ioc, "127.0.0.1", listener.port(), copts);

    ioc.stop();
    EXPECT_NO_THROW(ch->Shutdown());

    rpcpio::Status result{rpcpio::StatusCode::INTERNAL, "not set"};
    bool done = false;
    boost::asio::co_spawn(
        ioc,
        [ch, &result, &done]() -> boost::asio::awaitable<void> {
            rpcpio::ClientContext ctx;
            auto raw = co_await ch->UnaryCallRaw(kPath, ctx, "");
            result = raw.status;
            done = true;
        },
        boost::asio::detached);

    ioc.restart();
    ioc.run();

    ASSERT_TRUE(done);
    EXPECT_EQ(result.code(), rpcpio::StatusCode::CANCELLED);
    EXPECT_EQ(listener.connection_count(), 0);
}
