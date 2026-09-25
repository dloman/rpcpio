#include <gtest/gtest.h>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/use_awaitable.hpp>
#include "src/protocol/stream_queue.h"

using rpcpio::internal::StreamMessageQueue;

// ── Synchronous (no suspension) ───────────────────────────────────────────────

TEST(StreamMessageQueue, PushThenPop) {
    boost::asio::io_context ioc;
    StreamMessageQueue q(ioc);
    bool done = false;

    boost::asio::co_spawn(ioc, [&]() -> boost::asio::awaitable<void> {
        q.Push("hello");
        auto r = co_await q.Pop();
        EXPECT_TRUE(r.ok());
        EXPECT_TRUE(r.data.has_value());
        EXPECT_EQ(*r.data, "hello");
        done = true;
    }, boost::asio::detached);

    ioc.run();
    EXPECT_TRUE(done);
}

TEST(StreamMessageQueue, FifoOrdering) {
    boost::asio::io_context ioc;
    StreamMessageQueue q(ioc);
    bool done = false;

    boost::asio::co_spawn(ioc, [&]() -> boost::asio::awaitable<void> {
        q.Push("a");
        q.Push("b");
        q.Push("c");

        auto r1 = co_await q.Pop();
        auto r2 = co_await q.Pop();
        auto r3 = co_await q.Pop();

        EXPECT_TRUE(r1.data.has_value()); EXPECT_EQ(*r1.data, "a");
        EXPECT_TRUE(r2.data.has_value()); EXPECT_EQ(*r2.data, "b");
        EXPECT_TRUE(r3.data.has_value()); EXPECT_EQ(*r3.data, "c");
        done = true;
    }, boost::asio::detached);

    ioc.run();
    EXPECT_TRUE(done);
}

TEST(StreamMessageQueue, SetEosThenPop) {
    boost::asio::io_context ioc;
    StreamMessageQueue q(ioc);
    bool done = false;

    boost::asio::co_spawn(ioc, [&]() -> boost::asio::awaitable<void> {
        q.SetEos();
        auto r = co_await q.Pop();
        EXPECT_TRUE(r.ok());
        EXPECT_TRUE(r.eos());
        EXPECT_FALSE(r.data.has_value());
        done = true;
    }, boost::asio::detached);

    ioc.run();
    EXPECT_TRUE(done);
}

TEST(StreamMessageQueue, SetErrorThenPop) {
    boost::asio::io_context ioc;
    StreamMessageQueue q(ioc);
    bool done = false;

    boost::asio::co_spawn(ioc, [&]() -> boost::asio::awaitable<void> {
        q.SetError(rpcpio::Status{rpcpio::StatusCode::CANCELLED, "cancelled"});
        auto r = co_await q.Pop();
        EXPECT_FALSE(r.ok());
        EXPECT_EQ(r.status.code(), rpcpio::StatusCode::CANCELLED);
        EXPECT_EQ(r.status.message(), "cancelled");
        done = true;
    }, boost::asio::detached);

    ioc.run();
    EXPECT_TRUE(done);
}

TEST(StreamMessageQueue, DrainThenEos) {
    boost::asio::io_context ioc;
    StreamMessageQueue q(ioc);
    bool done = false;

    boost::asio::co_spawn(ioc, [&]() -> boost::asio::awaitable<void> {
        q.Push("msg");
        q.SetEos();

        auto r1 = co_await q.Pop();
        EXPECT_TRUE(r1.ok());
        EXPECT_TRUE(r1.data.has_value());
        EXPECT_EQ(*r1.data, "msg");

        auto r2 = co_await q.Pop();
        EXPECT_TRUE(r2.ok());
        EXPECT_TRUE(r2.eos());
        done = true;
    }, boost::asio::detached);

    ioc.run();
    EXPECT_TRUE(done);
}

// ── Asynchronous (Pop suspends until producer fires) ──────────────────────────

TEST(StreamMessageQueue, PopBeforePush) {
    boost::asio::io_context ioc;
    StreamMessageQueue q(ioc);
    bool done = false;

    // Spawn the consumer coroutine first so it suspends on Pop().
    boost::asio::co_spawn(ioc, [&]() -> boost::asio::awaitable<void> {
        auto r = co_await q.Pop();
        EXPECT_TRUE(r.ok());
        EXPECT_TRUE(r.data.has_value());
        EXPECT_EQ(*r.data, "async");
        done = true;
    }, boost::asio::detached);

    // Post the push to run after the coroutine has suspended.
    boost::asio::post(ioc, [&]() { q.Push("async"); });

    ioc.run();
    EXPECT_TRUE(done);
}

TEST(StreamMessageQueue, PopBeforeEos) {
    boost::asio::io_context ioc;
    StreamMessageQueue q(ioc);
    bool done = false;

    boost::asio::co_spawn(ioc, [&]() -> boost::asio::awaitable<void> {
        auto r = co_await q.Pop();
        EXPECT_TRUE(r.ok());
        EXPECT_TRUE(r.eos());
        done = true;
    }, boost::asio::detached);

    boost::asio::post(ioc, [&]() { q.SetEos(); });

    ioc.run();
    EXPECT_TRUE(done);
}

TEST(StreamMessageQueue, PopBeforeError) {
    boost::asio::io_context ioc;
    StreamMessageQueue q(ioc);
    bool done = false;

    boost::asio::co_spawn(ioc, [&]() -> boost::asio::awaitable<void> {
        auto r = co_await q.Pop();
        EXPECT_FALSE(r.ok());
        EXPECT_EQ(r.status.code(), rpcpio::StatusCode::INTERNAL);
        done = true;
    }, boost::asio::detached);

    boost::asio::post(ioc, [&]() {
        q.SetError(rpcpio::Status{rpcpio::StatusCode::INTERNAL, "boom"});
    });

    ioc.run();
    EXPECT_TRUE(done);
}

TEST(StreamMessageQueue, MultipleAsyncPops) {
    boost::asio::io_context ioc;
    StreamMessageQueue q(ioc);
    std::vector<std::string> received;

    boost::asio::co_spawn(ioc, [&]() -> boost::asio::awaitable<void> {
        for (int i = 0; i < 3; ++i) {
            auto r = co_await q.Pop();
            EXPECT_TRUE(r.ok());
            EXPECT_TRUE(r.data.has_value());
            received.push_back(*r.data);
        }
        auto eos = co_await q.Pop();
        EXPECT_TRUE(eos.eos());
    }, boost::asio::detached);

    // Push messages one by one via post so each Pop() suspends and wakes.
    boost::asio::post(ioc, [&]() { q.Push("x"); });
    boost::asio::post(ioc, [&]() { q.Push("y"); });
    boost::asio::post(ioc, [&]() { q.Push("z"); });
    boost::asio::post(ioc, [&]() { q.SetEos(); });

    ioc.run();

    ASSERT_EQ(received.size(), 3u);
    EXPECT_EQ(received[0], "x");
    EXPECT_EQ(received[1], "y");
    EXPECT_EQ(received[2], "z");
}
