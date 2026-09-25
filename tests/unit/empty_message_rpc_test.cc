#include <cstdint>
#include <exception>
#include <memory>
#include <string>
#include <string_view>
#include <thread>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <google/protobuf/empty.pb.h>
#include <gtest/gtest.h>
#include <nghttp2/asio_http2_server.h>
#include <nghttp2/nghttp2.h>

#include "rpcpio/channel.h"
#include "rpcpio/client_context.h"
#include "rpcpio/server.h"
#include "rpcpio/server_context.h"
#include "rpcpio/status.h"
#include "rpcpio/streaming_method.h"
#include "rpcpio/unary_method.h"

namespace {

using Empty = google::protobuf::Empty;

constexpr rpcpio::UnaryMethod<Empty, Empty> kUnary{
    "/test.Empty/Unary"};
constexpr rpcpio::ServerStreamingMethod<Empty, Empty> kServerStreaming{
    "/test.Empty/ServerStreaming"};
constexpr rpcpio::ClientStreamingMethod<Empty, Empty> kClientStreaming{
    "/test.Empty/ClientStreaming"};
constexpr rpcpio::BidiStreamingMethod<Empty, Empty> kBidi{
    "/test.Empty/Bidi"};

rpcpio::ChannelOptions H2cOptions() {
    rpcpio::ChannelOptions options;
    options.use_h2c = true;
    return options;
}

} // namespace

TEST(EmptyMessageRpc, AllFourRpcKindsSendAndReceiveEmptyMessages) {
    boost::asio::io_context server_ioc;
    rpcpio::ServerOptions server_options;
    server_options.use_h2c = true;
    server_options.num_threads = 2;
    rpcpio::Server server(server_ioc, server_options);

    server.RegisterUnary(
        kUnary,
        [](rpcpio::ServerContext&, const Empty&)
                -> boost::asio::awaitable<rpcpio::StatusOr<Empty>> {
            co_return Empty{};
        });
    server.RegisterServerStreaming(
        kServerStreaming,
        [](rpcpio::ServerContext&,
           const Empty&,
           rpcpio::ServerWriter<Empty>& writer)
                -> boost::asio::awaitable<rpcpio::Status> {
            co_return co_await writer.Write(Empty{});
        });
    server.RegisterClientStreaming(
        kClientStreaming,
        [](rpcpio::ServerContext&,
           rpcpio::ServerReader<Empty>& reader)
                -> boost::asio::awaitable<rpcpio::StatusOr<Empty>> {
            auto request = co_await reader.Read();
            if (!request.ok()) {
                co_return rpcpio::StatusOr<Empty>(request.status());
            }
            if (!request->has_value()) {
                co_return rpcpio::StatusOr<Empty>(rpcpio::Status{
                    rpcpio::StatusCode::INTERNAL, "missing request"});
            }
            co_return Empty{};
        });
    server.RegisterBidi(
        kBidi,
        [](rpcpio::ServerContext&,
           rpcpio::ServerReader<Empty>& reader,
           rpcpio::ServerWriter<Empty>& writer)
                -> boost::asio::awaitable<rpcpio::Status> {
            auto request = co_await reader.Read();
            if (!request.ok()) {
                co_return request.status();
            }
            if (!request->has_value()) {
                co_return rpcpio::Status{
                    rpcpio::StatusCode::INTERNAL, "missing request"};
            }
            co_return co_await writer.Write(Empty{});
        });
    ASSERT_TRUE(server.Start("127.0.0.1", 0).ok());

    boost::asio::io_context client_ioc;
    auto channel = std::make_shared<rpcpio::Channel>(
        client_ioc,
        "127.0.0.1",
        server.bound_port(),
        H2cOptions());
    bool completed = false;
    std::exception_ptr failure;
    boost::asio::co_spawn(
        client_ioc,
        [&]() -> boost::asio::awaitable<void> {
            Empty empty;

            rpcpio::ClientContext unary_context;
            auto unary =
                co_await channel->UnaryCall(kUnary, unary_context, empty);
            EXPECT_TRUE(unary.status.ok()) << unary.status.DebugString();
            EXPECT_TRUE(unary.response.has_value());

            rpcpio::ClientContext server_streaming_context;
            auto reader = co_await channel->ServerStreamingCall(
                kServerStreaming, server_streaming_context, empty);
            auto streamed_response = co_await reader.Read();
            EXPECT_TRUE(streamed_response.ok());
            EXPECT_TRUE(
                streamed_response.ok() &&
                streamed_response->has_value());
            if (streamed_response.ok() &&
                streamed_response->has_value()) {
                EXPECT_TRUE(
                    (**streamed_response).SerializeAsString().empty());
            }
            EXPECT_TRUE((co_await reader.Finish()).ok());

            rpcpio::ClientContext client_streaming_context;
            auto writer = co_await channel->ClientStreamingCall(
                kClientStreaming, client_streaming_context);
            EXPECT_TRUE((co_await writer.Write(empty)).ok());
            co_await writer.WritesDone();
            auto client_streaming_response =
                co_await writer.FinishAndGetResponse();
            EXPECT_TRUE(client_streaming_response.status.ok())
                << client_streaming_response.status.DebugString();
            EXPECT_TRUE(client_streaming_response.response.has_value());

            rpcpio::ClientContext bidi_context;
            auto stream =
                co_await channel->BidiStreamingCall(kBidi, bidi_context);
            EXPECT_TRUE((co_await stream.Write(empty)).ok());
            auto bidi_response = co_await stream.Read();
            EXPECT_TRUE(bidi_response.ok());
            EXPECT_TRUE(
                bidi_response.ok() && bidi_response->has_value());
            if (bidi_response.ok() && bidi_response->has_value()) {
                EXPECT_TRUE(
                    (**bidi_response).SerializeAsString().empty());
            }
            co_await stream.WritesDone();
            EXPECT_TRUE((co_await stream.Finish()).ok());
            completed = true;
        },
        [&](std::exception_ptr exception) {
            failure = exception;
            channel->Shutdown();
        });
    client_ioc.run();

    server.Shutdown();
    server.Wait();
    EXPECT_EQ(failure, nullptr);
    EXPECT_TRUE(completed);
}

TEST(EmptyMessageRpc, UnaryOkWithoutMessageIsInternal) {
    boost::asio::io_context server_ioc;
    nghttp2::asio_http2::server::http2 http2(server_ioc);
    ASSERT_TRUE(http2.handle(
        "/",
        [](const nghttp2::asio_http2::server::request&,
           const nghttp2::asio_http2::server::response& response) {
            response.write_head(
                200,
                {{"content-type",
                  {"application/grpc+proto", false}}});
            response.end(
                [&response](
                        std::uint8_t*,
                        std::size_t,
                        std::uint32_t* flags) -> ssize_t {
                    *flags |= NGHTTP2_DATA_FLAG_EOF;
                    *flags |= NGHTTP2_DATA_FLAG_NO_END_STREAM;
                    response.write_trailer(
                        {{"grpc-status", {"0", false}}});
                    return 0;
                });
        }));
    boost::system::error_code error;
    ASSERT_FALSE(http2.listen_and_serve(
        error, "127.0.0.1", "0", true));
    ASSERT_FALSE(error);
    const auto ports = http2.ports();
    ASSERT_EQ(ports.size(), 1);
    std::thread server_thread([&] { server_ioc.run(); });

    boost::asio::io_context client_ioc;
    auto channel = std::make_shared<rpcpio::Channel>(
        client_ioc, "127.0.0.1", ports.front(), H2cOptions());
    rpcpio::Status result;
    boost::asio::co_spawn(
        client_ioc,
        [&]() -> boost::asio::awaitable<void> {
            rpcpio::ClientContext context;
            Empty request;
            auto response =
                co_await channel->UnaryCall(kUnary, context, request);
            result = std::move(response.status);
        },
        [channel](std::exception_ptr) { channel->Shutdown(); });
    client_ioc.run();

    http2.stop();
    server_ioc.stop();
    server_thread.join();
    EXPECT_EQ(result.code(), rpcpio::StatusCode::INTERNAL);
    EXPECT_EQ(result.message(), "missing response message");
}
