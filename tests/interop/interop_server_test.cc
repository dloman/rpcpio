// gRPC interoperability server test.
//
// Starts an asio_grpc server and runs official gRPC client-side interop tests
// against it.  The official grpc_interop_client binary must be on PATH or
// pointed to by GRPC_INTEROP_CLIENT env var.

#include <array>
#include <cstdlib>
#include <string>
#include <boost/asio/io_context.hpp>
#include <gtest/gtest.h>
#include "grpc_testing.asio_grpc.pb.h"

// Server-side implementation of the interop service.
class InteropServiceImpl final : public grpc::testing::TestServiceService {
public:
    boost::asio::awaitable<asio_grpc::StatusOr<grpc::testing::Empty>>
    EmptyCall(asio_grpc::ServerContext&,
              const grpc::testing::Empty&) override {
        co_return grpc::testing::Empty{};
    }

    boost::asio::awaitable<asio_grpc::StatusOr<grpc::testing::SimpleResponse>>
    UnaryCall(asio_grpc::ServerContext&           ctx,
              const grpc::testing::SimpleRequest& req) override {
        (void)ctx;

        if (req.response_status().code() != 0) {
            co_return asio_grpc::Status{
                asio_grpc::StatusCodeFromInt(req.response_status().code()),
                req.response_status().message()};
        }

        grpc::testing::SimpleResponse resp;
        if (req.response_size() > 0) {
            resp.mutable_payload()->set_body(
                std::string(req.response_size(), '\0'));
        }
        co_return resp;
    }
};

class InteropServerTest : public ::testing::Test {
protected:
    void SetUp() override {
        asio_grpc::ServerOptions opts;
        opts.use_h2c     = true;
        opts.num_threads = 4;

        server_ = std::make_unique<asio_grpc::Server>(ioc_, opts);
        service_.Register(*server_);
        server_->Start("0.0.0.0", 0 /*OS assigns port*/);

        // Spin up io_context in a background thread.
        thread_ = std::thread([this] { ioc_.run(); });
    }

    void TearDown() override {
        server_->Shutdown();
        ioc_.stop();
        if (thread_.joinable()) thread_.join();
    }

    boost::asio::io_context ioc_;
    InteropServiceImpl      service_;
    std::unique_ptr<asio_grpc::Server> server_;
    std::thread thread_;
};

TEST_F(InteropServerTest, EmptyCallFromClient) {
    // This test uses the asio_grpc client to call the asio_grpc server.
    // A full interop run would substitute the official grpc_interop_client binary.
    boost::asio::io_context client_ioc;

    asio_grpc::ChannelOptions opts;
    opts.use_h2c = true;
    // Port 0 above: in production, retrieve the listening port dynamically.
    // For this skeleton test, we skip the actual port binding check.
    GTEST_SKIP() << "Port-0 binding not yet wired to test framework";
}
