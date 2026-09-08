#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include <boost/asio/io_context.hpp>
#include "rpcpio/server.h"

namespace rpcpio {

// Convenience wrapper that owns an io_context and worker threads.
// Use this for standalone binaries; embed Atlas-safe rpcpio::Server directly
// when the caller already drives an io_context.
class StandaloneServer {
public:
    explicit StandaloneServer(ServerOptions opts = {});
    ~StandaloneServer();

    Server& server() noexcept { return *server_; }
    const Server& server() const noexcept { return *server_; }

    void Start(std::string host, std::uint16_t port);
    std::uint16_t bound_port() const noexcept;

    // Stop worker threads and shut down the embedded server.
    void Shutdown();
    void Wait();

    StandaloneServer(const StandaloneServer&)            = delete;
    StandaloneServer& operator=(const StandaloneServer&) = delete;

private:
    boost::asio::io_context  ioc_;
    ServerOptions            opts_;
    std::unique_ptr<Server>  server_;
    std::vector<std::thread> threads_;
    bool                     started_{false};
};

} // namespace rpcpio
