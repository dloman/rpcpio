#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <google/protobuf/message.h>
#include "asio_grpc/server_context.h"
#include "asio_grpc/status.h"
#include "asio_grpc/unary_method.h"

namespace asio_grpc {

namespace internal { class ServerImpl; }

struct ServerOptions {
    std::uint32_t num_threads{4};

    // TLS (supply both cert and key to enable)
    std::string server_cert_file;
    std::string server_key_file;
    std::string ca_cert_file;       // for mTLS client verification

    // h2c plaintext (no TLS)
    bool use_h2c{false};

    // Graceful shutdown: wait up to this long for active calls to finish.
    std::chrono::seconds grace_period{30};

    // Per-call limits (bytes)
    std::size_t max_request_message_size{4 * 1024 * 1024};
    std::size_t max_response_message_size{4 * 1024 * 1024};
    std::size_t max_metadata_size{8192};
};

class Server {
public:
    Server(boost::asio::io_context& ioc, ServerOptions opts = {});
    ~Server();

    // Register a unary handler.  Must be called before Start().
    // Handler signature: awaitable<StatusOr<Resp>>(ServerContext&, const Req&)
    template<typename Req, typename Resp, typename Handler>
    void RegisterUnary(const UnaryMethod<Req, Resp>& method, Handler handler) {
        // Wrap the typed handler in a type-erased bytes-in/bytes-out adapter.
        RegisterUnaryRaw(
            method.path,
            [h = std::move(handler)](
                ServerContext&     ctx,
                std::string_view   req_bytes,
                std::string&       resp_bytes
            ) -> boost::asio::awaitable<Status> {
                // Deserialize request.
                Req req;
                if (!req.ParseFromArray(req_bytes.data(),
                                        static_cast<int>(req_bytes.size()))) {
                    co_return Status{StatusCode::INVALID_ARGUMENT,
                                     "failed to parse request"};
                }

                // Invoke handler.
                StatusOr<Resp> result = co_await h(ctx, req);
                if (!result.ok()) co_return result.status();

                // Serialize response.
                if (!result->SerializeToString(&resp_bytes)) {
                    co_return Status{StatusCode::INTERNAL,
                                     "failed to serialize response"};
                }
                co_return Status{};
            });
    }

    // Bind and start accepting connections on host:port.
    void Start(std::string host, std::uint16_t port);

    // Graceful shutdown: stop accepting, drain active calls, join workers.
    void Shutdown();

    // Block the calling thread until Shutdown() completes.
    void Wait();

    Server(const Server&)            = delete;
    Server& operator=(const Server&) = delete;

private:
    // Type-erased registration; implemented in server_impl.cc.
    using RawHandler = std::function<
        boost::asio::awaitable<Status>(
            ServerContext&, std::string_view, std::string&)>;

    void RegisterUnaryRaw(std::string_view path, RawHandler handler);

    std::shared_ptr<internal::ServerImpl> impl_;
};

} // namespace asio_grpc
