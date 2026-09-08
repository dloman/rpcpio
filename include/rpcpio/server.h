#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <google/protobuf/message.h>
#include "rpcpio/server_context.h"
#include "rpcpio/status.h"
#include "rpcpio/unary_method.h"
#include "rpcpio/streaming_method.h"
#include "rpcpio/server_reader.h"
#include "rpcpio/server_writer.h"
#include "rpcpio/internal/raw_server_reader.h"
#include "rpcpio/internal/raw_server_writer.h"

namespace rpcpio {

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

    // Register a server-streaming handler.
    // Handler signature: awaitable<Status>(ServerContext&, const Req&, ServerWriter<Resp>&)
    template<typename Req, typename Resp, typename Handler>
    void RegisterServerStreaming(const ServerStreamingMethod<Req, Resp>& method,
                                  Handler handler) {
        RegisterServerStreamingRaw(
            method.path,
            [h = std::move(handler)](
                ServerContext&              ctx,
                std::string_view            req_bytes,
                internal::RawServerWriter&  raw_writer
            ) -> boost::asio::awaitable<Status> {
                Req req;
                if (!req.ParseFromArray(req_bytes.data(),
                                        static_cast<int>(req_bytes.size()))) {
                    co_return Status{StatusCode::INVALID_ARGUMENT,
                                     "failed to parse request"};
                }
                ServerWriter<Resp> writer(&raw_writer);
                co_return co_await h(ctx, req, writer);
            });
    }

    // Register a client-streaming handler.
    // Handler signature: awaitable<StatusOr<Resp>>(ServerContext&, ServerReader<Req>&)
    template<typename Req, typename Resp, typename Handler>
    void RegisterClientStreaming(const ClientStreamingMethod<Req, Resp>& method,
                                  Handler handler) {
        RegisterClientStreamingRaw(
            method.path,
            [h = std::move(handler)](
                ServerContext&              ctx,
                internal::RawServerReader&  raw_reader,
                std::string&                resp_bytes
            ) -> boost::asio::awaitable<Status> {
                ServerReader<Req> reader(&raw_reader);
                StatusOr<Resp> result = co_await h(ctx, reader);
                if (!result.ok()) co_return result.status();
                if (!result->SerializeToString(&resp_bytes)) {
                    co_return Status{StatusCode::INTERNAL,
                                     "failed to serialize response"};
                }
                co_return Status{};
            });
    }

    // Register a bidirectional-streaming handler.
    // Handler signature: awaitable<Status>(ServerContext&, ServerReader<Req>&, ServerWriter<Resp>&)
    template<typename Req, typename Resp, typename Handler>
    void RegisterBidi(const BidiStreamingMethod<Req, Resp>& method,
                       Handler handler) {
        RegisterBidiRaw(
            method.path,
            [h = std::move(handler)](
                ServerContext&              ctx,
                internal::RawServerReader&  raw_reader,
                internal::RawServerWriter&  raw_writer
            ) -> boost::asio::awaitable<Status> {
                ServerReader<Req> reader(&raw_reader);
                ServerWriter<Resp> writer(&raw_writer);
                co_return co_await h(ctx, reader, writer);
            });
    }

    // Bind and start accepting connections on host:port.  Pass port=0 to let
    // the OS pick an ephemeral port; call bound_port() afterward to learn it.
    void Start(std::string host, std::uint16_t port);

    // Returns the port the server is actually listening on.
    // Valid only after a successful Start() call.
    std::uint16_t bound_port() const noexcept;

    // Graceful shutdown: stop accepting, drain active calls, join workers.
    void Shutdown();

    // Block the calling thread until Shutdown() completes.
    void Wait();

    Server(const Server&)            = delete;
    Server& operator=(const Server&) = delete;

private:
    // Type-erased registrations; implemented in server_impl.cc.
    using RawHandler = std::function<
        boost::asio::awaitable<Status>(
            ServerContext&, std::string_view, std::string&)>;

    using RawServerStreamingHandler = std::function<
        boost::asio::awaitable<Status>(
            ServerContext&, std::string_view, internal::RawServerWriter&)>;

    using RawClientStreamingHandler = std::function<
        boost::asio::awaitable<Status>(
            ServerContext&, internal::RawServerReader&, std::string&)>;

    using RawBidiStreamingHandler = std::function<
        boost::asio::awaitable<Status>(
            ServerContext&, internal::RawServerReader&, internal::RawServerWriter&)>;

    void RegisterUnaryRaw(std::string_view path, RawHandler handler);
    void RegisterServerStreamingRaw(std::string_view path, RawServerStreamingHandler handler);
    void RegisterClientStreamingRaw(std::string_view path, RawClientStreamingHandler handler);
    void RegisterBidiRaw(std::string_view path, RawBidiStreamingHandler handler);

    std::shared_ptr<internal::ServerImpl> impl_;
};

} // namespace rpcpio
