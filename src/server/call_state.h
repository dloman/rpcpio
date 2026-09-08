#pragma once

#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <nghttp2/asio_http2_server.h>
#include "asio_grpc/server_context.h"
#include "asio_grpc/status.h"
#include "src/protocol/compression.h"
#include "src/protocol/framing.h"

namespace asio_grpc::internal {

// Type-erased RPC handler: receives raw request bytes, fills raw response bytes.
using RawHandler = std::function<
    boost::asio::awaitable<Status>(
        ServerContext&, std::string_view, std::string&)>;

// Manages one inbound unary call.  Created for each HTTP/2 stream that passes
// basic gRPC validation.  Lifetime is tied to the nghttp2-asio request/response
// pair; it must not outlive the server session.
class ServerCallState : public std::enable_shared_from_this<ServerCallState> {
public:
    ServerCallState(boost::asio::io_context&              ioc,
                    const nghttp2::asio_http2::server::request&  req,
                    const nghttp2::asio_http2::server::response& resp,
                    RawHandler                            handler,
                    std::size_t                           max_request_size,
                    std::size_t                           max_metadata_size);

    // Begin receiving request data and, once complete, dispatch the handler.
    void Start();

private:
    void OnData(const uint8_t* data, std::size_t len);
    void OnRequestEnd();
    void SendError(Status status);

    // Sends HTTP 200 + initial headers, then DATA (via generator callback),
    // then writes trailing HEADERS with END_STREAM via write_trailer.
    void SendResponse(const Status& status, std::string_view resp_bytes,
                      const MetadataMap& initial_meta,
                      const MetadataMap& trailing_meta);

    boost::asio::io_context&                       ioc_;
    const nghttp2::asio_http2::server::request&    req_;
    const nghttp2::asio_http2::server::response&   resp_;
    RawHandler                                     handler_;
    std::size_t                                    max_request_size_;
    std::size_t                                    max_metadata_size_;

    protocol::FrameDecoder           decoder_;
    std::optional<protocol::Encoding> request_encoding_;  // from grpc-encoding header
    ServerContext                    ctx_;
    boost::asio::steady_timer        timer_;
    bool                             responded_{false};
};

} // namespace asio_grpc::internal
