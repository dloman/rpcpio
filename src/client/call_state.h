#pragma once

#include <atomic>
#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <boost/asio/io_context.hpp>
#include <nghttp2/asio_http2_client.h>
#include <nghttp2/nghttp2.h>
#include "rpcpio/unary_result_raw.h"
#include "src/client/unary_call_submission.h"
#include "src/protocol/compression.h"
#include "src/protocol/framing.h"
#include "src/protocol/metadata_codec.h"

namespace rpcpio::internal {

class ClientCallState : public std::enable_shared_from_this<ClientCallState> {
public:
    ClientCallState(boost::asio::io_context&                    ioc,
                    std::shared_ptr<UnaryCallSubmission>        submission,
                    std::size_t                                 max_receive_message_size);

    void Attach(const nghttp2::asio_http2::client::response& resp);
    void BindRequest(const nghttp2::asio_http2::client::request* req);

    void OnStreamClose(uint32_t error_code);
    void Fail(Status status);

private:
    void Complete(UnaryResultRaw result);
    void ResetStream();

    boost::asio::io_context&                    ioc_;
    std::shared_ptr<UnaryCallSubmission>        submission_;
    std::size_t                                 max_receive_message_size_;

    const nghttp2::asio_http2::client::request* request_{nullptr};

    protocol::FrameDecoder           decoder_;
    UnaryResultRaw                   result_;
    std::optional<protocol::Encoding> response_encoding_;

    bool                        initial_meta_done_{false};
    bool                        trailers_done_{false};
};

} // namespace rpcpio::internal
