# Phase 1: nghttp2-asio Trailer and GOAWAY Extensions

This document specifies the public API additions to the CESNET/nghttp2-asio
library that are required before `rpcpio` can function as a gRPC transport.
These changes are published as a successor BCR module version
(`nghttp2-asio@0.0.90-20260225-464f056` → next version).

The changes are minimal, targeted, and backward-compatible.

---

## Why These Changes Are Necessary

The current CESNET snapshot
(`464f05639223bce01556fd7d81547087d518e909`) explicitly discards received
trailing HEADERS blocks on the client side:

```cpp
// asio_client_session_impl.cc (current)
case NGHTTP2_HCAT_HEADERS:
    // Trailing headers — ignored.
    break;
```

gRPC status (`grpc-status`, `grpc-message`, `grpc-status-details-bin`) is
**always** delivered in trailing HEADERS.  Without trailer support, no gRPC
status can be received on the client side.

The server side similarly lacks a public `write_trailer` API, making it
impossible to produce standards-compliant gRPC responses.

---

## Client-Side Changes (asio_http2_client.h / asio_client_session_impl.cc)

### New public method: `response::on_trailers`

```cpp
// include/nghttp2/asio_http2_client.h

class response {
public:
    // ... existing methods ...

    // Register a callback invoked once the complete trailing HEADERS block
    // has been received (i.e., all fragments de-HPACK'd and concatenated).
    // The callback fires after on_data has delivered the final DATA chunk
    // and before on_close.
    //
    // For trailers-only responses (grpc-status in the sole HEADERS block
    // with END_STREAM), the callback fires immediately after on_response
    // with that same header map, before any DATA delivery.
    using trailer_cb = std::function<void(const header_map&)>;
    void on_trailers(trailer_cb cb);

private:
    trailer_cb trailer_cb_;
    header_map trailers_;
};
```

### Implementation change: `asio_client_session_impl.cc`

```cpp
// Before (current — discards trailers):
case NGHTTP2_HCAT_HEADERS:
    break;

// After:
case NGHTTP2_HCAT_HEADERS: {
    // Collect trailer name-value pairs into the response's trailer map.
    // Duplicate fields are preserved (order matches wire order).
    auto& resp = *stream->response;
    for (int i = 0; i < nv_len; ++i) {
        std::string name(reinterpret_cast<const char*>(nv[i].name), nv[i].namelen);
        std::string value(reinterpret_cast<const char*>(nv[i].value), nv[i].valuelen);
        bool sensitive = (nv[i].flags & NGHTTP2_NV_FLAG_NO_INDEX) != 0;
        resp.trailers_.emplace(name, header_value{std::move(value), sensitive});
    }
    break;
}
```

And in the END_STREAM handler for trailing HEADERS:
```cpp
// After the HCAT_HEADERS block above, if END_STREAM is set:
if (frame->hd.flags & NGHTTP2_FLAG_END_STREAM) {
    if (resp.trailer_cb_) {
        resp.trailer_cb_(resp.trailers_);
    }
}
```

### Trailers-only response support

When the first (and only) HEADERS frame carries `END_STREAM` and also contains
`grpc-status`, it is a trailers-only response.  Sequence:

1. Deliver `on_response` with the header map as initial metadata.
2. Deliver `on_trailers` with the **same** header map.
3. Signal DATA EOF (empty body) so that `on_data` is called with `len=0`.
4. Deliver `on_close`.

This order ensures that `rpcpio::internal::ClientCallState::Attach()`
can detect the trailers-only case by checking for `grpc-status` in the
initial HEADERS block.

### Size limit for trailers

A separate `max_trailer_size` configuration (default 8 KiB) is applied to
the cumulative byte size of all trailer fields.  Exceeding this limit causes
the stream to be reset with `NGHTTP2_ERR_HTTP_MESSAGING`.

---

## Server-Side Changes (asio_http2_server.h / asio_server_session_impl.cc)

### New public method: `response::write_trailer`

```cpp
// include/nghttp2/asio_http2_server.h

class response {
public:
    // ... existing methods ...

    // Send a trailing HEADERS frame with END_STREAM set.
    // Must be called after the DATA generator callback has set
    //   *data_flags |= NGHTTP2_DATA_FLAG_NO_END_STREAM
    // to indicate that the stream should not be closed by the DATA frame.
    //
    // Calling write_trailer before the DATA generator has finished is
    // undefined behaviour.
    void write_trailer(const header_map& trailers);
};
```

### Implementation: `asio_server_session_impl.cc`

```cpp
void response_impl::write_trailer(const header_map& trailers) {
    std::vector<nghttp2_nv> nvs;
    nvs.reserve(trailers.size());
    for (auto& [name, hv] : trailers) {
        nvs.push_back({
            reinterpret_cast<uint8_t*>(const_cast<char*>(name.data())),
            reinterpret_cast<uint8_t*>(const_cast<char*>(hv.value.data())),
            name.size(), hv.value.size(),
            hv.sensitive ? NGHTTP2_NV_FLAG_NO_INDEX : NGHTTP2_NV_FLAG_NONE
        });
    }
    nghttp2_submit_trailer(session_, stream_id_, nvs.data(), nvs.size());
    session_send();
}
```

---

## GOAWAY Support

### New public callback: `session::on_goaway`

```cpp
// Client session
class session {
public:
    using goaway_cb = std::function<void(
        std::uint32_t error_code,
        std::int32_t  last_stream_id)>;

    // Register a callback invoked when a GOAWAY frame is received.
    // error_code: HTTP/2 error code (0 = graceful shutdown)
    // last_stream_id: highest stream ID the peer processed
    void on_goaway(goaway_cb cb);
};
```

### Implementation hook

In `on_frame_recv_callback` (nghttp2 event handler):
```cpp
case NGHTTP2_GOAWAY: {
    std::uint32_t ec = frame->goaway.error_code;
    std::int32_t  ls = frame->goaway.last_stream_id;
    if (sess->goaway_cb_) sess->goaway_cb_(ec, ls);
    break;
}
```

`rpcpio` uses `on_goaway` in `ChannelImpl` to:
1. Mark the channel as failing.
2. Complete streams above `last_stream_id` with `UNAVAILABLE`.
3. Complete streams at or below `last_stream_id` based on whether trailers
   have arrived (if trailers arrived, use the trailer status; otherwise `UNAVAILABLE`).

---

## Backward Compatibility

All new APIs are additive.  Existing code that does not call `on_trailers`,
`write_trailer`, or `on_goaway` is unaffected.  The trailer accumulation
change replaces a `break` with active collection, which has negligible
performance impact (trailers are rare for non-gRPC HTTP/2 usage).

---

## Test Coverage (BCR Presubmit)

The following tests are added to the BCR module's presubmit matrix
(Linux/macOS × Bazel 7/8):

| Test                        | What it verifies                              |
|-----------------------------|-----------------------------------------------|
| `trailer_roundtrip_test`    | Headers + DATA + trailers sequence            |
| `trailers_only_test`        | Single HEADERS frame with END_STREAM          |
| `duplicate_trailer_test`    | Multiple values for same trailer key          |
| `oversized_trailer_test`    | Trailer size limit enforcement                |
| `rst_stream_test`           | Stream reset before trailers                  |
| `goaway_clean_test`         | GOAWAY with NO_ERROR                          |
| `goaway_error_test`         | GOAWAY with non-zero error code               |
| `callback_ordering_test`    | on_response → on_data → on_trailers → on_close |
| `fragmented_hpack_test`     | HPACK continuation frames for trailers        |
| `write_trailer_server_test` | Server write_trailer produces correct frames  |
