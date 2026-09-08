# Phase 2: nghttp2-asio Deferred Data-Provider Extensions

This document specifies the additional public API additions to the CESNET/nghttp2-asio
library required to support client-streaming and bidirectional-streaming RPCs in
`rpcpio`.  These are additive extensions on top of the Phase 1 changes described
in `docs/nghttp2_asio_phase1.md`.

---

## Why These Changes Are Necessary

gRPC client-streaming and bidi-streaming calls need to send request messages
incrementally: the caller produces messages one at a time (driven by application
logic), rather than supplying the entire body upfront as a `std::string`.

nghttp2-asio's existing `submit()` overload that accepts a generator callback
already allows deferred data production:

```cpp
// Generator sets *data_flags = NGHTTP2_DATA_FLAG_DEFERRED to pause, then
// the application calls resume() to restart it.
auto req = session.submit(ec, method, path, generator_fn, headers);
```

However, the CESNET snapshot used by rpcpio does not expose a `resume()` method
on the `client::request` object.  Without it there is no way to wake the
generator after new data becomes available.

Symmetrically, the server-side `response::resume()` method added in Phase 1 is
the server analogue; this document covers the **client-side** `request::resume()`.

---

## Client-Side Change: `request::resume()`

### New public method

```cpp
// include/nghttp2/asio_http2_client.h

namespace nghttp2::asio_http2::client {

class request {
public:
    // ... existing methods (on_response, on_close, stream_id, ...) ...

    // Wake a generator that previously returned NGHTTP2_DATA_FLAG_DEFERRED.
    // Safe to call from the same io_context executor as the session.
    // Idempotent: calling resume() when the generator is not deferred is a no-op.
    void resume();
};

} // namespace nghttp2::asio_http2::client
```

### Implementation: `asio_client_session_impl.cc`

```cpp
void request_impl::resume() {
    // nghttp2_session_resume_data re-schedules the data provider for this
    // stream.  session_send() flushes the session so the resumed frame is
    // transmitted without waiting for the next I/O event.
    nghttp2_session_resume_data(session_, stream_id_);
    session_send();
}
```

---

## How rpcpio Uses These Extensions

### Generator callback pattern (client streaming / bidi)

When `ChannelImpl::SubmitClientStreamingCall()` (or `SubmitBidiStreamingCall()`)
is called, it registers a generator callback that drains a `std::deque<std::string>`
of LPM-encoded request frames:

```cpp
auto req = session_->submit(ec, "POST", path,
    [writer_impl](uint8_t* buf, std::size_t len, uint32_t* flags) -> ssize_t {
        if (writer_impl->pending_.empty()) {
            if (writer_impl->writes_done_) {
                *flags = NGHTTP2_DATA_FLAG_EOF;
                return 0;
            }
            // No data yet and not done — pause the generator.
            *flags = NGHTTP2_DATA_FLAG_DEFERRED;
            return 0;
        }
        // Copy from front frame.
        auto& front = writer_impl->pending_.front();
        const std::size_t n = std::min(front.size() - writer_impl->offset_, len);
        std::memcpy(buf, front.data() + writer_impl->offset_, n);
        writer_impl->offset_ += n;
        if (writer_impl->offset_ == front.size()) {
            writer_impl->pending_.pop_front();
            writer_impl->offset_ = 0;
        }
        return static_cast<ssize_t>(n);
    },
    headers);
```

### Waking the generator

After the submit returns, `req` is stored in `RawClientWriterImpl::req_`.  Then:

- `RawClientWriterImpl::Enqueue(proto_bytes)` encodes the message as an LPM frame,
  appends it to `pending_`, and calls `req_->resume()` to re-enter the generator.
- `RawClientWriter::WritesDone()` sets `writes_done_ = true` and calls
  `req_->resume()` so the generator sees the EOF flag on its next invocation.

All of this happens on the single `io_context` executor — no locks are needed.

---

## Backward Compatibility

The `resume()` method is additive.  Existing client code that does not use
generator callbacks is unaffected.  The method is a thin wrapper around
`nghttp2_session_resume_data` + `session_send()`, both of which are already
used internally by the library.

---

## Phase 1 Recap

The Phase 1 extensions (documented in `nghttp2_asio_phase1.md`) added:

| API                        | Side   | Purpose                                             |
|----------------------------|--------|-----------------------------------------------------|
| `response::on_trailers`    | Client | Receive gRPC trailing HEADERS with status           |
| `response::write_trailer`  | Server | Emit trailing HEADERS after DATA generator finishes |
| `response::resume()`       | Server | Wake a deferred server-side DATA generator          |
| `session::on_goaway`       | Client | Receive GOAWAY with last_stream_id                  |

Phase 2 adds:

| API                        | Side   | Purpose                                             |
|----------------------------|--------|-----------------------------------------------------|
| `request::resume()`        | Client | Wake a deferred client-side DATA generator          |

Together, Phases 1 and 2 provide the complete set of nghttp2-asio extensions
required for full gRPC streaming support in `rpcpio`.
