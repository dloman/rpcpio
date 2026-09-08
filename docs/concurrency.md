# Concurrency and Ownership Model

## Threading Model

`rpcpio` uses **Boost.Asio** as its execution framework.  All internal state
is confined to a single `boost::asio::io_context` supplied by the caller.
There are **no hidden worker threads** in `rpcpio::Server` or `rpcpio::Channel`.

Use `rpcpio::StandaloneServer` when a binary should own an `io_context` and
worker threads (`ServerOptions::num_threads`).  Embedded integrations (Atlas,
existing event loops) construct `Server(ioc, opts)` and drive `ioc.run()` (or
equivalent) themselves.

### Client

```
┌─────────────────────────────────────────────────────────────┐
│ User coroutine or completion handler                        │
│     ↓ Channel::AsyncUnaryCallRaw (async_initiate)         │
│ ChannelImpl::SubmitCall  ←── runs on channel executor       │
│     ↓ nghttp2-asio session.submit                           │
│ ClientCallState (nghttp2 callbacks)                         │
│     ↓ boost::asio::post back to caller executor             │
│ Completion handler (resumes user coroutine / yield)         │
└─────────────────────────────────────────────────────────────┘
```

**Key invariant:** nghttp2-asio callbacks never run user completion handlers
directly.  `ClientCallState::Complete` always uses `boost::asio::post` to
deliver results through the Asio executor.

`Channel::AsyncUnaryCallRaw` is the Atlas-facing entry point: a
completion-token operation (including `boost::asio::yield_context`) that does
**not** launch an untracked C++20 coroutine inside the library.

### Server

```
┌─────────────────────────────────────────────────────────────┐
│ nghttp2-asio accept (caller’s io_context)                   │
│     ↓ HandleRequest                                         │
│ ServerCallState::Start                                      │
│     ↓ on_data accumulates LPM frames                        │
│ ServerCallState::OnRequestEnd                               │
│     ↓ callback handler OR co_spawn (typed RegisterUnary only)│
│ User handler → UnaryServerReply::Finish (callback path)     │
│     ↓ SendResponse (headers + DATA + trailers)              │
└─────────────────────────────────────────────────────────────┘
```

`Server::RegisterUnaryCallback` invokes the handler directly and passes a
`UnaryServerReply` handle.  `Finish()` may be called from any executor; it
dispatches to the server `io_context` before touching nghttp2 objects.

`Server::RegisterUnary` (typed coroutine API) remains a thin wrapper that may
use `co_spawn` for ordinary rpcpio users.  The callback API itself does not
require `co_spawn`.

### Server lifecycle (embedded)

| Method | Behaviour |
|--------|-----------|
| `Start(host, port)` | Registers async accept/work on `ioc`; **returns immediately** |
| `Shutdown()` | **Idempotent, non-blocking**: stop accepting, `http2_.stop()`, cancel active calls. Does **not** call `io_context::run/stop/join`, sleep, poll, or block. |
| `~Server` | Calls `Shutdown()` (non-blocking) |

After `Shutdown()`, the caller drains the `io_context` to destroy remaining
call state.

---

## Object Lifetimes

### ClientContext lifetime

`Channel::AsyncUnaryCallRaw` and `Channel::UnaryCall` take `ClientContext&`.
The reference must remain valid until the completion handler runs. The transport
copies deadline, outgoing metadata, and compression preference at submission time
and **does not** retain a `ClientContext` pointer after completion.

Associated completion-token cancellation slots and `ClientContext` cancellation
/deadline are wired at submission time and remain active while the call is queued
for connect as well as on the wire.

### Server TLS context

`ServerImpl` stores `ssl_context_` as a member declared before `http2_` so the
TLS context outlives the nghttp2 server during teardown.  `Shutdown()` is
idempotent (`shutdown_` guard), cancels active calls once, then calls
`http2_.stop()` without blocking on the external `io_context`.

### Channel and ChannelImpl

- `Channel` is a thin `shared_ptr<ChannelImpl>` wrapper.
- `ChannelImpl` stays alive while in-flight `ClientCallState` objects hold
  `shared_ptr` references.
- Destroying `Channel` does not cancel in-flight calls; connection teardown
  completes or fails them via stream close / GOAWAY.

### ClientCallState

- Created per RPC; `shared_ptr` shared between nghttp2 callbacks, the deadline
  timer, and the completion handler.
- `completed_` (`std::atomic<bool>`) guarantees exactly-once completion across
  normal trailers, deadline, cancellation, RST_STREAM, GOAWAY, and shutdown.
- Completions are always delivered via `boost::asio::post(ioc_, ...)`.

### ServerCallState

- Created per inbound unary request; tracked by `ServerImpl` until
  `response::on_close`.
- `responded_` (`std::atomic<bool>`) gates `SendResponse` / `SendError`.
- `UnaryServerReply::Finish` is single-fire; subsequent calls are ignored.
- `ServerContext` must not be retained after the reply is sent and the stream
  closes.

---

## Cancellation

### Client

1. Associated cancellation slots on the completion token (Atlas yield path).
2. `ClientContext::cancellation_slot()` during an active `AsyncUnaryCallRaw`.
3. `ClientContext::Cancel()` emits cancellation; the transport completes with
   `CANCELLED` and sends `RST_STREAM` (`NGHTTP2_CANCEL`).
4. Absolute deadlines via `ClientContext::set_deadline` → `DEADLINE_EXCEEDED`.

### Server

1. `ServerContext::cancellation_slot()` for coroutine handlers.
2. Peer stream reset / `on_close` before reply → handler cancellation, no
   response written.
3. `grpc-timeout` → `DEADLINE_EXCEEDED` reply if not yet responded.
4. `Server::Shutdown()` → active calls get `UNAVAILABLE`.

---

## Completion-Gate Pattern

Both client and server unary paths use an atomic single-fire gate:

```cpp
if (completed_.exchange(true)) return;  // or responded_ on server
timer_.cancel();
boost::asio::post(ioc_, [cb = std::move(cb), r = std::move(result)] {
    cb(std::move(r));
});
```

This resolves races between deadline expiry, cancellation, peer reset,
GOAWAY, normal completion, and shutdown deterministically.

---

## Thread-Safety Surface

| Object | Thread-safe methods | Non-thread-safe |
|--------|---------------------|-----------------|
| `Channel` | `AsyncUnaryCallRaw`, `UnaryCall`, `Connect` | Constructor, destructor |
| `Server` | `Shutdown` | `Register*`, `Start` (before start only for register) |
| `UnaryServerReply::Finish` | Yes (dispatches to server ioc) | — |
| `ClientContext` | `Cancel()` | Other fields (single-owner per call) |
| `ServerContext` | — | All (handler thread / server ioc only) |

`RegisterUnary` / `RegisterUnaryCallback` must complete before `Start()`.

---

## GOAWAY Handling

When the server sends GOAWAY, `ChannelImpl::OnGoaway`:

- Marks the channel failed (no automatic retry in v1).
- Completes streams **above** `last_stream_id` with `UNAVAILABLE`.
- Leaves accepted streams to finish via trailers or `on_close`.

When the embedded server shuts down, active calls are cancelled locally with
`UNAVAILABLE` without blocking on peer acknowledgement.
