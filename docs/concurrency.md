# Concurrency and Ownership Model

## Threading Model

`asio_grpc` uses **Boost.Asio** as its execution framework.  All internal state
is confined to a single `boost::asio::io_context` (the "channel executor") and
is accessed only from threads running that context.

### Client

```
┌─────────────────────────────────────────────────────────────┐
│ User coroutine (co_await channel.UnaryCall(...))            │
│     ↓ co_await async_initiate                               │
│ Channel::UnaryCallRaw                                       │
│     ↓ post to channel's io_context                          │
│ ChannelImpl::SubmitCall  ←── runs on channel executor       │
│     ↓ nghttp2-asio session.submit                           │
│ ClientCallState (callbacks run on nghttp2 thread)           │
│     ↓ boost::asio::post back to caller executor             │
│ Completion handler (resumes user coroutine)                 │
└─────────────────────────────────────────────────────────────┘
```

**Key invariant:** nghttp2-asio callbacks are never allowed to run user
completion handlers directly.  `ClientCallState::Complete` always uses
`boost::asio::post` to deliver results back through the Asio executor, ensuring
that user code never runs inside an nghttp2 callback frame.

### Server

```
┌─────────────────────────────────────────────────────────────┐
│ nghttp2-asio accept loop (one thread per io_context thread) │
│     ↓ HandleRequest (validates HTTP/gRPC headers)           │
│ ServerCallState::Start (registers on_data callback)         │
│     ↓ on_data accumulates LPM frames                        │
│ ServerCallState::OnRequestEnd                               │
│     ↓ boost::asio::co_spawn on server io_context            │
│ User handler coroutine (ServerContext&, Request&) → Status  │
│     ↓ result returned                                       │
│ ServerCallState::SendResponse (writes headers + DATA + trailers) │
└─────────────────────────────────────────────────────────────┘
```

Worker threads call `ioc.run()`.  Handlers are spawned onto the same executor,
so no additional synchronisation is required within a single call.

---

## Object Lifetimes

### Channel and ChannelImpl

- `Channel` is a thin `shared_ptr<ChannelImpl>` wrapper.
- `ChannelImpl` is reference-counted; it stays alive as long as any
  `ClientCallState` (for in-flight calls) holds a `shared_ptr` to it.
- After `Channel` is destroyed, in-flight calls complete normally (or are
  cancelled by the session shutdown).

### ClientCallState

- Created per RPC call; `shared_ptr` is shared between:
  - `ChannelImpl::SubmitCall` (briefly, until submission)
  - nghttp2 `on_response` / `on_data` / `on_trailers` / `on_close` lambdas
  - The deadline timer callback
- After `Complete()` fires once (guarded by `atomic<bool>`), all references
  are released and the object is destroyed.
- **Never** holds a raw reference to the nghttp2 `response` object past
  `on_close`.

### ServerCallState

- Created per inbound request; `shared_ptr` is shared between:
  - The nghttp2 `on_data` callback lambda
  - The `co_spawn`-ed user handler coroutine
  - The deadline timer callback
- Destroyed when both the nghttp2 callbacks and the user coroutine are done.
- `responded_` flag prevents duplicate response writes.

### ServerContext

- Owned exclusively by `ServerCallState` for the duration of the call.
- **Must not be retained** after the handler coroutine returns — nghttp2
  request/response wrappers become invalid at `on_close`.

---

## Cancellation

### Client

1. `ClientContext::Cancel()` emits `boost::asio::cancellation_type::all`.
2. The `Channel::UnaryCall` template propagates this to `UnaryCallRaw`.
3. `async_initiate` resolves the awaitable with `CANCELLED` status.
4. Simultaneously, a RST_STREAM CANCEL is sent to the server.
5. The deadline timer is also cancelled via `timer_.cancel()`.

### Server

1. `ServerContext::cancellation_slot()` exposes a slot the handler can `co_await`.
2. When the peer resets the stream or the deadline expires, `trigger_cancel()`
   emits a cancellation signal.
3. The handler must check this signal and return promptly; the server will not
   send a response if the stream has already been reset.

---

## Completion-Gate Pattern

Every call state uses `std::atomic<bool> completed_` as a single-fire gate:

```cpp
void Complete(UnaryResultRaw result) {
    if (completed_.exchange(true)) return;   // second caller is dropped
    timer_.cancel();
    boost::asio::post(ioc_, [cb = std::move(cb), r = std::move(result)] {
        cb(std::move(r));
    });
}
```

This ensures that deadline expiry, peer cancellation, RST_STREAM, and normal
completion races are resolved deterministically: **exactly one** completion
fires.

---

## Thread-Safety Surface

| Object            | Thread-safe methods         | Non-thread-safe             |
|-------------------|-----------------------------|-----------------------------|
| `Channel`         | `UnaryCall`, `Connect`      | Constructor, Destructor     |
| `Server`          | `Shutdown`, `Wait`          | `RegisterUnary`, `Start`    |
| `ClientContext`   | None (single-owner)         | All                         |
| `ServerContext`   | `cancellation_slot()`       | All others                  |
| `MetadataMap`     | Reads (after construction)  | Writes                      |

`RegisterUnary` must be called before `Server::Start()`.  After `Start()`,
the handler map is immutable.

---

## GOAWAY Handling

When the server sends GOAWAY, `nghttp2-asio` delivers an error to the session.
- Streams **above** `last_stream_id` were not processed by the server and can
  in principle be retried on a new connection.
- Streams **at or below** `last_stream_id` were accepted; their fate depends on
  whether trailers arrived before GOAWAY.
- Version 1 makes **no automatic retries**.  Affected calls complete with
  `UNAVAILABLE` (or with their trailer status if trailers arrived first).
