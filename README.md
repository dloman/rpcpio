# rpcpio

C++20 gRPC-over-HTTP/2 without gRPC Core.

---

## Problem

The official gRPC C++ library is a ~500 KLOC dependency that pulls in its own
HTTP/2 stack, its own TLS layer, its own thread pool, and its own event loop.
If you already use Boost.Asio for networking, you pay for two competing async
runtimes, two sets of threads, and a complex shutdown dance between them.

**rpcpio** implements the gRPC wire protocol on top of
[nghttp2-asio](https://github.com/cesnet/nghttp2) (CESNET fork) and
Boost.Asio coroutines. The result is a library that:

- speaks standard gRPC wire format — interoperates with any conformant peer
- runs on the caller's `io_context` — no hidden threads, no second event loop
- is expressed entirely in C++20 coroutines — no callbacks, no futures
- has zero dependency on gRPC Core

---

## Wire-protocol coverage

| Feature | Status |
|---------|--------|
| Unary RPC (client + server) | ✓ |
| Server-streaming RPC | ✓ |
| Client-streaming RPC | ✓ |
| Bidirectional streaming RPC | ✓ |
| h2c prior-knowledge (plaintext) | ✓ |
| TLS with ALPN h2 | ✓ |
| Trailing HEADERS (grpc-status / trailers) | ✓ |
| Trailers-only responses | ✓ |
| grpc-timeout / deadline propagation | ✓ |
| Initial + trailing metadata | ✓ |
| gzip message compression | ✓ (opt-in) |
| GOAWAY graceful shutdown | ✓ |

---

## Dependencies

| Library | Version | Role |
|---------|---------|------|
| [nghttp2-asio](https://github.com/cesnet/nghttp2) (CESNET fork) | `0.0.90-20260225-464f056` | HTTP/2 framing + TLS |
| Boost.Asio | ≥ 1.82 | async executor, coroutines, TLS |
| Protocol Buffers | 27.x | message serialisation |
| zlib | 1.3.x | gzip compression (optional) |

> **Extension notes** — rpcpio requires additions to the upstream nghttp2-asio
> library that are not yet merged upstream. The CESNET BCR module includes them:
>
> *Phase 1* (unary + trailers): `response::on_trailers`, `response::write_trailer`,
> `session::on_goaway` — spec: [`docs/nghttp2_asio_phase1.md`](docs/nghttp2_asio_phase1.md).
>
> *Phase 2* (streaming): `response::resume()` (wakes a deferred server-side data
> provider) and `request::resume()` (wakes a deferred client-side data provider)
> — spec: [`docs/nghttp2_asio_phase2.md`](docs/nghttp2_asio_phase2.md).
>
> Until the BCR module is updated, the library will not link against a stock
> nghttp2-asio build.

---

## Build

rpcpio uses [Bzlmod](https://bazel.build/external/bzlmod) (Bazel 7+).

```sh
bazel build //:rpcpio             # runtime library + headers
bazel build //:rpcpio_cpp_plugin  # protoc plugin binary
bazel test //tests/unit/...       # unit tests
bazel test //tests/fuzz/...       # fuzz targets
```

`.bazelrc` enables C++20 and provides `--config=asan`, `--config=ubsan`,
and `--config=tsan` presets.

---

## Protoc plugin

The plugin generates a `.rpcpio.pb.h` / `.rpcpio.pb.cc` pair for each
`.proto` file that contains `service` definitions.

```sh
protoc \
  --plugin=protoc-gen-rpcpio=/path/to/rpcpio_cpp_plugin \
  --rpcpio_out=. \
  your_service.proto
```

From Bazel, use the `rpcpio_proto_library` macro:

```python
load("@rpcpio//bazel:defs.bzl", "rpcpio_proto_library")

rpcpio_proto_library(
    name = "greeter",
    srcs = ["helloworld.proto"],
)
```

This creates a `cc_library` target named `greeter` that exposes:

- `GreeterStub` — typed client stub, one method per RPC
- `GreeterService` — abstract base class; implement and call `Register(server)`
- `GreeterMethods` — namespace of `constexpr` method descriptors (`UnaryMethod`,
  `ServerStreamingMethod`, `ClientStreamingMethod`, or `BidiStreamingMethod`
  depending on the proto definition)

---

## Source layout

```
include/rpcpio/      Public headers (channel, server, contexts, status, …)
src/client/          ChannelImpl, ClientCallState
src/server/          ServerImpl, ServerCallState
src/protocol/        LPM framing, metadata codec, timeout, compression, status map
compiler/            protoc plugin (CppGenerator)
bazel/               Starlark rule (_rpcpio_generate) and macros
tests/unit/          GoogleTest unit tests
tests/fuzz/          libFuzzer fuzz targets
tests/interop/       gRPC interop test client and server
examples/helloworld/ Unary RPC — basic greeter (client + server)
examples/streaming/  Server-streaming, client-streaming, and bidi-streaming
examples/deadline/   Setting a client deadline; handling DEADLINE_EXCEEDED
examples/metadata/   Sending and receiving custom call metadata
docs/                Wire protocol, concurrency model, compatibility matrix
```

---

## Quick start — server

```cpp
#include <boost/asio/io_context.hpp>
#include <boost/asio/signal_set.hpp>
#include "rpcpio/server.h"
#include "helloworld.rpcpio.pb.h"   // generated

class GreeterImpl : public helloworld::GreeterService {
public:
    // Unary
    boost::asio::awaitable<rpcpio::StatusOr<helloworld::HelloReply>>
    SayHello(rpcpio::ServerContext&,
             const helloworld::HelloRequest& req) override {
        helloworld::HelloReply reply;
        reply.set_message("Hello, " + req.name());
        co_return reply;
    }

    // Server streaming — send one reply per name character
    boost::asio::awaitable<rpcpio::Status>
    SayHelloStream(rpcpio::ServerContext&,
                   const helloworld::HelloRequest& req,
                   rpcpio::ServerWriter<helloworld::HelloReply>& writer) override {
        for (char c : req.name()) {
            helloworld::HelloReply reply;
            reply.set_message(std::string(1, c));
            auto st = co_await writer.Write(reply);
            if (!st.ok()) co_return st;
        }
        co_return rpcpio::Status{};
    }
};

int main() {
    boost::asio::io_context ioc;

    rpcpio::ServerOptions opts;
    opts.use_h2c     = true;   // plaintext for dev; remove for TLS
    opts.num_threads = 4;

    rpcpio::Server server(ioc, opts);
    GreeterImpl svc;
    svc.Register(server);
    server.Start("0.0.0.0", 50051);

    boost::asio::signal_set sigs(ioc, SIGINT, SIGTERM);
    sigs.async_wait([&](auto, auto) { server.Shutdown(); });

    ioc.run();
}
```

## Quick start — client

```cpp
#include <boost/asio/io_context.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include "rpcpio/channel.h"
#include "rpcpio/client_context.h"
#include "helloworld.rpcpio.pb.h"

int main() {
    boost::asio::io_context ioc;

    rpcpio::ChannelOptions opts;
    opts.use_h2c = true;
    auto ch = std::make_shared<rpcpio::Channel>(ioc, "localhost", 50051, opts);

    boost::asio::co_spawn(ioc, [ch]() -> boost::asio::awaitable<void> {
        helloworld::GreeterStub stub(ch);

        // ── Unary ──────────────────────────────────────────────────────────
        {
            rpcpio::ClientContext ctx;
            helloworld::HelloRequest req;
            req.set_name("world");
            auto result = co_await stub.SayHello(ctx, req);
            if (result.status.ok())
                std::cout << result.response->message() << '\n';
        }

        // ── Server streaming ───────────────────────────────────────────────
        {
            rpcpio::ClientContext ctx;
            helloworld::HelloRequest req;
            req.set_name("abc");
            auto reader = co_await stub.SayHelloStream(ctx, req);
            while (true) {
                auto r = co_await reader.Read();
                if (!r.ok() || !(*r).has_value()) break;
                std::cout << (**r).message() << '\n';
            }
            co_await reader.Finish();
        }
    }, boost::asio::detached);

    ioc.run();
}
```

## Quick start — client streaming and bidi

```cpp
// Client streaming: accumulate N requests, get one response.
boost::asio::co_spawn(ioc, [ch]() -> boost::asio::awaitable<void> {
    helloworld::GreeterStub stub(ch);
    rpcpio::ClientContext ctx;
    auto writer = co_await stub.AccumulateNames(ctx);
    for (auto& name : {"alice", "bob", "carol"}) {
        helloworld::HelloRequest req;
        req.set_name(name);
        co_await writer.Write(req);
    }
    co_await writer.WritesDone();
    auto result = co_await writer.FinishAndGetResponse();
    if (result.status.ok())
        std::cout << result.response->message() << '\n';
}, boost::asio::detached);

// Bidi streaming: interleave sends and receives.
boost::asio::co_spawn(ioc, [ch]() -> boost::asio::awaitable<void> {
    helloworld::GreeterStub stub(ch);
    rpcpio::ClientContext ctx;
    auto stream = co_await stub.Chat(ctx);
    for (auto& name : {"ping", "pong"}) {
        helloworld::HelloRequest req;
        req.set_name(name);
        co_await stream.Write(req);
        auto r = co_await stream.Read();
        if (r.ok() && (*r).has_value())
            std::cout << (**r).message() << '\n';
    }
    co_await stream.WritesDone();
    co_await stream.Finish();
}, boost::asio::detached);
```

---

## Key design decisions

**Single `io_context`.** Both the HTTP/2 I/O layer and handler coroutines run
on the same `io_context` the caller provides. There is no hidden thread pool.

**Type-erased core.** `Channel::UnaryCall<Req,Resp>` and
`Server::RegisterUnary<Req,Resp,Handler>` are templates that live entirely in
headers. They serialize/deserialize protobuf messages and delegate to
`UnaryCallRaw` / `RegisterUnaryRaw`, which operate on raw byte strings and are
compiled into the library. This keeps code size proportional to the number of
RPC methods, not to the template instantiation depth.

**Single-fire completion gate.** `ClientCallState` uses `std::atomic<bool>
completed_` with `exchange(true)` to guarantee exactly-once delivery of the
result even when deadline, RST_STREAM, GOAWAY, and normal trailer arrival race.

**Generator-callback trailers.** The server uses nghttp2's data-provider
callback to send the response body. When the body is exhausted, the callback
sets `NGHTTP2_DATA_FLAG_EOF | NGHTTP2_DATA_FLAG_NO_END_STREAM` and calls
`response::write_trailer(headers)`, producing the mandatory trailing HEADERS
frame with END_STREAM. This is the Phase 1 extension to nghttp2-asio.

**Streaming via deferred generators.** For streaming RPCs the same data-provider
callback returns `NGHTTP2_DATA_FLAG_DEFERRED` when no data is ready, suspending
the HTTP/2 stream. When `ServerWriter::Write()` or `ClientWriter::Write()` enqueues
a new frame it calls `response::resume()` / `request::resume()` (Phase 2
extensions) to wake the generator. `StreamMessageQueue` bridges the I/O callback
thread and the coroutine consumer using a `steady_timer` at `time_point::max()`
cancelled by `Wake()` — zero-overhead async notification on a single executor.

---

## Status and grpc-status codes

`rpcpio::Status` maps directly to gRPC status codes (OK=0 through
UNAUTHENTICATED=16). On the client, if a response carries no `grpc-status`
trailer, the status falls back to an HTTP→gRPC mapping per the gRPC spec.
If even that is absent the call completes with UNKNOWN.

---

## TLS

```cpp
rpcpio::ChannelOptions opts;
opts.use_tls       = true;          // default
opts.verify_peer   = true;          // default
opts.ca_cert_file  = "/etc/ssl/certs/ca-certificates.crt";  // or leave empty for system store
opts.client_cert_file = "client.pem";  // mTLS (optional)
opts.client_key_file  = "client.key";
```

```cpp
rpcpio::ServerOptions opts;
opts.server_cert_file = "server.pem";
opts.server_key_file  = "server.key";
opts.ca_cert_file     = "ca.pem";   // for mTLS client verification (optional)
```

ALPN `h2` is negotiated automatically.

---

## Interoperability tests

`tests/interop/` contains a client test and a self-contained server test.
`grpc_testing.proto` is a subset of the
[official gRPC interop proto](https://github.com/grpc/grpc/blob/master/src/proto/grpc/testing/test.proto)
extended to cover all four RPC variants.

### gRPC interop test coverage

| Test case | Status | Notes |
|-----------|--------|-------|
| `empty_unary` | ✓ | |
| `large_unary` | ✓ | 314 KiB response payload |
| `custom_metadata` | ✓ | `x-grpc-test-echo-*` headers |
| `status_code_and_message` | ✓ | |
| `special_status_message` | ✓ | percent-encoded message |
| `timeout_on_sleeping_server` | ✓ | 100 ms deadline |
| `unimplemented_method` | ✓ | expects UNIMPLEMENTED |
| `unimplemented_service` | ✓ | expects UNIMPLEMENTED |
| `client_compressed_unary` | ✓ | zlib only |
| `server_compressed_unary` | ✓ | zlib only |
| `server_streaming` (`StreamingOutputCall`) | ✓ | self-contained server test |
| `client_streaming` (`StreamingInputCall`) | ✓ | self-contained server test |
| `ping_pong` / `full_duplex` (`FullDuplexCall`) | ✓ | self-contained server test |

### Running the client test against a reference server

```sh
TEST_SERVER_HOST=localhost \
TEST_SERVER_PORT=10000 \
TEST_USE_TLS=false \
bazel test //tests/interop:interop_client_test --test_output=all
```

### Running the self-contained server test

```sh
bazel test //tests/interop:interop_server_test --test_output=all
```

The server test binds on an OS-assigned ephemeral port and exercises it
with the rpcpio client — no external binary required.

See [`tests/interop/README.md`](tests/interop/README.md) for TLS setup and
instructions for pointing the official `grpc_interop_client` at the rpcpio server.

---

## Docs

| Document | Contents |
|----------|----------|
| [`docs/protocol.md`](docs/protocol.md) | LPM framing, metadata rules, timeout encoding, HTTP→gRPC status table |
| [`docs/concurrency.md`](docs/concurrency.md) | Threading model, lifetime rules, cancellation flow, GOAWAY handling |
| [`docs/compatibility.md`](docs/compatibility.md) | Pinned dep versions, platform matrix, API stability |
| [`docs/nghttp2_asio_phase1.md`](docs/nghttp2_asio_phase1.md) | Spec for the Phase 1 nghttp2-asio extensions (trailers + GOAWAY) |
| [`docs/nghttp2_asio_phase2.md`](docs/nghttp2_asio_phase2.md) | Spec for the Phase 2 nghttp2-asio extensions (`resume()` for streaming) |
