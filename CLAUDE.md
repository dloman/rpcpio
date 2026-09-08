# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Project Is

**rpcpio** is a C++20 gRPC-over-HTTP/2 library that runs entirely on the caller's `boost::asio::io_context` — no gRPC Core dependency, no hidden threads. It provides unary RPC client/server with C++20 coroutines, TLS (ALPN h2) and plaintext (h2c), and a protoc plugin that generates typed stubs.

## Build Commands

Bazel 7.4.1 via Bzlmod. Always use the versions pinned in `MODULE.bazel`.

```bash
# Library and plugin
bazel build //:rpcpio
bazel build //:rpcpio_cpp_plugin

# All unit tests
bazel test //tests/unit/...

# Single test (e.g., framing_test)
bazel test //tests/unit:framing_test --test_output=all

# Self-contained interop test (starts rpcpio server internally)
bazel test //tests/interop:interop_server_test --test_output=all

# Interop against an external reference server
TEST_SERVER_HOST=localhost TEST_SERVER_PORT=10000 TEST_USE_TLS=false \
  bazel test //tests/interop:interop_client_test --test_output=all

# Fuzz targets
bazel test //tests/fuzz/...

# Sanitizers (defined in .bazelrc)
bazel test //tests/unit/... --config=asan
bazel test //tests/unit/... --config=ubsan
bazel test //tests/unit/... --config=tsan
```

## Architecture

### Threading Model

A single `io_context` drives everything — HTTP/2 I/O, coroutine resumption, and timer callbacks all run on the caller's executor. There are no hidden threads. All per-call state is confined to that executor; never touch call state from another thread.

### Type Erasure at the Protocol Boundary

Public API (`Channel::UnaryCall<Req,Resp>`, `Server::RegisterUnary<Req,Resp,Handler>`) are thin template facades. They serialize/deserialize protobuf messages and delegate to type-erased raw implementations (`ChannelImpl`, `ServerImpl`). This keeps compiled code size proportional to the number of RPC methods, not to library internals.

### Layer Breakdown

| Layer | Location | Role |
|---|---|---|
| Public API | `include/rpcpio/` | Typed C++20 coroutine interface |
| Client runtime | `src/client/` | `ChannelImpl` (multiplexing), `ClientCallState` (per-call state machine) |
| Server runtime | `src/server/` | `ServerImpl` (accept loop, route dispatch), `ServerCallState` (per-request state) |
| Protocol | `src/protocol/` | LPM framing, metadata codec, timeout encoding, gzip compression, status mapping |
| Protoc plugin | `compiler/` | Generates `.rpcpio.pb.h/.cc` with typed stubs and `ServiceBase` |
| Bazel rules | `bazel/defs.bzl` | `rpcpio_library()` and `rpcpio_proto_library()` macros |

### Key Invariants

- **Exactly-once completion delivery**: `ClientCallState::completed_` is a `std::atomic<bool>` tested with `exchange(true)`. Deadline expiry, RST_STREAM, and GOAWAY can all race; only the first one fires the completion.
- **nghttp2 callbacks post back to the executor**: Never resume a coroutine directly from an nghttp2 callback. Always use `boost::asio::post(executor_, ...)`.
- **Trailers-only detection**: An early rejection (unknown method, etc.) arrives as an initial HEADERS frame containing `grpc-status`. The call state detects this and skips waiting for a DATA frame.
- **Graceful degradation**: If `grpc-status` is absent from trailers, fall back to the HTTP status → gRPC status mapping in `src/protocol/status_map.cc` (per gRPC spec §A4).

### Protoc Plugin Output

Running the plugin on `foo.proto` produces `foo.rpcpio.pb.h` and `foo.rpcpio.pb.cc` containing:
- A `static constexpr UnaryMethod<Req,Resp>` descriptor per RPC
- A `ServiceBase` abstract class with pure-virtual coroutine handlers
- A `Stub` typed client that wraps a shared `Channel`

Use `rpcpio_library(name, proto)` or `rpcpio_proto_library(name, srcs)` in BUILD files instead of invoking the plugin manually.

## Key Source Files

- `include/rpcpio/channel.h` — Client entry point; `Channel` owns the connection, `UnaryCall<Req,Resp>` is the call facade
- `include/rpcpio/server.h` — Server entry point; `RegisterUnary` wires a coroutine handler to a route
- `include/rpcpio/status.h` — `Status`, `StatusCode`, `StatusOr<T>`, `UnaryResult<T>`
- `src/client/channel_impl.cc` — nghttp2-asio session, call multiplexing, GOAWAY handling
- `src/client/call_state.cc` — Response accumulation, deadline timer, completion gate
- `src/server/server_impl.cc` — HTTP/2 acceptor, ephemeral port resolution, graceful shutdown
- `src/server/call_state.cc` — Request framing, context population, handler dispatch, trailer transmission
- `src/protocol/framing.cc` — 5-byte LPM header encode/decode with incremental stateful decoder
- `src/protocol/timeout.cc` — `grpc-timeout` 8-digit value + unit (H/M/S/m/u/n)
- `compiler/cpp_generator.cc` — Protoc plugin code generation (~1 500 LOC)

## Documentation

- `docs/protocol.md` — Wire format, LPM framing, metadata rules, timeout encoding, HTTP→gRPC status table
- `docs/concurrency.md` — Executor confinement, object lifetimes, cancellation flow, GOAWAY handling
- `docs/compatibility.md` — C++20 requirement, compiler versions (Clang 14+, GCC 12+), dependency matrix
- `tests/interop/README.md` — TLS certificate generation, external reference server/client setup

## Dependencies

Resolved via Bazel Central Registry (`MODULE.bazel`):
- **nghttp2-asio** 0.0.90-20260225-464f056 (CESNET fork with trailer/GOAWAY extensions)
- **Boost.Asio** 1.90+ (pulled in via nghttp2-asio)
- **Protocol Buffers** 27.3
- **zlib** 1.3.1 (optional gzip compression)
- **googletest** 1.14.0 (tests only)
- **rules_fuzzing** 0.5.2 (fuzz targets only)
