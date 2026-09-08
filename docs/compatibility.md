# Compatibility and Dependency Matrix

## Language and Compiler Requirements

| Requirement        | Minimum version |
|--------------------|-----------------|
| C++ standard       | C++20           |
| Clang              | 14+             |
| GCC                | 12+             |
| MSVC               | Not supported (POSIX sockets required) |

C++20 features used: `co_await`, `co_return`, `std::atomic<bool>` with
`exchange`, `std::optional`, `std::string_view`, structured bindings.

---

## Pinned Dependency Versions

All versions are resolved by the GM Bazel Central Registry (BCR).

| Dependency                | Version pinned in BCR |
|---------------------------|-----------------------|
| nghttp2-asio (CESNET fork)| `0.0.90-20260225-464f056` |
| Boost (Asio, Thread, DateTime) | 1.90            |
| libnghttp2                | 1.65.0                |
| OpenSSL                   | 3.5.5                 |
| protobuf                  | 27.3                  |
| rules_proto               | 6.0.0                 |
| zlib                      | 1.3.1.bcr.3 (optional)|
| googletest                | 1.14.0 (dev only)     |
| rules_fuzzing             | 0.5.2 (dev only)      |

---

## BCR Module Provenance

```
bazel_dep(name = "nghttp2-asio", version = "0.0.90-20260225-464f056")
```

This module is published via GM-ADAS/DEVOPS.bazel-central-registry PR #357.
It wraps the CESNET/nghttp2-asio snapshot at commit
`464f05639223bce01556fd7d81547087d518e909` with the following additions
(Phase 1 of this library's development):

1. `response::on_trailers(trailer_cb cb)` — callback invoked with the complete
   trailing HEADERS block after DATA EOF.
2. Trailer accumulation in `asio_client_session_impl.cc` instead of discarding
   `NGHTTP2_HCAT_HEADERS` blocks.
3. Trailers-only response detection: a sole HEADERS frame with `END_STREAM`
   triggers `on_response` then `on_trailers` in sequence.
4. `session::on_goaway(error_code, last_stream_id)` callback for clean/error
   GOAWAY handling.
5. Separate size limit for trailers (configurable, default 8 KiB).

These changes are published as a successor version in the GM BCR.  The released
`rpcpio` library depends only on the numbered registry version; it never
references a Git branch or ad-hoc archive.

---

## Bazel Version Support

| Bazel version | Status                |
|---------------|-----------------------|
| 7.x           | Supported (tested)    |
| 8.x           | Supported (tested)    |
| 6.x and below | Not supported         |

Bzlmod (`--enable_bzlmod`) is required.  Legacy WORKSPACE mode is not supported.

---

## Platform Support

| Platform      | Status     |
|---------------|------------|
| Linux x86_64  | Tested     |
| Linux ARM64   | Tested     |
| macOS x86_64  | Tested     |
| macOS ARM64   | Tested     |
| Windows       | Not supported |

---

## API Stability Guarantees

`rpcpio` follows semantic versioning:

- **0.x.y** — public API may change between minor versions.
- **1.0+** — public headers in `include/rpcpio/` are stable (additions are
  backward compatible; removals require a major version bump).

The following are **not** part of the stable API:
- Anything under `src/` (internal headers)
- `internal::ChannelImpl`, `internal::ServerImpl`
- The compiler plugin's generated file format (may change between major versions)

---

## Dependency on gRPC Core

**Zero.**  Neither the runtime library nor the generated code have any
dependency on `grpc`, `grpc++`, or any `google::protobuf::grpc` namespace.

Verified by:
```
bazel cquery //... --output=build | grep -i grpc_core  # must produce no output
```

---

## Interoperability

The following official gRPC interop test cases pass in both directions
(this client ↔ reference server, reference client ↔ this server):

| Test case                      | Status |
|--------------------------------|--------|
| `empty_unary`                  | ✓      |
| `large_unary`                  | ✓      |
| `custom_metadata`              | ✓      |
| `status_code_and_message`      | ✓      |
| `special_status_message`       | ✓      |
| `timeout_on_sleeping_server`   | ✓      |
| `unimplemented_method`         | ✓      |
| `unimplemented_service`        | ✓      |
| `client_compressed_unary`      | ✓ (with zlib) |
| `server_compressed_unary`      | ✓ (with zlib) |

Reference implementations tested: gRPC C++ (v1.65), Go (`google.golang.org/grpc` v1.64).

---

## Sanitizer Support

| Sanitizer | Notes                                              |
|-----------|----------------------------------------------------|
| ASan      | Fully supported (`--config=asan`)                  |
| UBSan     | Fully supported (`--config=ubsan`)                 |
| TSan      | Supported; use thread-safe Boost build             |
| MSan      | Not tested                                         |

---

## Unsupported Features (Deferred to Future Versions)

- Streaming RPCs (client, server, bidirectional)
- grpc-web
- HTTP/3 / QUIC transport
- Service config (retry policy, hedging)
- xDS / gRPC-LB
- Reflection API (`grpc.reflection.v1`)
- Pluggable credential plugins
- Automatic retry on transient failures
- `grpc-previous-rpc-attempts` header
