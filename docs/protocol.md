# Protocol Mapping

This document describes how `rpcpio` maps the gRPC HTTP/2 protocol to its
internal primitives and how each wire element is translated to/from the public API.

## Scope

`rpcpio` implements **standard unary gRPC over HTTP/2** as specified in
[gRPC over HTTP2](https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md).
Streaming RPCs, grpc-web, HTTP/3, xDS, and client-side load balancing are
**explicitly deferred** and not part of this implementation.

---

## HTTP/2 Transport

### Connection

| Mode              | Transport      | ALPN             | Default port |
|-------------------|----------------|------------------|--------------|
| TLS (production)  | TLS 1.2+       | `h2`             | 443          |
| h2c (dev/test)    | TCP plaintext  | prior knowledge  | 80           |

ALPN negotiation is strict: if the server does not select `h2`, the connection
is torn down with `UNAVAILABLE`.

Certificate verification and hostname validation are **on by default**.
`verify_peer = false` is only supported for test configurations.

### Multiplexing

Each `Channel` owns a single `nghttp2-asio` client session.  Concurrent unary
calls are submitted as independent HTTP/2 streams on that one connection.

---

## Request Wire Format

```
POST /<fully-qualified-service>/<method>  HTTP/2
:authority:             <host>
content-type:           application/grpc+proto
te:                     trailers
grpc-timeout:           <TimeoutValue><TimeoutUnit>   (optional)
grpc-encoding:          <algorithm>                   (optional)
grpc-accept-encoding:   identity[,gzip]               (server → client)
user-agent:             rpcpio/0.1
<custom metadata key>:  <value>                       (optional, repeatable)

<LPM frame(s)>
```

### Length-Prefixed Message (LPM) Frame

```
Byte 0:    Compression flag  (0 = identity, 1 = compressed)
Bytes 1–4: Message length    (big-endian uint32)
Bytes 5+:  Protobuf payload
```

For unary calls exactly **one** LPM frame is sent and at most **one** is received.

---

## Response Wire Format

```
HTTP/2 200 OK
content-type: application/grpc+proto
<initial metadata>

<LPM frame(s)>

grpc-status:             <integer>          ← mandatory
grpc-message:            <percent-encoded>  ← optional
grpc-status-details-bin: <base64>           ← optional
<trailing metadata>
```

### Trailers-only Response

When the server rejects a call before producing a response body (e.g., unknown
method, malformed request), the gRPC status appears in the **initial** HEADERS
block.  `rpcpio` detects this by checking for `grpc-status` in the first
HEADERS frame and treats the call as complete without waiting for DATA frames.

---

## HTTP/2 Error Mapping

| HTTP/2 RST error code | gRPC status       |
|-----------------------|-------------------|
| REFUSED_STREAM (7)    | UNAVAILABLE        |
| CANCEL (8)            | CANCELLED          |
| ENHANCE_YOUR_CALM (11)| RESOURCE_EXHAUSTED |
| _others_              | INTERNAL           |

### HTTP Status → gRPC Fallback (no `grpc-status` trailer)

Used only when `grpc-status` is absent:

| HTTP status | gRPC status       |
|-------------|-------------------|
| 400         | INTERNAL          |
| 401         | UNAUTHENTICATED   |
| 403         | PERMISSION_DENIED |
| 404         | UNIMPLEMENTED     |
| 429         | UNAVAILABLE       |
| 502/503/504 | UNAVAILABLE       |
| _others_    | UNKNOWN           |

HTTP 200 without `grpc-status` → `UNKNOWN`.

---

## grpc-timeout Encoding

Format: `<value><unit>` where value ≤ 8 ASCII decimal digits and unit ∈ H M S m u n.

Encoding **rounds outward** (ceiling) so the formatted timeout is never shorter
than the caller's intended deadline.

---

## Metadata Rules

| Rule                   | Detail                                         |
|------------------------|------------------------------------------------|
| Key character set      | Lowercase `[a-z0-9-_]`                         |
| Binary keys            | End with `-bin`; value is unpadded base64      |
| Sensitive flag         | Preserved from nghttp2; not stripped           |
| Reserved keys          | `grpc-*`, `content-type`, `te`, `:*` — blocked from application code |
| Duplicate values       | Allowed; stored as separate map entries         |
| Size limit             | 8 192 bytes for initial metadata; 8 192 for trailers |

### Percent Encoding (grpc-message)

All bytes outside printable ASCII (0x21–0x7E), plus space (0x20) and `%`
(0x25), are encoded as `%XX`.  Decoding is lenient: malformed `%XX` sequences
are passed through unchanged.

---

## Status Codes

All 17 canonical gRPC status codes are supported (`OK` through `UNAUTHENTICATED`).
Unknown integer values map to `UNKNOWN`.

`grpc-status-details-bin` is accepted as opaque bytes and exposed via
`Status::details()`.  A helper for `google.rpc.Status` decoding is optional
and does not require the Google APIs protos in the core runtime.

---

## Compression

| Encoding   | Support           |
|------------|-------------------|
| `identity` | Always available  |
| `gzip`     | When `RPCPIO_ENABLE_GZIP=1` and zlib is linked |

The library never advertises an encoding it cannot decompress.
A compressed flag without a negotiated encoding → `INTERNAL` error.
Decompression is bounded by `max_receive_message_size` to prevent bombs.

---

## Unsupported Features (Version 1)

- **Streaming RPCs** — client/server/bidirectional streaming
- **grpc-web** — browser HTTP/1.1 bridging
- **HTTP/3 / QUIC**
- **Service config** — retries, hedging, load balancing policy
- **xDS / gRPC-LB**
- **Reflection API**
- **Pluggable authentication** — bring your own `grpc-authorization` metadata
- **Automatic retry** on GOAWAY or transient failures
