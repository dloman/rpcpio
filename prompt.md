Scope and protocol contract
Implement standard unary gRPC over HTTP/2 as specified by the official gRPC HTTP/2 protocol: one protobuf request message and zero-or-one protobuf response message, terminated by mandatory gRPC status trailers. Support both client and server, HTTP/2 multiplexing, h2c prior knowledge for tests/development, and TLS with ALPN h2 for production.
The library is independent of gRPC Core but intentionally depends on:
	•	C++20 and the GM BCR module nghttp2-asio@0.0.90-20260225-464f056, exposed as @nghttp2-asio//:nghttp2_asio.
	•	The registry module already pins the maintained CESNET nghttp2-asio snapshot 464f05639223bce01556fd7d81547087d518e909, Boost.Asio/Boost.Thread 1.90, libnghttp2 1.65.0, and OpenSSL 3.5.5.
	•	protobuf runtime for generated messages and protoc/libprotoc only while generating bindings.
	•	Optional zlib target for gzip message compression; identity encoding remains mandatory.
	•	Bazel/Bzlmod as the supported build and distribution system. Declare the dependency with bazel_dep(name = "nghttp2-asio", version = "0.0.90-20260225-464f056"); CMake packaging is outside the initial scope.
Explicitly defer streaming RPCs, grpc-web, HTTP/3, service-config retries, hedging, client-side load balancing, xDS, reflection, and pluggable authentication. Unknown streaming methods must be rejected by the code generator rather than silently behaving as unary calls.
Architecture and source layout
flowchart LR
    Proto[Proto service] --> Plugin[rpcpio_cpp_plugin]
    Plugin --> Bindings[Typed stub and service]
    Bindings --> Runtime[Unary runtime]
    Runtime --> Framing[gRPC framing and metadata]
    Framing --> Http2[GM BCR nghttp2-asio]
    Http2 --> Wire[HTTP2 TLS or h2c]
Create the standalone repository around these components:
	•	[include/rpcpio/status.h](include/rpcpio/status.h), [metadata.h](include/rpcpio/metadata.h), [client_context.h](include/rpcpio/client_context.h), and [server_context.h](include/rpcpio/server_context.h): stable public value types.
	•	[include/rpcpio/channel.h](include/rpcpio/channel.h), [server.h](include/rpcpio/server.h), and [unary_method.h](include/rpcpio/unary_method.h): coroutine-facing client/server APIs and compile-time method descriptors.
	•	[src/protocol/](src/protocol/BUILDING.md): framing, timeout, metadata, status, compression, and HTTP/2 error translation internals.
	•	[src/client/](src/client/BUILDING.md) and [src/server/](src/server/BUILDING.md): connection/session ownership and per-call state machines.
	•	[compiler/rpcpio_cpp_plugin.cpp](compiler/rpcpio_cpp_plugin.cpp) and [compiler/cpp_generator.cpp](compiler/cpp_generator.cpp): custom protoc plugin.
	•	[examples/helloworld/helloworld.proto](examples/helloworld/helloworld.proto): generated client/server example without any gRPC headers or libraries.
	•	[tests/interop/](tests/interop/README.md): official gRPC interop proto, peer launchers, certificates, and compatibility tests.
Phase 1: make the GM BCR nghttp2-asio module a viable gRPC transport
Use the module introduced by GM-ADAS/DEVOPS.bazel-central-registry PR #357 as the baseline. That PR is currently open, so landing it is the first dependency gate. It already provides a native Bazel target for the maintained CESNET snapshot, explicit Boost.DateTime compatibility patches, and Bazel 7/8 validation; do not create another nghttp2-asio fork or duplicate its BUILD overlay in this project.
The registered CESNET snapshot still explicitly discards received trailers, so develop the following transport changes against CESNET nghttp2-asio and publish the resulting immutable commit as a successor version in the GM registry. If upstream review blocks development, carry the minimal patch in the successor BCR module's source.json; the released gRPC library must still depend only on a numbered registry module version, never a Git branch or ad hoc archive.
Add the minimum public receive-side functionality absent from the current API:
	•	Add response::on_trailers(trailers_cb) and a read-only trailer map to asio_http2_client.h.
	•	In asio_client_session_impl.cc, collect NGHTTP2_HCAT_HEADERS blocks into the trailer map instead of explicitly discarding them, and invoke the callback only after the complete trailing HEADERS block is received.
	•	Treat a response HEADERS block carrying END_STREAM as a trailers-only response: invoke on_response, then expose that completed block to on_trailers, then signal body EOF. This supports immediate gRPC errors, where grpc-status appears in the sole HEADERS block.
	•	Preserve duplicate fields and sensitive flags; maintain a separate size limit for trailers.
	•	Expose GOAWAY information (error_code, last_stream_id) so the channel can distinguish calls accepted by the peer from calls that may be retried by a future policy. Version 1 completes affected calls deterministically and performs no automatic retries.
	•	Preserve the registry module's Boost 1.90 and Bazel 7/8 compatibility. Update its overlay and presubmit matrix only when the public callback changes require it.
Add module-level tests for regular headers + DATA + trailers, trailers-only, fragmented HPACK blocks, duplicate trailer fields, oversized trailers, RST_STREAM, clean/error GOAWAY, and callback ordering/lifetimes. Extend the GM BCR presubmit to build and test the public trailer API on its supported Linux/macOS and Bazel 7/8 matrix. This is a hard feasibility gate: do not build the typed runtime until a small client can retrieve grpc-status: 0 from a stock gRPC server and a small server can emit trailers readable by a stock client.
Phase 2: implement wire primitives
Implement a bounded incremental gRPC message decoder that accepts arbitrary HTTP/2 DATA chunk boundaries. It must:
	•	Parse the one-byte compression flag and four-byte unsigned big-endian length.
	•	Avoid allocation until the full length prefix is available and reject lengths above the configured maximum before allocating.
	•	Accept a frame split across any number of DATA callbacks or multiple frames coalesced into one callback.
	•	For unary calls, reject a second message, truncated prefixes/payloads, reserved compression-flag values, and bytes after the one allowed message.
	•	Distinguish an empty protobuf payload from no response message.
Implement the matching encoder using protobuf ByteSizeLong plus SerializeToArray/SerializeToString, checking serialization failure and the 32-bit gRPC frame-size limit. Keep the framing layer byte-oriented and testable without sockets even though its codec is protobuf.
Implement protocol value types and utilities:
	•	All canonical status codes OK through UNAUTHENTICATED, Status, StatusOr<T>, and UnaryResult<T> containing status, optional response, initial metadata, and trailing metadata.
	•	Lowercase metadata validation, reserved-header protection, duplicate-value preservation, and binary -bin metadata. Emit unpadded base64 and accept padded or unpadded input.
	•	Percent encoding/decoding for grpc-message, preserving UTF-8 bytes and safely handling malformed escapes.
	•	Opaque bytes for grpc-status-details-bin; offer an optional helper for google.rpc.Status rather than requiring the Google APIs protos in the core runtime.
	•	grpc-timeout formatting using at most eight digits and units H/M/S/m/u/n, rounding outward so transport formatting never shortens the caller's deadline; parse with overflow checks.
	•	HTTP-to-gRPC fallback mapping exactly as specified in HTTP status mapping, and only when grpc-status is absent.
	•	Message compression registry with mandatory identity and optional gzip. Never advertise an unavailable encoding; reject a compressed flag without a negotiated grpc-encoding; reset compression state for every message.
Fuzz the frame decoder, timeout parser, percent decoder, and base64 metadata decoder. Add deterministic tests for every byte split, zero-length messages, maximum boundaries, integer overflow, malformed status, unknown status numbers, and decompression bombs constrained by the uncompressed receive limit.
Phase 3: public coroutine API and call lifecycle
Expose C++20 coroutine APIs while keeping callbacks confined to an internal adapter:
	•	Channel::UnaryCall(const UnaryMethod<Request, Response>&, ClientContext&, const Request&) -> boost::asio::awaitable<UnaryResult<Response>>.
	•	Server::RegisterUnary(const UnaryMethod<Request, Response>&, Handler) where a handler is an awaitable taking ServerContext& and const Request&, returning StatusOr<Response>.
	•	Generated stubs call Channel::UnaryCall; generated service registration calls Server::RegisterUnary.
Implement completion-token operations with boost::asio::async_initiate/async_compose, then expose use_awaitable; do not block an Asio worker and do not run user completions inline from nghttp2 callbacks. Every call state is executor-confined, reference-counted, and has a single atomic/logical completion gate covering response completion, deadline, cancellation, stream reset, and connection failure.
ClientContext carries deadline, outgoing metadata, receive/send limits, compression preference, and an Asio cancellation slot. ServerContext exposes incoming metadata, peer, absolute deadline, cancellation slot, and methods to add initial/trailing metadata. Context and response objects must not expose nghttp2-asio references whose lifetime ends at on_close.
Phase 4: unary client transport
Build Channel as the owner of endpoint configuration, TLS context/verification policy, and one multiplexed nghttp2-asio session. Queue calls during connection establishment; submit concurrent calls as independent HTTP/2 streams after connection; fail queued/active calls predictably on connection failure. Reconnection may occur for later calls, but version 1 must not automatically replay an RPC.
For each call:
	•	Serialize and frame exactly one protobuf request.
	•	Submit POST /<fully-qualified-service>/<method> with te: trailers, content-type: application/grpc+proto, grpc-timeout when finite, negotiated compression headers, user agent, authority, and validated custom metadata.
	•	Validate the HTTP response status and gRPC content type before accepting DATA.
	•	Capture initial metadata, incrementally decode at most one response, then require the trailer callback and parse grpc-status, optional grpc-message, details, and trailing metadata.
	•	For OK, require exactly one valid response message. For non-OK status, discard any application response and return the status. Missing grpc-status uses the official HTTP fallback; HTTP 200 without it becomes UNKNOWN.
	•	On local deadline, complete as DEADLINE_EXCEEDED and send RST_STREAM CANCEL. On caller cancellation, complete as CANCELLED and send the same reset. Resolve deadline/response races once on the channel executor.
	•	Map RST_STREAM, GOAWAY, TLS, DNS, EOF, and nghttp2 errors to stable statuses, retaining a transport diagnostic separately from the peer-supplied gRPC status.
Require certificate verification and hostname validation by default. Support explicit test-only insecure TLS and h2c configurations. Configure ALPN strictly for h2; fail connection setup if HTTP/2 is not negotiated.
Phase 5: unary server transport
Register one exact route per generated method and a fallback route for unknown services/methods. For each request:
	•	Require POST, a content type beginning with application/grpc, te: trailers, a valid method path, supported grpc-encoding, bounded metadata, and at most one framed request.
	•	Return HTTP 415 for non-gRPC content types. For valid gRPC requests, use HTTP 200 and express application/protocol failures through gRPC status trailers.
	•	Parse grpc-timeout, arm an Asio timer, and propagate cancellation to the handler. Convert expiry to DEADLINE_EXCEEDED; detect peer RST/close and cancel the handler without writing afterward.
	•	Deserialize only after receiving a complete request and request EOS. Map malformed protobuf to INVALID_ARGUMENT, oversized messages/metadata to RESOURCE_EXHAUSTED, unsupported compression to UNIMPLEMENTED, unknown method to UNIMPLEMENTED, and uncaught handler exceptions to INTERNAL at the coroutine boundary.
	•	Run the handler on the request's associated server executor with co_spawn; never retain raw request/response wrappers past nghttp2-asio's close callback.
Write responses through a generator-backed body owned by the call state. For success, send HTTP 200 response headers, one framed protobuf DATA body, then set EOF | NO_END_STREAM and call write_trailer with grpc-status: 0. For failures and methods rejected before a body exists, use the same generator with no DATA to produce a standards-compliant trailers-only completion. Always put grpc-status in a final HEADERS block, not in an empty terminal DATA frame.
Support graceful shutdown: stop accepting new connections/calls, let active calls finish until a configurable grace deadline, cancel the remainder, send GOAWAY through the fork, then join worker threads. Make handler registration immutable after server start or synchronize it explicitly.
Phase 6: generate typed protobuf bindings
Implement protoc-gen-rpcpio with the protobuf compiler plugin API. For each non-streaming method generate:
	•	A static constexpr UnaryMethod<Request, Response> containing the exact fully-qualified path.
	•	A typed Service base with one pure virtual coroutine per RPC and a Register(Server&) helper.
	•	A typed Stub holding a shared Channel and one coroutine method per RPC.
	•	Separate .rpcpio.pb.h/.cpp files that include the normal generated .pb.h; never regenerate message classes.
Fail generation with a precise source-located diagnostic for client-, server-, or bidirectional-streaming methods. Preserve proto package and C++ namespace rules, nested message names, import visibility, and method deprecation annotations. Avoid static initialization order dependencies by using constexpr descriptors/functions.
Add golden-output tests, compile generated outputs from protos covering packages/imports/nested types/options, and run a generated client against a generated service. Provide a Starlark rule/macro that runs the normal protobuf C++ generation and protoc-gen-rpcpio, returning separate runtime and generated-code targets with strict dependencies. Publish the runtime and compiler as a versioned Bzlmod module consumable through the GM registry.
Phase 7: interoperability and production validation
Use the official gRPC interoperability cases and proto definitions. Run both directions:
	•	This client against official C++ and at least one independent Go or Python gRPC server.
	•	Official clients against this server.
Required unary cases are empty_unary, large_unary, custom_metadata, status_code_and_message, special_status_message, timeout_on_sleeping_server, unimplemented_method, unimplemented_service, and—when zlib is enabled—client_compressed_unary and server_compressed_unary. Add TLS/ALPN, h2c, concurrent multiplexed calls, trailers-only failures, binary metadata, fragmented DATA, malformed peers, cancellation races, graceful shutdown, and long-lived-channel soak tests.
Run ASan, UBSan, TSan-compatible concurrency tests, fuzz targets, and a dependency matrix covering the versions selected by the GM registry. Validate Bzlmod consumption from a clean external Bazel module using only registry-resolved dependencies. Inspect linkage and bazel cquery results to prove neither runtime nor examples depend on gRPC Core.
Delivery gates and acceptance criteria
	1	Registry transport gate: PR #357 is landed, a successor GM BCR module version exposes received trailers/GOAWAY, and that version exchanges normal and trailers-only status with official gRPC peers.
	2	Primitive gate: framing/metadata/status/deadline tests and fuzz smoke corpus pass with bounded allocation.
	3	Runtime gate: hand-declared unary methods work concurrently over h2c and TLS with cancellation and deadlines.
	4	Codegen gate: generated hello-world stub/service compile and run without gRPC libraries.
	5	Interop gate: all selected official unary cases pass in both directions.
	6	Release gate: sanitizers, supported dependency matrix, clean Bzlmod consumer test, API documentation, and registry provenance pass.
Document the protocol mapping, public ownership/lifetime rules, threading model, security defaults, nghttp2-asio registry delta, unsupported features, and compatibility guarantees in [docs/protocol.md](docs/protocol.md), [docs/concurrency.md](docs/concurrency.md), and [docs/compatibility.md](docs/compatibility.md).

