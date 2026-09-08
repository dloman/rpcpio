# Interoperability Tests

This directory contains the official gRPC interoperability test proto definitions
and test programs that verify `rpcpio` interoperates correctly with reference
gRPC implementations (C++, Go, Python).

## Tested Unary Cases

| Test case                    | Direction  |
|------------------------------|------------|
| `empty_unary`                | both       |
| `large_unary`                | both       |
| `custom_metadata`            | both       |
| `status_code_and_message`    | both       |
| `special_status_message`     | both       |
| `timeout_on_sleeping_server` | client     |
| `unimplemented_method`       | client     |
| `unimplemented_service`      | client     |
| `client_compressed_unary`    | client (zlib only) |
| `server_compressed_unary`    | server (zlib only) |

## Running Against a Reference Server

### h2c (plaintext)

Start a reference server (e.g. the official gRPC C++ interop server) in h2c
mode on port 10000, then run:

```sh
TEST_SERVER_HOST=localhost \
TEST_SERVER_PORT=10000 \
TEST_USE_TLS=false \
bazel test //tests/interop:interop_client_test --test_output=all
```

### TLS

```sh
TEST_SERVER_HOST=localhost \
TEST_SERVER_PORT=10000 \
TEST_USE_TLS=true \
bazel test //tests/interop:interop_client_test \
    --test_output=all \
    --test_env=SSL_CERT_FILE=/path/to/ca.pem
```

## Running a Reference Client Against `rpcpio` Server

Start the `rpcpio` server (from `interop_server_test`), then point the
official `grpc_interop_client` at it:

```sh
# Start rpcpio server
bazel run //tests/interop:interop_server_test &

# Run official client (adjust path)
grpc_interop_client \
    --server_host=localhost \
    --server_port=10000 \
    --use_tls=false \
    --test_case=empty_unary
```

## Certificates

For TLS interop tests, generate a self-signed CA and server certificate:

```sh
# CA key and certificate
openssl genrsa -out ca.key 4096
openssl req -new -x509 -days 3650 -key ca.key -out ca.pem \
    -subj "/CN=Test CA"

# Server key and certificate
openssl genrsa -out server.key 2048
openssl req -new -key server.key -out server.csr \
    -subj "/CN=localhost"
openssl x509 -req -days 3650 -in server.csr \
    -CA ca.pem -CAkey ca.key -CAcreateserial \
    -out server.pem
```

Pass `ca.pem` to the client via `--ca_cert_file` and `server.pem`/`server.key`
to the server via `--server_cert_file`/`--server_key_file`.

## Proto Source

`grpc_testing.proto` is a minimal subset of the official interop proto, including
only the unary methods (`EmptyCall` and `UnaryCall`).  Streaming variants are
intentionally omitted — `rpcpio` does not support streaming in version 1.

The original canonical proto lives at:
`github.com/grpc/grpc/blob/master/src/proto/grpc/testing/test.proto`
