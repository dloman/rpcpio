# Boost 1.87 compatibility tests

This root module forces Boost.Asio to the supported 1.87 floor, overriding the
newer version selected by the normal dependency graph.

Run the compatibility test from this directory:

```sh
bazelisk test --check_direct_dependencies=off //:boost_floor_test
```
