"""Starlark rules and macros for the asio_grpc library."""

load("@rules_proto//proto:defs.bzl", "proto_library")
load("@protobuf//bazel:cc_proto_library.bzl", "cc_proto_library")

def asio_grpc_library(
        name,
        proto,
        deps = [],
        visibility = None,
        **kwargs):
    """Generate asio_grpc typed stub + service for a proto_library target.

    Creates:
      <name>_proto       — proto_library (if proto is a string label, reused)
      <name>_cc_proto    — cc_proto_library for the message classes
      <name>             — cc_library with both message classes and asio_grpc bindings

    Args:
      name:       Name for the generated cc_library target.
      proto:      Label of a proto_library target containing the service definitions.
      deps:       Additional cc_library deps for the generated library.
      visibility: Bazel visibility list (default: current package).
      **kwargs:   Extra arguments forwarded to the inner cc_library.
    """
    proto_label = proto
    cc_proto_name = name + "_cc_proto"

    # Generate the protobuf C++ message classes.
    cc_proto_library(
        name = cc_proto_name,
        deps = [proto_label],
        visibility = visibility or ["//visibility:private"],
    )

    # Run asio_grpc_cpp_plugin via protoc to produce .asio_grpc.pb.{h,cc}.
    generated_srcs_name = name + "_generated_srcs"
    native.genrule(
        name = generated_srcs_name,
        srcs = [proto_label],
        outs = [
            # The output file names mirror the proto file names.
            # This genrule assumes one .proto file per asio_grpc_library call.
            # Multiple-proto cases require explicit outs.
        ],
        # Use a custom tool invocation via protoc.
        cmd = """
            $(location @protobuf//:protoc) \
                --plugin=protoc-gen-asio-grpc=$(location //:asio_grpc_cpp_plugin) \
                --asio-grpc_out=$(RULEDIR) \
                -I$(GENDIR) \
                $(SRCS)
        """,
        tools = [
            "@protobuf//:protoc",
            "//:asio_grpc_cpp_plugin",
        ],
        visibility = ["//visibility:private"],
    )

    # Full library combining the message classes and asio_grpc bindings.
    native.cc_library(
        name = name,
        deps = [
            ":" + cc_proto_name,
            "//:asio_grpc_runtime",
        ] + deps,
        visibility = visibility,
        **kwargs
    )

def asio_grpc_proto_library(
        name,
        srcs,
        deps = [],
        visibility = None):
    """Convenience macro: proto_library + asio_grpc_library in one call.

    Creates:
      <name>_proto  — proto_library
      <name>        — asio_grpc_library (cc_library with bindings)

    Args:
      name:       Base name.
      srcs:       List of .proto source files.
      deps:       Proto deps (labels of proto_library targets).
      visibility: Bazel visibility list.
    """
    proto_name = name + "_proto"
    proto_library(
        name = proto_name,
        srcs = srcs,
        deps = deps,
        visibility = visibility or ["//visibility:private"],
    )
    asio_grpc_library(
        name = name,
        proto = ":" + proto_name,
        visibility = visibility,
    )
