"""Starlark rules and macros for the asio_grpc library."""

load("@rules_proto//proto:defs.bzl", "proto_library")
load("@protobuf//bazel:cc_proto_library.bzl", "cc_proto_library")

# ── Private rule: run asio_grpc_cpp_plugin via protoc ────────────────────────

def _asio_grpc_generate_impl(ctx):
    proto_info = ctx.attr.proto[ProtoInfo]
    plugin = ctx.executable.plugin
    protoc = ctx.executable.protoc

    all_hdrs = []
    all_srcs = []

    for src in proto_info.direct_sources:
        stem = src.basename[:-len(".proto")]
        out_h  = ctx.actions.declare_file(stem + ".asio_grpc.pb.h")
        out_cc = ctx.actions.declare_file(stem + ".asio_grpc.pb.cc")
        all_hdrs.append(out_h)
        all_srcs.append(out_cc)

        args = ctx.actions.args()
        args.add("--plugin=protoc-gen-asio-grpc=" + plugin.path)
        args.add("--asio-grpc_out=" + out_h.dirname)

        # Setting --proto_path to the directory that contains this .proto file
        # causes protoc to pass only the bare filename (e.g. "foo.proto") to the
        # plugin, so the plugin emits "foo.asio_grpc.pb.h" directly into outdir.
        args.add("--proto_path=" + src.dirname)

        # Transitive proto paths: needed for any imports inside the .proto file
        # (e.g. google/protobuf/empty.proto).
        for path in proto_info.transitive_proto_path.to_list():
            args.add("--proto_path=" + path)

        args.add(src.path)

        ctx.actions.run(
            executable = protoc,
            arguments = [args],
            inputs = depset(
                direct = [src, plugin],
                transitive = [proto_info.transitive_sources],
            ),
            outputs = [out_h, out_cc],
            mnemonic = "AsioGrpcGenerate",
            progress_message = "Generating asio_grpc bindings for " + src.basename,
        )

    return [
        DefaultInfo(files = depset(all_hdrs + all_srcs)),
        OutputGroupInfo(
            hdrs = depset(all_hdrs),
            srcs = depset(all_srcs),
        ),
    ]

_asio_grpc_generate = rule(
    implementation = _asio_grpc_generate_impl,
    attrs = {
        "proto": attr.label(
            providers = [ProtoInfo],
            mandatory = True,
        ),
        "plugin": attr.label(
            executable = True,
            cfg = "exec",
            default = Label("//:asio_grpc_cpp_plugin"),
        ),
        "protoc": attr.label(
            executable = True,
            cfg = "exec",
            default = Label("@protobuf//:protoc"),
        ),
    },
)

# ── Public macros ─────────────────────────────────────────────────────────────

def asio_grpc_library(
        name,
        proto,
        deps = [],
        visibility = None,
        **kwargs):
    """Generate asio_grpc typed stub + service for a proto_library target.

    Creates:
      <name>_cc_proto   — cc_proto_library for the message classes
      <name>_gen        — rule that runs asio_grpc_cpp_plugin via protoc
      <name>_gen_hdrs   — filegroup selecting only the generated headers
      <name>_gen_srcs   — filegroup selecting only the generated sources
      <name>            — cc_library combining everything

    Args:
      name:       Name for the generated cc_library target.
      proto:      Label of a proto_library target.
      deps:       Additional cc_library deps.
      visibility: Bazel visibility list.
      **kwargs:   Extra arguments forwarded to cc_library.
    """
    cc_proto_name = name + "_cc_proto"
    gen_name      = name + "_gen"
    gen_hdrs_name = gen_name + "_hdrs"
    gen_srcs_name = gen_name + "_srcs"

    cc_proto_library(
        name = cc_proto_name,
        deps = [proto],
        visibility = ["//visibility:private"],
    )

    _asio_grpc_generate(
        name = gen_name,
        proto = proto,
        visibility = ["//visibility:private"],
    )

    # Separate header and source output groups so cc_library gets the right
    # files in hdrs (public) vs srcs (compiled, not re-exported).
    native.filegroup(
        name = gen_hdrs_name,
        srcs = [":" + gen_name],
        output_group = "hdrs",
        visibility = ["//visibility:private"],
    )

    native.filegroup(
        name = gen_srcs_name,
        srcs = [":" + gen_name],
        output_group = "srcs",
        visibility = ["//visibility:private"],
    )

    native.cc_library(
        name = name,
        srcs = [":" + gen_srcs_name],
        hdrs = [":" + gen_hdrs_name],
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
