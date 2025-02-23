load("//scala:providers.bzl", "declare_deps_provider")
load(
    "//scala_proto/default:default_deps.bzl",
    "DEFAULT_SCALAPB_COMPILE_DEPS",
    "DEFAULT_SCALAPB_GRPC_DEPS",
    "DEFAULT_SCALAPB_WORKER_DEPS",
)
load("//scala_proto/private:toolchain_deps.bzl", "export_scalapb_toolchain_deps")
load(
    "//scala_proto:scala_proto_toolchain.bzl",
    "scala_proto_deps_toolchain",
    "scala_proto_toolchain",
)

toolchain_type(
    name = "toolchain_type",
    visibility = ["//visibility:public"],
)

toolchain_type(
    name = "deps_toolchain_type",
    visibility = ["//visibility:public"],
)

scala_proto_deps_toolchain(
    name = "default_deps_toolchain_impl",
    visibility = ["//visibility:public"],
)

scala_proto_toolchain(
    name = "default_toolchain_impl",
    visibility = ["//visibility:public"],
    with_flat_package = False,
    with_grpc = True,
    with_single_line_to_string = False,
)

toolchain(
    name = "default_toolchain",
    toolchain = ":default_toolchain_impl",
    toolchain_type = "//scala_proto:toolchain_type",
    visibility = ["//visibility:public"],
)

toolchain(
    name = "default_deps_toolchain",
    toolchain = ":default_deps_toolchain_impl",
    toolchain_type = ":deps_toolchain_type",
)

scala_proto_toolchain(
    name = "enable_all_options_toolchain_impl",
    visibility = ["//visibility:public"],
    with_flat_package = True,
    with_grpc = True,
    with_single_line_to_string = True,
)

toolchain(
    name = "enable_all_options_toolchain",
    toolchain = ":enable_all_options_toolchain_impl",
    toolchain_type = "//scala_proto:toolchain_type",
    visibility = ["//visibility:public"],
)

declare_deps_provider(
    name = "scalapb_compile_deps_provider",
    deps_id = "scalapb_compile_deps",
    visibility = ["//visibility:public"],
    deps = DEFAULT_SCALAPB_COMPILE_DEPS,
)

declare_deps_provider(
    name = "scalapb_grpc_deps_provider",
    deps_id = "scalapb_grpc_deps",
    visibility = ["//visibility:public"],
    deps = DEFAULT_SCALAPB_GRPC_DEPS,
)

declare_deps_provider(
    name = "scalapb_worker_deps_provider",
    deps_id = "scalapb_worker_deps",
    visibility = ["//visibility:public"],
    deps = DEFAULT_SCALAPB_WORKER_DEPS,
)

export_scalapb_toolchain_deps(
    name = "scalapb_worker_deps",
    deps_id = "scalapb_worker_deps",
    visibility = ["//visibility:public"],
)
