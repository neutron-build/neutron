const std = @import("std");

// Zig conformance app for the cross-SDK contract suite.
//
// Builds against the in-repo SDK source directly (../../../zig/src), not a
// vendored copy, so the framework under test is the one the SDK ships. The
// SDK's root module needs `build_options`, so the same options module is
// constructed here with the same defaults its own build.zig uses. Mirrors
// live/executors/zig/build.zig.
//
// Toolchain: Zig 0.15.x (build.zig.zon pins 0.15.2; 0.16 cannot compile the
// SDK). The conformance runner resolves the pinned binary the same way
// live/executors/zig/run.sh does.
pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const options = b.addOptions();
    options.addOption(bool, "enable_layer1", true);
    options.addOption(bool, "enable_layer2", true);
    options.addOption(bool, "enable_layer3", true);
    options.addOption(bool, "enable_nucleus", false);
    options.addOption(bool, "enable_tls", false);

    const sdk = b.createModule(.{
        .root_source_file = b.path("../../../zig/src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    sdk.addOptions("build_options", options);

    const exe_mod = b.createModule(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });
    exe_mod.addImport("neutron", sdk);

    const exe = b.addExecutable(.{
        .name = "conformance-app",
        .root_module = exe_mod,
    });
    b.installArtifact(exe);
}
