const std = @import("std");

// Zig executor for the Nucleus live conformance spec.
//
// Builds against the in-repo SDK source directly (../../../../zig/src), not a
// vendored copy, so the client under test is the one the SDK ships. The SDK's
// root module needs `build_options`, so the same options module is constructed
// here with the same defaults its own build.zig uses.
pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const options = b.addOptions();
    options.addOption(bool, "enable_layer1", true);
    options.addOption(bool, "enable_layer2", true);
    options.addOption(bool, "enable_layer3", true);
    options.addOption(bool, "enable_nucleus", true);
    options.addOption(bool, "enable_tls", false);

    const sdk = b.createModule(.{
        .root_source_file = b.path("../../../../zig/src/root.zig"),
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
        .name = "run-live",
        .root_module = exe_mod,
    });
    b.installArtifact(exe);

    const run = b.addRunArtifact(exe);
    run.step.dependOn(b.getInstallStep());
    if (b.args) |args| run.addArgs(args);

    const run_step = b.step("run", "Run the live conformance spec");
    run_step.dependOn(&run.step);
}
