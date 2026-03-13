const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Feature flags for cherry-picking layers
    const enable_layer1 = b.option(bool, "layer1", "Enable Layer 1: HAL networking (std.Io based)") orelse true;
    const enable_layer2 = b.option(bool, "layer2", "Enable Layer 2: Protocol servers") orelse true;
    const enable_layer3 = b.option(bool, "layer3", "Enable Layer 3: Application framework") orelse true;
    const enable_nucleus = b.option(bool, "nucleus", "Enable Nucleus multi-model client") orelse true;
    const enable_tls = b.option(bool, "tls", "Enable TLS support (BearSSL)") orelse false;

    // Build options module
    const options = b.addOptions();
    options.addOption(bool, "enable_layer1", enable_layer1);
    options.addOption(bool, "enable_layer2", enable_layer2);
    options.addOption(bool, "enable_layer3", enable_layer3);
    options.addOption(bool, "enable_nucleus", enable_nucleus);
    options.addOption(bool, "enable_tls", enable_tls);

    // Root module used by both library and tests
    const root_mod = b.createModule(.{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    root_mod.addOptions("build_options", options);

    // Library artifact
    const lib = b.addLibrary(.{
        .name = "neutron",
        .root_module = root_mod,
    });
    b.installArtifact(lib);

    // Tests
    const test_step = b.step("test", "Run unit tests");

    const test_mod = b.createModule(.{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_mod.addOptions("build_options", options);

    const unit_tests = b.addTest(.{
        .root_module = test_mod,
    });

    const run_tests = b.addRunArtifact(unit_tests);
    test_step.dependOn(&run_tests.step);
}
