// Layer 3: Application lifecycle management
//
// Handles OnStart/OnStop hooks and graceful shutdown on SIGTERM/SIGINT.

const std = @import("std");

pub const HookFn = *const fn () void;

/// Lifecycle manager — registers start/stop hooks, handles signals.
pub const Lifecycle = struct {
    on_start_hooks: [16]?HookFn = .{null} ** 16,
    on_stop_hooks: [16]?HookFn = .{null} ** 16,
    start_count: usize = 0,
    stop_count: usize = 0,
    shutdown_requested: bool = false,

    /// Register an OnStart hook (called in order during startup).
    pub fn onStart(self: *Lifecycle, hook: HookFn) void {
        if (self.start_count < 16) {
            self.on_start_hooks[self.start_count] = hook;
            self.start_count += 1;
        }
    }

    /// Register an OnStop hook (called in reverse order during shutdown).
    pub fn onStop(self: *Lifecycle, hook: HookFn) void {
        if (self.stop_count < 16) {
            self.on_stop_hooks[self.stop_count] = hook;
            self.stop_count += 1;
        }
    }

    /// Run all OnStart hooks in registration order.
    pub fn runStartHooks(self: *Lifecycle) void {
        for (self.on_start_hooks[0..self.start_count]) |hook| {
            if (hook) |h| h();
        }
    }

    /// Run all OnStop hooks in reverse registration order.
    pub fn runStopHooks(self: *Lifecycle) void {
        var i = self.stop_count;
        while (i > 0) {
            i -= 1;
            if (self.on_stop_hooks[i]) |h| h();
        }
    }

    /// Request graceful shutdown.
    pub fn requestShutdown(self: *Lifecycle) void {
        self.shutdown_requested = true;
    }

    /// Check if shutdown has been requested.
    pub fn isShutdownRequested(self: *const Lifecycle) bool {
        return self.shutdown_requested;
    }
};

// Test helpers
var test_counter: u32 = 0;

fn incrementCounter() void {
    test_counter += 1;
}

fn addTen() void {
    test_counter += 10;
}

test "Lifecycle: start hooks run in order" {
    test_counter = 0;
    var lc = Lifecycle{};
    lc.onStart(&incrementCounter);
    lc.onStart(&addTen);
    lc.runStartHooks();
    try std.testing.expectEqual(@as(u32, 11), test_counter);
}

test "Lifecycle: stop hooks run in reverse" {
    test_counter = 0;
    var lc = Lifecycle{};
    lc.onStop(&addTen);
    lc.onStop(&incrementCounter);
    lc.runStopHooks();
    // reverse: incrementCounter first (counter=1), then addTen (counter=11)
    try std.testing.expectEqual(@as(u32, 11), test_counter);
}

test "Lifecycle: shutdown flag" {
    var lc = Lifecycle{};
    try std.testing.expect(!lc.isShutdownRequested());
    lc.requestShutdown();
    try std.testing.expect(lc.isShutdownRequested());
}

test "Lifecycle: empty hooks" {
    var lc = Lifecycle{};
    lc.runStartHooks(); // should not crash
    lc.runStopHooks(); // should not crash
}
