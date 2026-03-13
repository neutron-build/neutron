// Layer 1: Connection pool — pre-allocated, fixed-size
//
// All connection slots are allocated at init. No runtime allocation.
// Thread-safe via mutex. Supports health checks.

const std = @import("std");

pub fn ConnectionPool(comptime Conn: type, comptime max_size: comptime_int) type {
    return struct {
        const Self = @This();

        const Slot = struct {
            conn: Conn,
            in_use: bool,
            healthy: bool,
        };

        slots: [max_size]Slot,
        mutex: std.Thread.Mutex,
        available_count: usize,

        /// Initialize the pool. All slots start as unused.
        pub fn init() Self {
            var self: Self = undefined;
            self.mutex = .{};
            self.available_count = max_size;
            for (&self.slots) |*slot| {
                slot.in_use = false;
                slot.healthy = true;
                slot.conn = std.mem.zeroes(Conn);
            }
            return self;
        }

        /// Acquire a connection from the pool.
        /// Returns null if no connections are available.
        pub fn acquire(self: *Self) ?*Conn {
            self.mutex.lock();
            defer self.mutex.unlock();

            for (&self.slots) |*slot| {
                if (!slot.in_use and slot.healthy) {
                    slot.in_use = true;
                    self.available_count -= 1;
                    return &slot.conn;
                }
            }
            return null;
        }

        /// Release a connection back to the pool.
        pub fn release(self: *Self, conn: *Conn) void {
            self.mutex.lock();
            defer self.mutex.unlock();

            for (&self.slots) |*slot| {
                if (&slot.conn == conn) {
                    slot.in_use = false;
                    self.available_count += 1;
                    return;
                }
            }
        }

        /// Mark a connection as unhealthy (will not be acquired).
        pub fn markUnhealthy(self: *Self, conn: *Conn) void {
            self.mutex.lock();
            defer self.mutex.unlock();

            for (&self.slots) |*slot| {
                if (&slot.conn == conn) {
                    slot.healthy = false;
                    return;
                }
            }
        }

        /// Number of available (idle + healthy) connections.
        pub fn availableCount(self: *Self) usize {
            self.mutex.lock();
            defer self.mutex.unlock();
            return self.available_count;
        }

        /// Number of in-use connections.
        pub fn inUseCount(self: *Self) usize {
            self.mutex.lock();
            defer self.mutex.unlock();
            return max_size - self.available_count;
        }

        /// Total pool capacity.
        pub fn capacity(_: *const Self) usize {
            return max_size;
        }
    };
}

test "pool: acquire and release" {
    const TestConn = struct { id: u32 = 0 };
    var pool = ConnectionPool(TestConn, 4).init();

    const c1 = pool.acquire().?;
    c1.id = 1;
    try std.testing.expectEqual(@as(usize, 3), pool.availableCount());
    try std.testing.expectEqual(@as(usize, 1), pool.inUseCount());

    const c2 = pool.acquire().?;
    c2.id = 2;
    try std.testing.expectEqual(@as(usize, 2), pool.availableCount());

    pool.release(c1);
    try std.testing.expectEqual(@as(usize, 3), pool.availableCount());

    pool.release(c2);
    try std.testing.expectEqual(@as(usize, 4), pool.availableCount());
}

test "pool: exhaustion returns null" {
    const TestConn = struct { val: u8 = 0 };
    var pool = ConnectionPool(TestConn, 2).init();

    _ = pool.acquire().?;
    _ = pool.acquire().?;
    try std.testing.expectEqual(@as(?*@TypeOf(pool.slots[0].conn), null), pool.acquire());
}

test "pool: unhealthy connections skipped" {
    const TestConn = struct { val: u8 = 0 };
    var pool = ConnectionPool(TestConn, 2).init();

    const c1 = pool.acquire().?;
    pool.release(c1);
    pool.markUnhealthy(c1);

    // Should skip unhealthy c1 and return c2
    const c2 = pool.acquire().?;
    try std.testing.expect(c1 != c2);
}

test "pool: capacity" {
    const TestConn = struct {};
    const pool = ConnectionPool(TestConn, 8).init();
    try std.testing.expectEqual(@as(usize, 8), pool.capacity());
}
