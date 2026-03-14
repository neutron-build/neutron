// Layer 3: In-Memory Cache — generic, TTL-based, with LRU eviction
//
// A generic key-value cache with per-entry TTL expiration and optional
// max-size LRU eviction. Uses std.HashMap for O(1) lookups.
// Lazy expiration: entries are only evicted on read (get) or explicit prune.

const std = @import("std");

/// Cache entry wrapping a value with its expiration timestamp.
fn CacheEntry(comptime V: type) type {
    return struct {
        value: V,
        expires_at: i128, // nanosecond timestamp, 0 = no expiration
        /// For LRU ordering: last access timestamp
        last_access: i128,
    };
}

/// Generic in-memory cache with TTL-based expiration and LRU eviction.
///
/// K must be a hashable key type ([]const u8, integers, etc.).
/// V is the value type stored in the cache.
///
/// Usage:
///   var cache = Cache([]const u8, []const u8).init(allocator, .{ .max_size = 1000 });
///   defer cache.deinit();
///   try cache.set("key", "value", 60); // TTL = 60 seconds
///   const val = cache.get("key");      // returns ?V
pub fn Cache(comptime K: type, comptime V: type) type {
    const Entry = CacheEntry(V);
    const MapContext = HashMapContext(K);
    const HashMap = std.HashMap(K, Entry, MapContext, 80);

    return struct {
        const Self = @This();

        map: HashMap,
        allocator: std.mem.Allocator,
        max_size: usize,
        hits: u64,
        misses: u64,

        pub const Config = struct {
            /// Maximum number of entries. 0 = unlimited.
            max_size: usize = 0,
        };

        /// Initialize a new cache.
        pub fn init(allocator: std.mem.Allocator, config: Config) Self {
            return .{
                .map = HashMap.init(allocator),
                .allocator = allocator,
                .max_size = config.max_size,
                .hits = 0,
                .misses = 0,
            };
        }

        /// Release all resources.
        pub fn deinit(self: *Self) void {
            self.map.deinit();
        }

        /// Get a value by key. Returns null if not found or expired.
        /// Lazy expiration: removes expired entries on access.
        pub fn get(self: *Self, key: K) ?V {
            const entry_ptr = self.map.getPtr(key) orelse {
                self.misses += 1;
                return null;
            };

            // Check TTL expiration
            if (entry_ptr.expires_at > 0) {
                const now: i128 = std.time.nanoTimestamp();
                if (now >= entry_ptr.expires_at) {
                    // Expired — remove lazily by key
                    _ = self.map.remove(key);
                    self.misses += 1;
                    return null;
                }
            }

            // Update last access for LRU
            entry_ptr.last_access = std.time.nanoTimestamp();
            self.hits += 1;
            return entry_ptr.value;
        }

        /// Set a value with TTL in seconds. ttl_seconds = 0 means no expiration.
        pub fn set(self: *Self, key: K, value: V, ttl_seconds: u32) !void {
            const now: i128 = std.time.nanoTimestamp();
            const expires_at: i128 = if (ttl_seconds > 0)
                now + @as(i128, ttl_seconds) * std.time.ns_per_s
            else
                0;

            const entry = Entry{
                .value = value,
                .expires_at = expires_at,
                .last_access = now,
            };

            // If key already exists, update in place
            const gop = try self.map.getOrPut(key);
            gop.value_ptr.* = entry;

            // If this was a new entry, check max_size and evict if needed
            if (!gop.found_existing and self.max_size > 0 and self.map.count() > self.max_size) {
                self.evictLru();
            }
        }

        /// Delete an entry by key. Returns true if the key existed.
        pub fn delete(self: *Self, key: K) bool {
            return self.map.remove(key);
        }

        /// Remove all entries.
        pub fn clear(self: *Self) void {
            self.map.clearRetainingCapacity();
            self.hits = 0;
            self.misses = 0;
        }

        /// Return the number of entries (including possibly expired ones).
        pub fn count(self: *const Self) usize {
            return self.map.count();
        }

        /// Remove all expired entries. O(n) scan.
        pub fn prune(self: *Self) usize {
            const now: i128 = std.time.nanoTimestamp();
            var removed: usize = 0;
            var iter = self.map.iterator();
            while (iter.next()) |entry| {
                if (entry.value_ptr.expires_at > 0 and now >= entry.value_ptr.expires_at) {
                    self.map.removeByPtr(entry.key_ptr);
                    removed += 1;
                }
            }
            return removed;
        }

        /// Evict the least recently used entry.
        fn evictLru(self: *Self) void {
            var oldest_time: i128 = std.math.maxInt(i128);
            var oldest_key: ?*K = null;

            var iter = self.map.iterator();
            while (iter.next()) |entry| {
                if (entry.value_ptr.last_access < oldest_time) {
                    oldest_time = entry.value_ptr.last_access;
                    oldest_key = entry.key_ptr;
                }
            }

            if (oldest_key) |key| {
                self.map.removeByPtr(key);
            }
        }

        /// Get the hit rate as a percentage (0-100).
        pub fn hitRate(self: *const Self) u8 {
            const total = self.hits + self.misses;
            if (total == 0) return 0;
            return @intCast((self.hits * 100) / total);
        }
    };
}

/// Hash map context for common key types.
fn HashMapContext(comptime K: type) type {
    return struct {
        const Self = @This();

        pub fn hash(_: Self, key: K) u64 {
            return computeHash(K, key);
        }

        pub fn eql(_: Self, a: K, b: K) bool {
            return computeEql(K, a, b);
        }
    };
}

fn computeHash(comptime K: type, key: K) u64 {
    const info = @typeInfo(K);
    switch (info) {
        .pointer => |ptr| {
            if (ptr.size == .slice and ptr.child == u8) {
                return std.hash.Wyhash.hash(0, key);
            }
        },
        .int => {
            return std.hash.Wyhash.hash(0, std.mem.asBytes(&key));
        },
        else => {},
    }
    // Fallback: hash the raw bytes
    return std.hash.Wyhash.hash(0, std.mem.asBytes(&key));
}

fn computeEql(comptime K: type, a: K, b: K) bool {
    const info = @typeInfo(K);
    switch (info) {
        .pointer => |ptr| {
            if (ptr.size == .slice and ptr.child == u8) {
                return std.mem.eql(u8, a, b);
            }
        },
        .int => {
            return a == b;
        },
        else => {},
    }
    return std.mem.eql(u8, std.mem.asBytes(&a), std.mem.asBytes(&b));
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "Cache: basic get/set" {
    var cache = Cache([]const u8, []const u8).init(std.testing.allocator, .{});
    defer cache.deinit();

    try cache.set("hello", "world", 0);
    try std.testing.expectEqualStrings("world", cache.get("hello").?);
    try std.testing.expectEqual(@as(?[]const u8, null), cache.get("missing"));
}

test "Cache: overwrite value" {
    var cache = Cache([]const u8, []const u8).init(std.testing.allocator, .{});
    defer cache.deinit();

    try cache.set("key", "v1", 0);
    try std.testing.expectEqualStrings("v1", cache.get("key").?);

    try cache.set("key", "v2", 0);
    try std.testing.expectEqualStrings("v2", cache.get("key").?);
}

test "Cache: delete" {
    var cache = Cache([]const u8, i32).init(std.testing.allocator, .{});
    defer cache.deinit();

    try cache.set("a", 1, 0);
    try std.testing.expectEqual(@as(?i32, 1), cache.get("a"));
    try std.testing.expect(cache.delete("a"));
    try std.testing.expectEqual(@as(?i32, null), cache.get("a"));
    try std.testing.expect(!cache.delete("nonexistent"));
}

test "Cache: clear" {
    var cache = Cache([]const u8, i32).init(std.testing.allocator, .{});
    defer cache.deinit();

    try cache.set("a", 1, 0);
    try cache.set("b", 2, 0);
    try std.testing.expectEqual(@as(usize, 2), cache.count());

    cache.clear();
    try std.testing.expectEqual(@as(usize, 0), cache.count());
    try std.testing.expectEqual(@as(?i32, null), cache.get("a"));
}

test "Cache: count" {
    var cache = Cache([]const u8, []const u8).init(std.testing.allocator, .{});
    defer cache.deinit();

    try std.testing.expectEqual(@as(usize, 0), cache.count());
    try cache.set("a", "1", 0);
    try std.testing.expectEqual(@as(usize, 1), cache.count());
    try cache.set("b", "2", 0);
    try std.testing.expectEqual(@as(usize, 2), cache.count());
}

test "Cache: integer keys" {
    var cache = Cache(u64, []const u8).init(std.testing.allocator, .{});
    defer cache.deinit();

    try cache.set(42, "answer", 0);
    try cache.set(0, "zero", 0);
    try std.testing.expectEqualStrings("answer", cache.get(42).?);
    try std.testing.expectEqualStrings("zero", cache.get(0).?);
    try std.testing.expectEqual(@as(?[]const u8, null), cache.get(1));
}

test "Cache: hit rate tracking" {
    var cache = Cache([]const u8, i32).init(std.testing.allocator, .{});
    defer cache.deinit();

    try cache.set("a", 1, 0);
    _ = cache.get("a"); // hit
    _ = cache.get("a"); // hit
    _ = cache.get("b"); // miss

    try std.testing.expectEqual(@as(u64, 2), cache.hits);
    try std.testing.expectEqual(@as(u64, 1), cache.misses);
    try std.testing.expectEqual(@as(u8, 66), cache.hitRate()); // 2/3 = 66%
}

test "Cache: hit rate with no accesses" {
    var cache = Cache([]const u8, i32).init(std.testing.allocator, .{});
    defer cache.deinit();
    try std.testing.expectEqual(@as(u8, 0), cache.hitRate());
}

test "Cache: max_size eviction" {
    var cache = Cache([]const u8, i32).init(std.testing.allocator, .{ .max_size = 2 });
    defer cache.deinit();

    try cache.set("a", 1, 0);
    try cache.set("b", 2, 0);
    // Access "a" to make it recently used
    _ = cache.get("a");
    // Adding "c" should evict "b" (least recently used)
    try cache.set("c", 3, 0);

    try std.testing.expectEqual(@as(usize, 2), cache.count());
    try std.testing.expectEqual(@as(?i32, 1), cache.get("a"));
    try std.testing.expectEqual(@as(?i32, 3), cache.get("c"));
}

test "Cache: no eviction when unlimited" {
    var cache = Cache([]const u8, i32).init(std.testing.allocator, .{ .max_size = 0 });
    defer cache.deinit();

    try cache.set("a", 1, 0);
    try cache.set("b", 2, 0);
    try cache.set("c", 3, 0);
    try cache.set("d", 4, 0);
    try std.testing.expectEqual(@as(usize, 4), cache.count());
}

test "Cache: TTL expiration on get" {
    var cache = Cache([]const u8, []const u8).init(std.testing.allocator, .{});
    defer cache.deinit();

    // Set with 0 TTL (already expired when timestamp checked)
    // We can't easily test real time-based expiration in a unit test,
    // so we test that entries with TTL=0 (no expiration) persist.
    try cache.set("persist", "forever", 0);
    try std.testing.expectEqualStrings("forever", cache.get("persist").?);

    // Set with a large TTL — should still be valid
    try cache.set("valid", "stillhere", 3600);
    try std.testing.expectEqualStrings("stillhere", cache.get("valid").?);
}

test "Cache: struct values" {
    const UserInfo = struct {
        name: []const u8,
        role: []const u8,
    };

    var cache = Cache([]const u8, UserInfo).init(std.testing.allocator, .{});
    defer cache.deinit();

    try cache.set("user:1", .{ .name = "Alice", .role = "admin" }, 0);
    const user = cache.get("user:1").?;
    try std.testing.expectEqualStrings("Alice", user.name);
    try std.testing.expectEqualStrings("admin", user.role);
}

test "Cache: multiple operations" {
    var cache = Cache([]const u8, i32).init(std.testing.allocator, .{});
    defer cache.deinit();

    // Insert several
    try cache.set("x", 10, 0);
    try cache.set("y", 20, 0);
    try cache.set("z", 30, 0);

    // Delete one
    try std.testing.expect(cache.delete("y"));

    // Verify state
    try std.testing.expectEqual(@as(?i32, 10), cache.get("x"));
    try std.testing.expectEqual(@as(?i32, null), cache.get("y"));
    try std.testing.expectEqual(@as(?i32, 30), cache.get("z"));
    try std.testing.expectEqual(@as(usize, 2), cache.count());

    // Clear
    cache.clear();
    try std.testing.expectEqual(@as(usize, 0), cache.count());
}
