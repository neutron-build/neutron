//! Zig executor for the Nucleus live data-model conformance spec.
//!
//! Reads ../../spec.json, runs every case against a live engine through the
//! real in-repo Zig client (`zig/src/nucleus`), and prints one JSON result
//! document to stdout. It asserts nothing a mock could assert: only that a call
//! reaches the engine, is accepted over the wire, and comes back with the right
//! value.
//!
//!     NEUTRON_TEST_DATABASE_URL=postgresql://postgres@127.0.0.1:55432/postgres \
//!         zig build run
//!
//! Exit codes: 0 all cases behaved as the spec says, 1 otherwise. An `xfail`
//! case that PASSES is a failure — otherwise a fix lands and the note
//! explaining why the case is expected to fail quietly becomes a lie.
//!
//! Everything on stdout is the report. Diagnostics go to stderr, because the
//! orchestrator parses stdout.
//!
//! One structural difference from the other executors, worth stating because it
//! shapes every arm below: the Zig client's model methods return `!?[]const u8`
//! — an optional string — rather than typed values. The engine's JSON is parsed
//! HERE when the spec expects structure. That is not the executor doing the
//! SDK's job: the SDK's contract is deliberately "hand back what the engine
//! said" so it can stay zero-allocation, and the parsing a caller would have to
//! do is exactly what is measured.

const std = @import("std");
const neutron = @import("neutron");

const NucleusClient = neutron.nucleus.NucleusClient;

/// The instant the spec's time-series millisecond offsets are measured from:
/// 2026-08-11T12:00:00Z. Fixed so the cases are deterministic and comparable
/// across SDKs.
const TS_BASE_MS: i64 = 1786795200000;

const Status = enum {
    pass,
    fail,
    xfail,
    xpass,
    unsupported,

    fn str(self: Status) []const u8 {
        return switch (self) {
            .pass => "pass",
            .fail => "fail",
            .xfail => "xfail",
            .xpass => "xpass",
            .unsupported => "unsupported",
        };
    }
};

const StepError = error{
    Unsupported,
    Failed,
};

/// Detail for the most recent failure, kept out of the error union because Zig
/// errors carry no payload.
var fail_detail: std.ArrayList(u8) = undefined;
var gpa_alloc: std.mem.Allocator = undefined;
/// Set once in main; the reconnect case opens a second client with it.
var reconnect_url: []const u8 = "";

fn failWith(comptime fmt: []const u8, args: anytype) StepError {
    fail_detail.clearRetainingCapacity();
    fail_detail.writer(gpa_alloc).print(fmt, args) catch {};
    return StepError.Failed;
}

// ── argument helpers ─────────────────────────────────────────────────────────

fn argStr(args: []const std.json.Value, i: usize) ![]const u8 {
    if (i >= args.len) return failWith("arg {d} is missing", .{i});
    return switch (args[i]) {
        .string => |s| s,
        else => failWith("arg {d} must be a string", .{i}),
    };
}

fn argInt(args: []const std.json.Value, i: usize) !i64 {
    if (i >= args.len) return failWith("arg {d} is missing", .{i});
    return switch (args[i]) {
        .integer => |n| n,
        .float => |f| @intFromFloat(f),
        .string => |s| std.fmt.parseInt(i64, s, 10) catch
            failWith("arg {d} is not an integer", .{i}),
        else => failWith("arg {d} must be an integer", .{i}),
    };
}

fn argFloat(args: []const std.json.Value, i: usize) !f64 {
    if (i >= args.len) return failWith("arg {d} is missing", .{i});
    return switch (args[i]) {
        .float => |f| f,
        .integer => |n| @floatFromInt(n),
        else => failWith("arg {d} must be a number", .{i}),
    };
}

/// Render a JSON value back to its wire text — the Zig client takes SQL
/// fragments as strings, so a document or filter argument crosses as JSON text.
fn argJson(alloc: std.mem.Allocator, args: []const std.json.Value, i: usize) ![]const u8 {
    if (i >= args.len) return failWith("arg {d} is missing", .{i});
    var out: std.ArrayList(u8) = .empty;
    std.json.Stringify.value(args[i], .{}, out.writer(alloc)) catch
        return failWith("arg {d} could not be re-encoded", .{i});
    return out.items;
}

/// The spec passes vectors as JSON arrays of numbers; the client takes the
/// text form.
fn argVector(alloc: std.mem.Allocator, args: []const std.json.Value, i: usize) ![]const u8 {
    return argJson(alloc, args, i);
}

// ── expectation checking ─────────────────────────────────────────────────────

/// A step result. `null` is SQL NULL / absent; `text` is whatever the engine
/// returned; `parsed` is that text decoded when the spec asks for structure.
const Result = union(enum) {
    none,
    text: []const u8,
    parsed: std.json.Value,
};

fn resultToJson(alloc: std.mem.Allocator, r: Result) !std.json.Value {
    return switch (r) {
        .none => .null,
        .parsed => |v| v,
        .text => |s| blk: {
            // Numbers and booleans arrive as text over this client; decode them
            // so `equals: 1` compares against 1 rather than "1". A value that
            // is not JSON stays a string, which is the common case.
            const trimmed = std.mem.trim(u8, s, " \t\r\n");
            if (trimmed.len == 0) break :blk std.json.Value{ .string = s };
            const parsed = std.json.parseFromSlice(std.json.Value, alloc, trimmed, .{}) catch
                break :blk std.json.Value{ .string = s };
            break :blk parsed.value;
        },
    };
}

fn truthy(v: std.json.Value) bool {
    return switch (v) {
        .null => false,
        .bool => |b| b,
        .integer => |n| n != 0,
        .float => |f| f != 0,
        .string => |s| s.len > 0,
        .array => |a| a.items.len > 0,
        .object => |o| o.count() > 0,
        .number_string => |s| s.len > 0,
    };
}

fn lengthOf(v: std.json.Value) !usize {
    return switch (v) {
        .array => |a| a.items.len,
        .object => |o| o.count(),
        .string => |s| s.len,
        else => failWith("expected a collection, got {s}", .{@tagName(v)}),
    };
}

fn jsonEqual(a: std.json.Value, b: std.json.Value) bool {
    return switch (a) {
        .null => b == .null,
        .bool => |x| b == .bool and b.bool == x,
        .integer => |x| switch (b) {
            .integer => |y| x == y,
            .float => |y| @abs(@as(f64, @floatFromInt(x)) - y) < 1e-9,
            else => false,
        },
        .float => |x| switch (b) {
            .float => |y| @abs(x - y) < 1e-9,
            .integer => |y| @abs(x - @as(f64, @floatFromInt(y))) < 1e-9,
            else => false,
        },
        .string => |x| b == .string and std.mem.eql(u8, x, b.string),
        .array => |x| blk: {
            if (b != .array or b.array.items.len != x.items.len) break :blk false;
            for (x.items, b.array.items) |p, q| {
                if (!jsonEqual(p, q)) break :blk false;
            }
            break :blk true;
        },
        .object => |x| blk: {
            if (b != .object or b.object.count() != x.count()) break :blk false;
            var it = x.iterator();
            while (it.next()) |e| {
                const other = b.object.get(e.key_ptr.*) orelse break :blk false;
                if (!jsonEqual(e.value_ptr.*, other)) break :blk false;
            }
            break :blk true;
        },
        .number_string => |x| b == .number_string and std.mem.eql(u8, x, b.number_string),
    };
}

fn typeMatches(v: std.json.Value, want: []const u8) bool {
    if (std.mem.eql(u8, want, "list")) return v == .array;
    if (std.mem.eql(u8, want, "map")) return v == .object;
    if (std.mem.eql(u8, want, "string")) return v == .string;
    if (std.mem.eql(u8, want, "int")) return v == .integer;
    if (std.mem.eql(u8, want, "float")) return v == .float or v == .integer;
    if (std.mem.eql(u8, want, "bool")) return v == .bool;
    if (std.mem.eql(u8, want, "bytes")) return v == .string or v == .array;
    return false;
}

fn check(alloc: std.mem.Allocator, result: Result, expect: std.json.ObjectMap) !void {
    var actual = try resultToJson(alloc, result);

    if (expect.get("key")) |k| {
        if (actual != .object) return failWith("expected a map with key {s}", .{k.string});
        actual = actual.object.get(k.string) orelse
            return failWith("key {s} is absent", .{k.string});
    }

    if (expect.get("index")) |i| {
        if (actual != .array) return failWith("expected a list to index", .{});
        const idx: usize = @intCast(i.integer);
        if (idx >= actual.array.items.len)
            return failWith("index {d} out of range for {d}", .{ idx, actual.array.items.len });
        actual = actual.array.items[idx];
    }

    if (expect.get("jsonDecode")) |d| {
        if (d == .bool and d.bool and actual == .string) {
            const parsed = std.json.parseFromSlice(std.json.Value, alloc, actual.string, .{}) catch
                return failWith("jsonDecode failed", .{});
            actual = parsed.value;
        }
    }

    if (expect.get("notNull")) |n| {
        if (n == .bool and n.bool and actual == .null) return failWith("expected a value, got null", .{});
    }
    if (expect.get("isNull")) |n| {
        if (n == .bool and n.bool and actual != .null)
            return failWith("expected null, got {s}", .{@tagName(actual)});
    }
    if (expect.get("nonEmpty")) |n| {
        if (n == .bool and n.bool and !truthy(actual))
            return failWith("expected a non-empty collection", .{});
    }
    if (expect.get("length")) |want| {
        const n = try lengthOf(actual);
        if (@as(i64, @intCast(n)) != want.integer)
            return failWith("expected {d} elements, got {d}", .{ want.integer, n });
    }
    if (expect.get("type")) |want| {
        if (!typeMatches(actual, want.string))
            return failWith("expected {s}, got {s}", .{ want.string, @tagName(actual) });
    }
    if (expect.get("equals")) |want| {
        if (!jsonEqual(actual, want))
            return failWith("expected {s}, got {s}", .{ @tagName(want), @tagName(actual) });
    }
}

// ── fixtures ─────────────────────────────────────────────────────────────────

const Fixtures = struct {
    alloc: std.mem.Allocator,
    map: std.StringHashMap([]const u8),
    seed: u64,

    fn init(alloc: std.mem.Allocator, seed: u64) Fixtures {
        return .{ .alloc = alloc, .map = std.StringHashMap([]const u8).init(alloc), .seed = seed };
    }

    /// "@name" is a per-case unique fixture, stable within a case and unique
    /// across runs.
    fn expand(self: *Fixtures, s: []const u8) ![]const u8 {
        if (std.mem.indexOfScalar(u8, s, '@') == null) return s;
        var out: std.ArrayList(u8) = .empty;
        var i: usize = 0;
        while (i < s.len) {
            if (s[i] == '@') {
                var e = i + 1;
                while (e < s.len and (std.ascii.isAlphanumeric(s[e]) or s[e] == '_')) e += 1;
                if (e > i + 1) {
                    const name = s[i + 1 .. e];
                    const got = self.map.get(name) orelse blk: {
                        const v = try std.fmt.allocPrint(self.alloc, "{s}_{x}", .{ name, self.seed });
                        try self.map.put(try self.alloc.dupe(u8, name), v);
                        break :blk v;
                    };
                    try out.appendSlice(self.alloc, got);
                    i = e;
                    continue;
                }
            }
            try out.append(self.alloc, s[i]);
            i += 1;
        }
        return out.items;
    }
};

fn resolve(
    alloc: std.mem.Allocator,
    v: std.json.Value,
    fx: *Fixtures,
    bound: *std.StringHashMap(std.json.Value),
) !std.json.Value {
    switch (v) {
        .string => |s| {
            if (s.len > 0 and s[0] == '$') {
                return bound.get(s[1..]) orelse
                    failWith("step references {s} before it was bound", .{s});
            }
            return std.json.Value{ .string = try fx.expand(s) };
        },
        .array => |a| {
            var out = try std.ArrayList(std.json.Value).initCapacity(alloc, a.items.len);
            for (a.items) |item| out.appendAssumeCapacity(try resolve(alloc, item, fx, bound));
            return std.json.Value{ .array = .{ .items = out.items, .capacity = out.capacity, .allocator = alloc } };
        },
        .object => |o| {
            var out = std.json.ObjectMap.init(alloc);
            var it = o.iterator();
            while (it.next()) |e| try out.put(e.key_ptr.*, try resolve(alloc, e.value_ptr.*, fx, bound));
            return std.json.Value{ .object = out };
        },
        else => return v,
    }
}

// ── dispatch ─────────────────────────────────────────────────────────────────

fn opt(v: ?[]const u8) Result {
    return if (v) |s| .{ .text = s } else .none;
}

fn call(
    alloc: std.mem.Allocator,
    client: *NucleusClient,
    op: []const u8,
    args: []const std.json.Value,
) !Result {
    const eq = std.mem.eql;

    // ── core ──
    if (eq(u8, op, "features.isNucleus")) {
        return .{ .parsed = .{ .bool = client.isNucleus() } };
    }
    if (eq(u8, op, "connection.closeAndReconnect")) {
        // A second client opened, used and closed. The Terminate defect this
        // case exists for surfaced as the NEXT connect hanging, not as an
        // error on close.
        var probe = try NucleusClient.fromUrl(alloc, reconnect_url);
        try probe.connect();
        _ = try probe.query("SELECT 1");
        probe.close();
        return .{ .parsed = .{ .bool = true } };
    }

    // ── sql ──
    if (eq(u8, op, "sql.queryScalar")) {
        // The spec's parameters are interpolated into the statement text: this
        // client has no bind path at all, only comptime-checked SQL strings.
        // That is a real difference from every other SDK and it is why the
        // binary-parameter cases prove less here than elsewhere.
        const sql = try interpolate(alloc, try argStr(args, 0), args, 1);
        return opt(try client.execute(sql));
    }
    if (eq(u8, op, "sql.execute")) {
        const sql = try interpolate(alloc, try argStr(args, 0), args, 1);
        const tag = try client.exec(sql);
        return .{ .parsed = .{ .integer = rowsFromTag(tag) } };
    }
    if (eq(u8, op, "sql.begin")) {
        _ = try client.exec("BEGIN");
        return .none;
    }
    if (eq(u8, op, "sql.rollback")) {
        _ = try client.exec("ROLLBACK");
        return .none;
    }

    // ── kv ──
    var kv = client.kv();
    if (eq(u8, op, "kv.set")) {
        const ttl: i64 = if (args.len > 2) try argInt(args, 2) else 0;
        return opt(try kv.set(try argStr(args, 0), try argStr(args, 1), ttl));
    }
    if (eq(u8, op, "kv.get")) return opt(try kv.get(try argStr(args, 0)));
    if (eq(u8, op, "kv.delete")) return opt(try kv.del(try argStr(args, 0)));
    if (eq(u8, op, "kv.exists")) return opt(try kv.exists(try argStr(args, 0)));
    if (eq(u8, op, "kv.incr")) {
        const by: i64 = if (args.len > 1) try argInt(args, 1) else 1;
        return opt(try kv.incr(try argStr(args, 0), by));
    }
    if (eq(u8, op, "kv.ttl")) return opt(try kv.ttl(try argStr(args, 0)));
    if (eq(u8, op, "kv.expire")) return opt(try kv.expire(try argStr(args, 0), try argInt(args, 1)));
    if (eq(u8, op, "kv.rpush")) return opt(try kv.rpush(try argStr(args, 0), try argStr(args, 1)));
    if (eq(u8, op, "kv.llen")) return opt(try kv.llen(try argStr(args, 0)));
    if (eq(u8, op, "kv.lindex")) return opt(try kv.lindex(try argStr(args, 0), try argInt(args, 1)));
    if (eq(u8, op, "kv.lrange")) return opt(try kv.lrange(try argStr(args, 0), try argInt(args, 1), try argInt(args, 2)));
    if (eq(u8, op, "kv.hset")) return opt(try kv.hset(try argStr(args, 0), try argStr(args, 1), try argStr(args, 2)));
    if (eq(u8, op, "kv.hget")) return opt(try kv.hget(try argStr(args, 0), try argStr(args, 1)));
    if (eq(u8, op, "kv.hdel")) return opt(try kv.hdel(try argStr(args, 0), try argStr(args, 1)));
    if (eq(u8, op, "kv.hexists")) return opt(try kv.hexists(try argStr(args, 0), try argStr(args, 1)));
    if (eq(u8, op, "kv.hlen")) return opt(try kv.hlen(try argStr(args, 0)));
    if (eq(u8, op, "kv.hgetall")) return opt(try kv.hgetall(try argStr(args, 0)));
    if (eq(u8, op, "kv.sadd")) return opt(try kv.sadd(try argStr(args, 0), try argStr(args, 1)));
    if (eq(u8, op, "kv.srem")) return opt(try kv.srem(try argStr(args, 0), try argStr(args, 1)));
    if (eq(u8, op, "kv.smembers")) return opt(try kv.smembers(try argStr(args, 0)));
    if (eq(u8, op, "kv.zadd")) return opt(try kv.zadd(try argStr(args, 0), try argFloat(args, 1), try argStr(args, 2)));
    if (eq(u8, op, "kv.zrange")) return opt(try kv.zrange(try argStr(args, 0), try argInt(args, 1), try argInt(args, 2)));

    // ── document ──
    var doc = client.document();
    if (eq(u8, op, "document.insert"))
        return opt(try doc.docInsertIn(try argStr(args, 0), try argJson(alloc, args, 1)));
    if (eq(u8, op, "document.get"))
        return opt(try doc.get(@intCast(try argInt(args, 0))));
    if (eq(u8, op, "document.getIn"))
        return opt(try doc.getIn(try argStr(args, 0), @intCast(try argInt(args, 1))));
    if (eq(u8, op, "document.countIn"))
        return opt(try doc.count());
    if (eq(u8, op, "document.find"))
        return opt(try doc.docQueryIn(try argStr(args, 0), try argJson(alloc, args, 1)));
    if (eq(u8, op, "document.getPathIn")) {
        var keys = try std.ArrayList([]const u8).initCapacity(alloc, args.len - 2);
        for (args[2..]) |k| keys.appendAssumeCapacity(k.string);
        return opt(try doc.path(@intCast(try argInt(args, 1)), keys.items));
    }

    // ── vector ──
    var vec = client.vector();
    if (eq(u8, op, "vector.createCollection")) {
        var buf: [512]u8 = undefined;
        const sql = try std.fmt.bufPrint(&buf, "CREATE TABLE {s} (id TEXT PRIMARY KEY, embedding VECTOR({d}), metadata JSONB)", .{ try argStr(args, 0), try argInt(args, 1) });
        _ = try client.exec(sql);
        return .none;
    }
    if (eq(u8, op, "vector.insert"))
        return .{ .text = try vec.insert(try argStr(args, 0), try argStr(args, 1), try argVector(alloc, args, 2), "{}") };
    if (eq(u8, op, "vector.search"))
        return opt(try vec.search(try argStr(args, 0), try argVector(alloc, args, 1), @intCast(try argInt(args, 2)), "cosine"));

    // ── timeseries ──
    var ts = client.timeseries();
    if (eq(u8, op, "timeseries.write")) {
        for (args[1].array.items) |p| {
            const t = p.object.get("t").?.integer;
            const v = switch (p.object.get("v").?) {
                .float => |f| f,
                .integer => |n| @as(f64, @floatFromInt(n)),
                else => 0,
            };
            _ = try ts.tsInsert(try argStr(args, 0), TS_BASE_MS + t, v);
        }
        return .none;
    }
    if (eq(u8, op, "timeseries.count")) return opt(try ts.count(try argStr(args, 0)));
    if (eq(u8, op, "timeseries.last")) return opt(try ts.last(try argStr(args, 0)));

    // ── fts ──
    var fts = client.fts();
    if (eq(u8, op, "fts.indexDoc")) {
        // The client's FTS index is global — index(doc_id, text) has no index
        // name — so the spec's index argument is dropped and the field map is
        // flattened to the indexed text.
        var text: std.ArrayList(u8) = .empty;
        var it = args[2].object.iterator();
        while (it.next()) |e| {
            if (e.value_ptr.* == .string) {
                if (text.items.len > 0) try text.append(alloc, ' ');
                try text.appendSlice(alloc, e.value_ptr.string);
            }
        }
        return opt(try fts.ftsIndex(try argStr(args, 1), text.items));
    }
    if (eq(u8, op, "fts.search"))
        return opt(try fts.search(try argStr(args, 1), @intCast(try argInt(args, 2))));

    // ── graph ──
    var graph = client.graph();
    if (eq(u8, op, "graph.addNode"))
        return opt(try graph.addNode(args[0].array.items[0].string, try argJson(alloc, args, 1)));
    if (eq(u8, op, "graph.addEdge"))
        return opt(try graph.addEdge(try argStr(args, 1), try argStr(args, 2), try argStr(args, 0), "{}"));
    if (eq(u8, op, "graph.deleteNode")) return opt(try graph.deleteNode(try argStr(args, 0)));
    if (eq(u8, op, "graph.neighbors")) return opt(try graph.neighbors(try argStr(args, 0), "both"));
    if (eq(u8, op, "graph.shortestPath")) return opt(try graph.shortestPath(try argStr(args, 0), try argStr(args, 1)));
    if (eq(u8, op, "graph.nodeCount")) return opt(try graph.nodeCount());
    if (eq(u8, op, "graph.edgeCount")) return opt(try graph.edgeCount());

    // ── streams ──
    var streams = client.streams();
    if (eq(u8, op, "streams.xadd")) {
        var it = args[1].object.iterator();
        const first = it.next() orelse return failWith("xadd needs at least one field", .{});
        return opt(try streams.xadd(try argStr(args, 0), first.key_ptr.*, first.value_ptr.string));
    }
    if (eq(u8, op, "streams.xlen")) return opt(try streams.xlen(try argStr(args, 0)));
    if (eq(u8, op, "streams.xrange"))
        return opt(try streams.xrange(try argStr(args, 0), 0, std.math.maxInt(i64), 100));
    if (eq(u8, op, "streams.xread"))
        return opt(try streams.xread(try argStr(args, 0), 0, 100));
    if (eq(u8, op, "streams.xgroupCreate"))
        return opt(try streams.xgroupCreate(try argStr(args, 0), try argStr(args, 1), 0));
    if (eq(u8, op, "streams.xreadgroup"))
        return opt(try streams.xreadgroup(try argStr(args, 0), try argStr(args, 1), try argStr(args, 2), 100));
    if (eq(u8, op, "streams.xack"))
        return opt(try streams.xack(try argStr(args, 0), try argStr(args, 1), try argStr(args, 2)));

    // ── datalog ──
    var dl = client.datalog();
    if (eq(u8, op, "datalog.assertFact")) return opt(try dl.assertFact(try argStr(args, 0)));
    if (eq(u8, op, "datalog.query")) return opt(try dl.datalogQuery(try argStr(args, 0)));
    if (eq(u8, op, "datalog.clear")) return opt(try dl.clear(try argStr(args, 0)));

    // ── cdc ──
    var cdc = client.cdc();
    if (eq(u8, op, "cdc.read")) return opt(try cdc.cdcRead(try argInt(args, 0), try argInt(args, 1)));
    if (eq(u8, op, "cdc.count")) return opt(try cdc.count());

    // ── blob ──
    var blob = client.blob();
    if (eq(u8, op, "blob.put")) {
        const key = try scopedKey(alloc, try argStr(args, 0), try argStr(args, 1));
        const hex = try b64ToHex(alloc, try argStr(args, 2));
        return opt(try blob.store(key, hex, "application/octet-stream"));
    }
    if (eq(u8, op, "blob.get")) {
        const key = try scopedKey(alloc, try argStr(args, 0), try argStr(args, 1));
        const got = try blob.get(key);
        if (got) |hex| return .{ .parsed = .{ .string = try hexToB64(alloc, hex) } };
        return .none;
    }
    if (eq(u8, op, "blob.getMeta"))
        return opt(try blob.meta(try scopedKey(alloc, try argStr(args, 0), try argStr(args, 1))));
    if (eq(u8, op, "blob.exists")) {
        const meta = try blob.meta(try scopedKey(alloc, try argStr(args, 0), try argStr(args, 1)));
        return .{ .parsed = .{ .bool = meta != null and meta.?.len > 0 } };
    }
    if (eq(u8, op, "blob.delete"))
        return opt(try blob.delete(try scopedKey(alloc, try argStr(args, 0), try argStr(args, 1))));

    return StepError.Unsupported;
}

/// Buckets are a client-side "bucket/key" convention shared with every SDK; the
/// engine has one flat keyspace.
fn scopedKey(alloc: std.mem.Allocator, bucket: []const u8, key: []const u8) ![]const u8 {
    if (bucket.len == 0) return key;
    return std.fmt.allocPrint(alloc, "{s}/{s}", .{ bucket, key });
}

fn b64ToHex(alloc: std.mem.Allocator, b64: []const u8) ![]const u8 {
    const dec = std.base64.standard.Decoder;
    const n = dec.calcSizeForSlice(b64) catch return failWith("bad base64", .{});
    const raw = try alloc.alloc(u8, n);
    dec.decode(raw, b64) catch return failWith("bad base64", .{});
    return std.fmt.allocPrint(alloc, "{x}", .{raw});
}

fn hexToB64(alloc: std.mem.Allocator, hex: []const u8) ![]const u8 {
    if (hex.len % 2 != 0) return failWith("odd-length hex from BLOB_GET", .{});
    const raw = try alloc.alloc(u8, hex.len / 2);
    _ = std.fmt.hexToBytes(raw, hex) catch return failWith("BLOB_GET returned unparseable hex", .{});
    const enc = std.base64.standard.Encoder;
    const out = try alloc.alloc(u8, enc.calcSize(raw.len));
    return enc.encode(out, raw);
}

/// `UPDATE 3` → 3. The client returns the command tag rather than a count.
fn rowsFromTag(tag: []const u8) i64 {
    var it = std.mem.tokenizeScalar(u8, tag, ' ');
    var last: []const u8 = "";
    while (it.next()) |part| last = part;
    return std.fmt.parseInt(i64, last, 10) catch 0;
}

/// Substitute $1..$n with the spec's parameter values.
///
/// This client has no bind path — SQL is a comptime-checked string — so the
/// only way to run a parameterised case is to interpolate. Stated plainly
/// because it means the binary-parameter cases prove less here than in the
/// SDKs that bind: they exercise the engine's parser, not its parameter
/// decoding.
fn interpolate(
    alloc: std.mem.Allocator,
    sql: []const u8,
    args: []const std.json.Value,
    param_arg: usize,
) ![]const u8 {
    if (param_arg >= args.len or args[param_arg] != .array) return sql;
    const params = args[param_arg].array.items;
    if (params.len == 0) return sql;

    var out: std.ArrayList(u8) = .empty;
    var i: usize = 0;
    while (i < sql.len) {
        if (sql[i] == '$' and i + 1 < sql.len and std.ascii.isDigit(sql[i + 1])) {
            var e = i + 1;
            while (e < sql.len and std.ascii.isDigit(sql[e])) e += 1;
            const idx = std.fmt.parseInt(usize, sql[i + 1 .. e], 10) catch 0;
            if (idx >= 1 and idx <= params.len) {
                switch (params[idx - 1]) {
                    .string => |s| try out.print(alloc, "'{s}'", .{s}),
                    .integer => |n| try out.print(alloc, "{d}", .{n}),
                    .float => |f| try out.print(alloc, "{d}", .{f}),
                    .bool => |b| try out.appendSlice(alloc, if (b) "true" else "false"),
                    .null => try out.appendSlice(alloc, "NULL"),
                    else => {
                        var enc: std.ArrayList(u8) = .empty;
                        try std.json.Stringify.value(params[idx - 1], .{}, enc.writer(alloc));
                        try out.print(alloc, "'{s}'", .{enc.items});
                    },
                }
                i = e;
                continue;
            }
        }
        try out.append(alloc, sql[i]);
        i += 1;
    }
    return out.items;
}

// ── main ─────────────────────────────────────────────────────────────────────

pub fn main(init: std.process.Init) !void {
    // Zig 0.16 hands the process its environment and an Io implementation
    // through `Init` rather than exposing globals: `std.os.environ`,
    // `std.posix.getenv` and `std.fs.cwd()` are all gone. Taking `Init` is the
    // supported way to get both, and it is why this file targets 0.16 rather
    // than the 0.14 the SDK's own CI pins — 0.14 cannot link against this
    // machine's macOS SDK at all (undefined __availability_version_check).
    const alloc = init.arena.allocator();
    const io = init.io;
    gpa_alloc = alloc;
    fail_detail = .empty;

    const url = init.environ_map.get("NEUTRON_TEST_DATABASE_URL") orelse "";
    reconnect_url = url;
    if (url.len == 0) {
        std.debug.print(
            "::error::NEUTRON_TEST_DATABASE_URL is not set. This suite is only " ++
                "meaningful against a live engine; refusing to report a green run " ++
                "for zero executed cases.\n",
            .{},
        );
        std.process.exit(1);
    }

    const spec_path = "../../spec.json";
    const spec_bytes = std.Io.Dir.cwd().readFileAlloc(io, spec_path, alloc, .limited(8 << 20)) catch |e| {
        std.debug.print("::error::cannot read {s}: {s}\n", .{ spec_path, @errorName(e) });
        std.process.exit(1);
    };
    const spec = try std.json.parseFromSlice(std.json.Value, alloc, spec_bytes, .{});

    var unsupported = std.StringHashMap([]const u8).init(alloc);
    if (std.Io.Dir.cwd().readFileAlloc(io, "unsupported.json", alloc, .limited(1 << 20))) |ub| {
        const u = try std.json.parseFromSlice(std.json.Value, alloc, ub, .{});
        if (u.value.object.get("cases")) |c| {
            var it = c.object.iterator();
            while (it.next()) |e| try unsupported.put(e.key_ptr.*, e.value_ptr.string);
        }
    } else |_| {}

    var client = try NucleusClient.fromUrl(alloc, url);
    try client.connect();
    defer client.close();

    var out: std.ArrayList(u8) = .empty;
    const w = out.writer(alloc);
    try w.writeAll("{\n  \"sdk\": \"zig\",\n  \"specVersion\": 1,\n  \"cases\": [\n");

    var failures: usize = 0;
    var counts = [_]usize{0} ** 5;
    const cases = spec.value.object.get("cases").?.array.items;

    for (cases, 0..) |case, ci| {
        const id = case.object.get("id").?.string;
        const model = if (case.object.get("model")) |m| m.string else "";

        const xfail_applies = blk: {
            const x = case.object.get("xfail") orelse break :blk false;
            const sdks = x.object.get("sdks") orelse break :blk true;
            for (sdks.array.items) |s| {
                if (std.mem.eql(u8, s.string, "zig")) break :blk true;
            }
            break :blk false;
        };

        fail_detail.clearRetainingCapacity();
        const outcome = runCase(alloc, &client, case, @intCast(ci));

        var status: Status = undefined;
        var detail: []const u8 = "";
        if (outcome) |_| {
            status = if (xfail_applies) .xpass else .pass;
            if (xfail_applies) detail = "case is marked xfail but passed — the underlying bug is fixed and the xfail note is now false";
        } else |err| switch (err) {
            StepError.Unsupported => {
                if (unsupported.get(id)) |reason| {
                    status = .unsupported;
                    detail = reason;
                } else {
                    status = .fail;
                    detail = "no Zig mapping for this case, and it is not declared in unsupported.json";
                }
            },
            else => {
                status = if (xfail_applies) .xfail else .fail;
                detail = fail_detail.items;
            },
        }

        counts[@intFromEnum(status)] += 1;
        if (status == .fail or status == .xpass) {
            failures += 1;
            std.debug.print("::error::{s}: {s} — {s}\n", .{ id, status.str(), detail });
        }

        if (ci > 0) try w.writeAll(",\n");
        try w.print("    {{\"id\": \"{s}\", \"model\": \"{s}\", \"status\": \"{s}\"", .{ id, model, status.str() });
        if (detail.len > 0) {
            try w.writeAll(", \"detail\": ");
            try std.json.Stringify.value(std.json.Value{ .string = detail }, .{}, w);
        }
        try w.writeAll("}");
    }

    try w.writeAll("\n  ]\n}\n");
    try std.fs.File.stdout().writeAll(out.items);

    std.debug.print("zig: pass={d} fail={d} xfail={d} xpass={d} unsupported={d}\n", .{
        counts[@intFromEnum(Status.pass)],
        counts[@intFromEnum(Status.fail)],
        counts[@intFromEnum(Status.xfail)],
        counts[@intFromEnum(Status.xpass)],
        counts[@intFromEnum(Status.unsupported)],
    });

    if (failures > 0) std.process.exit(1);
}

fn runCase(
    alloc: std.mem.Allocator,
    client: *NucleusClient,
    case: std.json.Value,
    seed: u64,
) !void {
    var fx = Fixtures.init(alloc, 0x9E3779B97F4A7C15 *% (seed +% 1));
    var bound = std.StringHashMap(std.json.Value).init(alloc);

    for (case.object.get("steps").?.array.items, 0..) |step, si| {
        const op = step.object.get("op").?.string;
        const raw_args = if (step.object.get("args")) |a| a.array.items else &[_]std.json.Value{};

        var args = try std.ArrayList(std.json.Value).initCapacity(alloc, raw_args.len);
        for (raw_args) |a| args.appendAssumeCapacity(try resolve(alloc, a, &fx, &bound));

        const result = call(alloc, client, op, args.items) catch |e| {
            if (e == StepError.Unsupported) return e;
            if (e == StepError.Failed) {
                const d = try std.fmt.allocPrint(alloc, "step {d} ({s}): {s}", .{ si, op, fail_detail.items });
                fail_detail.clearRetainingCapacity();
                try fail_detail.appendSlice(gpa_alloc, d);
                return StepError.Failed;
            }
            return failWith("step {d} ({s}): client error: {s}", .{ si, op, @errorName(e) });
        };

        if (step.object.get("bind")) |b| {
            try bound.put(b.string, try resultToJson(alloc, result));
        }
        if (step.object.get("expect")) |ex| {
            check(alloc, result, ex.object) catch |e| {
                const d = try std.fmt.allocPrint(alloc, "step {d} ({s}): {s}", .{ si, op, fail_detail.items });
                fail_detail.clearRetainingCapacity();
                try fail_detail.appendSlice(gpa_alloc, d);
                return e;
            };
        }
    }
}
