// Layer 0: PostgreSQL wire protocol codec — zero allocation
//
// Encodes frontend (client→server) messages and decodes backend (server→client)
// messages. All operations use caller-provided buffers — zero heap allocation.
// Fields in decoded messages are slices into the input buffer (zero-copy).

const std = @import("std");
const types = @import("types.zig");
const rdr = @import("reader.zig");
const endian = @import("../binary/endian.zig");

pub const FrontendTag = types.FrontendTag;
pub const BackendTag = types.BackendTag;
pub const TransactionStatus = types.TransactionStatus;
pub const AuthType = types.AuthType;
pub const FieldDescription = types.FieldDescription;
pub const ErrorField = types.ErrorField;
pub const Oid = types.Oid;
pub const PROTOCOL_VERSION = types.PROTOCOL_VERSION;
pub const SSL_REQUEST = types.SSL_REQUEST;

pub const Reader = rdr.Reader;
pub const Writer = rdr.Writer;

pub const Error = error{
    BufferTooShort,
    MissingNullTerminator,
    InvalidMessage,
    UnknownTag,
    MessageTooLarge,
};

/// Maximum message size we'll accept (256 MB).
const MAX_MESSAGE_SIZE: u32 = 256 * 1024 * 1024;

// ---------------------------------------------------------------------------
// Frontend (client → server) encoding
// ---------------------------------------------------------------------------

/// Encode a StartupMessage into buf. Returns bytes written.
/// Format: len(u32) + version(u32) + param pairs + \0
pub fn encodeStartup(buf: []u8, user: []const u8, database: []const u8) Error!usize {
    var w = Writer.init(buf);
    const len_pos = w.getPos();
    try w.writeU32(0); // placeholder for length
    try w.writeU32(PROTOCOL_VERSION);
    try w.writeCString("user");
    try w.writeCString(user);
    try w.writeCString("database");
    try w.writeCString(database);
    try w.writeByte(0); // terminator
    const total: u32 = @intCast(w.getPos());
    w.patchU32(len_pos, total);
    return w.getPos();
}

/// Encode a SimpleQuery message: 'Q' + len + sql + \0
pub fn encodeQuery(buf: []u8, sql: []const u8) Error!usize {
    var w = Writer.init(buf);
    try w.writeByte(@intFromEnum(FrontendTag.query));
    const len_pos = w.getPos();
    try w.writeU32(0); // placeholder
    try w.writeCString(sql);
    const msg_len: u32 = @intCast(w.getPos() - len_pos);
    w.patchU32(len_pos, msg_len);
    return w.getPos();
}

/// Encode a password message (cleartext or md5 response).
pub fn encodePassword(buf: []u8, password: []const u8) Error!usize {
    var w = Writer.init(buf);
    try w.writeByte(@intFromEnum(FrontendTag.password));
    const len_pos = w.getPos();
    try w.writeU32(0);
    try w.writeCString(password);
    const msg_len: u32 = @intCast(w.getPos() - len_pos);
    w.patchU32(len_pos, msg_len);
    return w.getPos();
}

/// Encode a SASL initial response for SCRAM-SHA-256.
pub fn encodeSASLInitialResponse(buf: []u8, mechanism: []const u8, client_first: []const u8) Error!usize {
    var w = Writer.init(buf);
    try w.writeByte(@intFromEnum(FrontendTag.password));
    const len_pos = w.getPos();
    try w.writeU32(0);
    try w.writeCString(mechanism);
    const data_len: u32 = @intCast(client_first.len);
    try w.writeU32(data_len);
    try w.writeBytes(client_first);
    const msg_len: u32 = @intCast(w.getPos() - len_pos);
    w.patchU32(len_pos, msg_len);
    return w.getPos();
}

/// Encode a SASL response (continuation).
pub fn encodeSASLResponse(buf: []u8, data: []const u8) Error!usize {
    var w = Writer.init(buf);
    try w.writeByte(@intFromEnum(FrontendTag.password));
    const len_pos = w.getPos();
    try w.writeU32(0);
    try w.writeBytes(data);
    const msg_len: u32 = @intCast(w.getPos() - len_pos);
    w.patchU32(len_pos, msg_len);
    return w.getPos();
}

/// Encode a Parse message (extended query protocol).
pub fn encodeParse(buf: []u8, name: []const u8, sql: []const u8, param_oids: []const u32) Error!usize {
    var w = Writer.init(buf);
    try w.writeByte(@intFromEnum(FrontendTag.parse));
    const len_pos = w.getPos();
    try w.writeU32(0);
    try w.writeCString(name);
    try w.writeCString(sql);
    const n_params: u16 = @intCast(param_oids.len);
    try w.writeU16(n_params);
    for (param_oids) |oid| {
        try w.writeU32(oid);
    }
    const msg_len: u32 = @intCast(w.getPos() - len_pos);
    w.patchU32(len_pos, msg_len);
    return w.getPos();
}

/// Encode a Bind message.
pub fn encodeBind(buf: []u8, portal: []const u8, stmt: []const u8, params: []const ?[]const u8) Error!usize {
    var w = Writer.init(buf);
    try w.writeByte(@intFromEnum(FrontendTag.bind));
    const len_pos = w.getPos();
    try w.writeU32(0);
    try w.writeCString(portal);
    try w.writeCString(stmt);
    // Format codes: 0 = all text
    try w.writeU16(0);
    // Parameters
    const n_params: u16 = @intCast(params.len);
    try w.writeU16(n_params);
    for (params) |param| {
        if (param) |p| {
            const plen: u32 = @intCast(p.len);
            try w.writeI32(@intCast(plen));
            try w.writeBytes(p);
        } else {
            try w.writeI32(-1); // NULL
        }
    }
    // Result format codes: 0 = all text
    try w.writeU16(0);
    const msg_len: u32 = @intCast(w.getPos() - len_pos);
    w.patchU32(len_pos, msg_len);
    return w.getPos();
}

/// Encode an Execute message.
pub fn encodeExecute(buf: []u8, portal: []const u8, max_rows: u32) Error!usize {
    var w = Writer.init(buf);
    try w.writeByte(@intFromEnum(FrontendTag.execute));
    const len_pos = w.getPos();
    try w.writeU32(0);
    try w.writeCString(portal);
    try w.writeU32(max_rows);
    const msg_len: u32 = @intCast(w.getPos() - len_pos);
    w.patchU32(len_pos, msg_len);
    return w.getPos();
}

/// Encode a Sync message.
pub fn encodeSync(buf: []u8) Error!usize {
    if (buf.len < 5) return Error.BufferTooShort;
    buf[0] = @intFromEnum(FrontendTag.sync);
    endian.writeU32(@ptrCast(buf[1..5]), 4);
    return 5;
}

/// Encode a Terminate message.
pub fn encodeTerminate(buf: []u8) Error!usize {
    if (buf.len < 5) return Error.BufferTooShort;
    buf[0] = @intFromEnum(FrontendTag.terminate);
    endian.writeU32(@ptrCast(buf[1..5]), 4);
    return 5;
}

// ---------------------------------------------------------------------------
// Backend (server → client) decoding
// ---------------------------------------------------------------------------

/// Decoded backend message — fields are slices into the input buffer.
pub const BackendMessage = union(BackendTag) {
    auth_request: AuthRequest,
    parameter_status: ParameterStatus,
    backend_key_data: BackendKeyData,
    ready_for_query: TransactionStatus,
    row_description: RawRowDescription,
    data_row: RawDataRow,
    command_complete: []const u8,
    error_response: RawErrorNotice,
    notice_response: RawErrorNotice,
    empty_query_response: void,
    parse_complete: void,
    bind_complete: void,
    close_complete: void,
    no_data: void,
    parameter_description: []const u8,
    notification_response: NotificationResponse,
    copy_in_response: []const u8,
    copy_out_response: []const u8,
    copy_both_response: []const u8,
};

pub const AuthRequest = struct {
    auth_type: AuthType,
    data: []const u8, // salt for MD5, SASL mechanism list, etc.
};

pub const ParameterStatus = struct {
    name: []const u8,
    value: []const u8,
};

pub const BackendKeyData = struct {
    process_id: u32,
    secret_key: u32,
};

/// Raw row description — parse fields lazily to stay zero-alloc.
pub const RawRowDescription = struct {
    field_count: u16,
    data: []const u8,

    /// Iterate over field descriptions.
    pub fn iterator(self: RawRowDescription) FieldIterator {
        return .{ .reader = Reader.init(self.data), .remaining = self.field_count };
    }
};

pub const FieldIterator = struct {
    reader: Reader,
    remaining: u16,

    pub fn next(self: *FieldIterator) Error!?FieldDescription {
        if (self.remaining == 0) return null;
        self.remaining -= 1;
        const name = try self.reader.readCString();
        const table_oid = try self.reader.readU32();
        const column_attr = try self.reader.readU16();
        const type_oid = try self.reader.readU32();
        const type_len = try self.reader.readI16();
        const type_modifier = try self.reader.readI32();
        const format_code = try self.reader.readU16();
        return .{
            .name = name,
            .table_oid = table_oid,
            .column_attr = column_attr,
            .type_oid = type_oid,
            .type_len = type_len,
            .type_modifier = type_modifier,
            .format_code = format_code,
        };
    }
};

/// Raw data row — parse columns lazily.
pub const RawDataRow = struct {
    column_count: u16,
    data: []const u8,

    pub fn iterator(self: RawDataRow) ColumnIterator {
        return .{ .reader = Reader.init(self.data), .remaining = self.column_count };
    }
};

pub const ColumnIterator = struct {
    reader: Reader,
    remaining: u16,

    /// Returns null for SQL NULL, or the column bytes.
    pub fn next(self: *ColumnIterator) Error!?ColumnValue {
        if (self.remaining == 0) return null;
        self.remaining -= 1;
        const len = try self.reader.readI32();
        if (len == -1) return .{ .value = null };
        const data = try self.reader.readBytes(@intCast(len));
        return .{ .value = data };
    }
};

pub const ColumnValue = struct {
    value: ?[]const u8,
};

/// Raw error/notice response — iterate fields lazily.
pub const RawErrorNotice = struct {
    data: []const u8,

    pub fn iterator(self: RawErrorNotice) ErrorFieldIterator {
        return .{ .reader = Reader.init(self.data) };
    }

    /// Get a specific field by type.
    pub fn getField(self: RawErrorNotice, field: ErrorField) ?[]const u8 {
        var iter = self.iterator();
        while (iter.next() catch null) |entry| {
            if (entry.field == field) return entry.value;
        }
        return null;
    }

    /// Get the error message.
    pub fn message(self: RawErrorNotice) ?[]const u8 {
        return self.getField(.message);
    }

    /// Get the SQLSTATE code.
    pub fn code(self: RawErrorNotice) ?[]const u8 {
        return self.getField(.code);
    }
};

pub const ErrorFieldEntry = struct {
    field: ErrorField,
    value: []const u8,
};

pub const ErrorFieldIterator = struct {
    reader: Reader,

    pub fn next(self: *ErrorFieldIterator) Error!?ErrorFieldEntry {
        const tag = self.reader.readByte() catch return null;
        if (tag == 0) return null;
        const value = try self.reader.readCString();
        return .{
            .field = @enumFromInt(tag),
            .value = value,
        };
    }
};

pub const NotificationResponse = struct {
    process_id: u32,
    channel: []const u8,
    payload: []const u8,
};

/// Decode a single backend message from the buffer.
/// Returns the decoded message and bytes consumed.
/// All string/data fields are slices into `buf` (zero-copy).
pub fn decode(buf: []const u8) Error!struct { msg: BackendMessage, consumed: usize } {
    if (buf.len < 5) return Error.BufferTooShort;

    const tag_byte = buf[0];
    const msg_len = endian.readU32(@ptrCast(buf[1..5]));
    if (msg_len < 4) return Error.InvalidMessage;
    if (msg_len > MAX_MESSAGE_SIZE) return Error.MessageTooLarge;
    const total_len = @as(usize, msg_len) + 1; // tag + payload
    if (buf.len < total_len) return Error.BufferTooShort;

    const payload = buf[5..total_len];
    var r = Reader.init(payload);

    const tag: BackendTag = @enumFromInt(tag_byte);
    const msg: BackendMessage = switch (tag) {
        .auth_request => blk: {
            const auth_code = try r.readU32();
            const rest = r.rest();
            break :blk .{ .auth_request = .{
                .auth_type = @enumFromInt(auth_code),
                .data = rest,
            } };
        },
        .parameter_status => blk: {
            const name = try r.readCString();
            const value = try r.readCString();
            break :blk .{ .parameter_status = .{ .name = name, .value = value } };
        },
        .backend_key_data => blk: {
            const pid = try r.readU32();
            const key = try r.readU32();
            break :blk .{ .backend_key_data = .{ .process_id = pid, .secret_key = key } };
        },
        .ready_for_query => blk: {
            const status = try r.readByte();
            break :blk .{ .ready_for_query = @enumFromInt(status) };
        },
        .row_description => blk: {
            const field_count = try r.readU16();
            break :blk .{ .row_description = .{
                .field_count = field_count,
                .data = r.rest(),
            } };
        },
        .data_row => blk: {
            const col_count = try r.readU16();
            break :blk .{ .data_row = .{
                .column_count = col_count,
                .data = r.rest(),
            } };
        },
        .command_complete => .{ .command_complete = try r.readCString() },
        .error_response => .{ .error_response = .{ .data = payload } },
        .notice_response => .{ .notice_response = .{ .data = payload } },
        .empty_query_response => .{ .empty_query_response = {} },
        .parse_complete => .{ .parse_complete = {} },
        .bind_complete => .{ .bind_complete = {} },
        .close_complete => .{ .close_complete = {} },
        .no_data => .{ .no_data = {} },
        .parameter_description => .{ .parameter_description = payload },
        .notification_response => blk: {
            const pid = try r.readU32();
            const channel = try r.readCString();
            const pl = try r.readCString();
            break :blk .{ .notification_response = .{
                .process_id = pid,
                .channel = channel,
                .payload = pl,
            } };
        },
        .copy_in_response => .{ .copy_in_response = payload },
        .copy_out_response => .{ .copy_out_response = payload },
        .copy_both_response => .{ .copy_both_response = payload },
        _ => return Error.UnknownTag,
    };

    return .{ .msg = msg, .consumed = total_len };
}

// ---------------------------------------------------------------------------
// Comptime SQL QueryType
// ---------------------------------------------------------------------------

/// Comptime-validated SQL query type.
///
/// Usage:
/// ```
/// const FindUser = QueryType("SELECT * FROM users WHERE email = $1 AND active = $2");
/// var buf: [FindUser.max_sql_len]u8 = undefined;
/// const sql = FindUser.bind(&buf, .{ "alice@example.com", true });
/// ```
///
/// Wrong argument count → compile error.
pub fn QueryType(comptime sql: []const u8) type {
    const param_count = comptime countParams(sql);
    const segments = comptime parseSegments(sql);

    return struct {
        /// Maximum possible SQL length after parameter substitution.
        pub const max_sql_len: usize = sql.len + param_count * 128;

        /// Number of expected parameters.
        pub const n_params: usize = param_count;

        /// The original SQL template.
        pub const template: []const u8 = sql;

        /// Bind parameters into the SQL string, writing into the provided buffer.
        /// Returns the rendered SQL as a slice of buf.
        pub fn bind(buf: *[max_sql_len]u8, args: anytype) []const u8 {
            const ArgsType = @TypeOf(args);
            const args_info = @typeInfo(ArgsType);
            const fields_len = switch (args_info) {
                .@"struct" => |s| s.fields.len,
                else => @compileError("bind args must be a tuple/struct"),
            };

            if (fields_len != param_count) {
                @compileError(std.fmt.comptimePrint(
                    "expected {d} parameters but got {d}",
                    .{ param_count, fields_len },
                ));
            }

            var pos: usize = 0;
            inline for (segments) |seg| {
                if (seg.is_param) {
                    pos = formatParam(buf, pos, args, seg.param_idx);
                } else {
                    @memcpy(buf[pos .. pos + seg.literal.len], seg.literal);
                    pos += seg.literal.len;
                }
            }
            return buf[0..pos];
        }

        fn formatParam(buf: *[max_sql_len]u8, start_pos: usize, args: anytype, comptime idx: usize) usize {
            var pos = start_pos;
            const value = args[idx];
            const T = @TypeOf(value);

            switch (@typeInfo(T)) {
                .bool => {
                    const s = if (value) "true" else "false";
                    @memcpy(buf[pos .. pos + s.len], s);
                    pos += s.len;
                },
                .comptime_int, .int => {
                    var tmp: [20]u8 = undefined;
                    const formatted = std.fmt.bufPrint(&tmp, "{d}", .{value}) catch unreachable;
                    @memcpy(buf[pos .. pos + formatted.len], formatted);
                    pos += formatted.len;
                },
                .pointer => |ptr| {
                    // Handle both []const u8 and *const [N:0]u8 (string literals)
                    if (ptr.size == .slice and ptr.child == u8) {
                        buf[pos] = '\'';
                        pos += 1;
                        @memcpy(buf[pos .. pos + value.len], value);
                        pos += value.len;
                        buf[pos] = '\'';
                        pos += 1;
                    } else if (ptr.size == .one and @typeInfo(ptr.child) == .array) {
                        const slice: []const u8 = value;
                        buf[pos] = '\'';
                        pos += 1;
                        @memcpy(buf[pos .. pos + slice.len], slice);
                        pos += slice.len;
                        buf[pos] = '\'';
                        pos += 1;
                    } else {
                        @compileError("unsupported parameter type: " ++ @typeName(T));
                    }
                },
                .optional => {
                    if (value) |v| {
                        pos = formatParam(buf, pos, .{v}, 0);
                    } else {
                        const s = "NULL";
                        @memcpy(buf[pos .. pos + s.len], s);
                        pos += s.len;
                    }
                },
                else => @compileError("unsupported parameter type: " ++ @typeName(T)),
            }
            return pos;
        }
    };
}

/// A parsed segment: either a literal string or a parameter reference.
const Segment = struct {
    is_param: bool,
    param_idx: usize, // only valid when is_param
    literal: []const u8, // only valid when !is_param
};

/// Parse SQL template at comptime into segments of literals and parameter refs.
fn parseSegments(comptime sql: []const u8) []const Segment {
    var result: []const Segment = &.{};
    var i: usize = 0;
    var lit_start: usize = 0;

    while (i < sql.len) {
        if (sql[i] == '$' and i + 1 < sql.len and sql[i + 1] >= '1' and sql[i + 1] <= '9') {
            // Flush literal before this param
            if (i > lit_start) {
                result = result ++ &[_]Segment{.{
                    .is_param = false,
                    .param_idx = 0,
                    .literal = sql[lit_start..i],
                }};
            }
            // Parse parameter number
            var num: usize = 0;
            var j = i + 1;
            while (j < sql.len and sql[j] >= '0' and sql[j] <= '9') {
                num = num * 10 + (sql[j] - '0');
                j += 1;
            }
            // Skip optional type annotation {Type}
            if (j < sql.len and sql[j] == '{') {
                while (j < sql.len and sql[j] != '}') j += 1;
                if (j < sql.len) j += 1;
            }
            result = result ++ &[_]Segment{.{
                .is_param = true,
                .param_idx = num - 1,
                .literal = "",
            }};
            i = j;
            lit_start = j;
        } else {
            i += 1;
        }
    }
    // Flush trailing literal
    if (i > lit_start) {
        result = result ++ &[_]Segment{.{
            .is_param = false,
            .param_idx = 0,
            .literal = sql[lit_start..i],
        }};
    }
    return result;
}

/// Count the number of unique $N parameters in a SQL string at comptime.
fn countParams(comptime sql: []const u8) usize {
    var max_param: usize = 0;
    var i: usize = 0;
    while (i < sql.len) {
        if (sql[i] == '$' and i + 1 < sql.len and sql[i + 1] >= '1' and sql[i + 1] <= '9') {
            var num: usize = 0;
            var j = i + 1;
            while (j < sql.len and sql[j] >= '0' and sql[j] <= '9') {
                num = num * 10 + (sql[j] - '0');
                j += 1;
            }
            if (num > max_param) max_param = num;
            i = j;
        } else {
            i += 1;
        }
    }
    return max_param;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "encode and decode StartupMessage" {
    var buf: [256]u8 = undefined;
    const n = try encodeStartup(&buf, "postgres", "mydb");
    try std.testing.expect(n > 0);
    // First 4 bytes = length (big-endian)
    const len = endian.readU32(@ptrCast(buf[0..4]));
    try std.testing.expectEqual(@as(u32, @intCast(n)), len);
    // Next 4 bytes = protocol version
    const ver = endian.readU32(@ptrCast(buf[4..8]));
    try std.testing.expectEqual(PROTOCOL_VERSION, ver);
}

test "encode SimpleQuery" {
    var buf: [256]u8 = undefined;
    const n = try encodeQuery(&buf, "SELECT 1");
    try std.testing.expect(n > 0);
    try std.testing.expectEqual(@as(u8, 'Q'), buf[0]);
}

test "encode Terminate" {
    var buf: [8]u8 = undefined;
    const n = try encodeTerminate(&buf);
    try std.testing.expectEqual(@as(usize, 5), n);
    try std.testing.expectEqual(@as(u8, 'X'), buf[0]);
    const len = endian.readU32(@ptrCast(buf[1..5]));
    try std.testing.expectEqual(@as(u32, 4), len);
}

test "encode Sync" {
    var buf: [8]u8 = undefined;
    const n = try encodeSync(&buf);
    try std.testing.expectEqual(@as(usize, 5), n);
    try std.testing.expectEqual(@as(u8, 'S'), buf[0]);
}

test "decode ReadyForQuery" {
    // 'Z' + len(5) + 'I'
    const buf = [_]u8{ 'Z', 0, 0, 0, 5, 'I' };
    const result = try decode(&buf);
    switch (result.msg) {
        .ready_for_query => |status| try std.testing.expectEqual(TransactionStatus.idle, status),
        else => return error.UnexpectedMessage,
    }
    try std.testing.expectEqual(@as(usize, 6), result.consumed);
}

test "decode CommandComplete" {
    // 'C' + len + "INSERT 0 1\0"
    const tag = "INSERT 0 1";
    const payload_len: u32 = @intCast(4 + tag.len + 1);
    var buf: [32]u8 = undefined;
    buf[0] = 'C';
    endian.writeU32(@ptrCast(buf[1..5]), payload_len);
    @memcpy(buf[5 .. 5 + tag.len], tag);
    buf[5 + tag.len] = 0;

    const result = try decode(&buf);
    switch (result.msg) {
        .command_complete => |cmd| try std.testing.expectEqualStrings("INSERT 0 1", cmd),
        else => return error.UnexpectedMessage,
    }
}

test "decode ParameterStatus" {
    // Build: 'S' + len + "server_version\0" + "16.0\0"
    var buf: [64]u8 = undefined;
    buf[0] = 'S';
    const name = "server_version";
    const value = "16.0";
    const payload_len: u32 = @intCast(4 + name.len + 1 + value.len + 1);
    endian.writeU32(@ptrCast(buf[1..5]), payload_len);
    var pos: usize = 5;
    @memcpy(buf[pos .. pos + name.len], name);
    pos += name.len;
    buf[pos] = 0;
    pos += 1;
    @memcpy(buf[pos .. pos + value.len], value);
    pos += value.len;
    buf[pos] = 0;

    const result = try decode(&buf);
    switch (result.msg) {
        .parameter_status => |ps| {
            try std.testing.expectEqualStrings("server_version", ps.name);
            try std.testing.expectEqualStrings("16.0", ps.value);
        },
        else => return error.UnexpectedMessage,
    }
}

test "decode AuthenticationOk" {
    var buf: [16]u8 = undefined;
    buf[0] = 'R';
    endian.writeU32(@ptrCast(buf[1..5]), 8);
    endian.writeU32(@ptrCast(buf[5..9]), 0); // AuthOk
    const result = try decode(&buf);
    switch (result.msg) {
        .auth_request => |auth| try std.testing.expectEqual(AuthType.ok, auth.auth_type),
        else => return error.UnexpectedMessage,
    }
}

test "decode ErrorResponse" {
    // Build an error: 'E' + len + 'S' + "ERROR\0" + 'M' + "test error\0" + 'C' + "42P01\0" + \0
    var buf: [128]u8 = undefined;
    var w = Writer.init(buf[5..]);
    try w.writeByte('S');
    try w.writeCString("ERROR");
    try w.writeByte('M');
    try w.writeCString("test error");
    try w.writeByte('C');
    try w.writeCString("42P01");
    try w.writeByte(0);

    buf[0] = 'E';
    const payload_len: u32 = @intCast(4 + w.getPos());
    endian.writeU32(@ptrCast(buf[1..5]), payload_len);

    const result = try decode(&buf);
    switch (result.msg) {
        .error_response => |err| {
            try std.testing.expectEqualStrings("test error", err.message().?);
            try std.testing.expectEqualStrings("42P01", err.code().?);
        },
        else => return error.UnexpectedMessage,
    }
}

test "decode DataRow" {
    // 'D' + len + num_cols(2) + col1_len(5) + "hello" + col2_len(-1=NULL)
    var buf: [64]u8 = undefined;
    var w = Writer.init(buf[5..]);
    try w.writeU16(2); // 2 columns
    try w.writeI32(5); // col1 length
    try w.writeBytes("hello");
    try w.writeI32(-1); // col2 = NULL

    buf[0] = 'D';
    const payload_len: u32 = @intCast(4 + w.getPos());
    endian.writeU32(@ptrCast(buf[1..5]), payload_len);

    const result = try decode(&buf);
    switch (result.msg) {
        .data_row => |row| {
            try std.testing.expectEqual(@as(u16, 2), row.column_count);
            var iter = row.iterator();
            const col1 = (try iter.next()).?;
            try std.testing.expectEqualStrings("hello", col1.value.?);
            const col2 = (try iter.next()).?;
            try std.testing.expectEqual(@as(?[]const u8, null), col2.value);
            try std.testing.expectEqual(@as(?ColumnValue, null), try iter.next());
        },
        else => return error.UnexpectedMessage,
    }
}

test "decode RowDescription" {
    // 'T' + len + num_fields(1) + field data
    var buf: [128]u8 = undefined;
    var w = Writer.init(buf[5..]);
    try w.writeU16(1); // 1 field
    try w.writeCString("id"); // name
    try w.writeU32(0); // table OID
    try w.writeU16(0); // column attr
    try w.writeU32(Oid.int4); // type OID
    try w.writeI16(4); // type len
    try w.writeI32(-1); // type modifier
    try w.writeU16(0); // format code (text)

    buf[0] = 'T';
    const payload_len: u32 = @intCast(4 + w.getPos());
    endian.writeU32(@ptrCast(buf[1..5]), payload_len);

    const result = try decode(&buf);
    switch (result.msg) {
        .row_description => |desc| {
            try std.testing.expectEqual(@as(u16, 1), desc.field_count);
            var iter = desc.iterator();
            const field = (try iter.next()).?;
            try std.testing.expectEqualStrings("id", field.name);
            try std.testing.expectEqual(Oid.int4, field.type_oid);
        },
        else => return error.UnexpectedMessage,
    }
}

test "decode buffer too short" {
    const buf = [_]u8{ 'Z', 0, 0 };
    try std.testing.expectError(Error.BufferTooShort, decode(&buf));
}

test "QueryType: parameter counting" {
    const Q1 = QueryType("SELECT 1");
    try std.testing.expectEqual(@as(usize, 0), Q1.n_params);

    const Q2 = QueryType("SELECT * FROM users WHERE id = $1");
    try std.testing.expectEqual(@as(usize, 1), Q2.n_params);

    const Q3 = QueryType("SELECT * FROM users WHERE email = $1 AND active = $2");
    try std.testing.expectEqual(@as(usize, 2), Q3.n_params);

    const Q4 = QueryType("INSERT INTO t VALUES ($1, $2, $3)");
    try std.testing.expectEqual(@as(usize, 3), Q4.n_params);
}

test "QueryType: bind string and bool" {
    const Q = QueryType("SELECT * FROM users WHERE email = $1 AND active = $2");
    var buf: [Q.max_sql_len]u8 = undefined;
    const sql = Q.bind(&buf, .{ "alice@example.com", true });
    try std.testing.expectEqualStrings("SELECT * FROM users WHERE email = 'alice@example.com' AND active = true", sql);
}

test "QueryType: bind integer" {
    const Q = QueryType("SELECT * FROM users WHERE id = $1");
    var buf: [Q.max_sql_len]u8 = undefined;
    const sql = Q.bind(&buf, .{42});
    try std.testing.expectEqualStrings("SELECT * FROM users WHERE id = 42", sql);
}

test "QueryType: bind with type annotations" {
    const Q = QueryType("SELECT * FROM users WHERE email = $1{[]const u8} AND active = $2{bool}");
    var buf: [Q.max_sql_len]u8 = undefined;
    const sql = Q.bind(&buf, .{ "bob@test.com", false });
    try std.testing.expectEqualStrings("SELECT * FROM users WHERE email = 'bob@test.com' AND active = false", sql);
}

test "QueryType: no params" {
    const Q = QueryType("SELECT 1");
    var buf: [Q.max_sql_len]u8 = undefined;
    const sql = Q.bind(&buf, .{});
    try std.testing.expectEqualStrings("SELECT 1", sql);
}

test "encode Parse" {
    var buf: [256]u8 = undefined;
    const n = try encodeParse(&buf, "", "SELECT $1::text", &[_]u32{Oid.text});
    try std.testing.expect(n > 0);
    try std.testing.expectEqual(@as(u8, 'P'), buf[0]);
}

test "encode Bind" {
    var buf: [256]u8 = undefined;
    const params = [_]?[]const u8{ "hello", null };
    const n = try encodeBind(&buf, "", "", &params);
    try std.testing.expect(n > 0);
    try std.testing.expectEqual(@as(u8, 'B'), buf[0]);
}

test "encode Execute" {
    var buf: [64]u8 = undefined;
    const n = try encodeExecute(&buf, "", 0);
    try std.testing.expect(n > 0);
    try std.testing.expectEqual(@as(u8, 'E'), buf[0]);
}
