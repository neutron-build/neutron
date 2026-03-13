// Layer 2: PostgreSQL client — composes Layer 0 pgwire codec + Layer 1 TCP
//
// Simple query and extended query protocol support.
// Uses caller-provided buffers — pre-allocate at connection setup.

const std = @import("std");
const tcp = @import("../layer1/tcp.zig");
const io_mod = @import("../layer1/io.zig");
const codec = @import("../layer0/pgwire/codec.zig");
const auth = @import("../layer0/pgwire/auth.zig");

pub const Error = error{
    ConnectionFailed,
    AuthenticationFailed,
    QueryFailed,
    UnexpectedMessage,
    BufferTooShort,
    InvalidResponse,
    ServerError,
};

pub const PgClient = struct {
    stream: tcp.TcpStream,
    send_buf: []u8,
    recv_buf: []u8,
    // Connection state
    ready: bool,
    transaction_status: codec.TransactionStatus,
    // Server info (from ParameterStatus messages)
    server_version: [64]u8,
    server_version_len: usize,
    is_nucleus: bool,

    pub const Config = struct {
        host: []const u8 = "127.0.0.1",
        port: u16 = 5432,
        user: []const u8 = "postgres",
        password: []const u8 = "",
        database: []const u8 = "postgres",
        send_buf_size: usize = 4096,
        recv_buf_size: usize = 8192,
    };

    /// Connect to a PostgreSQL server and complete authentication.
    pub fn connect(allocator: std.mem.Allocator, config: Config) !PgClient {
        const addr = try io_mod.resolveAddress(config.host, config.port);
        var stream = tcp.TcpStream{
            .stream = try std.net.tcpConnectToAddress(addr),
            .address = addr,
        };
        errdefer stream.close();

        const send_buf = try allocator.alloc(u8, config.send_buf_size);
        errdefer allocator.free(send_buf);
        const recv_buf = try allocator.alloc(u8, config.recv_buf_size);
        errdefer allocator.free(recv_buf);

        var client = PgClient{
            .stream = stream,
            .send_buf = send_buf,
            .recv_buf = recv_buf,
            .ready = false,
            .transaction_status = .idle,
            .server_version = undefined,
            .server_version_len = 0,
            .is_nucleus = false,
        };

        // Send StartupMessage
        const startup_len = codec.encodeStartup(send_buf, config.user, config.database) catch
            return Error.BufferTooShort;
        try client.stream.writeAll(send_buf[0..startup_len]);

        // Process authentication flow
        try client.processAuth(config.user, config.password);

        return client;
    }

    fn processAuth(self: *PgClient, user: []const u8, password: []const u8) !void {
        while (true) {
            const n = try self.stream.read(self.recv_buf);
            if (n == 0) return Error.ConnectionFailed;

            var pos: usize = 0;
            while (pos < n) {
                const result = codec.decode(self.recv_buf[pos..n]) catch return Error.InvalidResponse;
                pos += result.consumed;

                switch (result.msg) {
                    .auth_request => |ar| {
                        switch (ar.auth_type) {
                            .ok => {},
                            .cleartext_password => {
                                const msg_len = codec.encodePassword(self.send_buf, password) catch
                                    return Error.BufferTooShort;
                                try self.stream.writeAll(self.send_buf[0..msg_len]);
                            },
                            .md5_password => {
                                if (ar.data.len < 4) return Error.InvalidResponse;
                                var md5_out: [35]u8 = undefined;
                                auth.md5Password(user, password, ar.data[0..4].*, &md5_out);
                                const msg_len = codec.encodePassword(self.send_buf, &md5_out) catch
                                    return Error.BufferTooShort;
                                try self.stream.writeAll(self.send_buf[0..msg_len]);
                            },
                            .sasl => {
                                var scram = auth.ScramClient{};
                                var first_buf: [auth.MAX_CLIENT_FIRST_LEN]u8 = undefined;
                                const first_msg = scram.clientFirstMessage(user, &first_buf) catch
                                    return Error.AuthenticationFailed;
                                const msg_len = codec.encodeSASLInitialResponse(
                                    self.send_buf,
                                    "SCRAM-SHA-256",
                                    first_msg,
                                ) catch return Error.BufferTooShort;
                                try self.stream.writeAll(self.send_buf[0..msg_len]);

                                // Read server-first
                                const n2 = try self.stream.read(self.recv_buf);
                                if (n2 == 0) return Error.ConnectionFailed;
                                const r2 = codec.decode(self.recv_buf[0..n2]) catch return Error.InvalidResponse;
                                switch (r2.msg) {
                                    .auth_request => |ar2| {
                                        if (ar2.auth_type != .sasl_continue) return Error.AuthenticationFailed;
                                        var final_buf: [auth.MAX_CLIENT_FINAL_LEN]u8 = undefined;
                                        const final_msg = scram.processServerFirst(ar2.data, password, &final_buf) catch
                                            return Error.AuthenticationFailed;
                                        const resp_len = codec.encodeSASLResponse(self.send_buf, final_msg) catch
                                            return Error.BufferTooShort;
                                        try self.stream.writeAll(self.send_buf[0..resp_len]);

                                        // Read server-final
                                        const n3 = try self.stream.read(self.recv_buf);
                                        if (n3 == 0) return Error.ConnectionFailed;
                                        const r3 = codec.decode(self.recv_buf[0..n3]) catch return Error.InvalidResponse;
                                        switch (r3.msg) {
                                            .auth_request => |ar3| {
                                                if (ar3.auth_type != .sasl_final) return Error.AuthenticationFailed;
                                                scram.verifyServerFinal(ar3.data) catch return Error.AuthenticationFailed;
                                            },
                                            else => return Error.UnexpectedMessage,
                                        }
                                    },
                                    else => return Error.UnexpectedMessage,
                                }
                            },
                            else => return Error.AuthenticationFailed,
                        }
                    },
                    .parameter_status => |ps| {
                        if (std.mem.eql(u8, ps.name, "server_version")) {
                            const copy_len = @min(ps.value.len, self.server_version.len);
                            @memcpy(self.server_version[0..copy_len], ps.value[0..copy_len]);
                            self.server_version_len = copy_len;
                            self.is_nucleus = std.mem.indexOf(u8, ps.value, "Nucleus") != null;
                        }
                    },
                    .backend_key_data => {},
                    .ready_for_query => |status| {
                        self.transaction_status = status;
                        self.ready = true;
                        return;
                    },
                    .error_response => |err| {
                        _ = err;
                        return Error.AuthenticationFailed;
                    },
                    else => {},
                }
            }
        }
    }

    /// Execute a simple query. Returns the command tag string.
    pub fn execute(self: *PgClient, sql: []const u8) ![]const u8 {
        const msg_len = codec.encodeQuery(self.send_buf, sql) catch return Error.BufferTooShort;
        try self.stream.writeAll(self.send_buf[0..msg_len]);

        // Read response
        const n = try self.stream.read(self.recv_buf);
        if (n == 0) return Error.ConnectionFailed;

        var pos: usize = 0;
        var command_tag: []const u8 = "";

        while (pos < n) {
            const result = codec.decode(self.recv_buf[pos..n]) catch break;
            pos += result.consumed;

            switch (result.msg) {
                .command_complete => |tag| command_tag = tag,
                .ready_for_query => |status| {
                    self.transaction_status = status;
                    return command_tag;
                },
                .error_response => |err| {
                    _ = err;
                    return Error.ServerError;
                },
                else => {},
            }
        }
        return command_tag;
    }

    /// Execute a simple query and collect DataRow results.
    /// Returns a QueryResult containing rows and column metadata.
    pub fn query(self: *PgClient, sql: []const u8) !QueryResult {
        const msg_len = codec.encodeQuery(self.send_buf, sql) catch return Error.BufferTooShort;
        try self.stream.writeAll(self.send_buf[0..msg_len]);

        var result = QueryResult{};

        while (true) {
            const n = try self.stream.read(self.recv_buf);
            if (n == 0) return Error.ConnectionFailed;

            var pos: usize = 0;
            while (pos < n) {
                const decoded = codec.decode(self.recv_buf[pos..n]) catch break;
                pos += decoded.consumed;

                switch (decoded.msg) {
                    .row_description => |desc| {
                        result.column_count = desc.field_count;
                    },
                    .data_row => |row| {
                        // Store the first column value of each row (sufficient for
                        // Nucleus functions that return a single scalar value).
                        if (result.row_count < QueryResult.MAX_ROWS) {
                            var col_iter = row.iterator();
                            if (col_iter.next() catch null) |col| {
                                if (col.value) |v| {
                                    const copy_len = @min(v.len, QueryResult.MAX_VALUE_LEN);
                                    @memcpy(result.rows[result.row_count].data[0..copy_len], v[0..copy_len]);
                                    result.rows[result.row_count].len = copy_len;
                                } else {
                                    result.rows[result.row_count].len = 0;
                                    result.rows[result.row_count].is_null = true;
                                }
                            }
                            result.row_count += 1;
                        }
                    },
                    .command_complete => |tag| {
                        result.command_tag_len = @min(tag.len, result.command_tag.len);
                        @memcpy(result.command_tag[0..result.command_tag_len], tag[0..result.command_tag_len]);
                    },
                    .ready_for_query => |status| {
                        self.transaction_status = status;
                        return result;
                    },
                    .error_response => {
                        return Error.ServerError;
                    },
                    else => {},
                }
            }
        }
    }

    /// Execute using extended query protocol: Parse/Bind/Execute/Sync.
    /// Returns a PreparedStatement handle.
    pub fn prepare(self: *PgClient, name: []const u8, sql: []const u8) !PreparedStatement {
        var total: usize = 0;

        // Parse
        const parse_len = codec.encodeParse(self.send_buf[total..], name, sql, &.{}) catch return Error.BufferTooShort;
        total += parse_len;

        // Sync
        const sync_len = codec.encodeSync(self.send_buf[total..]) catch return Error.BufferTooShort;
        total += sync_len;

        try self.stream.writeAll(self.send_buf[0..total]);

        // Read until ReadyForQuery
        while (true) {
            const n = try self.stream.read(self.recv_buf);
            if (n == 0) return Error.ConnectionFailed;

            var pos: usize = 0;
            while (pos < n) {
                const decoded = codec.decode(self.recv_buf[pos..n]) catch break;
                pos += decoded.consumed;

                switch (decoded.msg) {
                    .parse_complete => {},
                    .ready_for_query => |status| {
                        self.transaction_status = status;
                        return PreparedStatement{ .name = name, .client = self };
                    },
                    .error_response => return Error.ServerError,
                    else => {},
                }
            }
        }
    }

    /// Get the server version string.
    pub fn serverVersion(self: *const PgClient) []const u8 {
        return self.server_version[0..self.server_version_len];
    }

    /// Check if connected to Nucleus (vs plain PostgreSQL).
    pub fn isNucleus(self: *const PgClient) bool {
        return self.is_nucleus;
    }

    pub fn close(self: *PgClient, allocator: std.mem.Allocator) void {
        // Send Terminate message
        const term_len = codec.encodeTerminate(self.send_buf) catch 0;
        if (term_len > 0) {
            self.stream.writeAll(self.send_buf[0..term_len]) catch {};
        }
        self.stream.close();
        allocator.free(self.send_buf);
        allocator.free(self.recv_buf);
    }
};

/// Result of a simple query — holds rows in stack-allocated buffers.
pub const QueryResult = struct {
    pub const MAX_ROWS = 256;
    pub const MAX_VALUE_LEN = 4096;

    pub const RowData = struct {
        data: [MAX_VALUE_LEN]u8 = undefined,
        len: usize = 0,
        is_null: bool = false,

        pub fn value(self: *const RowData) ?[]const u8 {
            if (self.is_null) return null;
            if (self.len == 0) return null;
            return self.data[0..self.len];
        }
    };

    rows: [MAX_ROWS]RowData = [_]RowData{.{}} ** MAX_ROWS,
    row_count: usize = 0,
    column_count: u16 = 0,
    command_tag: [64]u8 = undefined,
    command_tag_len: usize = 0,

    /// Get value of first column in the first row (common for scalar queries).
    pub fn scalar(self: *const QueryResult) ?[]const u8 {
        if (self.row_count == 0) return null;
        return self.rows[0].value();
    }

    /// Get the command tag string.
    pub fn commandTag(self: *const QueryResult) []const u8 {
        return self.command_tag[0..self.command_tag_len];
    }
};

/// Prepared statement handle for extended query protocol.
pub const PreparedStatement = struct {
    name: []const u8,
    client: *PgClient,

    /// Execute the prepared statement with parameter values.
    pub fn execute(self: *PreparedStatement, params: []const ?[]const u8) !QueryResult {
        var total: usize = 0;

        // Bind
        const bind_len = codec.encodeBind(self.client.send_buf[total..], "", self.name, params) catch
            return Error.BufferTooShort;
        total += bind_len;

        // Execute
        const exec_len = codec.encodeExecute(self.client.send_buf[total..], "", 0) catch
            return Error.BufferTooShort;
        total += exec_len;

        // Sync
        const sync_len = codec.encodeSync(self.client.send_buf[total..]) catch
            return Error.BufferTooShort;
        total += sync_len;

        try self.client.stream.writeAll(self.client.send_buf[0..total]);

        var result = QueryResult{};

        while (true) {
            const n = try self.client.stream.read(self.client.recv_buf);
            if (n == 0) return Error.ConnectionFailed;

            var pos: usize = 0;
            while (pos < n) {
                const decoded = codec.decode(self.client.recv_buf[pos..n]) catch break;
                pos += decoded.consumed;

                switch (decoded.msg) {
                    .bind_complete => {},
                    .data_row => |row| {
                        if (result.row_count < QueryResult.MAX_ROWS) {
                            var col_iter = row.iterator();
                            if (col_iter.next() catch null) |col| {
                                if (col.value) |v| {
                                    const copy_len = @min(v.len, QueryResult.MAX_VALUE_LEN);
                                    @memcpy(result.rows[result.row_count].data[0..copy_len], v[0..copy_len]);
                                    result.rows[result.row_count].len = copy_len;
                                } else {
                                    result.rows[result.row_count].is_null = true;
                                }
                            }
                            result.row_count += 1;
                        }
                    },
                    .command_complete => |tag| {
                        result.command_tag_len = @min(tag.len, result.command_tag.len);
                        @memcpy(result.command_tag[0..result.command_tag_len], tag[0..result.command_tag_len]);
                    },
                    .ready_for_query => |status| {
                        self.client.transaction_status = status;
                        return result;
                    },
                    .error_response => return Error.ServerError,
                    else => {},
                }
            }
        }
    }
};

test "PgClient Config defaults" {
    const cfg = PgClient.Config{};
    try std.testing.expectEqualStrings("127.0.0.1", cfg.host);
    try std.testing.expectEqual(@as(u16, 5432), cfg.port);
    try std.testing.expectEqualStrings("postgres", cfg.user);
    try std.testing.expectEqualStrings("postgres", cfg.database);
}

test "PgClient struct layout" {
    // Verify the struct can be created
    try std.testing.expect(@sizeOf(PgClient) > 0);
}
