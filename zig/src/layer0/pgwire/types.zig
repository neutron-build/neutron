// Layer 0: PostgreSQL wire protocol types — zero allocation
//
// Defines all message types for the PostgreSQL v3 wire protocol.
// Frontend = client → server, Backend = server → client.

/// Frontend (client → server) message type identifiers.
pub const FrontendTag = enum(u8) {
    query = 'Q',
    parse = 'P',
    bind = 'B',
    describe = 'D',
    execute = 'E',
    sync = 'S',
    close = 'C',
    password = 'p',
    terminate = 'X',
    flush = 'H',
    copy_data = 'd',
    copy_done = 'c',
    copy_fail = 'f',
};

/// Backend (server → client) message type identifiers.
pub const BackendTag = enum(u8) {
    auth_request = 'R',
    parameter_status = 'S',
    backend_key_data = 'K',
    ready_for_query = 'Z',
    row_description = 'T',
    data_row = 'D',
    command_complete = 'C',
    error_response = 'E',
    notice_response = 'N',
    empty_query_response = 'I',
    parse_complete = '1',
    bind_complete = '2',
    close_complete = '3',
    no_data = 'n',
    parameter_description = 't',
    notification_response = 'A',
    copy_in_response = 'G',
    copy_out_response = 'H',
    copy_both_response = 'W',
    _,
};

/// Transaction status indicators (ready_for_query payload).
pub const TransactionStatus = enum(u8) {
    idle = 'I',
    in_transaction = 'T',
    failed = 'E',
    _,
};

/// Authentication request types.
pub const AuthType = enum(u32) {
    ok = 0,
    kerberos_v5 = 2,
    cleartext_password = 3,
    md5_password = 5,
    gss = 7,
    gss_continue = 8,
    sspi = 9,
    sasl = 10,
    sasl_continue = 11,
    sasl_final = 12,
    _,
};

/// A row description field (column metadata).
pub const FieldDescription = struct {
    name: []const u8,
    table_oid: u32,
    column_attr: u16,
    type_oid: u32,
    type_len: i16,
    type_modifier: i32,
    format_code: u16,
};

/// Common PostgreSQL type OIDs.
pub const Oid = struct {
    pub const bool_oid: u32 = 16;
    pub const bytea: u32 = 17;
    pub const int8: u32 = 20;
    pub const int2: u32 = 21;
    pub const int4: u32 = 23;
    pub const text: u32 = 25;
    pub const float4: u32 = 700;
    pub const float8: u32 = 701;
    pub const varchar: u32 = 1043;
    pub const timestamp: u32 = 1114;
    pub const timestamptz: u32 = 1184;
    pub const uuid: u32 = 2950;
    pub const jsonb: u32 = 3802;
};

/// Error/Notice field identifiers.
pub const ErrorField = enum(u8) {
    severity = 'S',
    severity_v = 'V',
    code = 'C',
    message = 'M',
    detail = 'D',
    hint = 'H',
    position = 'P',
    internal_position = 'p',
    internal_query = 'q',
    where = 'W',
    schema = 's',
    table = 't',
    column = 'c',
    data_type = 'd',
    constraint = 'n',
    file = 'F',
    line = 'L',
    routine = 'R',
    _,
};

/// PostgreSQL wire protocol version 3.0
pub const PROTOCOL_VERSION: u32 = 196608; // 3 << 16

/// SSL request magic number
pub const SSL_REQUEST: u32 = 80877103;

/// Cancel request magic number
pub const CANCEL_REQUEST: u32 = 80877102;

const std = @import("std");

test "protocol version is 3.0" {
    try std.testing.expectEqual(@as(u32, 3 << 16), PROTOCOL_VERSION);
}

test "frontend tags match expected byte values" {
    try std.testing.expectEqual(@as(u8, 'Q'), @intFromEnum(FrontendTag.query));
    try std.testing.expectEqual(@as(u8, 'X'), @intFromEnum(FrontendTag.terminate));
    try std.testing.expectEqual(@as(u8, 'P'), @intFromEnum(FrontendTag.parse));
}

test "backend tags match expected byte values" {
    try std.testing.expectEqual(@as(u8, 'R'), @intFromEnum(BackendTag.auth_request));
    try std.testing.expectEqual(@as(u8, 'Z'), @intFromEnum(BackendTag.ready_for_query));
    try std.testing.expectEqual(@as(u8, 'T'), @intFromEnum(BackendTag.row_description));
    try std.testing.expectEqual(@as(u8, 'D'), @intFromEnum(BackendTag.data_row));
    try std.testing.expectEqual(@as(u8, 'C'), @intFromEnum(BackendTag.command_complete));
    try std.testing.expectEqual(@as(u8, 'E'), @intFromEnum(BackendTag.error_response));
}
