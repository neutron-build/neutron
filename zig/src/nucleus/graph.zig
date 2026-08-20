// Nucleus Graph Model — SQL generation + execution for GRAPH_* functions
//
// SQL functions: GRAPH_ADD_NODE, GRAPH_ADD_EDGE, GRAPH_DELETE_NODE,
// GRAPH_DELETE_EDGE, GRAPH_QUERY, GRAPH_NEIGHBORS, GRAPH_SHORTEST_PATH,
// GRAPH_NODE_COUNT, GRAPH_EDGE_COUNT.

const std = @import("std");
const NucleusClient = @import("client.zig").NucleusClient;

pub const GraphModel = struct {
    client: *NucleusClient,

    // ── SQL generators ───────────────────────────────────────────

    pub fn addNodeSql(label: []const u8, props_json: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT GRAPH_ADD_NODE('{s}', '{s}')", .{ label, props_json }) catch return error.BufferTooShort;
    }

    // Node and edge ids are i64: GRAPH_ADD_NODE answers with an integer id
    // and GRAPH_ADD_EDGE rejects anything else ("expected integer, got
    // \"zz1\"", measured live). The string-typed signatures generated SQL
    // that the engine never accepted.

    pub fn addEdgeSql(from_id: i64, to_id: i64, edge_type: []const u8, props_json: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT GRAPH_ADD_EDGE({d}, {d}, '{s}', '{s}')", .{ from_id, to_id, edge_type, props_json }) catch return error.BufferTooShort;
    }

    pub fn deleteNodeSql(id: i64, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT GRAPH_DELETE_NODE({d})", .{id}) catch return error.BufferTooShort;
    }

    pub fn deleteEdgeSql(id: i64, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT GRAPH_DELETE_EDGE({d})", .{id}) catch return error.BufferTooShort;
    }

    pub fn graphQuerySql(cypher: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT GRAPH_QUERY('{s}')", .{cypher}) catch return error.BufferTooShort;
    }

    pub fn neighborsSql(id: i64, direction: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT GRAPH_NEIGHBORS({d}, '{s}')", .{ id, direction }) catch return error.BufferTooShort;
    }

    pub fn shortestPathSql(from_id: i64, to_id: i64, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT GRAPH_SHORTEST_PATH({d}, {d})", .{ from_id, to_id }) catch return error.BufferTooShort;
    }

    pub fn nodeCountSql(buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT GRAPH_NODE_COUNT()", .{}) catch return error.BufferTooShort;
    }

    pub fn edgeCountSql(buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT GRAPH_EDGE_COUNT()", .{}) catch return error.BufferTooShort;
    }

    // ── Execution methods ────────────────────────────────────────

    pub fn addNode(self: GraphModel, label: []const u8, props_json: []const u8) !?[]const u8 {
        var buf: [1024]u8 = undefined;
        const sql = try addNodeSql(label, props_json, &buf);
        return try self.client.executeModel(sql);
    }

    pub fn addEdge(self: GraphModel, from_id: i64, to_id: i64, edge_type: []const u8, props_json: []const u8) !?[]const u8 {
        var buf: [1024]u8 = undefined;
        const sql = try addEdgeSql(from_id, to_id, edge_type, props_json, &buf);
        return try self.client.executeModel(sql);
    }

    pub fn deleteNode(self: GraphModel, id: i64) !?[]const u8 {
        var buf: [256]u8 = undefined;
        const sql = try deleteNodeSql(id, &buf);
        return try self.client.executeModel(sql);
    }

    pub fn deleteEdge(self: GraphModel, id: i64) !?[]const u8 {
        var buf: [256]u8 = undefined;
        const sql = try deleteEdgeSql(id, &buf);
        return try self.client.executeModel(sql);
    }

    pub fn graphQuery(self: GraphModel, cypher: []const u8) !?[]const u8 {
        var buf: [4096]u8 = undefined;
        const sql = try graphQuerySql(cypher, &buf);
        return try self.client.executeModel(sql);
    }

    pub fn neighbors(self: GraphModel, id: i64, direction: []const u8) !?[]const u8 {
        var buf: [512]u8 = undefined;
        const sql = try neighborsSql(id, direction, &buf);
        return try self.client.executeModel(sql);
    }

    pub fn shortestPath(self: GraphModel, from_id: i64, to_id: i64) !?[]const u8 {
        var buf: [512]u8 = undefined;
        const sql = try shortestPathSql(from_id, to_id, &buf);
        return try self.client.executeModel(sql);
    }

    pub fn nodeCount(self: GraphModel) !?[]const u8 {
        var buf: [256]u8 = undefined;
        const sql = try nodeCountSql(&buf);
        return try self.client.executeModel(sql);
    }

    pub fn edgeCount(self: GraphModel) !?[]const u8 {
        var buf: [256]u8 = undefined;
        const sql = try edgeCountSql(&buf);
        return try self.client.executeModel(sql);
    }
};

// ── Tests ─────────────────────────────────────────────────────

test "GRAPH_ADD_NODE sql" {
    var buf: [512]u8 = undefined;
    const sql = try GraphModel.addNodeSql("Person", "{\"name\":\"Alice\",\"age\":30}", &buf);
    try std.testing.expectEqualStrings("SELECT GRAPH_ADD_NODE('Person', '{\"name\":\"Alice\",\"age\":30}')", sql);
}

test "GRAPH_ADD_EDGE sql" {
    var buf: [512]u8 = undefined;
    const sql = try GraphModel.addEdgeSql(1, 2, "KNOWS", "{\"since\":2020}", &buf);
    try std.testing.expectEqualStrings("SELECT GRAPH_ADD_EDGE(1, 2, 'KNOWS', '{\"since\":2020}')", sql);
}

test "GRAPH_DELETE_NODE sql" {
    var buf: [256]u8 = undefined;
    const sql = try GraphModel.deleteNodeSql(99, &buf);
    try std.testing.expectEqualStrings("SELECT GRAPH_DELETE_NODE(99)", sql);
}

test "GRAPH_QUERY sql" {
    var buf: [512]u8 = undefined;
    const sql = try GraphModel.graphQuerySql("MATCH (n:Person)-[:KNOWS]->(m) RETURN m", &buf);
    try std.testing.expectEqualStrings("SELECT GRAPH_QUERY('MATCH (n:Person)-[:KNOWS]->(m) RETURN m')", sql);
}

test "GRAPH_NEIGHBORS sql" {
    var buf: [256]u8 = undefined;
    const sql = try GraphModel.neighborsSql(1, "out", &buf);
    try std.testing.expectEqualStrings("SELECT GRAPH_NEIGHBORS(1, 'out')", sql);
}

test "GRAPH_SHORTEST_PATH sql" {
    var buf: [256]u8 = undefined;
    const sql = try GraphModel.shortestPathSql(1, 50, &buf);
    try std.testing.expectEqualStrings("SELECT GRAPH_SHORTEST_PATH(1, 50)", sql);
}

test "GRAPH_NODE_COUNT sql" {
    var buf: [256]u8 = undefined;
    const sql = try GraphModel.nodeCountSql(&buf);
    try std.testing.expectEqualStrings("SELECT GRAPH_NODE_COUNT()", sql);
}

test "GRAPH_EDGE_COUNT sql" {
    var buf: [256]u8 = undefined;
    const sql = try GraphModel.edgeCountSql(&buf);
    try std.testing.expectEqualStrings("SELECT GRAPH_EDGE_COUNT()", sql);
}
