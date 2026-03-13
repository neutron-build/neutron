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

    pub fn addEdgeSql(from_id: []const u8, to_id: []const u8, edge_type: []const u8, props_json: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT GRAPH_ADD_EDGE('{s}', '{s}', '{s}', '{s}')", .{ from_id, to_id, edge_type, props_json }) catch return error.BufferTooShort;
    }

    pub fn deleteNodeSql(id: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT GRAPH_DELETE_NODE('{s}')", .{id}) catch return error.BufferTooShort;
    }

    pub fn deleteEdgeSql(id: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT GRAPH_DELETE_EDGE('{s}')", .{id}) catch return error.BufferTooShort;
    }

    pub fn graphQuerySql(cypher: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT GRAPH_QUERY('{s}')", .{cypher}) catch return error.BufferTooShort;
    }

    pub fn neighborsSql(id: []const u8, direction: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT GRAPH_NEIGHBORS('{s}', '{s}')", .{ id, direction }) catch return error.BufferTooShort;
    }

    pub fn shortestPathSql(from_id: []const u8, to_id: []const u8, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "SELECT GRAPH_SHORTEST_PATH('{s}', '{s}')", .{ from_id, to_id }) catch return error.BufferTooShort;
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
        return try self.client.execute(sql);
    }

    pub fn addEdge(self: GraphModel, from_id: []const u8, to_id: []const u8, edge_type: []const u8, props_json: []const u8) !?[]const u8 {
        var buf: [1024]u8 = undefined;
        const sql = try addEdgeSql(from_id, to_id, edge_type, props_json, &buf);
        return try self.client.execute(sql);
    }

    pub fn deleteNode(self: GraphModel, id: []const u8) !?[]const u8 {
        var buf: [256]u8 = undefined;
        const sql = try deleteNodeSql(id, &buf);
        return try self.client.execute(sql);
    }

    pub fn deleteEdge(self: GraphModel, id: []const u8) !?[]const u8 {
        var buf: [256]u8 = undefined;
        const sql = try deleteEdgeSql(id, &buf);
        return try self.client.execute(sql);
    }

    pub fn graphQuery(self: GraphModel, cypher: []const u8) !?[]const u8 {
        var buf: [4096]u8 = undefined;
        const sql = try graphQuerySql(cypher, &buf);
        return try self.client.execute(sql);
    }

    pub fn neighbors(self: GraphModel, id: []const u8, direction: []const u8) !?[]const u8 {
        var buf: [512]u8 = undefined;
        const sql = try neighborsSql(id, direction, &buf);
        return try self.client.execute(sql);
    }

    pub fn shortestPath(self: GraphModel, from_id: []const u8, to_id: []const u8) !?[]const u8 {
        var buf: [512]u8 = undefined;
        const sql = try shortestPathSql(from_id, to_id, &buf);
        return try self.client.execute(sql);
    }

    pub fn nodeCount(self: GraphModel) !?[]const u8 {
        var buf: [256]u8 = undefined;
        const sql = try nodeCountSql(&buf);
        return try self.client.execute(sql);
    }

    pub fn edgeCount(self: GraphModel) !?[]const u8 {
        var buf: [256]u8 = undefined;
        const sql = try edgeCountSql(&buf);
        return try self.client.execute(sql);
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
    const sql = try GraphModel.addEdgeSql("node-1", "node-2", "KNOWS", "{\"since\":2020}", &buf);
    try std.testing.expectEqualStrings("SELECT GRAPH_ADD_EDGE('node-1', 'node-2', 'KNOWS', '{\"since\":2020}')", sql);
}

test "GRAPH_DELETE_NODE sql" {
    var buf: [256]u8 = undefined;
    const sql = try GraphModel.deleteNodeSql("node-99", &buf);
    try std.testing.expectEqualStrings("SELECT GRAPH_DELETE_NODE('node-99')", sql);
}

test "GRAPH_QUERY sql" {
    var buf: [512]u8 = undefined;
    const sql = try GraphModel.graphQuerySql("MATCH (n:Person)-[:KNOWS]->(m) RETURN m", &buf);
    try std.testing.expectEqualStrings("SELECT GRAPH_QUERY('MATCH (n:Person)-[:KNOWS]->(m) RETURN m')", sql);
}

test "GRAPH_NEIGHBORS sql" {
    var buf: [256]u8 = undefined;
    const sql = try GraphModel.neighborsSql("node-1", "out", &buf);
    try std.testing.expectEqualStrings("SELECT GRAPH_NEIGHBORS('node-1', 'out')", sql);
}

test "GRAPH_SHORTEST_PATH sql" {
    var buf: [256]u8 = undefined;
    const sql = try GraphModel.shortestPathSql("node-1", "node-50", &buf);
    try std.testing.expectEqualStrings("SELECT GRAPH_SHORTEST_PATH('node-1', 'node-50')", sql);
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
