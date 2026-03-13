// Nucleus Transaction — wraps a connection with BEGIN/COMMIT/ROLLBACK
//
// Provides model accessors that operate within the transaction scope.

const std = @import("std");
const NucleusClient = @import("client.zig").NucleusClient;

pub const Transaction = struct {
    client: *NucleusClient,
    committed: bool = false,
    rolled_back: bool = false,

    /// Commit the transaction.
    pub fn commit(self: *Transaction) !void {
        if (self.committed or self.rolled_back) return;
        _ = try self.client.exec("COMMIT");
        self.committed = true;
    }

    /// Rollback the transaction.
    pub fn rollback(self: *Transaction) !void {
        if (self.committed or self.rolled_back) return;
        _ = try self.client.exec("ROLLBACK");
        self.rolled_back = true;
    }

    /// Check if the transaction is still active.
    pub fn isActive(self: *const Transaction) bool {
        return !self.committed and !self.rolled_back;
    }

    // ── Model accessors within this transaction ──────────────────

    pub fn sql(self: *Transaction) @import("sql.zig").SqlModel {
        return .{ .client = self.client };
    }

    pub fn kv(self: *Transaction) @import("kv.zig").KVModel {
        return .{ .client = self.client };
    }

    pub fn vector(self: *Transaction) @import("vector.zig").VectorModel {
        return .{ .client = self.client };
    }

    pub fn timeseries(self: *Transaction) @import("timeseries.zig").TimeSeriesModel {
        return .{ .client = self.client };
    }

    pub fn document(self: *Transaction) @import("document.zig").DocumentModel {
        return .{ .client = self.client };
    }

    pub fn fts(self: *Transaction) @import("fts.zig").FTSModel {
        return .{ .client = self.client };
    }

    pub fn graph(self: *Transaction) @import("graph.zig").GraphModel {
        return .{ .client = self.client };
    }

    pub fn geo(self: *Transaction) @import("geo.zig").GeoModel {
        return .{ .client = self.client };
    }

    pub fn blob(self: *Transaction) @import("blob.zig").BlobModel {
        return .{ .client = self.client };
    }

    pub fn streams(self: *Transaction) @import("streams.zig").StreamsModel {
        return .{ .client = self.client };
    }

    pub fn pubsub(self: *Transaction) @import("pubsub.zig").PubSubModel {
        return .{ .client = self.client };
    }

    pub fn columnar(self: *Transaction) @import("columnar.zig").ColumnarModel {
        return .{ .client = self.client };
    }

    pub fn datalog(self: *Transaction) @import("datalog.zig").DatalogModel {
        return .{ .client = self.client };
    }

    pub fn cdc(self: *Transaction) @import("cdc.zig").CdcModel {
        return .{ .client = self.client };
    }
};

// ── Tests ─────────────────────────────────────────────────────

test "Transaction: initial state" {
    // Verify struct layout (cannot test execution without a live DB)
    var tx = Transaction{ .client = undefined };
    try std.testing.expect(tx.isActive());
    tx.committed = true;
    try std.testing.expect(!tx.isActive());
}

test "Transaction: rollback flag" {
    var tx = Transaction{ .client = undefined };
    try std.testing.expect(tx.isActive());
    tx.rolled_back = true;
    try std.testing.expect(!tx.isActive());
}
