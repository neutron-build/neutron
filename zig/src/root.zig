// Neutron Zig — Composable 4-layer systems library
//
// Layer 0: Wire codecs (zero-alloc, freestanding)
// Layer 1: HAL networking (std.Io based)
// Layer 2: Protocol servers
// Layer 3: Application framework
//
// Feature flags: pass -Dlayer1=false, -Dlayer2=false, -Dlayer3=false,
// -Dnucleus=false to cherry-pick layers at build time.

const build_options = @import("build_options");

// Layer 0 — always available (zero-alloc, freestanding)
pub const pgwire = @import("layer0/pgwire/codec.zig");
pub const http = @import("layer0/http/parser.zig");
pub const websocket = @import("layer0/websocket/frame.zig");
pub const binary = struct {
    pub const varint = @import("layer0/binary/varint.zig");
    pub const endian = @import("layer0/binary/endian.zig");
};

// Re-export commonly used Layer 0 types
pub const FrontendMessage = pgwire.FrontendTag;
pub const BackendMessage = pgwire.BackendMessage;
pub const QueryType = pgwire.QueryType;

// Layer 1 — HAL networking (requires std)
pub const io = if (build_options.enable_layer1) @import("layer1/io.zig") else struct {};
pub const tcp = if (build_options.enable_layer1) @import("layer1/tcp.zig") else struct {};
pub const pool = if (build_options.enable_layer1) @import("layer1/pool.zig") else struct {};
pub const timer = if (build_options.enable_layer1) @import("layer1/timer.zig") else struct {};

// Re-export commonly used Layer 1 types (conditionally)
pub const TcpListener = if (build_options.enable_layer1) tcp.TcpListener else void;
pub const TcpStream = if (build_options.enable_layer1) tcp.TcpStream else void;
pub const ConnectionPool = if (build_options.enable_layer1) pool.ConnectionPool else void;
pub const Deadline = if (build_options.enable_layer1) timer.Deadline else void;

// Layer 2 — Protocol servers
pub const http_server = if (build_options.enable_layer2) @import("layer2/http_server.zig") else struct {};
pub const pg_client = if (build_options.enable_layer2) @import("layer2/pg_client.zig") else struct {};
pub const ws_server = if (build_options.enable_layer2) @import("layer2/ws_server.zig") else struct {};
pub const static = if (build_options.enable_layer2) @import("layer2/static.zig") else struct {};

// Re-export commonly used Layer 2 types (conditionally)
pub const HttpServer = if (build_options.enable_layer2) http_server.HttpServer else void;
pub const RequestContext = if (build_options.enable_layer2) http_server.RequestContext else void;
pub const PgClient = if (build_options.enable_layer2) pg_client.PgClient else void;

// Nucleus multi-model client
pub const nucleus = if (build_options.enable_nucleus) @import("nucleus/client.zig") else struct {};
pub const NucleusClient = if (build_options.enable_nucleus) nucleus.NucleusClient else void;

// Nucleus data models (conditionally available)
pub const nucleus_kv = if (build_options.enable_nucleus) @import("nucleus/kv.zig") else struct {};
pub const nucleus_vector = if (build_options.enable_nucleus) @import("nucleus/vector.zig") else struct {};
pub const nucleus_timeseries = if (build_options.enable_nucleus) @import("nucleus/timeseries.zig") else struct {};
pub const nucleus_document = if (build_options.enable_nucleus) @import("nucleus/document.zig") else struct {};
pub const nucleus_fts = if (build_options.enable_nucleus) @import("nucleus/fts.zig") else struct {};
pub const nucleus_graph = if (build_options.enable_nucleus) @import("nucleus/graph.zig") else struct {};
pub const nucleus_geo = if (build_options.enable_nucleus) @import("nucleus/geo.zig") else struct {};
pub const nucleus_blob = if (build_options.enable_nucleus) @import("nucleus/blob.zig") else struct {};
pub const nucleus_streams = if (build_options.enable_nucleus) @import("nucleus/streams.zig") else struct {};
pub const nucleus_pubsub = if (build_options.enable_nucleus) @import("nucleus/pubsub.zig") else struct {};
pub const nucleus_columnar = if (build_options.enable_nucleus) @import("nucleus/columnar.zig") else struct {};
pub const nucleus_datalog = if (build_options.enable_nucleus) @import("nucleus/datalog.zig") else struct {};
pub const nucleus_cdc = if (build_options.enable_nucleus) @import("nucleus/cdc.zig") else struct {};
pub const nucleus_sql = if (build_options.enable_nucleus) @import("nucleus/sql.zig") else struct {};
pub const nucleus_tx = if (build_options.enable_nucleus) @import("nucleus/tx.zig") else struct {};

// Layer 3 — Application framework
pub const app = if (build_options.enable_layer3) @import("layer3/app.zig") else struct {};
pub const router = if (build_options.enable_layer3) @import("layer3/router.zig") else struct {};
pub const middleware = if (build_options.enable_layer3) @import("layer3/middleware.zig") else struct {};
pub const app_error = if (build_options.enable_layer3) @import("layer3/error.zig") else struct {};
pub const openapi = if (build_options.enable_layer3) @import("layer3/openapi.zig") else struct {};
pub const config = if (build_options.enable_layer3) @import("layer3/config.zig") else struct {};
pub const lifecycle = if (build_options.enable_layer3) @import("layer3/lifecycle.zig") else struct {};
pub const respond = if (build_options.enable_layer3) @import("layer3/respond.zig") else struct {};
pub const json = if (build_options.enable_layer3) @import("layer3/json.zig") else struct {};
pub const handler = if (build_options.enable_layer3) @import("layer3/handler.zig") else struct {};

// Re-export commonly used Layer 3 types (conditionally)
pub const App = if (build_options.enable_layer3) app.App else void;
pub const Router = if (build_options.enable_layer3) router.Router else void;
pub const Route = if (build_options.enable_layer3) router.Route else void;
pub const Middleware = if (build_options.enable_layer3) middleware.Middleware else void;
pub const AppError = if (build_options.enable_layer3) app_error.AppError else void;
pub const Config = if (build_options.enable_layer3) config.Config else void;

test {
    // Layer 0 tests — always run
    _ = pgwire;
    _ = @import("layer0/pgwire/types.zig");
    _ = @import("layer0/pgwire/reader.zig");
    _ = @import("layer0/pgwire/auth.zig");
    _ = http;
    _ = websocket;
    _ = @import("layer0/websocket/mask.zig");
    _ = binary.varint;
    _ = binary.endian;

    // Layer 1 tests — gated on feature flag
    if (build_options.enable_layer1) {
        _ = @import("layer1/io.zig");
        _ = @import("layer1/tcp.zig");
        _ = @import("layer1/pool.zig");
        _ = @import("layer1/timer.zig");
    }

    // Layer 2 tests — gated on feature flag
    if (build_options.enable_layer2) {
        _ = @import("layer2/http_server.zig");
        _ = @import("layer2/pg_client.zig");
        _ = @import("layer2/ws_server.zig");
        _ = @import("layer2/static.zig");
    }

    // Nucleus client + data model tests — gated on feature flag
    if (build_options.enable_nucleus) {
        _ = @import("nucleus/client.zig");
        _ = @import("nucleus/kv.zig");
        _ = @import("nucleus/vector.zig");
        _ = @import("nucleus/timeseries.zig");
        _ = @import("nucleus/document.zig");
        _ = @import("nucleus/fts.zig");
        _ = @import("nucleus/graph.zig");
        _ = @import("nucleus/geo.zig");
        _ = @import("nucleus/blob.zig");
        _ = @import("nucleus/streams.zig");
        _ = @import("nucleus/pubsub.zig");
        _ = @import("nucleus/columnar.zig");
        _ = @import("nucleus/datalog.zig");
        _ = @import("nucleus/cdc.zig");
        _ = @import("nucleus/sql.zig");
        _ = @import("nucleus/tx.zig");
    }

    // Layer 3 tests — gated on feature flag
    if (build_options.enable_layer3) {
        _ = @import("layer3/app.zig");
        _ = @import("layer3/router.zig");
        _ = @import("layer3/middleware.zig");
        _ = @import("layer3/error.zig");
        _ = @import("layer3/openapi.zig");
        _ = @import("layer3/config.zig");
        _ = @import("layer3/lifecycle.zig");
        _ = @import("layer3/respond.zig");
        _ = @import("layer3/json.zig");
        _ = @import("layer3/handler.zig");
    }
}
