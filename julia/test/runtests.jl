using Test
using NeutronJulia

# ── Helpers ───────────────────────────────────────────────────────────────────

nucleus_features() = NucleusFeatures(true, "1.0.0",
    true, true, true, true, true, true, true, true,
    true, true, true, true, true)

plain_pg_features() = NucleusFeatures(false, nothing,
    false, false, false, false, false, false, false, false,
    false, false, false, false, false)

# Integration test URL — set to skip/enable DB tests
const TEST_URL = get(ENV, "NUCLEUS_TEST_URL", get(ENV, "POSTGRES_TEST_URL", ""))
const TEST_NUCLEUS = parse(Bool, get(ENV, "NUCLEUS_TEST_NUCLEUS", "false"))

# ── Unit Tests (no DB required) ───────────────────────────────────────────────

@testset "NeutronJulia" begin

@testset "Error types" begin
    e = NucleusError("https://neutron.dev/errors/not-found", "Not Found", 404, "key missing")
    @test e.status == 404
    @test e.title == "Not Found"
    @test occursin("not-found", e.type)
    @test e.detail == "key missing"
    @test e isa Exception

    ne = NotNucleusError("KV", "KV requires Nucleus")
    @test ne.model == "KV"
    @test ne isa Exception

    buf = IOBuffer()
    showerror(buf, e)
    @test occursin("404", String(take!(buf)))

    buf2 = IOBuffer()
    showerror(buf2, ne)
    @test occursin("KV", String(take!(buf2)))
end

@testset "NucleusFeatures" begin
    f = nucleus_features()
    @test f.is_nucleus == true
    @test f.version == "1.0.0"
    @test f.has_kv == true
    @test f.has_vector == true
    @test f.has_timeseries == true

    g = plain_pg_features()
    @test g.is_nucleus == false
    @test g.version === nothing
    @test g.has_kv == false
end

@testset "require_nucleus guard" begin
    f = plain_pg_features()
    @test_throws NotNucleusError require_nucleus(f, "KV")
    @test_throws NotNucleusError require_nucleus(f, "Vector")

    nf = nucleus_features()
    @test (require_nucleus(nf, "KV"); true)
end

@testset "to_vector_literal" begin
    lit = to_vector_literal([1.0, 2.0, 3.0])
    @test lit == "[1.0,2.0,3.0]"

    lit32 = to_vector_literal(Float32[0.1, 0.2, 0.5])
    @test startswith(lit32, "[")
    @test endswith(lit32, "]")
    @test length(split(lit32[2:end-1], ",")) == 3

    @test to_vector_literal(Float64[]) == "[]"
end

@testset "Value types" begin
    pt = TimeSeriesPoint(Int64(1_700_000_000_000), 23.5)
    @test pt.timestamp_ms == 1_700_000_000_000
    @test pt.value == 23.5

    sr = SearchResult(42, 0.12, Dict{String,Any}())
    @test sr.id == 42
    @test sr.distance ≈ 0.12

    r = FTSResult(Int64(7), 0.95)
    @test r.doc_id == 7
    @test r.score ≈ 0.95

    p = GeoPoint(37.7749, -122.4194)
    @test p.lat ≈ 37.7749
    @test p.lon ≈ -122.4194

    n = GraphNode(Int64(1), "Person", Dict{String,Any}("name" => "Alice"))
    @test n.id == 1
    @test n.label == "Person"

    edge = GraphEdge(Int64(10), Int64(1), Int64(2), "KNOWS", Dict{String,Any}())
    @test edge.edge_type == "KNOWS"
end

@testset "Enums" begin
    @test L2 isa DistanceMetric
    @test Cosine isa DistanceMetric
    @test InnerProduct isa DistanceMetric
    @test L2 != Cosine

    @test GraphOut isa GraphDirection
    @test GraphIn  isa GraphDirection
    @test GraphBoth isa GraphDirection
end

@testset "Model struct field names" begin
    @test fieldnames(SQLModel)       == (:conn,)
    @test fieldnames(KVModel)        == (:conn, :features)
    @test fieldnames(VectorModel)    == (:conn, :features)
    @test fieldnames(TimeSeriesModel)== (:conn, :features)
    @test fieldnames(DocumentModel)  == (:conn, :features)
    @test fieldnames(GraphModel)     == (:conn, :features)
    @test fieldnames(FTSModel)       == (:conn, :features)
    @test fieldnames(GeoModel)       == (:conn, :features)
    @test fieldnames(BlobModel)      == (:conn, :features)
    @test fieldnames(StreamsModel)   == (:conn, :features)
    @test fieldnames(ColumnarModel)  == (:conn, :features)
    @test fieldnames(DatalogModel)   == (:conn, :features)
    @test fieldnames(CDCModel)       == (:conn, :features)
    @test fieldnames(PubSubModel)    == (:conn, :features)
end

@testset "NucleusClient / NucleusTransaction field names" begin
    @test :conn     in fieldnames(NucleusClient)
    @test :features in fieldnames(NucleusClient)
    @test :url      in fieldnames(NucleusClient)
    @test :conn     in fieldnames(NucleusTransaction)
    @test :active   in fieldnames(NucleusTransaction)
end

@testset "ConnectionPool struct" begin
    @test fieldnames(ConnectionPool) == (:url, :channel, :size)
    @test ConnectionPool <: Any  # type exists and is exported
end

end # @testset "NeutronJulia"

# ── Integration Tests (require NUCLEUS_TEST_URL or POSTGRES_TEST_URL) ─────────

if isempty(TEST_URL)
    @info "Skipping integration tests — set NUCLEUS_TEST_URL or POSTGRES_TEST_URL to enable"
else
    @testset "Integration: connect + feature detection" begin
        client = NeutronJulia.connect(TEST_URL)
        @test client isa NucleusClient
        @test client.features isa NucleusFeatures
        @test isopen(client)
        close(client)
    end

    @testset "Integration: SQL model" begin
        client = NeutronJulia.connect(TEST_URL)
        s = sql(client)
        rows = query(s, "SELECT 1 AS n")
        @test rows.n == [1]

        one = query_one(s, "SELECT 42 AS x")
        @test one.x == 42

        @test_throws NucleusError query_one(s, "SELECT 1 WHERE false")
        close(client)
    end

    if TEST_NUCLEUS
        @testset "Integration: KV model (Nucleus only)" begin
            client = NeutronJulia.connect(TEST_URL)
            k = kv(client)

            set!(k, "test:key", "hello")
            @test get(k, "test:key") == "hello"
            @test exists(k, "test:key") == true

            set!(k, "test:counter", "0")
            @test incr!(k, "test:counter") == 1

            @test delete!(k, "test:key") == true
            @test get(k, "test:key") === nothing

            rpush!(k, "test:list", "a"); rpush!(k, "test:list", "b")
            @test llen(k, "test:list") == 2
            @test lrange(k, "test:list", 0, -1) == ["a", "b"]

            hset!(k, "test:hash", "field1", "val1")
            @test hget(k, "test:hash", "field1") == "val1"
            @test hexists(k, "test:hash", "field1") == true

            sadd!(k, "test:set", "member1")
            @test sismember(k, "test:set", "member1") == true
            @test scard(k, "test:set") == 1

            pfadd!(k, "test:hll", "item1"); pfadd!(k, "test:hll", "item2")
            @test pfcount(k, "test:hll") >= 1

            flushdb!(k)
            close(client)
        end

        @testset "Integration: TimeSeries model (Nucleus only)" begin
            client = NeutronJulia.connect(TEST_URL)
            ts = timeseries(client)
            now_ms = Int64(round(time() * 1000))
            series = "test:ts:$(now_ms)"

            insert!(ts, series, now_ms, 42.0)
            @test ts_count(ts, series) >= 1
            @test last_value(ts, series) ≈ 42.0

            t2, t3 = now_ms + 1000, now_ms + 2000
            insert!(ts, series, [t2, t3], [43.0, 44.0])
            @test ts_count(ts, series) >= 3
            close(client)
        end

        @testset "Integration: Transaction (Nucleus only)" begin
            client = NeutronJulia.connect(TEST_URL)
            k = kv(client)

            transaction(client) do tx
                set!(kv(tx), "tx:inside", "committed")
            end
            @test get(k, "tx:inside") == "committed"

            @test_throws Exception transaction(client) do tx
                set!(kv(tx), "tx:will_rollback", "value")
                error("forced rollback")
            end
            @test get(k, "tx:will_rollback") === nothing

            flushdb!(k)
            close(client)
        end
    end
end
