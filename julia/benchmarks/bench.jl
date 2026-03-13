"""
NeutronJulia Benchmarks

Measures throughput for the most performance-sensitive operations:
- TimeSeries: single-point insert vs batch insert
- Vector: literal generation (to_vector_literal)
- KV: get/set round-trip
- ConnectionPool: overhead vs bare connection

Run:
    julia --project=. benchmarks/bench.jl [postgres-url]

If no URL is provided, only the pure-Julia (no-DB) benchmarks run.
"""

using NeutronJulia

const URL = get(ARGS, 1, get(ENV, "NUCLEUS_TEST_URL", ""))

# ── Pure Julia benchmarks (no DB needed) ─────────────────────────────────────

println("=" ^ 60)
println("NeutronJulia Benchmarks")
println("=" ^ 60)

println("\n[1] to_vector_literal — generating VECTOR(...) SQL literal")
for dim in [64, 256, 1024, 4096]
    v = rand(Float32, dim)
    # warmup
    for _ in 1:100; to_vector_literal(v); end
    # time
    t = @elapsed for _ in 1:10_000; to_vector_literal(v); end
    throughput = 10_000 / t
    println("  dim=$dim: $(round(throughput; digits=0)) calls/s  ($(round(t*1000; digits=1)) ms / 10k)")
end

println("\n[2] Feature detection struct construction")
let
    # warmup
    for _ in 1:1000
        NucleusFeatures(true, "1.0", true, true, true, true, true, true, true,
                        true, true, true, true, true, true)
    end
    t = @elapsed for _ in 1:100_000
        NucleusFeatures(true, "1.0", true, true, true, true, true, true, true,
                        true, true, true, true, true, true)
    end
    println("  $(round(100_000 / t; digits=0)) constructions/s")
end

println("\n[3] TimeSeriesPoint batch construction (1000 points)")
let
    ts_ms = Int64.(1:1000) .* 1000
    vals  = rand(Float64, 1000)
    for _ in 1:100
        [TimeSeriesPoint(t, v) for (t, v) in zip(ts_ms, vals)]
    end
    t = @elapsed for _ in 1:1000
        [TimeSeriesPoint(t, v) for (t, v) in zip(ts_ms, vals)]
    end
    println("  $(round(1000 / t; digits=0)) batches/s  ($(round(t * 1e6 / 1000; digits=1)) µs/batch)")
end

# ── Database benchmarks (require URL) ────────────────────────────────────────

if isempty(URL)
    println("\nSkipping database benchmarks — set NUCLEUS_TEST_URL to enable")
else
    println("\n[4] Connecting and feature detection")
    t = @elapsed begin
        client = NeutronJulia.connect(URL)
    end
    println("  connect(): $(round(t * 1000; digits=1)) ms")
    println("  is_nucleus: $(client.features.is_nucleus)")

    println("\n[5] SQL round-trip latency (SELECT 1)")
    s = sql(client)
    for _ in 1:5; query(s, "SELECT 1"); end   # warmup
    t = @elapsed for _ in 1:200; query(s, "SELECT 1"); end
    println("  $(round(200 / t; digits=0)) queries/s  ($(round(t * 1000 / 200; digits=2)) ms/query avg)")

    if client.features.is_nucleus
        println("\n[6] KV set/get round-trip")
        k = kv(client)
        for _ in 1:10; kv_set!(k, "bench:key", "value"); kv_get(k, "bench:key"); end
        t = @elapsed for i in 1:500
            kv_set!(k, "bench:key:$i", "value$i")
            kv_get(k, "bench:key:$i")
        end
        println("  $(round(500 / t; digits=0)) set+get pairs/s  ($(round(t * 1000 / 500; digits=2)) ms/pair avg)")

        println("\n[7] TimeSeries single-point insert throughput")
        ts = timeseries(client)
        series = "bench:ts:$(round(Int, time()))"
        t0 = Int64(round(time() * 1000))
        for _ in 1:5; Base.insert!(ts, series * ":warmup", t0, 1.0); end
        t = @elapsed for i in 1:200
            Base.insert!(ts, series, t0 + i, Float64(i))
        end
        println("  $(round(200 / t; digits=0)) inserts/s  ($(round(t * 1000 / 200; digits=2)) ms/insert avg)")

        println("\n[8] TimeSeries batch insert (1000 points)")
        pts = [TimeSeriesPoint(t0 + Int64(i * 100), Float64(i)) for i in 1:1000]
        t = @elapsed Base.insert!(ts, series * ":batch", pts)
        println("  1000 points in $(round(t * 1000; digits=1)) ms  ($(round(1000 / t; digits=0)) points/s)")
    end

    println("\n[9] ConnectionPool vs bare connection")
    pool = ConnectionPool(URL; size=4)
    # Pool overhead
    for _ in 1:10; with_pool(pool) do c; query(sql(c), "SELECT 1"); end; end
    t_pool = @elapsed for _ in 1:100
        with_pool(pool) do c; query(sql(c), "SELECT 1"); end
    end
    # Bare connection
    bare = NeutronJulia.connect(URL)
    for _ in 1:10; query(sql(bare), "SELECT 1"); end
    t_bare = @elapsed for _ in 1:100
        query(sql(bare), "SELECT 1")
    end
    println("  Pool:           $(round(t_pool * 1000 / 100; digits=3)) ms/query")
    println("  Bare conn:      $(round(t_bare * 1000 / 100; digits=3)) ms/query")
    println("  Pool overhead:  $(round((t_pool - t_bare) * 1e6 / 100; digits=1)) µs/acquire+release")

    close(pool)
    close(bare)
    close(client)
end

println("\n" * "=" ^ 60)
println("Benchmarks complete.")
println("=" ^ 60)
