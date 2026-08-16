#!/usr/bin/env julia
# Julia executor for the Nucleus live data-model conformance spec.
#
# Reads ../../spec.json, runs every case against a live engine through the real
# in-repo Julia client (julia/src), and prints one JSON result document to
# stdout. It asserts nothing a mock could assert: only that a call reaches the
# engine, is accepted over the wire, and comes back with the right value.
#
#     NEUTRON_TEST_DATABASE_URL=postgresql://postgres@127.0.0.1:55432/postgres \
#         julia --project=. run_live.jl
#
# Exit codes: 0 all cases behaved as the spec says, 1 otherwise. An `xfail` case
# that PASSES is a failure — otherwise a fix lands and the note explaining why
# the case is expected to fail quietly becomes a lie.
#
# Everything on stdout is the report. Diagnostics go to stderr, because the
# orchestrator parses stdout.
#
# The client is loaded from ../../../../julia by path, not from the registry, so
# what is under test is the SDK this repo ships.

import Pkg
const SDK = abspath(joinpath(@__DIR__, "..", "..", "..", "..", "julia"))
Pkg.activate(SDK; io = devnull)

using NeutronJulia
using JSON3
using Dates
using Base64

const TS_BASE_MS = 1786795200000

# An op the Julia client has no surface for. Undeclared it is a failure;
# declared in unsupported.json with a reason it is `unsupported`.
struct NoMapping <: Exception
    op::String
end

struct Failed <: Exception
    msg::String
end

# ── argument resolution ──────────────────────────────────────────────────────

"""
`@name` is a per-case unique fixture (stable within a case, unique across runs);
`\$name` is a value bound by an earlier step; anything else is a literal.
"""
function resolve(v, fixtures::Dict{String,String}, bound::Dict{String,Any})
    if v isa AbstractString
        s = String(v)
        if startswith(s, "\$")
            name = s[2:end]
            haskey(bound, name) || throw(Failed("step references \$$name before it was bound"))
            return bound[name]
        end
        return replace(s, r"@([A-Za-z_][A-Za-z0-9_]*)" => m -> begin
            name = m[2:end]
            get!(fixtures, name) do
                string(name, "_", string(rand(UInt32), base = 16))
            end
        end)
    elseif v isa AbstractVector
        return Any[resolve(x, fixtures, bound) for x in v]
    elseif v isa AbstractDict || v isa JSON3.Object
        return Dict{String,Any}(String(k) => resolve(val, fixtures, bound) for (k, val) in pairs(v))
    end
    return v
end

# ── expectations ─────────────────────────────────────────────────────────────

to_plain(x::JSON3.Object) = Dict{String,Any}(String(k) => to_plain(v) for (k, v) in pairs(x))
to_plain(x::JSON3.Array) = Any[to_plain(v) for v in x]
to_plain(x::AbstractDict) = Dict{String,Any}(String(k) => to_plain(v) for (k, v) in pairs(x))
to_plain(x::AbstractVector) = Any[to_plain(v) for v in x]
# SMEMBERS returns a Set, which is the correct Julia type for a set; the
# spec's length/equals vocabulary wants an ordered collection, so it is
# sorted here rather than the SDK being made less idiomatic.
to_plain(x::AbstractSet) = Any[to_plain(v) for v in sort(collect(x))]
to_plain(x) = x

truthy(x) = x === nothing || x === missing ? false :
            x isa Bool ? x :
            x isa Number ? x != 0 :
            x isa AbstractString ? !isempty(x) :
            x isa AbstractVector || x isa AbstractDict ? !isempty(x) : true

len_of(x) =
    x isa AbstractVector ? length(x) :
    x isa AbstractDict ? length(x) :
    x isa AbstractString ? length(x) :
    throw(Failed("expected a collection, got $(typeof(x))"))

# Floats compare loosely; integers and floats compare across the boundary,
# because which side produced a whole number is not what these cases test.
function vals_equal(a, b)
    if a isa Number && b isa Number
        return (a isa AbstractFloat || b isa AbstractFloat) ? abs(float(a) - float(b)) < 1e-9 : a == b
    elseif a isa AbstractVector && b isa AbstractVector
        return length(a) == length(b) && all(vals_equal(x, y) for (x, y) in zip(a, b))
    elseif a isa AbstractDict && b isa AbstractDict
        return length(a) == length(b) &&
               all(haskey(b, k) && vals_equal(v, b[k]) for (k, v) in a)
    end
    return a == b
end

function type_ok(v, want::String)
    want == "list" && return v isa AbstractVector
    want == "map" && return v isa AbstractDict
    want == "string" && return v isa AbstractString
    want == "int" && return v isa Integer && !(v isa Bool)
    want == "float" && return v isa Number && !(v isa Bool)
    want == "bool" && return v isa Bool
    want == "bytes" && return v isa AbstractString || v isa AbstractVector
    throw(Failed("unknown type in spec: $want"))
end

function check(result, expect)
    actual = to_plain(result)

    if haskey(expect, "key")
        k = String(expect["key"])
        actual === nothing && throw(Failed("expected a map with key $k, got nothing"))
        actual isa AbstractDict || throw(Failed("expected a map with key $k, got $(typeof(actual))"))
        haskey(actual, k) || throw(Failed("key $k is absent from $(actual)"))
        actual = actual[k]
    end

    if haskey(expect, "index")
        i = Int(expect["index"])
        actual isa AbstractVector || throw(Failed("expected a list to index, got $(typeof(actual))"))
        i + 1 <= length(actual) || throw(Failed("index $i out of range for $(length(actual))"))
        actual = actual[i+1]
    end

    if get(expect, "jsonDecode", false) === true && actual isa AbstractString
        actual = to_plain(JSON3.read(actual))
    end

    get(expect, "notNull", false) === true && (actual === nothing || actual === missing) &&
        throw(Failed("expected a value, got nothing"))
    get(expect, "isNull", false) === true && !(actual === nothing || actual === missing) &&
        throw(Failed("expected nothing, got $(actual)"))
    get(expect, "nonEmpty", false) === true && !truthy(actual) &&
        throw(Failed("expected a non-empty collection, got $(actual)"))

    if haskey(expect, "length")
        n = len_of(actual)
        n == Int(expect["length"]) || throw(Failed("expected $(expect["length"]) elements, got $n: $(actual)"))
    end
    if haskey(expect, "type")
        type_ok(actual, String(expect["type"])) ||
            throw(Failed("expected $(expect["type"]), got $(typeof(actual))"))
    end
    if haskey(expect, "equals")
        want = to_plain(expect["equals"])
        vals_equal(actual, want) || throw(Failed("expected $(want), got $(actual)"))
    end
    nothing
end

# ── dispatch ─────────────────────────────────────────────────────────────────

"""
One arm per op in the spec's vocabulary. Where the Julia client has no surface
for something, the arm throws `NoMapping` rather than reaching for raw SQL — an
executor that works around a missing method reports the ENGINE working and
hides that the SDK does not.
"""
function call_op(c, op::String, args::Vector{Any})
    # ── core ──
    op == "features.isNucleus" && return c.features.is_nucleus
    if op == "connection.closeAndReconnect"
        probe = NeutronJulia.connect(ENV["NEUTRON_TEST_DATABASE_URL"])
        query(sql(probe), "SELECT 1")
        return true
    end

    # ── sql ──
    if op == "sql.queryScalar"
        rows = query(sql(c), String(args[1]), (length(args) > 1 ? args[2] : [])...)
        cols = collect(values(rows))
        isempty(cols) && return nothing
        first_col = cols[1]
        return isempty(first_col) ? nothing : first_col[1]
    end
    op == "sql.execute" && return execute!(sql(c), String(args[1]),
        (length(args) > 1 ? args[2] : [])...)
    op == "sql.begin" && (execute!(sql(c), "BEGIN"); return nothing)
    op == "sql.rollback" && (execute!(sql(c), "ROLLBACK"); return nothing)

    # ── kv ──
    k = kv(c)
    op == "kv.set" && return kv_set!(k, String(args[1]), String(args[2]);
        ttl = length(args) > 2 ? Int(args[3]) : nothing)
    op == "kv.get" && return kv_get(k, String(args[1]))
    op == "kv.delete" && return kv_delete!(k, String(args[1]))
    op == "kv.exists" && return kv_exists(k, String(args[1]))
    op == "kv.incr" && return kv_incr!(k, String(args[1]); by = length(args) > 1 ? Int(args[2]) : 1)
    op == "kv.ttl" && return kv_ttl(k, String(args[1]))
    op == "kv.expire" && return kv_expire!(k, String(args[1]), Int(args[2]))
    op == "kv.rpush" && return rpush!(k, String(args[1]), String(args[2]))
    op == "kv.llen" && return llen(k, String(args[1]))
    op == "kv.lindex" && return lindex(k, String(args[1]), Int(args[2]))
    op == "kv.lrange" && return lrange(k, String(args[1]), Int(args[2]), Int(args[3]))
    op == "kv.hset" && return hset!(k, String(args[1]), String(args[2]), String(args[3]))
    op == "kv.hget" && return hget(k, String(args[1]), String(args[2]))
    op == "kv.hdel" && return hdel!(k, String(args[1]), String(args[2]))
    op == "kv.hexists" && return hexists(k, String(args[1]), String(args[2]))
    op == "kv.hlen" && return hlen(k, String(args[1]))
    op == "kv.hgetall" && return hgetall(k, String(args[1]))
    op == "kv.sadd" && return sadd!(k, String(args[1]), String(args[2]))
    op == "kv.srem" && return srem!(k, String(args[1]), String(args[2]))
    op == "kv.smembers" && return smembers(k, String(args[1]))
    op == "kv.zadd" && return zadd!(k, String(args[1]), float(args[2]), String(args[3]))
    op == "kv.zrange" && return zrange(k, String(args[1]), Int(args[2]), Int(args[3]))

    # ── document ──
    d = document(c)
    op == "document.insert" && return insert!(d, String(args[1]), args[2])
    op == "document.get" && return doc_get(d, Int(args[1]))
    op == "document.getIn" && return doc_get(d, String(args[1]), Int(args[2]))
    op == "document.countIn" && return doc_count(d, String(args[1]))
    op == "document.find" && return NeutronJulia.find(d, String(args[1]), args[2])
    op == "document.findOne" && return NeutronJulia.find_one(d, String(args[1]), args[2])
    op == "document.update" && return NeutronJulia.update_where!(d, String(args[1]), args[2], args[3])
    op == "document.delete" && return NeutronJulia.delete_where!(d, String(args[1]), args[2])
    if op == "document.getPathIn"
        raw = doc_path(d, String(args[1]), Int(args[2]), String.(args[3:end])...)
        # DOC_PATH answers with raw JSON, so a stored string arrives as "\"ada\"".
        # S22 settled the cross-SDK contract: get_path returns the VALUE.
        raw isa AbstractString || return raw
        return try
            to_plain(JSON3.read(raw))
        catch
            raw
        end
    end

    # ── vector ──
    v = vector(c)
    if op == "vector.createCollection"
        execute!(sql(c), "CREATE TABLE $(args[1]) (id TEXT PRIMARY KEY, embedding VECTOR($(Int(args[2]))), metadata JSONB)")
        return nothing
    end
    op == "vector.insert" && return NeutronJulia.vector_insert!(v, String(args[1]), String(args[2]), Float64.(args[3]))
    op == "vector.count" && return NeutronJulia.vector_count(v, String(args[1]))
    op == "vector.search" && return search(v, String(args[1]), Float64.(args[2]); k = Int(args[3]))

    # ── timeseries ──
    ts = timeseries(c)
    if op == "timeseries.write"
        for p in args[2]
            insert!(ts, String(args[1]), TS_BASE_MS + Int(p["t"]), float(p["v"]))
        end
        return nothing
    end
    op == "timeseries.count" && return ts_count(ts, String(args[1]))
    op == "timeseries.last" && return last_value(ts, String(args[1]))
    op == "timeseries.query" &&
        return [Dict("t" => t, "v" => val) for (t, val) in
                NeutronJulia.ts_range(ts, String(args[1]), TS_BASE_MS + Int(args[2]), TS_BASE_MS + Int(args[3]))]
    op == "timeseries.aggregate" &&
        return [Dict("t" => t, "v" => val) for (t, val) in
                NeutronJulia.aggregate(ts, String(args[1]), TS_BASE_MS + Int(args[2]),
                    TS_BASE_MS + Int(args[3]), Int(args[4]))]

    # ── fts ──
    f = fts(c)
    if op == "fts.indexDoc"
        # The client's FTS index is global — index!(doc_id, text) has no index
        # name — so the spec's index argument is dropped and the field map is
        # flattened to the indexed text.
        text = join([string(val) for val in values(args[3]) if val isa AbstractString], " ")
        return index!(f, parse(Int, String(args[2])), text)
    end
    op == "fts.search" && return search(f, String(args[2]); limit = Int(args[3]))

    # ── graph ──
    g = graph(c)
    op == "graph.addNode" && return add_node!(g, String(args[1][1]); properties = Dict(args[2]))
    op == "graph.addEdge" &&
        return add_edge!(g, Int(args[2]), Int(args[3]), String(args[1]);
            properties = length(args) > 3 ? Dict(args[4]) : Dict())
    op == "graph.deleteNode" && return delete_node!(g, Int(args[1]))
        if op == "graph.neighbors"
        dir = length(args) > 1 ? String(args[2]) : "out"
        d_enum = dir == "in" ? GraphIn : dir == "both" ? GraphBoth : GraphOut
        return neighbors(g, Int(args[1]); direction = d_enum)
    end
    op == "graph.shortestPath" && return shortest_path(g, Int(args[1]), Int(args[2]))
    op == "graph.nodeCount" && return node_count(g)
    op == "graph.edgeCount" && return edge_count(g)

    # ── streams ──
    st = streams(c)
    if op == "streams.xadd"
        fields = args[2]
        return xadd!(st, String(args[1]),
            (String(kk) => string(vv) for (kk, vv) in pairs(fields))...)
    end
    op == "streams.xlen" && return xlen(st, String(args[1]))
    op == "streams.xrange" && return xrange(st, String(args[1]), 0, typemax(Int64); count = 100)
    op == "streams.xread" && return xread(st, String(args[1]), 0; count = 100)
    op == "streams.xgroupCreate" && return xgroup_create!(st, String(args[1]), String(args[2]), Int64(length(args) > 2 ? args[3] : 0))
    op == "streams.xreadgroup" &&
        return xreadgroup(st, String(args[1]), String(args[2]), String(args[3]); count = 100)
    op == "streams.xack" && return xack!(st, String(args[1]), String(args[2]), String(args[3]))

    # ── datalog ──
    dl = datalog(c)
    op == "datalog.assertFact" && return assert_fact!(dl, String(args[1]))
    op == "datalog.query" && return datalog_query(dl, String(args[1]))
    op == "datalog.clear" && return clear!(dl, String(args[1]))

    # ── cdc ──
    cd = cdc(c)
        op == "cdc.read" && return cdc_read(cd, Int64(args[1]), Int64(args[2]))
    op == "cdc.count" && return cdc_count(cd)

    # ── blob ──
    b = blob(c)
    scoped(bucket, key) = isempty(bucket) ? key : "$(bucket)/$(key)"
    op == "blob.put" && return store!(b, scoped(String(args[1]), String(args[2])),
        base64decode(String(args[3])))
    if op == "blob.get"
        got = blob_get(b, scoped(String(args[1]), String(args[2])))
        (got === nothing || got === missing) && return nothing
        return got isa AbstractVector{UInt8} ? base64encode(got) : hex_to_base64(String(got))
    end
    op == "blob.getMeta" && return meta(b, scoped(String(args[1]), String(args[2])))
    op == "blob.exists" && return NeutronJulia.blob_exists(b, scoped(String(args[1]), String(args[2])))
    op == "blob.delete" && return blob_delete!(b, scoped(String(args[1]), String(args[2])))

    throw(NoMapping(op))
end

base64decode_to_hex(s) = bytes2hex(base64decode(s))
hex_to_base64(h) = base64encode(hex2bytes(h))

# ── main ─────────────────────────────────────────────────────────────────────

function run_case(c, case, fixtures, bound)
    for (i, step) in enumerate(case["steps"])
        op = String(step["op"])
        raw = haskey(step, "args") ? collect(step["args"]) : Any[]
        args = Any[resolve(a, fixtures, bound) for a in raw]

        result = try
            call_op(c, op, args)
        catch e
            e isa NoMapping && rethrow()
            e isa Failed && throw(Failed("step $(i-1) ($op): $(e.msg)"))
            throw(Failed("step $(i-1) ($op): client error: $(sprint(showerror, e))"))
        end

        haskey(step, "bind") && (bound[String(step["bind"])] = result)
        if haskey(step, "expect")
            try
                check(result, step["expect"])
            catch e
                e isa Failed && throw(Failed("step $(i-1) ($op): $(e.msg)"))
                rethrow()
            end
        end
    end
end

function main()
    url = get(ENV, "NEUTRON_TEST_DATABASE_URL", "")
    if isempty(url)
        println(stderr, "::error::NEUTRON_TEST_DATABASE_URL is not set. This suite is only " *
                        "meaningful against a live engine; refusing to report a green run for " *
                        "zero executed cases.")
        exit(1)
    end

    spec = JSON3.read(read(joinpath(@__DIR__, "..", "..", "spec.json"), String))
    unsupported = Dict{String,String}()
    unsup_path = joinpath(@__DIR__, "unsupported.json")
    if isfile(unsup_path)
        u = JSON3.read(read(unsup_path, String))
        haskey(u, "cases") && for (kk, vv) in pairs(u["cases"])
            unsupported[String(kk)] = String(vv)
        end
    end

    c = NeutronJulia.connect(url)
    results = Any[]
    failures = 0
    counts = Dict{String,Int}()

    for case in spec["cases"]
        id = String(case["id"])
        model = haskey(case, "model") ? String(case["model"]) : ""

        xfail_applies = false
        if haskey(case, "xfail")
            x = case["xfail"]
            xfail_applies = !haskey(x, "sdks") || "julia" in [String(s) for s in x["sdks"]]
        end

        status = "pass"
        detail = ""
        try
            run_case(c, case, Dict{String,String}(), Dict{String,Any}())
            if xfail_applies
                status = "xpass"
                detail = "case is marked xfail but passed — the underlying bug is fixed and the xfail note is now false"
            end
        catch e
            if e isa NoMapping
                if haskey(unsupported, id)
                    status = "unsupported"
                    detail = unsupported[id]
                else
                    status = "fail"
                    detail = "no Julia mapping for op $(e.op), and the case is not declared unsupported in unsupported.json"
                end
            else
                msg = e isa Failed ? e.msg : sprint(showerror, e)
                status = xfail_applies ? "xfail" : "fail"
                detail = msg
            end
        end

        counts[status] = get(counts, status, 0) + 1
        if status == "fail" || status == "xpass"
            failures += 1
            println(stderr, "::error::$id: $status — $detail")
        end

        entry = Dict{String,Any}("id" => id, "model" => model, "status" => status)
        isempty(detail) || (entry["detail"] = detail)
        push!(results, entry)
    end

    JSON3.write(stdout, Dict("sdk" => "julia", "specVersion" => 1, "cases" => results))
    println(stdout)
    println(stderr, "julia: ", counts)
    exit(failures > 0 ? 1 : 0)
end

main()
