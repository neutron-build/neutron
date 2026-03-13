"""Full-Text Search model — FTS_* SQL functions."""

struct FTSModel
    conn::LibPQ.Connection
    features::NucleusFeatures
end

struct FTSResult
    doc_id::Int64
    score::Float64
end

"""FTS_INDEX(doc_id, text) → Bool"""
function index!(m::FTSModel, doc_id::Int64, text::String)::Bool
    require_nucleus(m.features, "FTS")
    result = LibPQ.execute(m.conn, "SELECT FTS_INDEX(\$1, \$2)", [doc_id, text])
    return _bool(result)
end

"""FTS_SEARCH(query, limit) → Vector{FTSResult}"""
function search(m::FTSModel, query::String; limit::Int=10)::Vector{FTSResult}
    require_nucleus(m.features, "FTS")
    result = LibPQ.execute(m.conn, "SELECT FTS_SEARCH(\$1, \$2)", [query, limit])
    raw = first(result)[1]
    ismissing(raw) && return FTSResult[]
    parsed = JSON3.read(raw, Vector{Dict{String, Any}})
    return [FTSResult(Int64(r["doc_id"]), Float64(r["score"])) for r in parsed]
end

"""FTS_FUZZY_SEARCH(query, max_distance, limit) → Vector{FTSResult}"""
function fuzzy_search(m::FTSModel, query::String;
                      max_distance::Int=2, limit::Int=10)::Vector{FTSResult}
    require_nucleus(m.features, "FTS")
    result = LibPQ.execute(m.conn, "SELECT FTS_FUZZY_SEARCH(\$1, \$2, \$3)",
                           [query, max_distance, limit])
    raw = first(result)[1]
    ismissing(raw) && return FTSResult[]
    parsed = JSON3.read(raw, Vector{Dict{String, Any}})
    return [FTSResult(Int64(r["doc_id"]), Float64(r["score"])) for r in parsed]
end

"""FTS_REMOVE(doc_id) → Bool"""
function remove!(m::FTSModel, doc_id::Int64)::Bool
    require_nucleus(m.features, "FTS")
    result = LibPQ.execute(m.conn, "SELECT FTS_REMOVE(\$1)", [doc_id])
    return _bool(result)
end

"""FTS_DOC_COUNT() → Int64"""
function fts_doc_count(m::FTSModel)::Int64
    require_nucleus(m.features, "FTS")
    result = LibPQ.execute(m.conn, "SELECT FTS_DOC_COUNT()")
    return _int(result)
end

"""FTS_TERM_COUNT() → Int64"""
function fts_term_count(m::FTSModel)::Int64
    require_nucleus(m.features, "FTS")
    result = LibPQ.execute(m.conn, "SELECT FTS_TERM_COUNT()")
    return _int(result)
end
