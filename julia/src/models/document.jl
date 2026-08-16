"""Document model — JSON documents via DOC_* SQL functions with JSON3.jl."""

struct DocumentModel
    conn::LibPQ.Connection
    features::NucleusFeatures
end

"""DOC_INSERT(json) → Int64 doc ID"""
function insert!(m::DocumentModel, doc)::Int64
    require_nucleus(m.features, "Document")
    json_str = JSON3.write(doc)
    result = LibPQ.execute(m.conn, "SELECT DOC_INSERT(\$1)", [json_str])
    return _int(result)
end

"""DOC_GET(id) → Dict{String,Any} or nothing"""
function doc_get(m::DocumentModel, id::Int64)::Union{Dict{String, Any}, Nothing}
    require_nucleus(m.features, "Document")
    result = LibPQ.execute(m.conn, "SELECT DOC_GET(\$1)", [id])
    val = first(result)[1]
    return ismissing(val) ? nothing : JSON3.read(val, Dict{String, Any})
end

"""DOC_GET(id, T) → T or nothing — typed deserialization via StructTypes.jl"""
function doc_get(m::DocumentModel, id::Int64, ::Type{T})::Union{T, Nothing} where T
    require_nucleus(m.features, "Document")
    result = LibPQ.execute(m.conn, "SELECT DOC_GET(\$1)", [id])
    val = first(result)[1]
    return ismissing(val) ? nothing : JSON3.read(val, T)
end

"""DOC_QUERY(json_query) → Vector{Int64} matching doc IDs"""
function doc_query(m::DocumentModel, query_json::String)::Vector{Int64}
    require_nucleus(m.features, "Document")
    result = LibPQ.execute(m.conn, "SELECT DOC_QUERY(\$1)", [query_json])
    raw = first(result)[1]
    (ismissing(raw) || isempty(raw)) && return Int64[]
    return parse.(Int64, split(raw, ","))
end

"""DOC_PATH(id, keys...) → value or nothing"""
function doc_path(m::DocumentModel, id::Int64, keys::String...)
    require_nucleus(m.features, "Document")
    key_params = join(["\$$(i+1)" for i in 1:length(keys)], ", ")
    sql_str = "SELECT DOC_PATH(\$1, $key_params)"
    result = LibPQ.execute(m.conn, sql_str, [id, keys...])
    val = first(result)[1]
    return ismissing(val) ? nothing : val
end

"""DOC_COUNT() → Int64"""
function doc_count(m::DocumentModel)::Int64
    require_nucleus(m.features, "Document")
    result = LibPQ.execute(m.conn, "SELECT DOC_COUNT()")
    return _int(result)
end

# ── Collections ──────────────────────────────────────────────────────────────
#
# A document belongs to exactly one collection, and an operation naming a
# collection sees only that one — a document elsewhere reads as `nothing`
# rather than raising, so an id cannot be used to probe across the boundary.
# The methods above address the default (unnamed) collection, which is where
# documents written before collections existed live.
#
# LibPQ sends parameters as text, so a document id needs no special encoding
# here (other SDKs must send it as text explicitly — Nucleus reports a
# parameter it cannot infer as TEXT and their drivers refuse to bind an
# integer to it).

"""DOC_INSERT(collection, json) → Int64 doc ID"""
function insert!(m::DocumentModel, collection::AbstractString, doc)::Int64
    require_nucleus(m.features, "Document")
    json_str = JSON3.write(doc)
    # The one-argument form when no collection is named, so this still works
    # against a server that predates collections.
    if isempty(collection)
        return insert!(m, doc)
    end
    result = LibPQ.execute(m.conn, "SELECT DOC_INSERT(\$1, \$2)", [collection, json_str])
    return _int(result)
end

"""DOC_GET(collection, id) → Dict{String,Any} or nothing (scoped)"""
function doc_get(m::DocumentModel, collection::AbstractString, id::Int64)::Union{Dict{String, Any}, Nothing}
    require_nucleus(m.features, "Document")
    isempty(collection) && return doc_get(m, id)
    result = LibPQ.execute(m.conn, "SELECT DOC_GET(\$1, \$2)", [collection, string(id)])
    val = first(result)[1]
    return ismissing(val) ? nothing : JSON3.read(val, Dict{String, Any})
end

"""DOC_GET(collection, id, T) → T or nothing (scoped, typed)"""
function doc_get(m::DocumentModel, collection::AbstractString, id::Int64, ::Type{T})::Union{T, Nothing} where T
    require_nucleus(m.features, "Document")
    isempty(collection) && return doc_get(m, id, T)
    result = LibPQ.execute(m.conn, "SELECT DOC_GET(\$1, \$2)", [collection, string(id)])
    val = first(result)[1]
    return ismissing(val) ? nothing : JSON3.read(val, T)
end

"""DOC_QUERY(collection, json_query) → Vector{Int64} — matches in other collections are not returned"""
function doc_query(m::DocumentModel, collection::AbstractString, query_json::String)::Vector{Int64}
    require_nucleus(m.features, "Document")
    isempty(collection) && return doc_query(m, query_json)
    result = LibPQ.execute(m.conn, "SELECT DOC_QUERY(\$1, \$2)", [collection, query_json])
    raw = first(result)[1]
    (ismissing(raw) || isempty(raw)) && return Int64[]
    return parse.(Int64, split(raw, ","))
end

"""
DOC_PATH_IN(collection, id, keys...) → value or nothing

A distinct FUNCTION rather than an extra argument: the key tail is variadic, so
a leading collection could not be told apart from a leading id.
"""
function doc_path(m::DocumentModel, collection::AbstractString, id::Int64, keys::String...)
    require_nucleus(m.features, "Document")
    isempty(collection) && return doc_path(m, id, keys...)
    key_params = join(["\$$(i+2)" for i in 1:length(keys)], ", ")
    sql_str = "SELECT DOC_PATH_IN(\$1, \$2, $key_params)"
    result = LibPQ.execute(m.conn, sql_str, [collection, string(id), keys...])
    val = first(result)[1]
    return ismissing(val) ? nothing : val
end

"""DOC_COUNT(collection) → Int64"""
function doc_count(m::DocumentModel, collection::AbstractString)::Int64
    require_nucleus(m.features, "Document")
    isempty(collection) && return doc_count(m)
    result = LibPQ.execute(m.conn, "SELECT DOC_COUNT(\$1)", [collection])
    return _int(result)
end


"""DOC_UPDATE(id, json) → Bool — replaces the document in place."""
function doc_update!(m::DocumentModel, id::Int64, doc)::Bool
    require_nucleus(m.features, "Document")
    result = LibPQ.execute(m.conn, "SELECT DOC_UPDATE(\$1, \$2)",
                           [string(id), JSON3.write(doc)])
    v = first(result)[1]
    return v === true || v == "t" || v == "true"
end

"""DOC_UPDATE(collection, id, json) → Bool (scoped)."""
function doc_update!(m::DocumentModel, collection::AbstractString, id::Int64, doc)::Bool
    isempty(collection) && return doc_update!(m, id, doc)
    require_nucleus(m.features, "Document")
    result = LibPQ.execute(m.conn, "SELECT DOC_UPDATE(\$1, \$2, \$3)",
                           [String(collection), string(id), JSON3.write(doc)])
    v = first(result)[1]
    return v === true || v == "t" || v == "true"
end

"""DOC_DELETE(id) → Bool."""
function doc_delete!(m::DocumentModel, id::Int64)::Bool
    require_nucleus(m.features, "Document")
    result = LibPQ.execute(m.conn, "SELECT DOC_DELETE(\$1)", [string(id)])
    v = first(result)[1]
    return v === true || v == "t" || v == "true"
end

"""DOC_DELETE(collection, id) → Bool (scoped)."""
function doc_delete!(m::DocumentModel, collection::AbstractString, id::Int64)::Bool
    isempty(collection) && return doc_delete!(m, id)
    require_nucleus(m.features, "Document")
    result = LibPQ.execute(m.conn, "SELECT DOC_DELETE(\$1, \$2)",
                           [String(collection), string(id)])
    v = first(result)[1]
    return v === true || v == "t" || v == "true"
end

"""DOC_QUERY with a Dict/NamedTuple filter rather than a pre-encoded string."""
doc_query(m::DocumentModel, collection::AbstractString, filter::AbstractDict) =
    doc_query(m, collection, JSON3.write(filter))
doc_query(m::DocumentModel, filter::AbstractDict) = doc_query(m, JSON3.write(filter))

# ── Filter-based operations ──────────────────────────────────────────────────
#
# The engine indexes documents but only DOC_QUERY takes a filter, and it answers
# with ids. Find, update-by-filter and delete-by-filter therefore resolve ids
# first and act per id — which is what the Python, Go, Rust and Elixir clients
# do. The contract is defined by behaviour, not by where the loop runs.

"""Find whole documents in `collection` matching `filter`."""
function find(m::DocumentModel, collection::String, filter; limit::Int=100, skip::Int=0)
    pairs_ = _find_with_ids(m, collection, filter, limit, skip)
    return [doc for (_id, doc) in pairs_]
end

"""Find the first document matching `filter`, or `nothing`. Absence is not an error."""
function find_one(m::DocumentModel, collection::String, filter)
    pairs_ = _find_with_ids(m, collection, filter, 1, 0)
    return isempty(pairs_) ? nothing : pairs_[1][2]
end

"""Merge `patch` into every document matching `filter`. Returns the count.

A PARTIAL update: keys in `patch` overwrite, keys absent from it survive.
"""
function update_where!(m::DocumentModel, collection::String, filter, patch)
    n = 0
    for (id, existing) in _find_with_ids(m, collection, filter, 10_000, 0)
        merged = merge(Dict{String, Any}(string(k) => v for (k, v) in pairs(existing)),
                       Dict{String, Any}(string(k) => v for (k, v) in pairs(patch)))
        doc_update!(m, collection, id, merged) && (n += 1)
    end
    return n
end

"""Delete every document matching `filter`. Returns the count."""
function delete_where!(m::DocumentModel, collection::String, filter)
    n = 0
    for (id, _doc) in _find_with_ids(m, collection, filter, 10_000, 0)
        doc_delete!(m, collection, id) && (n += 1)
    end
    return n
end

function _find_with_ids(m::DocumentModel, collection::String, filter, limit::Int, skip::Int)
    ids = doc_query(m, collection, filter)
    out = Tuple{Int64, Any}[]
    for id in Iterators.take(Iterators.drop(ids, skip), limit)
        doc = doc_get(m, collection, id)
        doc === nothing || push!(out, (Int64(id), doc))
    end
    return out
end
