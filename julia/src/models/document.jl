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
