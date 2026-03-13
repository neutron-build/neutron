"""Vector model — Julia arrays as native embeddings with GPU support via CUDA.jl extension."""

struct VectorModel
    conn::LibPQ.Connection
    features::NucleusFeatures
end

@enum DistanceMetric L2 Cosine InnerProduct

struct SearchResult
    id::Any
    distance::Float64
    metadata::Dict{String, Any}
end

"""Convert a Julia numeric vector to a Nucleus VECTOR literal string, e.g. \"[1.0,2.0,3.0]\"."""
function to_vector_literal(v::AbstractVector{<:Real})::String
    return "[" * join(string.(v), ",") * "]"
end

"""
    search(m, table, query_vec; k, metric, column) → Vector{SearchResult}

Nearest-neighbour search using ORDER BY VECTOR_DISTANCE(...) LIMIT k.
"""
function search(m::VectorModel, table::String, query_vec::AbstractVector{<:Real};
                k::Int=10, metric::DistanceMetric=Cosine,
                column::String="embedding")::Vector{SearchResult}
    require_nucleus(m.features, "Vector")
    metric_str = metric == L2 ? "l2" : metric == Cosine ? "cosine" : "inner"
    sql_str = """
        SELECT id, VECTOR_DISTANCE($column, VECTOR(\$1), '$metric_str') AS distance
        FROM $table
        ORDER BY distance
        LIMIT \$2
    """
    result = LibPQ.execute(m.conn, sql_str, [to_vector_literal(query_vec), k])
    return [SearchResult(row[1], Float64(row[2]), Dict{String,Any}()) for row in result]
end

"""
    dims(m, table; column) → Int64

Return the number of dimensions of the vector column via VECTOR_DIMS.
"""
function dims(m::VectorModel, table::String; column::String="embedding")::Int64
    require_nucleus(m.features, "Vector")
    result = LibPQ.execute(m.conn, "SELECT VECTOR_DIMS($column) FROM $table LIMIT 1")
    return _int(result)
end

"""
    cosine_distance(m, v1, v2) → Float64

Compute cosine distance between two vectors via Nucleus COSINE_DISTANCE.
"""
function cosine_distance(m::VectorModel,
                         v1::AbstractVector{<:Real},
                         v2::AbstractVector{<:Real})::Float64
    require_nucleus(m.features, "Vector")
    result = LibPQ.execute(m.conn,
        "SELECT COSINE_DISTANCE(VECTOR(\$1), VECTOR(\$2))",
        [to_vector_literal(v1), to_vector_literal(v2)])
    return Float64(first(result)[1])
end

"""
    inner_product(m, v1, v2) → Float64

Compute inner product between two vectors via Nucleus INNER_PRODUCT.
"""
function inner_product(m::VectorModel,
                       v1::AbstractVector{<:Real},
                       v2::AbstractVector{<:Real})::Float64
    require_nucleus(m.features, "Vector")
    result = LibPQ.execute(m.conn,
        "SELECT INNER_PRODUCT(VECTOR(\$1), VECTOR(\$2))",
        [to_vector_literal(v1), to_vector_literal(v2)])
    return Float64(first(result)[1])
end

"""
    vector_distance(m, v1, v2; metric) → Float64

Compute VECTOR_DISTANCE with a selectable metric (:L2, :Cosine, :InnerProduct).
"""
function vector_distance(m::VectorModel,
                         v1::AbstractVector{<:Real},
                         v2::AbstractVector{<:Real};
                         metric::DistanceMetric=L2)::Float64
    require_nucleus(m.features, "Vector")
    metric_str = metric == L2 ? "l2" : metric == Cosine ? "cosine" : "inner"
    result = LibPQ.execute(m.conn,
        "SELECT VECTOR_DISTANCE(VECTOR(\$1), VECTOR(\$2), \$3)",
        [to_vector_literal(v1), to_vector_literal(v2), metric_str])
    return Float64(first(result)[1])
end

"""
    create_index!(m, table; column, metric, ef, m_param)

CREATE INDEX ... USING VECTOR with the specified metric and HNSW parameters.
"""
function create_index!(m::VectorModel, table::String;
                       column::String="embedding",
                       metric::DistanceMetric=Cosine,
                       ef::Int=200, m_param::Int=16)
    require_nucleus(m.features, "Vector")
    metric_str = metric == L2 ? "l2" : metric == Cosine ? "cosine" : "inner"
    sql_str = """
        CREATE INDEX idx_$(table)_$(column) ON $table
        USING VECTOR ($column)
        WITH (metric = '$metric_str', ef = $ef, m = $m_param)
    """
    LibPQ.execute(m.conn, sql_str)
    return nothing
end
