"""
    NucleusFeatures

Capabilities detected from VERSION() on connect. All fields are true when
connected to Nucleus, false when connected to plain PostgreSQL.
"""
struct NucleusFeatures
    is_nucleus::Bool
    version::Union{String, Nothing}
    has_kv::Bool
    has_vector::Bool
    has_timeseries::Bool
    has_document::Bool
    has_graph::Bool
    has_fts::Bool
    has_geo::Bool
    has_blob::Bool
    has_streams::Bool
    has_columnar::Bool
    has_datalog::Bool
    has_cdc::Bool
    has_pubsub::Bool
end

"""Parse VERSION() output and return NucleusFeatures."""
function detect_features(conn::LibPQ.Connection)::NucleusFeatures
    result = LibPQ.execute(conn, "SELECT VERSION()")
    version_str = first(result)[1]
    # Nucleus returns: "PostgreSQL 16.0 (Nucleus X.Y.Z — The Definitive Database)"
    is_nucleus = occursin("Nucleus", version_str)
    m = match(r"Nucleus (\S+)", version_str)
    nucleus_version = (is_nucleus && m !== nothing) ? m.captures[1] : nothing
    return NucleusFeatures(
        is_nucleus, nucleus_version,
        is_nucleus, is_nucleus, is_nucleus, is_nucleus,
        is_nucleus, is_nucleus, is_nucleus, is_nucleus,
        is_nucleus, is_nucleus, is_nucleus, is_nucleus,
        is_nucleus,
    )
end
