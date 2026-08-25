"""
Julia <-> PostgreSQL type mapping utilities.
"""

# Extract a scalar value from a LibPQ result row, converting Missing to nothing.
_scalar(result) = begin
    row = first(result)
    val = row[1]
    ismissing(val) ? nothing : val
end

# Extract a boolean scalar.
_bool(result)::Bool = begin
    v = _scalar(result)
    v === nothing ? false : v
end

# Extract an Int64 scalar.
_int(result)::Int64 = begin
    v = _scalar(result)
    v === nothing ? Int64(0) : Int64(v)
end

# Extract a Float64 scalar.
_float(result)::Union{Float64, Nothing} = begin
    v = _scalar(result)
    v === nothing ? nothing : Float64(v)
end

# Split a comma-separated string into a vector. Only for functions that still
# comma-join (e.g. PUBSUB_CHANNELS) — the KV collection functions return JSON
# arrays; use _json_strings for those.
#
# PUBSUB_CHANNELS's only Ok arm is Value::Text(chans.join(",")), so the
# reachable empty case is "" — never NULL — and missing/nothing is a contract
# violation.
function _split_csv(raw)::Vector{String}
    (ismissing(raw) || raw === nothing) &&
        error("PUBSUB_CHANNELS returned NULL; success is always comma-joined Text")
    isempty(raw) && return String[]
    return split(raw, ",")
end

# Parse a JSON array of strings (KV_LRANGE / KV_SMEMBERS). Their scalar_fns
# arms answer Value::Text on every success path — an empty collection arrives
# as "[]", which JSON3.read maps to an empty vector — so a NULL cell is a
# contract violation, not an empty result.
function _json_strings(raw)::Vector{String}
    ismissing(raw) && error("KV_LRANGE/KV_SMEMBERS returned NULL; success is always a JSON array")
    return JSON3.read(raw, Vector{String})
end
