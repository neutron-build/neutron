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

# Split a comma-separated string into a vector, returning empty vector for missing/empty.
function _split_csv(raw)::Vector{String}
    (ismissing(raw) || raw === nothing || isempty(raw)) && return String[]
    return split(raw, ",")
end
