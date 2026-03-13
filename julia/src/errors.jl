"""RFC 7807 Problem Details error from Nucleus."""
struct NucleusError <: Exception
    type::String      # e.g., "https://neutron.dev/errors/not-found"
    title::String     # e.g., "Not Found"
    status::Int       # e.g., 404
    detail::String    # e.g., "Key 'session:abc' not found"
end

Base.showerror(io::IO, e::NucleusError) =
    print(io, "NucleusError($(e.status) $(e.title)): $(e.detail)")

"""Thrown when a Nucleus-specific feature is called against plain PostgreSQL."""
struct NotNucleusError <: Exception
    model::String     # e.g., "KV"
    msg::String
end

Base.showerror(io::IO, e::NotNucleusError) =
    print(io, "NotNucleusError[$(e.model)]: $(e.msg)")

"""Guard: throw NotNucleusError if not connected to Nucleus."""
function require_nucleus(features, model::String)
    if !features.is_nucleus
        throw(NotNucleusError(model,
            "$(model) operations require Nucleus, but connected to plain PostgreSQL"))
    end
end
