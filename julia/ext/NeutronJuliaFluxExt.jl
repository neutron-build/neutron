"""
Flux.jl extension — loaded as NeutronJuliaFluxExt.
Generate embeddings with Flux.jl neural networks and store in Nucleus Vector.

Usage:
```julia
using Flux, NeutronJulia
vec = vector(client)
encoder = Chain(Dense(784, 128, relu))
embed_and_store!(vec, encoder, "embeddings_table", inputs, ids)
```
"""

module NeutronJuliaFluxExt

using NeutronJulia
using Flux

"""
    embed_and_store!(m::VectorModel, model, table, inputs, ids; column)

Run `inputs` through a Flux model, then INSERT each embedding into `table`.
`ids` should be a vector of row IDs matching `inputs`.
"""
function NeutronJulia.embed_and_store!(m::NeutronJulia.VectorModel,
                                       model,
                                       table::String,
                                       inputs,
                                       ids;
                                       column::String="embedding")
    NeutronJulia.require_nucleus(m.features, "Vector")
    s = NeutronJulia.SQLModel(m.conn)
    for (id, input) in zip(ids, inputs)
        embedding = Float32.(vec(model(input)))
        lit = NeutronJulia.to_vector_literal(embedding)
        NeutronJulia.execute!(s,
            "INSERT INTO $table (id, $column) VALUES (\$1, VECTOR(\$2))
             ON CONFLICT (id) DO UPDATE SET $column = EXCLUDED.$column",
            id, lit)
    end
    return nothing
end

end # module NeutronJuliaFluxExt
