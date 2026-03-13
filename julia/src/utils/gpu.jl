"""
CUDA.jl extension — loaded as NeutronJuliaCUDAExt.
GPU-accelerated vector operations for large-scale similarity search.

Usage:
```julia
using CUDA, NeutronJulia
vec = vector(client)
embeddings_gpu = load_embeddings_gpu(vec, "image_embeddings")
distances = gpu_batch_cosine(embeddings_gpu, query_vectors_gpu)
top_k = gpu_topk(distances, k=10)
```
"""

module NeutronJuliaCUDAExt

using NeutronJulia
using CUDA

"""
    load_embeddings_gpu(m::VectorModel, table; column) → CuMatrix{Float32}

Load all embeddings from a Nucleus table onto the GPU as a CuMatrix (dim × N).
"""
function NeutronJulia.load_embeddings_gpu(m::NeutronJulia.VectorModel,
                                          table::String;
                                          column::String="embedding")::CUDA.CuMatrix{Float32}
    NeutronJulia.require_nucleus(m.features, "Vector")
    result = LibPQ.execute(m.conn, "SELECT $column::text FROM $table")
    rows = collect(result)
    isempty(rows) && return CUDA.CuMatrix{Float32}(undef, 0, 0)

    # Parse JSON array strings back to Float32 vectors
    vecs = [Float32.(JSON3.read(row[1], Vector{Float32})) for row in rows]
    dim = length(first(vecs))
    cpu_mat = Matrix{Float32}(undef, dim, length(vecs))
    for (j, v) in enumerate(vecs)
        cpu_mat[:, j] .= v
    end
    return CUDA.CuMatrix(cpu_mat)
end

"""
    gpu_batch_cosine(embeddings, queries) → CuMatrix{Float32}

GPU-accelerated cosine distance matrix. Returns (N × M) distances where
N = # stored embeddings, M = # query vectors.
"""
function NeutronJulia.gpu_batch_cosine(embeddings::CUDA.CuMatrix{Float32},
                                       queries::CUDA.CuMatrix{Float32})::CUDA.CuMatrix{Float32}
    # Normalize columns
    emb_norm = embeddings ./ (CUDA.sqrt.(sum(embeddings .^ 2, dims=1)) .+ Float32(1e-10))
    q_norm   = queries    ./ (CUDA.sqrt.(sum(queries    .^ 2, dims=1)) .+ Float32(1e-10))
    # Cosine similarity (higher = closer), convert to distance
    sim = emb_norm' * q_norm        # (N × M)
    return 1.0f0 .- sim
end

"""
    gpu_topk(distances, k) → CuMatrix{Int32}

Return the indices of the k smallest distances per query column. Shape: (k × M).
"""
function NeutronJulia.gpu_topk(distances::CUDA.CuMatrix{Float32}; k::Int=10)::Matrix{Int32}
    cpu = Array(distances)          # bring to CPU for sorting
    n, m = size(cpu)
    k = min(k, n)
    result = Matrix{Int32}(undef, k, m)
    for j in 1:m
        result[:, j] = partialsortperm(cpu[:, j], 1:k)
    end
    return result
end

end # module NeutronJuliaCUDAExt
