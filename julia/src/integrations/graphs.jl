"""
Graphs.jl extension — loaded as NeutronJuliaGraphsExt.
Bidirectional conversion between Nucleus Graph and Graphs.jl SimpleDiGraph.

Usage:
```julia
using Graphs, NeutronJulia
g = graph(client)
jl_graph, node_map = to_graphs_jl(g)
pr = pagerank(jl_graph)
```
"""

module NeutronJuliaGraphsExt

using NeutronJulia
using Graphs

"""
    to_graphs_jl(m::GraphModel) -> (SimpleDiGraph, Dict{Int,Int64})

Export all Nucleus graph nodes and edges to a Graphs.jl SimpleDiGraph.
Returns the graph and a mapping from vertex index (1-based) → Nucleus node ID.
"""
function NeutronJulia.to_graphs_jl(m::NeutronJulia.GraphModel)
    NeutronJulia.require_nucleus(m.features, "Graph")
    # Query all nodes
    nodes_raw = NeutronJulia.graph_query(m, "MATCH (n) RETURN id(n) AS id")
    node_ids = Int64[row[1] for row in get(nodes_raw, "rows", [])]

    # Build vertex index
    id_to_vertex = Dict(nid => i for (i, nid) in enumerate(node_ids))
    vertex_to_id = Dict(i => nid for (i, nid) in enumerate(node_ids))

    n = length(node_ids)
    g = SimpleDiGraph(n)

    # Query all edges
    edges_raw = NeutronJulia.graph_query(m, "MATCH (a)-[r]->(b) RETURN id(a), id(b)")
    for row in get(edges_raw, "rows", [])
        src_id, dst_id = Int64(row[1]), Int64(row[2])
        src_v = get(id_to_vertex, src_id, nothing)
        dst_v = get(id_to_vertex, dst_id, nothing)
        (src_v !== nothing && dst_v !== nothing) && add_edge!(g, src_v, dst_v)
    end

    return g, vertex_to_id
end

"""
    import_from_graphs_jl!(m::GraphModel, g::SimpleDiGraph; label) → nothing

Import a Graphs.jl graph into Nucleus as nodes and edges.
"""
function NeutronJulia.import_from_graphs_jl!(m::NeutronJulia.GraphModel,
                                              g::Graphs.SimpleDiGraph;
                                              label::String="Vertex")
    NeutronJulia.require_nucleus(m.features, "Graph")
    # Create a node for each vertex
    vertex_to_nucleus_id = Dict{Int, Int64}()
    for v in Graphs.vertices(g)
        nid = NeutronJulia.add_node!(m, label, properties=Dict("vertex" => v))
        vertex_to_nucleus_id[v] = nid
    end
    # Create edges
    for e in Graphs.edges(g)
        src_nid = vertex_to_nucleus_id[Graphs.src(e)]
        dst_nid = vertex_to_nucleus_id[Graphs.dst(e)]
        NeutronJulia.add_edge!(m, src_nid, dst_nid, "EDGE")
    end
    return nothing
end

end # module NeutronJuliaGraphsExt
