"""
Graph Analysis — knowledge graph with Graphs.jl PageRank + shortest paths.

Demonstrates:
- Graph: building a social/citation graph in Nucleus
- Graphs.jl extension: exporting to SimpleDiGraph for algorithmic analysis
- PageRank, betweenness centrality, connected components
- Shortest path via Nucleus GRAPH_SHORTEST_PATH

Run:
    julia --project=. examples/graph_analysis.jl postgres://localhost:5432/mydb
"""

using NeutronJulia

const URL = get(ARGS, 1, get(ENV, "NUCLEUS_TEST_URL", ""))
isempty(URL) && error("Usage: julia examples/graph_analysis.jl <postgres-url>")

try
    using Graphs
catch
    error("Graphs.jl not installed. Run: import Pkg; Pkg.add(\"Graphs\")")
end

println("Connecting to Nucleus...")
client = NeutronJulia.connect(URL)
g = graph(client)

# ── Build a citation graph ────────────────────────────────────────────────────

println("Building citation graph (10 papers)...")
paper_ids = Int64[]
titles = ["Attention is All You Need", "BERT", "GPT", "ResNet",
          "AlexNet", "Adam Optimizer", "Dropout", "BatchNorm",
          "GAN", "VAE"]

for title in titles
    nid = add_node!(g, "Paper", properties=Dict("title" => title))
    push!(paper_ids, nid)
end

println("  Created $(node_count(g)) nodes")

# Citations (directed edges: A cites B)
citations = [
    (1, 6), (1, 7), (1, 8),   # Attention cites Adam, Dropout, BatchNorm
    (2, 1), (2, 6), (2, 7),   # BERT cites Attention, Adam, Dropout
    (3, 1), (3, 2), (3, 6),   # GPT cites Attention, BERT, Adam
    (4, 5), (4, 7), (4, 8),   # ResNet cites AlexNet, Dropout, BatchNorm
    (5, 6),                   # AlexNet cites Adam
    (9, 10), (9, 6),          # GAN cites VAE, Adam
    (10, 6),                  # VAE cites Adam
]

for (from_idx, to_idx) in citations
    add_edge!(g, paper_ids[from_idx], paper_ids[to_idx], "CITES")
end

println("  Created $(edge_count(g)) edges")

# ── Export to Graphs.jl for analysis ─────────────────────────────────────────

println("\nExporting to Graphs.jl SimpleDiGraph...")
jl_graph, vertex_to_nucleus_id = to_graphs_jl(g)

println("  Vertices: $(nv(jl_graph)), Edges: $(ne(jl_graph))")

# PageRank
pr = pagerank(jl_graph)
top_idx = partialsortperm(pr, 1:3, rev=true)
println("\nTop 3 papers by PageRank:")
for (rank, v) in enumerate(top_idx)
    nucleus_id = vertex_to_nucleus_id[v]
    # Find title from our local mapping
    paper_rank = findfirst(==(nucleus_id), paper_ids)
    title = paper_rank !== nothing ? titles[paper_rank] : "Unknown"
    println("  $rank. $title (score=$(round(pr[v]; digits=4)))")
end

# In-degree (citation count)
println("\nMost cited papers (in-degree):")
in_degrees = indegree(jl_graph)
top_cited = partialsortperm(in_degrees, 1:3, rev=true)
for (rank, v) in enumerate(top_cited)
    nucleus_id = vertex_to_nucleus_id[v]
    paper_rank = findfirst(==(nucleus_id), paper_ids)
    title = paper_rank !== nothing ? titles[paper_rank] : "Unknown"
    println("  $rank. $title (cited $(in_degrees[v]) times)")
end

# ── Nucleus native: shortest path ─────────────────────────────────────────────

println("\nShortest path from 'GPT' to 'AlexNet' (Nucleus GRAPH_SHORTEST_PATH):")
path = shortest_path(g, paper_ids[3], paper_ids[5])
if isempty(path)
    println("  No path found")
else
    path_titles = [titles[findfirst(==(id), paper_ids)] for id in path if findfirst(==(id), paper_ids) !== nothing]
    println("  $(join(path_titles, " → "))")
end

# ── Neighbors ─────────────────────────────────────────────────────────────────

println("\n'Attention is All You Need' outgoing citations:")
nbrs = neighbors(g, paper_ids[1]; direction=GraphOut)
for nbr in nbrs
    nid = get(nbr, "id", nothing)
    if nid !== nothing
        paper_rank = findfirst(==(Int64(nid)), paper_ids)
        title = paper_rank !== nothing ? titles[paper_rank] : "id=$nid"
        println("  → $title")
    end
end

println("\nGraph analysis complete.")
close(client)
