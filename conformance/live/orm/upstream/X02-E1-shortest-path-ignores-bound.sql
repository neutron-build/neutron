-- X02-E1 — GRAPH_SHORTEST_PATH accepts a third (max-depth) argument and ignores it
--
-- Run on a DISPOSABLE engine with an EMPTY graph, one session:
--   psql "postgresql://postgres@127.0.0.1:<port>/postgres" -X -At -f X02-E1-shortest-path-ignores-bound.sql
-- There is no PostgreSQL oracle (Nucleus-only function). Expected: step 3
-- returns no path (the only path has 2 hops) OR the call is rejected for
-- arity; never a path longer than the bound.
-- Observed on Nucleus 1.0.2 (nucleus/ tree 3313729a): step 3 returns
-- [1,2,3], identical to the two-argument call.
-- Client impact: @neutron-build/nucleus used to forward maxDepth here and
-- returned paths past the bound; it now runs a bounded BFS on the client.

\echo 1 chain a -> b -> c
select graph_add_node('X02E1');
select graph_add_node('X02E1');
select graph_add_node('X02E1');
select graph_add_edge(1, 2, 'NEXT');
select graph_add_edge(2, 3, 'NEXT');

\echo 2 unbounded path: [1,2,3]
select graph_shortest_path(1, 3);

\echo 3 max depth 1: expected no path or an arity error
select graph_shortest_path(1, 3, 1);
