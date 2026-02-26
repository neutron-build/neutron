# Quick test for Graph IR

from neutron_mojo.fusion.graph import ComputationGraph, OpKind

fn main() raises:
    print("test_graph_ir:")

    var g = ComputationGraph()

    # Build: y = (x + 2) * 3
    var x = g.input()
    var c1 = g.constant()
    var c2 = g.constant()
    var add_result = g.add(x, c1)
    var mul_result = g.mul(add_result, c2)

    print("  Graph has " + String(len(g.nodes)) + " nodes")
    if len(g.nodes) != 5:
        raise Error("Expected 5 nodes, got " + String(len(g.nodes)))

    # Check node types
    if g.nodes[0].op != OpKind.Input:
        raise Error("Node 0 should be Input")
    if g.nodes[3].op != OpKind.Add:
        raise Error("Node 3 should be Add")
    if g.nodes[4].op != OpKind.Mul:
        raise Error("Node 4 should be Mul")

    print("  graph_construction: PASS")
    print("ALL PASSED")
