# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Graph Executor Tests
# ===----------------------------------------------------------------------=== #

"""Tests for the computation graph executor."""

from math import abs

from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.tensor.simd_math import (
    par_simd_matvec, simd_rmsnorm, simd_silu, simd_swiglu,
    fused_rmsnorm_matvec, fused_matvec_residual_add,
)
from neutron_mojo.fusion.graph import OpKind, ValueId, ENode, ComputationGraph
from neutron_mojo.fusion.executor import TensorValue, GraphExecutor, optimize_and_execute
from neutron_mojo.fusion.rules import create_default_ruleset


fn assert_true(cond: Bool, msg: String) raises:
    if not cond:
        raise Error("Assertion failed: " + msg)


fn approx_eq(a: Float32, b: Float32, tol: Float32 = 1e-4) -> Bool:
    return abs(a - b) < tol


fn make_vector(size: Int, val: Float32) -> Tensor[DType.float32]:
    """Create a tensor filled with a constant value."""
    var t = Tensor[DType.float32](Shape(size))
    for i in range(size):
        t.set(i, val)
    return t^


fn make_range_vector(size: Int) -> Tensor[DType.float32]:
    """Create a tensor with values 1.0, 2.0, ..., size."""
    var t = Tensor[DType.float32](Shape(size))
    for i in range(size):
        t.set(i, Float32(i + 1))
    return t^


fn make_matrix(rows: Int, cols: Int, val: Float32) -> Tensor[DType.float32]:
    """Create a matrix (flat row-major) filled with a constant."""
    var t = Tensor[DType.float32](Shape(rows * cols))
    for i in range(rows * cols):
        t.set(i, val)
    return t^


fn make_identity_matrix(dim: Int) -> Tensor[DType.float32]:
    """Create an identity matrix (flat row-major)."""
    var t = Tensor[DType.float32](Shape(dim * dim))
    for i in range(dim * dim):
        t.set(i, 0.0)
    for i in range(dim):
        t.set(i * dim + i, 1.0)
    return t^


# ===----------------------------------------------------------------------=== #
# Tests
# ===----------------------------------------------------------------------=== #

fn test_execute_input() raises:
    """Pass-through input."""
    var graph = ComputationGraph()
    _ = graph.input()

    var inputs = List[TensorValue]()
    var v = make_vector(4, 3.0)
    inputs.append(TensorValue(v^))

    var executor = GraphExecutor()
    var result = executor.execute(graph, inputs)

    assert_true(result.numel() == 4, "Should have 4 elements")
    assert_true(approx_eq(result.data.get(0), 3.0), "Value should be 3.0")
    assert_true(approx_eq(result.data.get(3), 3.0), "Last value should be 3.0")
    print("  execute_input: PASS")


fn test_execute_add() raises:
    """Elementwise add."""
    var graph = ComputationGraph()
    var a = graph.input()
    var b = graph.input()
    _ = graph.add(a, b)

    var inputs = List[TensorValue]()
    inputs.append(TensorValue(make_vector(4, 2.0)))
    inputs.append(TensorValue(make_vector(4, 3.0)))

    var executor = GraphExecutor()
    var result = executor.execute(graph, inputs)

    assert_true(result.numel() == 4, "Should have 4 elements")
    assert_true(approx_eq(result.data.get(0), 5.0), "2+3=5")
    assert_true(approx_eq(result.data.get(3), 5.0), "All should be 5")
    print("  execute_add: PASS")


fn test_execute_mul() raises:
    """Elementwise mul."""
    var graph = ComputationGraph()
    var a = graph.input()
    var b = graph.input()
    _ = graph.mul(a, b)

    var inputs = List[TensorValue]()
    inputs.append(TensorValue(make_vector(4, 2.0)))
    inputs.append(TensorValue(make_vector(4, 3.0)))

    var executor = GraphExecutor()
    var result = executor.execute(graph, inputs)

    assert_true(result.numel() == 4, "Should have 4 elements")
    assert_true(approx_eq(result.data.get(0), 6.0), "2*3=6")
    print("  execute_mul: PASS")


fn test_execute_matmul() raises:
    """Matrix-vector multiply using identity matrix."""
    var dim = 4
    var graph = ComputationGraph()
    var w = graph.input()  # weight (identity)
    var x = graph.input()  # input vector
    _ = graph.matmul(w, x)

    var inputs = List[TensorValue]()
    inputs.append(TensorValue(make_identity_matrix(dim), dim, dim))
    inputs.append(TensorValue(make_range_vector(dim)))

    var executor = GraphExecutor()
    var result = executor.execute(graph, inputs)

    assert_true(result.numel() == dim, "Should have dim elements")
    # Identity * [1,2,3,4] = [1,2,3,4]
    for i in range(dim):
        assert_true(approx_eq(result.data.get(i), Float32(i + 1)),
                    "Identity matmul should preserve input")
    print("  execute_matmul: PASS")


fn test_execute_rmsnorm() raises:
    """RMSNorm with ones weight."""
    var dim = 4
    var graph = ComputationGraph()
    var x = graph.input()
    var w = graph.input()
    _ = graph.rmsnorm(x, w)

    var inputs = List[TensorValue]()
    inputs.append(TensorValue(make_vector(dim, 2.0)))
    inputs.append(TensorValue(make_vector(dim, 1.0)))

    var executor = GraphExecutor()
    var result = executor.execute(graph, inputs)

    # RMSNorm of constant vector with ones weight = 1.0 (approximately)
    assert_true(result.numel() == dim, "Should have dim elements")
    # rms = sqrt(mean(x^2)) = sqrt(4) = 2, so x/rms = 2/2 = 1.0
    assert_true(approx_eq(result.data.get(0), 1.0, 0.01),
                "RMSNorm(2,2,2,2) with weight=1 should be ~1.0")
    print("  execute_rmsnorm: PASS")


fn test_execute_silu() raises:
    """SiLU activation."""
    var dim = 4
    var graph = ComputationGraph()
    var x = graph.input()
    _ = graph.silu(x)

    var inputs = List[TensorValue]()
    inputs.append(TensorValue(make_vector(dim, 0.0)))

    var executor = GraphExecutor()
    var result = executor.execute(graph, inputs)

    # SiLU(0) = 0 * sigmoid(0) = 0 * 0.5 = 0
    assert_true(result.numel() == dim, "Should have dim elements")
    assert_true(approx_eq(result.data.get(0), 0.0), "SiLU(0)=0")
    print("  execute_silu: PASS")


fn test_execute_swiglu() raises:
    """Fused SwiGLU."""
    var dim = 4
    var graph = ComputationGraph()
    var gate = graph.input()
    var up = graph.input()
    _ = graph.swiglu(gate, up)

    var inputs = List[TensorValue]()
    inputs.append(TensorValue(make_vector(dim, 0.0)))  # gate
    inputs.append(TensorValue(make_vector(dim, 1.0)))  # up

    var executor = GraphExecutor()
    var result = executor.execute(graph, inputs)

    # SwiGLU(0, 1) = silu(0) * 1 = 0
    assert_true(result.numel() == dim, "Should have dim elements")
    assert_true(approx_eq(result.data.get(0), 0.0), "SwiGLU(0,1)=0")
    print("  execute_swiglu: PASS")


fn test_execute_chain() raises:
    """Multi-step chain: x -> rmsnorm -> matmul -> add residual."""
    var dim = 4
    var graph = ComputationGraph()
    var x = graph.input()           # 0: input vector
    var norm_w = graph.input()      # 1: norm weight (ones)
    var weight = graph.input()      # 2: projection weight (identity)
    var residual = graph.input()    # 3: residual vector
    var normed = graph.rmsnorm(x, norm_w)     # 4
    var projected = graph.matmul(weight, normed)  # 5
    _ = graph.add(residual, projected)  # 6

    var inputs = List[TensorValue]()
    inputs.append(TensorValue(make_vector(dim, 2.0)))      # x
    inputs.append(TensorValue(make_vector(dim, 1.0)))      # norm_w
    inputs.append(TensorValue(make_identity_matrix(dim), dim, dim))  # weight
    inputs.append(TensorValue(make_vector(dim, 1.0)))      # residual

    var executor = GraphExecutor()
    var result = executor.execute(graph, inputs)

    # rmsnorm([2,2,2,2], ones) ≈ [1,1,1,1], identity matmul ≈ [1,1,1,1], + residual [1,1,1,1] ≈ [2,2,2,2]
    assert_true(result.numel() == dim, "Should have dim elements")
    assert_true(approx_eq(result.data.get(0), 2.0, 0.05),
                "Chain result should be ~2.0")
    print("  execute_chain: PASS")


fn test_execute_fused_rmsnorm_linear() raises:
    """Fused RMSNorm + linear matches unfused sequence."""
    var dim = 4
    # Unfused: rmsnorm then matmul
    var g1 = ComputationGraph()
    var x1 = g1.input()
    var nw1 = g1.input()
    var pw1 = g1.input()
    var normed1 = g1.rmsnorm(x1, nw1)
    _ = g1.matmul(pw1, normed1)

    # Fused
    var g2 = ComputationGraph()
    var x2 = g2.input()
    var nw2 = g2.input()
    var pw2 = g2.input()
    _ = g2.fused_rmsnorm_linear(x2, nw2, pw2)

    var inputs1 = List[TensorValue]()
    inputs1.append(TensorValue(make_range_vector(dim)))
    inputs1.append(TensorValue(make_vector(dim, 1.0)))
    inputs1.append(TensorValue(make_identity_matrix(dim), dim, dim))

    var inputs2 = List[TensorValue]()
    inputs2.append(TensorValue(make_range_vector(dim)))
    inputs2.append(TensorValue(make_vector(dim, 1.0)))
    inputs2.append(TensorValue(make_identity_matrix(dim), dim, dim))

    var executor = GraphExecutor()
    var r1 = executor.execute(g1, inputs1)
    var r2 = executor.execute(g2, inputs2)

    assert_true(r1.numel() == r2.numel(), "Same output size")
    for i in range(r1.numel()):
        assert_true(approx_eq(r1.data.get(i), r2.data.get(i), 0.01),
                    "Fused should match unfused")
    print("  execute_fused_rmsnorm_linear: PASS")


fn test_execute_fused_linear_res_add() raises:
    """Fused linear + residual add matches unfused sequence."""
    var dim = 4
    # Unfused: matmul then add
    var g1 = ComputationGraph()
    var res1 = g1.input()
    var w1 = g1.input()
    var x1 = g1.input()
    var proj1 = g1.matmul(w1, x1)
    _ = g1.add(res1, proj1)

    # Fused
    var g2 = ComputationGraph()
    var res2 = g2.input()
    var w2 = g2.input()
    var x2 = g2.input()
    _ = g2.fused_linear_res_add(res2, w2, x2)

    var inputs1 = List[TensorValue]()
    inputs1.append(TensorValue(make_vector(dim, 1.0)))      # residual
    inputs1.append(TensorValue(make_identity_matrix(dim), dim, dim))  # weight
    inputs1.append(TensorValue(make_range_vector(dim)))      # x

    var inputs2 = List[TensorValue]()
    inputs2.append(TensorValue(make_vector(dim, 1.0)))
    inputs2.append(TensorValue(make_identity_matrix(dim), dim, dim))
    inputs2.append(TensorValue(make_range_vector(dim)))

    var executor = GraphExecutor()
    var r1 = executor.execute(g1, inputs1)
    var r2 = executor.execute(g2, inputs2)

    assert_true(r1.numel() == r2.numel(), "Same output size")
    for i in range(r1.numel()):
        assert_true(approx_eq(r1.data.get(i), r2.data.get(i), 0.01),
                    "Fused should match unfused")
    print("  execute_fused_linear_res_add: PASS")


fn test_optimize_then_execute() raises:
    """Build graph, optimize via e-graph, execute optimized version."""
    var dim = 4
    # Simple add graph — no fusions, just passes through
    var graph = ComputationGraph()
    var a = graph.input()
    var b = graph.input()
    _ = graph.add(a, b)

    var inputs = List[TensorValue]()
    inputs.append(TensorValue(make_vector(dim, 2.0)))
    inputs.append(TensorValue(make_vector(dim, 3.0)))

    var ruleset = create_default_ruleset()
    var result = optimize_and_execute(graph, inputs, ruleset)

    assert_true(result.numel() == dim, "Should have dim elements, got " + String(result.numel()))
    assert_true(approx_eq(result.data.get(0), 5.0, 0.01), "2+3=5 after optimize")
    print("  optimize_then_execute: PASS")


fn test_optimized_matches_unoptimized() raises:
    """Results from optimized graph should match unoptimized within tolerance."""
    var dim = 4
    var graph = ComputationGraph()
    var a = graph.input()
    var b = graph.input()
    var sum_ab = graph.add(a, b)
    var c = graph.input()
    _ = graph.mul(sum_ab, c)

    # Unoptimized
    var inputs1 = List[TensorValue]()
    inputs1.append(TensorValue(make_vector(dim, 2.0)))
    inputs1.append(TensorValue(make_vector(dim, 3.0)))
    inputs1.append(TensorValue(make_vector(dim, 4.0)))

    var executor = GraphExecutor()
    var r1 = executor.execute(graph, inputs1)

    # Optimized (same graph, but runs through e-graph)
    var inputs2 = List[TensorValue]()
    inputs2.append(TensorValue(make_vector(dim, 2.0)))
    inputs2.append(TensorValue(make_vector(dim, 3.0)))
    inputs2.append(TensorValue(make_vector(dim, 4.0)))

    var ruleset = create_default_ruleset()
    var r2 = optimize_and_execute(graph, inputs2, ruleset)

    assert_true(r1.numel() == r2.numel(), "Same size")
    for i in range(r1.numel()):
        assert_true(approx_eq(r1.data.get(i), r2.data.get(i), 0.01),
                    "Optimized should match unoptimized")
    print("  optimized_matches_unoptimized: PASS")


fn test_swiglu_fusion_end_to_end() raises:
    """Graph with silu+mul, optimize to swiglu, execute."""
    var dim = 4
    # Build: mul(silu(gate), up) — should fuse to swiglu
    var graph = ComputationGraph()
    var gate = graph.input()
    var up = graph.input()
    var silu_gate = graph.silu(gate)
    _ = graph.mul(silu_gate, up)

    # Direct unfused execution
    var inputs1 = List[TensorValue]()
    inputs1.append(TensorValue(make_vector(dim, 1.0)))
    inputs1.append(TensorValue(make_vector(dim, 2.0)))

    var executor = GraphExecutor()
    var r1 = executor.execute(graph, inputs1)

    # Optimized (should fuse silu+mul -> swiglu)
    var inputs2 = List[TensorValue]()
    inputs2.append(TensorValue(make_vector(dim, 1.0)))
    inputs2.append(TensorValue(make_vector(dim, 2.0)))

    var ruleset = create_default_ruleset()
    var r2 = optimize_and_execute(graph, inputs2, ruleset)

    assert_true(r1.numel() == r2.numel(), "Same size")
    for i in range(r1.numel()):
        assert_true(approx_eq(r1.data.get(i), r2.data.get(i), 0.01),
                    "Fused swiglu should match unfused silu+mul")
    print("  swiglu_fusion_end_to_end: PASS")


fn test_execute_with_constant() raises:
    """Const tensor in graph."""
    var graph = ComputationGraph()
    var x = graph.input()
    var c = graph.constant()
    _ = graph.add(x, c)

    var inputs = List[TensorValue]()
    inputs.append(TensorValue(make_vector(4, 5.0)))
    inputs.append(TensorValue(make_vector(4, 10.0)))

    var executor = GraphExecutor()
    var result = executor.execute(graph, inputs)

    assert_true(result.numel() == 4, "Should have 4 elements")
    assert_true(approx_eq(result.data.get(0), 15.0), "5+10=15")
    print("  execute_with_constant: PASS")


fn test_execute_larger_matmul() raises:
    """Larger matmul to exercise SIMD paths."""
    var dim = 32
    var graph = ComputationGraph()
    var w = graph.input()
    var x = graph.input()
    _ = graph.matmul(w, x)

    # Weight: each row sums to 1 (uniform 1/dim)
    var wt = Tensor[DType.float32](Shape(dim * dim))
    var inv_dim = Float32(1.0) / Float32(dim)
    for i in range(dim * dim):
        wt.set(i, inv_dim)

    var inputs = List[TensorValue]()
    inputs.append(TensorValue(wt^, dim, dim))
    inputs.append(TensorValue(make_vector(dim, 1.0)))

    var executor = GraphExecutor()
    var result = executor.execute(graph, inputs)

    # Each output = sum(1/dim * 1.0 for _ in range(dim)) = 1.0
    assert_true(result.numel() == dim, "Should have dim elements")
    assert_true(approx_eq(result.data.get(0), 1.0, 0.01), "Uniform matvec should give 1.0")
    print("  execute_larger_matmul: PASS")


fn main() raises:
    print("test_executor")
    test_execute_input()
    test_execute_add()
    test_execute_mul()
    test_execute_matmul()
    test_execute_rmsnorm()
    test_execute_silu()
    test_execute_swiglu()
    test_execute_chain()
    test_execute_fused_rmsnorm_linear()
    test_execute_fused_linear_res_add()
    test_optimize_then_execute()
    test_optimized_matches_unoptimized()
    test_swiglu_fusion_end_to_end()
    test_execute_with_constant()
    test_execute_larger_matmul()
    print("All 15 executor tests passed!")
