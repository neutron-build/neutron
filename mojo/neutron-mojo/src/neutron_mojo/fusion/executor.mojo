# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Graph Executor
# ===----------------------------------------------------------------------=== #

"""Execute computation graphs using SIMD kernels.

Bridges the fusion/e-graph symbolic optimizer with actual tensor execution.
Walks the DAG in topological order, dispatching each OpKind to the
corresponding SIMD kernel from tensor/simd_math.mojo.
"""

from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.tensor.simd_math import (
    par_simd_matvec,
    simd_rmsnorm,
    simd_silu,
    simd_swiglu,
    simd_axpy,
    fused_rmsnorm_matvec,
    fused_matvec_residual_add,
)
from .graph import OpKind, ValueId, ENode, ComputationGraph
from .egraph import EGraph, CanonicalNode
from .eclass import ClassId
from .rewrite import RewriteEngine, RewriteStats
from .rules import RuleSet


# ===----------------------------------------------------------------------=== #
# TensorValue — Wraps a Tensor with shape metadata for the executor
# ===----------------------------------------------------------------------=== #

struct TensorValue(Copyable, Movable):
    """Tensor with shape metadata for graph execution."""
    var data: Tensor[DType.float32]
    var rows: Int
    var cols: Int

    fn __init__(out self, var data: Tensor[DType.float32], rows: Int, cols: Int):
        self.data = data^
        self.rows = rows
        self.cols = cols

    fn __init__(out self, var data: Tensor[DType.float32]):
        """Create a 1D TensorValue (vector)."""
        var n = data.numel()
        self.data = data^
        self.rows = 1
        self.cols = n

    fn __copyinit__(out self, existing: Self):
        var n = existing.data.numel()
        var t = Tensor[DType.float32](existing.data.shape())
        for i in range(n):
            t.set(i, existing.data.get(i))
        self.data = t^
        self.rows = existing.rows
        self.cols = existing.cols

    fn __moveinit__(out self, deinit other: Self):
        self.data = other.data^
        self.rows = other.rows
        self.cols = other.cols

    fn numel(self) -> Int:
        return self.data.numel()

    fn copy(self) -> TensorValue:
        """Return a deep copy."""
        var t = Tensor[DType.float32](self.data.shape())
        var n = self.data.numel()
        for i in range(n):
            t.set(i, self.data.get(i))
        return TensorValue(t^, self.rows, self.cols)


# ===----------------------------------------------------------------------=== #
# Helper: elementwise ops via get/set (safe against data_ptr aliasing)
# ===----------------------------------------------------------------------=== #

fn _elementwise_add(
    a: Tensor[DType.float32],
    b: Tensor[DType.float32],
    n: Int,
) -> Tensor[DType.float32]:
    var out = Tensor[DType.float32](Shape(n))
    for i in range(n):
        out.set(i, a.get(i) + b.get(i))
    return out^


fn _elementwise_mul(
    a: Tensor[DType.float32],
    b: Tensor[DType.float32],
    n: Int,
) -> Tensor[DType.float32]:
    var out = Tensor[DType.float32](Shape(n))
    for i in range(n):
        out.set(i, a.get(i) * b.get(i))
    return out^


# ===----------------------------------------------------------------------=== #
# GraphExecutor — Walks DAG and dispatches to SIMD kernels
# ===----------------------------------------------------------------------=== #

struct GraphExecutor:
    """Executes a ComputationGraph by walking nodes in order."""
    var _state: Int

    fn __init__(out self):
        self._state = 0

    fn execute(
        self,
        graph: ComputationGraph,
        inputs: List[TensorValue],
    ) raises -> TensorValue:
        """Execute a computation graph with the given inputs."""
        if len(graph.nodes) == 0:
            raise Error("Cannot execute empty graph")

        var values = List[TensorValue]()
        var input_idx = 0

        for node_idx in range(len(graph.nodes)):
            var node = graph.nodes[node_idx].copy()
            var op = node.op

            if op == OpKind.Input or op == OpKind.Const:
                if input_idx >= len(inputs):
                    raise Error("Not enough inputs for graph")
                values.append(inputs[input_idx].copy())
                input_idx += 1

            elif op == OpKind.Add:
                var a = values[node.inputs[0].id()].copy()
                var b = values[node.inputs[1].id()].copy()
                var n = a.numel()
                var out = _elementwise_add(a.data, b.data, n)
                values.append(TensorValue(out^, a.rows, a.cols))

            elif op == OpKind.Mul:
                var a = values[node.inputs[0].id()].copy()
                var b = values[node.inputs[1].id()].copy()
                var n = a.numel()
                var out = _elementwise_mul(a.data, b.data, n)
                values.append(TensorValue(out^, a.rows, a.cols))

            elif op == OpKind.Matmul:
                var weight = values[node.inputs[0].id()].copy()
                var x = values[node.inputs[1].id()].copy()
                var mrows = weight.rows
                var mcols = weight.cols
                var out = Tensor[DType.float32](Shape(mrows))
                par_simd_matvec(out, 0, weight.data, 0, x.data, 0, mrows, mcols)
                values.append(TensorValue(out^, 1, mrows))

            elif op == OpKind.RMSNorm:
                var x = values[node.inputs[0].id()].copy()
                var w = values[node.inputs[1].id()].copy()
                var n = x.numel()
                var out = Tensor[DType.float32](Shape(n))
                simd_rmsnorm(out, 0, x.data, 0, w.data, 0, n)
                values.append(TensorValue(out^, 1, n))

            elif op == OpKind.SiLU:
                var x = values[node.inputs[0].id()].copy()
                var n = x.numel()
                var out = Tensor[DType.float32](Shape(n))
                simd_silu(out, 0, x.data, 0, n)
                values.append(TensorValue(out^, x.rows, x.cols))

            elif op == OpKind.SwiGLU:
                var gate = values[node.inputs[0].id()].copy()
                var up = values[node.inputs[1].id()].copy()
                var n = gate.numel()
                var out = Tensor[DType.float32](Shape(n))
                simd_swiglu(out, 0, gate.data, 0, up.data, 0, n)
                values.append(TensorValue(out^, gate.rows, gate.cols))

            elif op == OpKind.FusedRMSNormLinear:
                var x = values[node.inputs[0].id()].copy()
                var norm_w = values[node.inputs[1].id()].copy()
                var proj_w = values[node.inputs[2].id()].copy()
                var hidden = x.numel()
                var out_dim = proj_w.rows
                var out = Tensor[DType.float32](Shape(out_dim))
                fused_rmsnorm_matvec(
                    out, 0, x.data, 0, norm_w.data, 0,
                    proj_w.data, 0, hidden, out_dim,
                )
                values.append(TensorValue(out^, 1, out_dim))

            elif op == OpKind.FusedLinearResAdd:
                var residual = values[node.inputs[0].id()].copy()
                var weight = values[node.inputs[1].id()].copy()
                var x = values[node.inputs[2].id()].copy()
                var rdim = residual.numel()
                var rcols = x.numel()
                var out = Tensor[DType.float32](Shape(rdim))
                fused_matvec_residual_add(
                    out, 0, residual.data, 0,
                    weight.data, 0, x.data, 0, rdim, rcols,
                )
                values.append(TensorValue(out^, 1, rdim))

            else:
                raise Error("Unsupported op in executor")

        return values[len(values) - 1].copy()


# ===----------------------------------------------------------------------=== #
# Convenience: optimize then execute
# ===----------------------------------------------------------------------=== #

fn optimize_and_execute(
    graph: ComputationGraph,
    inputs: List[TensorValue],
    ruleset: RuleSet,
) raises -> TensorValue:
    """Build e-graph from graph, run Phase 1 rewrites to find optimizations,
    then execute the original graph.

    The e-graph analysis identifies potential fusions/simplifications.
    Actual graph-to-graph extraction is deferred to a future sprint;
    for now we execute the original graph to guarantee correctness.

    Args:
        graph: The computation graph to optimize and execute.
        inputs: Input tensors.
        ruleset: Rewrite rules to apply.

    Returns:
        The result TensorValue.
    """
    # Build e-graph to analyze potential optimizations
    var egraph = EGraph()
    var class_ids = List[ClassId]()

    for node_idx in range(len(graph.nodes)):
        var node = graph.nodes[node_idx].copy()
        var cn = CanonicalNode(node.op)
        for i in range(len(node.inputs)):
            cn.inputs.append(class_ids[node.inputs[i].id()])
        var cid = egraph.add(cn^)
        class_ids.append(cid)

    # Run Phase 1 rewrites (analysis only)
    var engine = RewriteEngine(max_iterations=10)
    _ = engine.run_phase1(egraph, ruleset)

    # Execute the original graph (correct by construction)
    var executor = GraphExecutor()
    return executor.execute(graph, inputs)
