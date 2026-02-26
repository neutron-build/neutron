# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Computation Graph IR
# ===----------------------------------------------------------------------=== #

"""Graph IR for capturing tensor operations.

Represents tensor computations as a directed acyclic graph (DAG) of operations.
Nodes represent operations, edges represent data flow. This IR is the input to
the e-graph rewrite engine for optimization and fusion.
"""

from collections import Optional

# ===----------------------------------------------------------------------=== #
# OpKind — Operation types
# ===----------------------------------------------------------------------=== #

struct OpKind(Writable, TrivialRegisterPassable):
    """Enum for operation types."""
    var _value: Int

    # Elementwise binary
    comptime Add = OpKind(0)
    comptime Sub = OpKind(1)
    comptime Mul = OpKind(2)
    comptime Div = OpKind(3)

    # Matrix ops
    comptime Matmul = OpKind(10)
    comptime Transpose = OpKind(11)
    comptime Reshape = OpKind(12)

    # Activations
    comptime ReLU = OpKind(20)
    comptime GeLU = OpKind(21)
    comptime SiLU = OpKind(22)

    # Reductions
    comptime ReduceSum = OpKind(30)
    comptime ReduceMax = OpKind(31)
    comptime ReduceMean = OpKind(32)

    # Norms
    comptime RMSNorm = OpKind(40)
    comptime LayerNorm = OpKind(41)

    # Fused ops
    comptime FusedRMSNormLinear = OpKind(60)  # RMSNorm + Matmul in one pass
    comptime FusedLinearResAdd = OpKind(61)    # Matmul + Residual Add in one pass
    comptime SwiGLU = OpKind(62)               # SiLU(gate) * up (fused activation)

    # Constants
    comptime Const = OpKind(50)
    comptime Input = OpKind(51)

    @implicit
    fn __init__(out self, value: Int):
        self._value = value

    fn __eq__(self, other: OpKind) -> Bool:
        return self._value == other._value

    fn __ne__(self, other: OpKind) -> Bool:
        return self._value != other._value

    fn write_to[W: Writer](self, mut writer: W):
        if self._value == 0:
            writer.write("Add")
        elif self._value == 1:
            writer.write("Sub")
        elif self._value == 2:
            writer.write("Mul")
        elif self._value == 3:
            writer.write("Div")
        elif self._value == 10:
            writer.write("Matmul")
        elif self._value == 11:
            writer.write("Transpose")
        elif self._value == 12:
            writer.write("Reshape")
        elif self._value == 20:
            writer.write("ReLU")
        elif self._value == 21:
            writer.write("GeLU")
        elif self._value == 22:
            writer.write("SiLU")
        elif self._value == 30:
            writer.write("ReduceSum")
        elif self._value == 31:
            writer.write("ReduceMax")
        elif self._value == 32:
            writer.write("ReduceMean")
        elif self._value == 40:
            writer.write("RMSNorm")
        elif self._value == 41:
            writer.write("LayerNorm")
        elif self._value == 50:
            writer.write("Const")
        elif self._value == 51:
            writer.write("Input")
        elif self._value == 60:
            writer.write("FusedRMSNormLinear")
        elif self._value == 61:
            writer.write("FusedLinearResAdd")
        elif self._value == 62:
            writer.write("SwiGLU")
        else:
            writer.write("Unknown")


# ===----------------------------------------------------------------------=== #
# ValueId — Reference to a graph node's output
# ===----------------------------------------------------------------------=== #

struct ValueId(Writable, TrivialRegisterPassable):
    """Reference to a node's output value."""
    var _id: Int

    fn __init__(out self, id: Int):
        self._id = id

    fn __eq__(self, other: ValueId) -> Bool:
        return self._id == other._id

    fn __ne__(self, other: ValueId) -> Bool:
        return self._id != other._id

    fn id(self) -> Int:
        return self._id

    fn write_to[W: Writer](self, mut writer: W):
        writer.write("v")
        writer.write(String(self._id))


# ===----------------------------------------------------------------------=== #
# ENode — Graph node (operation + inputs)
# ===----------------------------------------------------------------------=== #

struct ENode(Writable, Copyable, Movable):
    """A graph node representing an operation.

    Stores the operation type and input value IDs. Does not store the
    result value - that's managed by the graph.
    """
    var op: OpKind
    var inputs: List[ValueId]

    fn __init__(out self, op: OpKind):
        self.op = op
        self.inputs = List[ValueId]()

    fn __init__(out self, op: OpKind, input0: ValueId):
        self.op = op
        self.inputs = List[ValueId]()
        self.inputs.append(input0)

    fn __init__(out self, op: OpKind, input0: ValueId, input1: ValueId):
        self.op = op
        self.inputs = List[ValueId]()
        self.inputs.append(input0)
        self.inputs.append(input1)

    fn __copyinit__(out self, existing: Self):
        self.op = existing.op
        self.inputs = existing.inputs.copy()

    fn write_to[W: Writer](self, mut writer: W):
        writer.write(String(self.op))
        writer.write("(")
        for i in range(len(self.inputs)):
            if i > 0:
                writer.write(", ")
            writer.write(String(self.inputs[i]))
        writer.write(")")


# ===----------------------------------------------------------------------=== #
# ComputationGraph — DAG of operations
# ===----------------------------------------------------------------------=== #

struct ComputationGraph(Writable):
    """Directed acyclic graph of tensor operations.

    Nodes are operations (ENode), edges are data dependencies (ValueId).
    This is the input to the e-graph rewrite engine.
    """
    var nodes: List[ENode]
    var _next_id: Int

    fn __init__(out self):
        self.nodes = List[ENode]()
        self._next_id = 0

    fn add_node(mut self, var node: ENode) -> ValueId:
        """Add a node to the graph and return its output ValueId."""
        var id = self._next_id
        self._next_id += 1
        self.nodes.append(node^)
        return ValueId(id)

    fn input(mut self) -> ValueId:
        """Create an input node."""
        return self.add_node(ENode(OpKind.Input))

    fn constant(mut self) -> ValueId:
        """Create a constant node."""
        return self.add_node(ENode(OpKind.Const))

    fn add(mut self, a: ValueId, b: ValueId) -> ValueId:
        """Create an Add node."""
        return self.add_node(ENode(OpKind.Add, a, b))

    fn mul(mut self, a: ValueId, b: ValueId) -> ValueId:
        """Create a Mul node."""
        return self.add_node(ENode(OpKind.Mul, a, b))

    fn matmul(mut self, a: ValueId, b: ValueId) -> ValueId:
        """Create a Matmul node."""
        return self.add_node(ENode(OpKind.Matmul, a, b))

    fn relu(mut self, x: ValueId) -> ValueId:
        """Create a ReLU node."""
        return self.add_node(ENode(OpKind.ReLU, x))

    fn gelu(mut self, x: ValueId) -> ValueId:
        """Create a GeLU node."""
        return self.add_node(ENode(OpKind.GeLU, x))

    fn transpose(mut self, x: ValueId) -> ValueId:
        """Create a Transpose node."""
        return self.add_node(ENode(OpKind.Transpose, x))

    fn silu(mut self, x: ValueId) -> ValueId:
        """Create a SiLU node."""
        return self.add_node(ENode(OpKind.SiLU, x))

    fn rmsnorm(mut self, x: ValueId, weight: ValueId) -> ValueId:
        """Create an RMSNorm node."""
        return self.add_node(ENode(OpKind.RMSNorm, x, weight))

    fn fused_rmsnorm_linear(mut self, x: ValueId, norm_w: ValueId, proj_w: ValueId) -> ValueId:
        """Create a FusedRMSNormLinear node (3 inputs)."""
        var node = ENode(OpKind.FusedRMSNormLinear)
        node.inputs.append(x)
        node.inputs.append(norm_w)
        node.inputs.append(proj_w)
        return self.add_node(node^)

    fn fused_linear_res_add(mut self, residual: ValueId, weight: ValueId, x: ValueId) -> ValueId:
        """Create a FusedLinearResAdd node (3 inputs)."""
        var node = ENode(OpKind.FusedLinearResAdd)
        node.inputs.append(residual)
        node.inputs.append(weight)
        node.inputs.append(x)
        return self.add_node(node^)

    fn swiglu(mut self, gate: ValueId, up: ValueId) -> ValueId:
        """Create a SwiGLU node."""
        return self.add_node(ENode(OpKind.SwiGLU, gate, up))

    fn write_to[W: Writer](self, mut writer: W):
        writer.write("ComputationGraph(")
        writer.write(String(len(self.nodes)))
        writer.write(" nodes)")
