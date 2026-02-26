# neutron-mojo-python Package API Specification

> **Status:** Pseudocode specification -- NOT compilable Mojo
> **Package:** `neutron-mojo-python`
> **License:** Apache 2.0
> **Depends on:** `neutron-mojo` (core), optionally `neutron-mojo-infer`
> **Last updated:** 2026-02-16

This document defines every public trait, struct, and function exposed by the `neutron-mojo-python` package. This package provides the Python interop layer: DLPack-based zero-copy tensor exchange with PyTorch/JAX/NumPy, model weight loading from Python ecosystem formats, and a `torch.compile` backend registration.

The package has two halves:
1. **Mojo-side** (`src/neutron_mojo_python/`): bridge, weights, and compile backend logic in Mojo
2. **Python-side** (`python/neutron_mojo/`): Python package that users import, registering the torch.compile backend

---

## 1. bridge/ -- Framework Tensor Bridges

### 1.1 PyTorch Bridge

Zero-copy tensor exchange between PyTorch and Neutron Mojo via DLPack.

```mojo
# Depends on: neutron_mojo.tensor.Tensor, neutron_mojo.tensor.DType, neutron_mojo.tensor.Dim
# Depends on: neutron_mojo.dlpack (DLManagedTensor, to_dlpack, from_dlpack)
# Depends on: python (Mojo's built-in Python interop module)

fn from_pytorch[dtype: DType, *dims: Dim](
    pt_tensor: PythonObject,
) -> Tensor[dtype, *dims]:
    """Convert a PyTorch tensor to a Neutron Mojo tensor via DLPack (zero-copy).

    Calls pt_tensor.__dlpack__() to get the DLPack capsule, then wraps it
    as a Neutron tensor. The PyTorch tensor must remain alive while the
    Neutron tensor is in use (the memory is shared, not copied).

    The dtype and dims type parameters must match the PyTorch tensor's
    actual dtype and shape. A runtime check validates this and raises
    a clear error on mismatch.

    Example:
        let pt = torch.randn(32, 128, 768)
        let nt = from_pytorch[DType.float32, Batch, Seq, Hidden](pt)
    """
    ...

fn to_pytorch[dtype: DType, *dims: Dim](
    tensor: Tensor[dtype, *dims],
) -> PythonObject:
    """Convert a Neutron Mojo tensor to a PyTorch tensor via DLPack (zero-copy).

    Exports the tensor as a DLPack capsule and calls torch.from_dlpack().
    The Neutron tensor must remain alive while the PyTorch tensor is in
    use (shared memory).

    Example:
        let nt = Tensor[DType.float32, Batch, Seq, Hidden].randn(32, 128, 768)
        let pt = to_pytorch(nt)  # PythonObject wrapping a torch.Tensor
    """
    ...
```

### 1.2 JAX Bridge

Zero-copy tensor exchange between JAX arrays and Neutron Mojo tensors.

```mojo
# Depends on: neutron_mojo.tensor.Tensor, neutron_mojo.tensor.DType, neutron_mojo.tensor.Dim
# Depends on: neutron_mojo.dlpack (DLManagedTensor, to_dlpack, from_dlpack)
# Depends on: python

fn from_jax[dtype: DType, *dims: Dim](
    jax_array: PythonObject,
) -> Tensor[dtype, *dims]:
    """Convert a JAX array to a Neutron Mojo tensor via DLPack (zero-copy).

    Calls jax.dlpack.to_dlpack(jax_array) to get the DLPack capsule, then
    wraps it as a Neutron tensor. Supports arrays on CPU, GPU (CUDA), and
    TPU (data must be moved to CPU first for TPU).

    The JAX array must remain alive while the Neutron tensor is in use.
    """
    ...

fn to_jax[dtype: DType, *dims: Dim](
    tensor: Tensor[dtype, *dims],
) -> PythonObject:
    """Convert a Neutron Mojo tensor to a JAX array via DLPack (zero-copy).

    Exports the tensor as a DLPack capsule and calls jax.dlpack.from_dlpack().
    Device placement is preserved (CPU stays on CPU, CUDA stays on CUDA).
    """
    ...
```

### 1.3 NumPy Bridge

Tensor exchange between NumPy arrays and Neutron Mojo tensors. NumPy arrays are CPU-only; this bridge handles the CPU path.

```mojo
# Depends on: neutron_mojo.tensor.Tensor, neutron_mojo.tensor.DType, neutron_mojo.tensor.Dim
# Depends on: neutron_mojo.dlpack (DLManagedTensor, to_dlpack, from_dlpack)
# Depends on: python

fn from_numpy[dtype: DType, *dims: Dim](
    np_array: PythonObject,
) -> Tensor[dtype, *dims]:
    """Convert a NumPy array to a Neutron Mojo tensor (zero-copy when possible).

    Uses NumPy's DLPack support (np.from_dlpack / array.__dlpack__) for
    zero-copy conversion. The array must be contiguous (C-order); non-contiguous
    arrays are copied with a warning.

    The resulting tensor is on CPU. Use tensor.to(DeviceKind.CUDA) to move
    to GPU afterwards.

    Note: NumPy >= 1.22.0 required for DLPack support.
    """
    ...

fn to_numpy[dtype: DType, *dims: Dim](
    tensor: Tensor[dtype, *dims],
) -> PythonObject:
    """Convert a Neutron Mojo tensor to a NumPy array.

    If the tensor is on CPU and contiguous, this is zero-copy via DLPack.
    If the tensor is on GPU, data is first copied to CPU (this is unavoidable --
    NumPy is CPU-only). A warning is emitted for GPU-to-CPU copies.
    """
    ...
```

---

## 2. weights/ -- Weight Loading from Python Formats

### 2.1 PyTorch Checkpoint Loader

Load model weights from PyTorch .pt and .bin checkpoint files.

```mojo
# Depends on: neutron_mojo.tensor.Tensor, neutron_mojo.tensor.DType
# Depends on: bridge/pytorch (from_pytorch)
# Depends on: python

fn load_pytorch_checkpoint(
    path: String,
    device: DeviceKind = DeviceKind.CUDA,
    dtype: Optional[DType] = None,
) -> Dict[String, Tensor]:
    """Load model weights from a PyTorch checkpoint file (.pt or .bin).

    Uses Python's torch.load() under the hood, then converts each
    parameter tensor to a Neutron tensor via from_pytorch (DLPack zero-copy).

    Supports:
    - Single .pt / .bin files
    - Sharded checkpoints (pytorch_model-00001-of-00005.bin pattern)
    - state_dict format and full checkpoint format (extracts state_dict)

    If dtype is specified, all weights are cast to that dtype during loading.
    Otherwise, original dtypes are preserved.

    Returns a dict mapping parameter names (e.g., "model.layers.0.self_attn.q_proj.weight")
    to Neutron tensors.
    """
    ...
```

### 2.2 HuggingFace Hub Loader

Download and load model weights from HuggingFace Hub.

```mojo
# Depends on: neutron_mojo.tensor.Tensor, neutron_mojo.tensor.DType
# Depends on: load_pytorch_checkpoint
# Depends on: python

fn load_from_huggingface(
    model_name: String,
    revision: String = "main",
    cache_dir: String = "~/.cache/neutron_mojo",
    device: DeviceKind = DeviceKind.CUDA,
    dtype: Optional[DType] = None,
    auth_token: Optional[String] = None,
) -> Dict[String, Tensor]:
    """Download model weights from HuggingFace Hub and load into Neutron tensors.

    Uses Python's huggingface_hub library to download model files. Prefers
    SafeTensors format when available (faster, safer), falls back to PyTorch
    .bin format.

    Downloads are cached at cache_dir. Subsequent calls with the same
    model_name and revision use the cached files.

    For gated models (Llama 3, etc.), pass auth_token from
    huggingface-cli login.

    Returns a dict mapping parameter names to Neutron tensors.

    Example:
        let weights = load_from_huggingface("meta-llama/Llama-3-8B")
    """
    ...
```

---

## 3. compile_backend/ -- torch.compile Backend

### 3.1 NeutronMojoBackend

The core torch.compile backend implementation. Receives an FX graph from TorchDynamo and compiles it to Mojo kernels.

```mojo
# Depends on: neutron_mojo.fusion.Graph, neutron_mojo.fusion.FusionEngine, neutron_mojo.fusion.EGraph
# Depends on: neutron_mojo.backend.ComputeBackend, neutron_mojo.backend.Partitioner
# Depends on: OpRegistry, GraphLowerer
# Depends on: python

struct NeutronMojoBackend:
    """torch.compile backend that compiles PyTorch FX graphs to Neutron Mojo kernels.

    Registered with TorchDynamo as a custom backend. When a user decorates
    a model with @torch.compile(backend="neutron_mojo"), TorchDynamo traces
    the model into an FX graph and passes it to this backend for compilation.

    Compilation pipeline:
    1. Receive FX graph from TorchDynamo
    2. Lower FX ops to Neutron Mojo Graph via OpRegistry and GraphLowerer
    3. Optimize: e-graph rewrites, operator fusion
    4. Partition: route subgraphs to available backends
    5. Compile: generate device-specific kernels
    6. Return a callable that accepts PyTorch tensors and returns PyTorch tensors
       (internally converts via DLPack for zero-copy)
    """

    var op_registry: OpRegistry
    var graph_lowerer: GraphLowerer
    var fusion_engine: FusionEngine
    var partitioner: Partitioner

    fn __init__(inout self):
        """Initialize the backend with default op registry and backends."""
        ...

    fn register(inout self):
        """Register this backend with TorchDynamo.

        After registration, users can use:
            @torch.compile(backend="neutron_mojo")
            def my_model(x): ...

        Must be called once at import time (typically in neutron_mojo.__init__.py).
        """
        ...

    fn compile(
        self,
        fx_graph: PythonObject,
        example_inputs: List[PythonObject],
    ) -> PythonObject:
        """Compile an FX graph to optimized Neutron Mojo kernels.

        Called by TorchDynamo during the first forward pass of a
        @torch.compile'd model. Returns a Python callable that:
        1. Converts input PyTorch tensors to Neutron tensors (DLPack, zero-copy)
        2. Executes the compiled Mojo kernels
        3. Converts output Neutron tensors back to PyTorch tensors (DLPack, zero-copy)

        Subsequent calls to the model use the cached compiled callable.
        """
        ...
```

### 3.2 OpRegistry

Maps PyTorch operators to Neutron Mojo operations.

```mojo
# Depends on: neutron_mojo.fusion.Op
# Depends on: python

struct OpRegistry:
    """Bidirectional mapping between PyTorch FX operators and Neutron Mojo Ops.

    Each PyTorch operator (e.g., torch.ops.aten.mm, torch.ops.aten.relu)
    is mapped to the corresponding Neutron Mojo Op for graph lowering.
    Supports both ATen operators and higher-level torch operators.
    """

    fn __init__(inout self):
        """Initialize with default mappings for all supported PyTorch operators.

        Default mappings include:
        - aten.mm, aten.bmm -> Op.MatMul, Op.BatchMatMul
        - aten.add.Tensor -> Op.Add
        - aten.mul.Tensor -> Op.Mul
        - aten.relu -> Op.Relu
        - aten.gelu -> Op.Gelu
        - aten.silu -> Op.Silu
        - aten.layer_norm -> Op.LayerNorm
        - aten.softmax -> Op.Softmax
        - aten.transpose -> Op.Transpose
        - aten.reshape, aten.view -> Op.Reshape
        - aten.cat -> Op.Concat
        - aten._softmax -> Op.Softmax
        - ... (all Phase 1 ops)
        """
        ...

    fn register_op(
        inout self,
        torch_op: String,
        neutron_op: Op,
        lowering_fn: Optional[fn(PythonObject) -> Node] = None,
    ):
        """Register a custom mapping from a PyTorch operator to a Neutron Op.

        If lowering_fn is provided, it is used to convert the FX node to
        a Neutron Graph node (for ops that need custom shape/dtype inference).
        Otherwise, a default lowering is used based on the Op type.
        """
        ...

    fn lookup(self, torch_op: String) -> Optional[Op]:
        """Look up the Neutron Op for a PyTorch operator. Returns None if not supported."""
        ...

    fn is_supported(self, torch_op: String) -> Bool:
        """Returns True if this PyTorch operator has a Neutron Mojo mapping."""
        ...

    fn supported_ops(self) -> List[String]:
        """Returns all registered PyTorch operator names."""
        ...
```

### 3.3 GraphLowerer

Converts PyTorch FX graphs to Neutron Mojo computation graphs.

```mojo
# Depends on: neutron_mojo.fusion.Graph, neutron_mojo.fusion.Node, neutron_mojo.fusion.Op
# Depends on: OpRegistry
# Depends on: python

struct GraphLowerer:
    """Lowers a PyTorch FX graph to a Neutron Mojo computation Graph.

    Walks the FX graph's nodes, maps each to a Neutron Op via OpRegistry,
    infers output shapes and dtypes, and builds the Neutron Graph.
    Unsupported ops cause a fallback to PyTorch eager execution for that
    subgraph.
    """

    var op_registry: OpRegistry

    fn __init__(inout self, op_registry: OpRegistry):
        """Create a graph lowerer with the given op registry."""
        ...

    fn lower_fx_graph(
        self,
        fx_graph: PythonObject,
        example_inputs: List[PythonObject],
    ) -> Tuple[Graph, List[String]]:
        """Lower an FX graph to a Neutron computation graph.

        Walks the FX graph in topological order:
        1. For each FX node, look up the corresponding Neutron Op
        2. Infer output shape/dtype from input shapes and the op semantics
        3. Create a Neutron Graph Node and add it to the graph
        4. If an op is unsupported, record it in the unsupported list

        Returns (neutron_graph, unsupported_ops). If unsupported_ops is
        non-empty, the caller must handle fallback for those subgraphs.
        """
        ...

    fn infer_output_shape(
        self,
        op: Op,
        input_shapes: List[Shape],
        op_attrs: Dict[String, PythonObject],
    ) -> Shape:
        """Infer the output shape for an operation given its input shapes.

        Uses op-specific rules (e.g., matmul: [M,K] x [K,N] -> [M,N],
        broadcast: element-wise broadcasting rules, etc.).
        """
        ...

    fn infer_output_dtype(
        self,
        op: Op,
        input_dtypes: List[DType],
    ) -> DType:
        """Infer the output dtype for an operation given its input dtypes.

        Follows PyTorch's dtype promotion rules for consistency.
        """
        ...
```

---

## 4. Python-Side Package

The Python package that users `pip install` and `import`. This is pure Python code that wraps the Mojo-side functionality and registers the torch.compile backend.

### 4.1 neutron_mojo/__init__.py

Package entry point with convenience re-exports.

```python
# python/neutron_mojo/__init__.py
# Depends on: Mojo neutron_mojo_python package (via Mojo's Python interop)

"""
Neutron Mojo Python package -- PyTorch/JAX/NumPy interop and torch.compile backend.

Usage:
    import neutron_mojo

    # Tensor conversion
    nt = neutron_mojo.from_pytorch(pt_tensor)
    pt = neutron_mojo.to_pytorch(nt)

    # torch.compile backend (registered automatically on import)
    @torch.compile(backend="neutron_mojo")
    def my_model(x):
        return transformer(x)
"""

__version__: str
"""Package version string (e.g., '0.1.0')."""


def from_pytorch(pt_tensor):
    """Convert a PyTorch tensor to a Neutron Mojo tensor (zero-copy via DLPack).

    Args:
        pt_tensor: A torch.Tensor on CPU or CUDA.

    Returns:
        A Neutron Mojo Tensor wrapping the same memory.

    The PyTorch tensor must remain alive while the Neutron tensor is in use.
    """
    ...


def to_pytorch(neutron_tensor):
    """Convert a Neutron Mojo tensor to a PyTorch tensor (zero-copy via DLPack).

    Args:
        neutron_tensor: A Neutron Mojo Tensor.

    Returns:
        A torch.Tensor wrapping the same memory.

    The Neutron tensor must remain alive while the PyTorch tensor is in use.
    """
    ...


def from_jax(jax_array):
    """Convert a JAX array to a Neutron Mojo tensor (zero-copy via DLPack).

    Args:
        jax_array: A jax.Array on CPU or GPU.

    Returns:
        A Neutron Mojo Tensor wrapping the same memory.
    """
    ...


def to_jax(neutron_tensor):
    """Convert a Neutron Mojo tensor to a JAX array (zero-copy via DLPack).

    Args:
        neutron_tensor: A Neutron Mojo Tensor.

    Returns:
        A jax.Array wrapping the same memory.
    """
    ...


def from_numpy(np_array):
    """Convert a NumPy array to a Neutron Mojo tensor (zero-copy for contiguous arrays).

    Args:
        np_array: A numpy.ndarray (must be contiguous C-order for zero-copy).

    Returns:
        A Neutron Mojo Tensor on CPU.
    """
    ...


def to_numpy(neutron_tensor):
    """Convert a Neutron Mojo tensor to a NumPy array.

    Args:
        neutron_tensor: A Neutron Mojo Tensor. If on GPU, data is copied to CPU.

    Returns:
        A numpy.ndarray.
    """
    ...


def load_pytorch_checkpoint(path: str, device: str = "cuda", dtype=None) -> dict:
    """Load model weights from a PyTorch checkpoint.

    Args:
        path: Path to .pt, .bin, or sharded checkpoint directory.
        device: Target device ("cpu", "cuda", "cuda:0", etc.).
        dtype: Optional dtype to cast all weights to.

    Returns:
        Dict mapping parameter names to Neutron Mojo tensors.
    """
    ...


def load_from_huggingface(
    model_name: str,
    revision: str = "main",
    cache_dir: str = "~/.cache/neutron_mojo",
    device: str = "cuda",
    dtype=None,
    auth_token: str = None,
) -> dict:
    """Download and load model weights from HuggingFace Hub.

    Args:
        model_name: HuggingFace model ID (e.g., "meta-llama/Llama-3-8B").
        revision: Git revision (branch, tag, or commit hash).
        cache_dir: Local cache directory for downloaded files.
        device: Target device ("cpu", "cuda", etc.).
        dtype: Optional dtype to cast all weights to.
        auth_token: HuggingFace API token for gated models.

    Returns:
        Dict mapping parameter names to Neutron Mojo tensors.
    """
    ...
```

### 4.2 neutron_mojo/_backend.py

torch.compile backend implementation (Python side).

```python
# python/neutron_mojo/_backend.py
# Depends on: torch, torch._dynamo
# Depends on: Mojo NeutronMojoBackend (via Mojo Python interop)

"""
torch.compile backend registration for Neutron Mojo.

This module registers "neutron_mojo" as a valid backend for torch.compile.
When a user writes:

    @torch.compile(backend="neutron_mojo")
    def my_model(x):
        return F.linear(F.relu(x), weight)

TorchDynamo traces my_model into an FX graph and passes it to the
neutron_mojo_compile function below.
"""


def neutron_mojo_compile(fx_graph, example_inputs):
    """torch.compile backend entry point.

    Called by TorchDynamo with an FX graph and example inputs.
    Lowers the FX graph to Neutron Mojo's computation graph, optimizes
    it (e-graph rewrites, fusion, partitioning), compiles to device
    kernels, and returns a callable that accepts/returns PyTorch tensors.

    Args:
        fx_graph: A torch.fx.GraphModule from TorchDynamo tracing.
        example_inputs: List of example torch.Tensors for shape inference.

    Returns:
        A callable that takes the same input signature as the original
        function and returns PyTorch tensors. Internally uses DLPack
        for zero-copy conversion at the boundary.
    """
    ...


def register_backend():
    """Register the Neutron Mojo backend with TorchDynamo.

    Called automatically on `import neutron_mojo`. After registration,
    `backend="neutron_mojo"` is valid in torch.compile().

    Internally calls:
        torch._dynamo.register_backend("neutron_mojo", neutron_mojo_compile)
    """
    ...
```

### 4.3 neutron_mojo/_bridge.py

Python-side DLPack helper utilities.

```python
# python/neutron_mojo/_bridge.py
# Depends on: torch, numpy, jax (optional)

"""
Python-side DLPack conversion helpers.

Provides the Python side of the zero-copy tensor exchange. The Mojo side
handles the DLPack capsule creation/consumption; this module provides
convenient Python wrappers.
"""


def pytorch_to_dlpack(pt_tensor):
    """Get a DLPack capsule from a PyTorch tensor.

    Args:
        pt_tensor: A torch.Tensor.

    Returns:
        A PyCapsule containing a DLManagedTensor.

    Uses torch.to_dlpack() (PyTorch >= 1.10) or pt_tensor.__dlpack__()
    (PyTorch >= 1.12, preferred).
    """
    ...


def dlpack_to_pytorch(dlpack_capsule):
    """Create a PyTorch tensor from a DLPack capsule.

    Args:
        dlpack_capsule: A PyCapsule containing a DLManagedTensor.

    Returns:
        A torch.Tensor wrapping the same memory.

    Uses torch.from_dlpack() (zero-copy).
    """
    ...


def numpy_to_dlpack(np_array):
    """Get a DLPack capsule from a NumPy array.

    Args:
        np_array: A numpy.ndarray (must be contiguous).

    Returns:
        A PyCapsule containing a DLManagedTensor.

    Requires NumPy >= 1.22.0 for DLPack support.
    """
    ...


def dlpack_to_numpy(dlpack_capsule):
    """Create a NumPy array from a DLPack capsule.

    Args:
        dlpack_capsule: A PyCapsule containing a DLManagedTensor.
            Must refer to CPU memory.

    Returns:
        A numpy.ndarray wrapping the same memory.
    """
    ...


def jax_to_dlpack(jax_array):
    """Get a DLPack capsule from a JAX array.

    Args:
        jax_array: A jax.Array.

    Returns:
        A PyCapsule containing a DLManagedTensor.
    """
    ...


def dlpack_to_jax(dlpack_capsule):
    """Create a JAX array from a DLPack capsule.

    Args:
        dlpack_capsule: A PyCapsule containing a DLManagedTensor.

    Returns:
        A jax.Array wrapping the same memory.
    """
    ...
```

---

## 5. Usage Examples

### 5.1 Basic Tensor Conversion

```python
import torch
import neutron_mojo

# PyTorch -> Neutron Mojo (zero-copy)
pt_tensor = torch.randn(32, 128, 768, device="cuda")
nt_tensor = neutron_mojo.from_pytorch(pt_tensor)

# ... run Mojo kernels on nt_tensor ...

# Neutron Mojo -> PyTorch (zero-copy)
result_pt = neutron_mojo.to_pytorch(nt_tensor)
```

### 5.2 torch.compile Integration

```python
import torch
import neutron_mojo  # Registers backend automatically

model = MyTransformer().cuda()

# Compile with Neutron Mojo backend
compiled_model = torch.compile(model, backend="neutron_mojo")

# First call traces + compiles; subsequent calls use cached kernels
output = compiled_model(input_tensor)
```

### 5.3 Weight Loading

```python
import neutron_mojo

# Load from local PyTorch checkpoint
weights = neutron_mojo.load_pytorch_checkpoint("./model.pt", device="cuda")

# Load from HuggingFace Hub
weights = neutron_mojo.load_from_huggingface(
    "meta-llama/Llama-3-8B",
    auth_token="hf_..."
)

# Each weight is a Neutron Mojo Tensor; use in Mojo inference or convert back
for name, tensor in weights.items():
    print(f"{name}: shape={tensor.shape()}, dtype={tensor.dtype()}")
```

---

## Cross-Reference: Module Dependencies

```
compile_backend/* ──> neutron_mojo.fusion/*, neutron_mojo.backend/*, bridge/*
weights/*         ──> neutron_mojo.tensor/*, bridge/pytorch
bridge/*          ──> neutron_mojo.tensor/*, neutron_mojo.dlpack/*
python/*.py       ──> compile_backend/* (via Mojo Python interop)
```

### Package-Level Dependency Chain

```
neutron-mojo (core)          ← zero external deps (only Mojo stdlib)
    ^            ^
    |            |
neutron-mojo-infer    neutron-mojo-python
    (depends on core)     (depends on core, optionally infer)
                          (Python side depends on torch, numpy, jax)
```
