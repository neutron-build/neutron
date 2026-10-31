# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Binary File Reader
# ===----------------------------------------------------------------------=== #

"""Low-level binary reader for parsing model file formats.

Supports two modes:
  1. Slurp: Loads entire file into memory via Path.read_bytes()
  2. Mmap: Memory-maps the file for zero-copy access (OS-managed paging)

Provides typed read methods (u8, u16_le, u32_le, u64_le, i32_le, f32_le,
f64_le, string, byte arrays) with a cursor that advances on each read.

For FP16 data, provides manual bit-manipulation conversion to FP32.
"""

from std.pathlib import Path
from std.memory import Pointer, alloc
from std.ffi import external_call, c_int
from std.sys import CompilationTarget
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape


# ===----------------------------------------------------------------------=== #
# Bitcast helpers
# ===----------------------------------------------------------------------=== #

def _u32_to_f32(bits: UInt32) -> Float32:
    """Reinterpret UInt32 bits as Float32 via heap alloc + pointer cast."""
    var p = alloc[UInt32](1)
    p.unsafe_store(bits)
    var result = p.bitcast[Float32]().unsafe_load()
    p.free()
    return result


def _u64_to_f64(bits: UInt64) -> Float64:
    """Reinterpret UInt64 bits as Float64 via heap alloc + pointer cast."""
    var p = alloc[UInt64](1)
    p.unsafe_store(bits)
    var result = p.bitcast[Float64]().unsafe_load()
    p.free()
    return result


# ===----------------------------------------------------------------------=== #
# BinaryReader
# ===----------------------------------------------------------------------=== #

struct BinaryReader(Movable):
    """Binary reader with a cursor, supporting slurp or mmap modes.

    Slurp mode: loads entire file into List[UInt8] buffer.
    Mmap mode: memory-maps file for zero-copy, demand-paged access.
    Both modes provide identical typed read methods.
    """
    var path: String
    var data: List[UInt8]
    var cursor: Int
    var size: Int
    var _mmap_ptr: Optional[Pointer[UInt8, MutUntrackedOrigin]]
    var _is_mmap: Bool

    def __init__(out self, path: String) raises:
        """Load entire file into memory (slurp mode).

        Args:
            path: File path to read.
        """
        self.path = path
        self.data = Path(path).read_bytes()
        self.cursor = 0
        self.size = len(self.data)
        self._mmap_ptr = None
        self._is_mmap = False

    def __init__(out self, var buf: List[UInt8]):
        """Create reader from an in-memory buffer (for testing).

        Args:
            buf: Raw byte buffer.
        """
        self.path = String("<memory>")
        self.size = len(buf)
        self.data = buf^
        self.cursor = 0
        self._mmap_ptr = None
        self._is_mmap = False

    def __init__(
        out self,
        path: String,
        mmap_ptr: Pointer[UInt8, MutUntrackedOrigin],
        file_size: Int,
    ):
        """Create reader backed by mmap (internal use by mmap_reader()).

        Args:
            path: Original file path (for error messages).
            mmap_ptr: Pointer to mmap'd region.
            file_size: Size of the file in bytes.
        """
        self.path = path
        self.data = List[UInt8]()
        self.cursor = 0
        self.size = file_size
        self._mmap_ptr = mmap_ptr
        self._is_mmap = True

    def __init__(out self, *, deinit move: Self):
        self.path = move.path^
        self.data = move.data^
        self.cursor = move.cursor^
        self.size = move.size^
        self._mmap_ptr = move._mmap_ptr
        move._mmap_ptr = None
        self._is_mmap = move._is_mmap^

    def __deinit__(deinit self):
        """Clean up mmap mapping if in mmap mode."""
        if self._is_mmap and self._mmap_ptr and self.size > 0:
            _ = external_call["munmap", c_int](self._mmap_ptr.unsafe_value(), self.size)

    # --- Byte access ---

    @always_inline
    def _byte_at(self, offset: Int) -> UInt8:
        """Read a single byte at the given offset.

        Uses mmap pointer or List depending on mode.
        """
        if self._is_mmap:
            return (self._mmap_ptr.unsafe_value() + offset).load()
        return self.data[offset]

    # --- Position control ---

    def seek(mut self, pos: Int) raises:
        """Set cursor position.

        Args:
            pos: Absolute byte position.
        """
        if pos < 0 or pos > self.size:
            raise Error("seek out of bounds: " + String(pos))
        self.cursor = pos

    def tell(self) -> Int:
        """Get current cursor position."""
        return self.cursor

    def skip(mut self, n: Int) raises:
        """Advance cursor by N bytes.

        Args:
            n: Number of bytes to skip.
        """
        self._check(n)
        self.cursor += n

    def remaining(self) -> Int:
        """Bytes remaining from cursor to end."""
        return self.size - self.cursor

    # --- Typed reads ---

    def _check(self, n: Int) raises:
        """Check that N bytes are available."""
        if n < 0 or self.cursor < 0 or self.cursor > self.size or n > self.size - self.cursor:
            raise Error(
                "read past end: need " + String(n)
                + " bytes at offset " + String(self.cursor)
                + " but size is " + String(self.size)
            )

    def _check_elements(self, count: Int, width: Int) raises:
        # Divide before multiplying, so hostile counts cannot overflow.
        self._check(0)
        if count < 0 or count > self.remaining() // width:
            raise Error("element count exceeds remaining data")

    def read_u8(mut self) raises -> UInt8:
        """Read 1 byte."""
        self._check(1)
        var v = self._byte_at(self.cursor)
        self.cursor += 1
        return v

    def read_u16_le(mut self) raises -> Int:
        """Read 2 bytes little-endian as Int."""
        self._check(2)
        var b0 = Int(self._byte_at(self.cursor))
        var b1 = Int(self._byte_at(self.cursor + 1))
        self.cursor += 2
        return b0 | (b1 << 8)

    def read_u32_le(mut self) raises -> Int:
        """Read 4 bytes little-endian as Int."""
        self._check(4)
        var b0 = Int(self._byte_at(self.cursor))
        var b1 = Int(self._byte_at(self.cursor + 1))
        var b2 = Int(self._byte_at(self.cursor + 2))
        var b3 = Int(self._byte_at(self.cursor + 3))
        self.cursor += 4
        return b0 | (b1 << 8) | (b2 << 16) | (b3 << 24)

    def read_u64_le(mut self) raises -> Int:
        """Read 8 bytes little-endian as Int.

        Cap at 63-bit for Mojo Int safety (Int is signed 64-bit).
        """
        self._check(8)
        if Int(self._byte_at(self.cursor + 7)) & 0x80:
            raise Error("u64 value exceeds signed Int range")
        var result = 0
        for i in range(8):
            result |= Int(self._byte_at(self.cursor + i)) << (i * 8)
        self.cursor += 8
        return result

    def read_i32_le(mut self) raises -> Int:
        """Read 4 bytes little-endian as signed Int."""
        self._check(4)
        var b0 = Int(self._byte_at(self.cursor))
        var b1 = Int(self._byte_at(self.cursor + 1))
        var b2 = Int(self._byte_at(self.cursor + 2))
        var b3 = Int(self._byte_at(self.cursor + 3))
        self.cursor += 4
        var val = b0 | (b1 << 8) | (b2 << 16) | (b3 << 24)
        # Sign extension for 32-bit
        if val & 0x80000000:
            val = val - 0x100000000
        return val

    def read_f32_le(mut self) raises -> Float32:
        """Read 4 bytes as IEEE 754 float32."""
        self._check(4)
        var bits = UInt32(self._byte_at(self.cursor))
        bits = bits | (UInt32(self._byte_at(self.cursor + 1)) << 8)
        bits = bits | (UInt32(self._byte_at(self.cursor + 2)) << 16)
        bits = bits | (UInt32(self._byte_at(self.cursor + 3)) << 24)
        self.cursor += 4
        return _u32_to_f32(bits)

    def read_f64_le(mut self) raises -> Float64:
        """Read 8 bytes as IEEE 754 float64."""
        self._check(8)
        var bits = UInt64(0)
        for i in range(8):
            bits = bits | (UInt64(self._byte_at(self.cursor + i)) << UInt64(i * 8))
        self.cursor += 8
        return _u64_to_f64(bits)

    def read_bytes(mut self, n: Int) raises -> List[UInt8]:
        """Read N raw bytes.

        Args:
            n: Number of bytes to read.

        Returns:
            List of bytes.
        """
        self._check(n)
        var result = List[UInt8]()
        for i in range(n):
            result.append(self._byte_at(self.cursor + i))
        self.cursor += n
        return result^

    def read_string_gguf(mut self) raises -> String:
        """Read a GGUF string (u64 length prefix + bytes).

        Returns:
            Decoded string.
        """
        var length = self.read_u64_le()
        if length == 0:
            return String("")
        self._check(length)
        var saved = self.cursor
        var bytes = self.read_bytes(length)
        try:
            return String(from_utf8=bytes)
        except e:
            self.cursor = saved
            raise e

    def read_f32_array(mut self, count: Int) raises -> Tensor[DType.float32]:
        """Read N float32 values into a tensor.

        Args:
            count: Number of floats to read.

        Returns:
            Tensor with the values.
        """
        self._check_elements(count, 4)
        var result = Tensor[DType.float32](Shape(count))
        for i in range(count):
            var base = self.cursor + i * 4
            var bits = UInt32(self._byte_at(base))
            bits = bits | (UInt32(self._byte_at(base + 1)) << 8)
            bits = bits | (UInt32(self._byte_at(base + 2)) << 16)
            bits = bits | (UInt32(self._byte_at(base + 3)) << 24)
            result.set(i, _u32_to_f32(bits))
        self.cursor += count * 4
        return result^

    def read_f16_to_f32_array(mut self, count: Int) raises -> Tensor[DType.float32]:
        """Read N FP16 values, convert to FP32 tensor.

        Uses manual bit manipulation for FP16->FP32 conversion.
        Handles normals, denormals, infinity, and NaN.

        Args:
            count: Number of FP16 values to read.

        Returns:
            Tensor[float32] with converted values.
        """
        self._check_elements(count, 2)
        var result = Tensor[DType.float32](Shape(count))
        for i in range(count):
            var base = self.cursor + i * 2
            var h = Int(self._byte_at(base)) | (Int(self._byte_at(base + 1)) << 8)
            result.set(i, _fp16_to_fp32(h))
        self.cursor += count * 2
        return result^

    def is_mmap(self) -> Bool:
        """Check if this reader is backed by mmap."""
        return self._is_mmap


# ===----------------------------------------------------------------------=== #
# FP16 -> FP32 Conversion
# ===----------------------------------------------------------------------=== #

def _fp16_to_fp32(h: Int) raises -> Float32:
    """Convert a 16-bit IEEE 754 half-precision float to Float32.

    Layout of FP16 (16 bits):
        bit 15: sign
        bits 14-10: exponent (5 bits, bias 15)
        bits 9-0: mantissa (10 bits)

    Args:
        h: 16-bit integer containing FP16 bits.

    Returns:
        Equivalent Float32 value.
    """
    var sign = (h >> 15) & 1
    var exp = (h >> 10) & 0x1F
    var mant = h & 0x3FF

    var f32_bits: UInt32

    if exp == 0:
        if mant == 0:
            # Zero (positive or negative)
            f32_bits = UInt32(sign) << 31
        else:
            # Denormal: normalize by shifting mantissa until leading 1 appears
            # MJ-02: e starts at 0. The value is mant * 2^-24; with the
            # leading-1 normalization below, the -1 start made every nonzero
            # subnormal ONE EXPONENT TOO SMALL (0x0001 decoded as 2^-25, not
            # 2^-24 — all 2,046 signed nonzero subnormals were halved).
            var m = mant
            var e = 0
            while (m & 0x400) == 0:
                m <<= 1
                e -= 1
            m &= 0x3FF  # Remove the leading 1
            var f32_exp = UInt32(127 - 15 + e + 1)
            f32_bits = (UInt32(sign) << 31) | (f32_exp << 23) | (UInt32(m) << 13)
    elif exp == 0x1F:
        # Inf or NaN
        f32_bits = (UInt32(sign) << 31) | (UInt32(0xFF) << 23) | (UInt32(mant) << 13)
    else:
        # Normal number: adjust exponent bias from 15 to 127
        var f32_exp = UInt32(exp - 15 + 127)
        f32_bits = (UInt32(sign) << 31) | (f32_exp << 23) | (UInt32(mant) << 13)

    return _u32_to_f32(f32_bits)


# ===----------------------------------------------------------------------=== #
# Mmap Reader Factory
# ===----------------------------------------------------------------------=== #

def _posix_errno() -> Int:
    """Read thread-local errno immediately, before another libc call."""
    comptime if CompilationTarget.is_macos():
        return Int(external_call["__error", Pointer[c_int, MutUntrackedOrigin]]().unsafe_load())
    elif CompilationTarget.is_linux():
        return Int(external_call["__errno_location", Pointer[c_int, MutUntrackedOrigin]]().unsafe_load())
    else:
        return 0


def _map_readonly_descriptor(fd: c_int, file_size: Int) raises -> Pointer[UInt8, MutUntrackedOrigin]:
    """Mapping seam: borrowed fd, no close and no failed-pointer dereference."""
    if file_size <= 0:
        raise Error("mmap: nonpositive mapping length")
    var ptr = external_call["mmap", Optional[Pointer[UInt8, MutUntrackedOrigin]]](
        None, file_size, c_int(1), c_int(2), fd, Int64(0)
    )
    var mapping_errno = _posix_errno()
    if ptr == None:
        # NULL differs from MAP_FAILED: mapping at address zero succeeded, but
        # Optional cannot retain it. Release that mapping before refusing it.
        _ = external_call["munmap", c_int](
            Optional[Pointer[UInt8, MutUntrackedOrigin]]().unsafe_value(), file_size
        )
        raise Error("mmap: null address cannot be represented; errno=" + String(mapping_errno))
    if Int(ptr.unsafe_value()) == -1:
        raise Error("mmap: mapping failed; errno=" + String(mapping_errno))
    return ptr.unsafe_value()


def mmap_reader(path: String) raises -> BinaryReader:
    """Create a memory-mapped BinaryReader.

    Uses mmap() for zero-copy file access. Only pages that are
    actually read get loaded into physical memory by the OS.
    ~4x less peak memory for large model files vs slurp mode.

    Args:
        path: Path to the file to memory-map.

    Returns:
        BinaryReader in mmap mode.
    """
    # Open file read-only (O_RDONLY = 0 on Linux)
    # Mojo String buffers are NUL-terminated, so the raw pointer is a valid
    # C string for open(2).
    var c_path = path
    var fd = external_call["open", c_int, num_fixed_args=2](
        c_path.unsafe_ptr(),
        c_int(0),
    )
    if Int(fd) < 0:
        raise Error("mmap: open() failed; errno=" + String(_posix_errno()) + " for: " + path)

    # Obtain size from the opened descriptor, not a separately resolved path.
    var file_size = Int(external_call["lseek", Int64](fd, Int64(0), c_int(2)))
    if file_size < 0:
        var size_errno = _posix_errno()
        _ = external_call["close", c_int](fd)
        raise Error("mmap: cannot determine opened file size; errno=" + String(size_errno) + " for: " + path)
    if file_size == 0:
        _ = external_call["close", c_int](fd)
        return BinaryReader(List[UInt8]())

    var ptr: Pointer[UInt8, MutUntrackedOrigin]
    try:
        ptr = _map_readonly_descriptor(fd, file_size)
    except error:
        _ = external_call["close", c_int](fd)
        raise Error(String(error) + " for: " + path)
    _ = external_call["close", c_int](fd)

    _ = external_call["madvise", c_int](ptr, file_size, c_int(2))
    return BinaryReader(path, ptr, file_size)
