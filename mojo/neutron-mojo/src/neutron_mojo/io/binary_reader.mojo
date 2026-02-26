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

from pathlib import Path
from memory import UnsafePointer, alloc
from ffi import external_call, c_int
from os import stat as os_stat
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape


# ===----------------------------------------------------------------------=== #
# Bitcast helpers
# ===----------------------------------------------------------------------=== #

fn _u32_to_f32(bits: UInt32) -> Float32:
    """Reinterpret UInt32 bits as Float32 via heap alloc + pointer cast."""
    var p = alloc[UInt32](1)
    p.store(bits)
    var result = p.bitcast[Float32]().load()
    p.free()
    return result


fn _u64_to_f64(bits: UInt64) -> Float64:
    """Reinterpret UInt64 bits as Float64 via heap alloc + pointer cast."""
    var p = alloc[UInt64](1)
    p.store(bits)
    var result = p.bitcast[Float64]().load()
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
    var _mmap_ptr: UnsafePointer[UInt8, MutExternalOrigin]
    var _is_mmap: Bool

    fn __init__(out self, path: String) raises:
        """Load entire file into memory (slurp mode).

        Args:
            path: File path to read.
        """
        self.path = path
        self.data = Path(path).read_bytes()
        self.cursor = 0
        self.size = len(self.data)
        self._mmap_ptr = UnsafePointer[UInt8, MutExternalOrigin]()
        self._is_mmap = False

    fn __init__(out self, var buf: List[UInt8]):
        """Create reader from an in-memory buffer (for testing).

        Args:
            buf: Raw byte buffer.
        """
        self.path = String("<memory>")
        self.size = len(buf)
        self.data = buf^
        self.cursor = 0
        self._mmap_ptr = UnsafePointer[UInt8, MutExternalOrigin]()
        self._is_mmap = False

    fn __init__(
        out self,
        path: String,
        mmap_ptr: UnsafePointer[UInt8, MutExternalOrigin],
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

    fn __moveinit__(out self, deinit other: Self):
        self.path = other.path^
        self.data = other.data^
        self.cursor = other.cursor
        self.size = other.size
        self._mmap_ptr = other._mmap_ptr
        self._is_mmap = other._is_mmap

    fn __del__(deinit self):
        """Clean up mmap mapping if in mmap mode."""
        if self._is_mmap and self.size > 0:
            _ = external_call["munmap", c_int](self._mmap_ptr, self.size)

    # --- Byte access ---

    @always_inline
    fn _byte_at(self, offset: Int) -> UInt8:
        """Read a single byte at the given offset.

        Uses mmap pointer or List depending on mode.
        """
        if self._is_mmap:
            return (self._mmap_ptr + offset).load()
        return self.data[offset]

    # --- Position control ---

    fn seek(mut self, pos: Int) raises:
        """Set cursor position.

        Args:
            pos: Absolute byte position.
        """
        if pos < 0 or pos > self.size:
            raise Error("seek out of bounds: " + String(pos))
        self.cursor = pos

    fn tell(self) -> Int:
        """Get current cursor position."""
        return self.cursor

    fn skip(mut self, n: Int) raises:
        """Advance cursor by N bytes.

        Args:
            n: Number of bytes to skip.
        """
        if self.cursor + n > self.size:
            raise Error("skip past end of data")
        self.cursor += n

    fn remaining(self) -> Int:
        """Bytes remaining from cursor to end."""
        return self.size - self.cursor

    # --- Typed reads ---

    fn _check(self, n: Int) raises:
        """Check that N bytes are available."""
        if self.cursor + n > self.size:
            raise Error(
                "read past end: need " + String(n)
                + " bytes at offset " + String(self.cursor)
                + " but size is " + String(self.size)
            )

    fn read_u8(mut self) raises -> UInt8:
        """Read 1 byte."""
        self._check(1)
        var v = self._byte_at(self.cursor)
        self.cursor += 1
        return v

    fn read_u16_le(mut self) raises -> Int:
        """Read 2 bytes little-endian as Int."""
        self._check(2)
        var b0 = Int(self._byte_at(self.cursor))
        var b1 = Int(self._byte_at(self.cursor + 1))
        self.cursor += 2
        return b0 | (b1 << 8)

    fn read_u32_le(mut self) raises -> Int:
        """Read 4 bytes little-endian as Int."""
        self._check(4)
        var b0 = Int(self._byte_at(self.cursor))
        var b1 = Int(self._byte_at(self.cursor + 1))
        var b2 = Int(self._byte_at(self.cursor + 2))
        var b3 = Int(self._byte_at(self.cursor + 3))
        self.cursor += 4
        return b0 | (b1 << 8) | (b2 << 16) | (b3 << 24)

    fn read_u64_le(mut self) raises -> Int:
        """Read 8 bytes little-endian as Int.

        Cap at 63-bit for Mojo Int safety (Int is signed 64-bit).
        """
        self._check(8)
        var result = 0
        for i in range(8):
            result |= Int(self._byte_at(self.cursor + i)) << (i * 8)
        self.cursor += 8
        # Mask to 63 bits to keep Int positive
        return result & 0x7FFFFFFFFFFFFFFF

    fn read_i32_le(mut self) raises -> Int:
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

    fn read_f32_le(mut self) raises -> Float32:
        """Read 4 bytes as IEEE 754 float32."""
        self._check(4)
        var bits = UInt32(self._byte_at(self.cursor))
        bits = bits | (UInt32(self._byte_at(self.cursor + 1)) << 8)
        bits = bits | (UInt32(self._byte_at(self.cursor + 2)) << 16)
        bits = bits | (UInt32(self._byte_at(self.cursor + 3)) << 24)
        self.cursor += 4
        return _u32_to_f32(bits)

    fn read_f64_le(mut self) raises -> Float64:
        """Read 8 bytes as IEEE 754 float64."""
        self._check(8)
        var bits = UInt64(0)
        for i in range(8):
            bits = bits | (UInt64(self._byte_at(self.cursor + i)) << UInt64(i * 8))
        self.cursor += 8
        return _u64_to_f64(bits)

    fn read_bytes(mut self, n: Int) raises -> List[UInt8]:
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

    fn read_string_gguf(mut self) raises -> String:
        """Read a GGUF string (u64 length prefix + bytes).

        Returns:
            Decoded string.
        """
        var length = self.read_u64_le()
        if length == 0:
            return String("")
        self._check(length)
        # Build string character by character
        var result = String("")
        for i in range(length):
            result += chr(Int(self._byte_at(self.cursor + i)))
        self.cursor += length
        return result^

    fn read_f32_array(mut self, count: Int) raises -> Tensor[DType.float32]:
        """Read N float32 values into a tensor.

        Args:
            count: Number of floats to read.

        Returns:
            Tensor with the values.
        """
        self._check(count * 4)
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

    fn read_f16_to_f32_array(mut self, count: Int) raises -> Tensor[DType.float32]:
        """Read N FP16 values, convert to FP32 tensor.

        Uses manual bit manipulation for FP16->FP32 conversion.
        Handles normals, denormals, infinity, and NaN.

        Args:
            count: Number of FP16 values to read.

        Returns:
            Tensor[float32] with converted values.
        """
        self._check(count * 2)
        var result = Tensor[DType.float32](Shape(count))
        for i in range(count):
            var base = self.cursor + i * 2
            var h = Int(self._byte_at(base)) | (Int(self._byte_at(base + 1)) << 8)
            result.set(i, _fp16_to_fp32(h))
        self.cursor += count * 2
        return result^

    fn is_mmap(self) -> Bool:
        """Check if this reader is backed by mmap."""
        return self._is_mmap


# ===----------------------------------------------------------------------=== #
# FP16 -> FP32 Conversion
# ===----------------------------------------------------------------------=== #

fn _fp16_to_fp32(h: Int) -> Float32:
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
            var m = mant
            var e = -1
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

fn mmap_reader(path: String) raises -> BinaryReader:
    """Create a memory-mapped BinaryReader.

    Uses mmap() for zero-copy file access. Only pages that are
    actually read get loaded into physical memory by the OS.
    ~4x less peak memory for large model files vs slurp mode.

    Args:
        path: Path to the file to memory-map.

    Returns:
        BinaryReader in mmap mode.
    """
    # Get file size
    var st = os_stat(path)
    var file_size = st.st_size

    # Open file read-only (O_RDONLY = 0 on Linux)
    var path_bytes = path.as_bytes()
    var fd = external_call["open", c_int](
        path_bytes.unsafe_ptr(),
        c_int(0),
        c_int(0),
    )
    if Int(fd) < 0:
        raise Error("mmap: open() failed for: " + path)

    # mmap the file (PROT_READ=1, MAP_PRIVATE=2)
    var ptr = external_call["mmap", UnsafePointer[UInt8, MutExternalOrigin]](
        UnsafePointer[UInt8, MutExternalOrigin](),
        file_size,
        c_int(1),
        c_int(2),
        fd,
        Int64(0),
    )

    # Close fd (mmap keeps its own reference to the file)
    _ = external_call["close", c_int](fd)

    # Basic check — null pointer means mmap returned NULL (shouldn't happen
    # for valid files, but check anyway). Note: MAP_FAILED is (void*)-1
    # which we can't easily check, but null is caught here.
    if not ptr:
        raise Error("mmap: returned null for: " + path)

    # Hint OS for sequential access (MADV_SEQUENTIAL=2)
    _ = external_call["madvise", c_int](ptr, file_size, c_int(2))

    return BinaryReader(path, ptr, file_size)
