//! Gorilla-style compression for time-series data.
//!
//! Implements the Facebook Gorilla paper's compression schemes:
//! - **Timestamps**: delta-of-delta encoding with variable-length bit packing
//! - **Values**: XOR-based encoding with leading/trailing zero optimization
//!
//! Achieves ~1.37 bytes/point for regular metrics (vs 16 bytes uncompressed).

// ============================================================================
// Bit-level I/O
// ============================================================================

/// Bit-level writer that packs bits into a byte buffer.
pub struct BitWriter {
    buf: Vec<u8>,
    current_byte: u8,
    bit_pos: u8, // 0..8, number of bits written into current_byte
}

impl Default for BitWriter {
    fn default() -> Self {
        Self::new()
    }
}

impl BitWriter {
    pub fn new() -> Self {
        Self {
            buf: Vec::new(),
            current_byte: 0,
            bit_pos: 0,
        }
    }

    /// Write a single bit (0 or 1).
    #[inline]
    pub fn write_bit(&mut self, bit: bool) {
        if bit {
            self.current_byte |= 1 << (7 - self.bit_pos);
        }
        self.bit_pos += 1;
        if self.bit_pos == 8 {
            self.buf.push(self.current_byte);
            self.current_byte = 0;
            self.bit_pos = 0;
        }
    }

    /// Write `n` bits from `value` (MSB first). `n` must be <= 64.
    pub fn write_bits(&mut self, value: u64, n: u8) {
        debug_assert!(n <= 64);
        for i in (0..n).rev() {
            self.write_bit((value >> i) & 1 == 1);
        }
    }

    /// Flush any partial byte and return the buffer.
    pub fn finish(mut self) -> Vec<u8> {
        if self.bit_pos > 0 {
            self.buf.push(self.current_byte);
        }
        self.buf
    }

    /// Total bits written so far.
    pub fn bits_written(&self) -> usize {
        self.buf.len() * 8 + self.bit_pos as usize
    }
}

/// Bit-level reader that unpacks bits from a byte buffer.
pub struct BitReader<'a> {
    buf: &'a [u8],
    byte_pos: usize,
    bit_pos: u8, // 0..8
}

impl<'a> BitReader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self {
            buf,
            byte_pos: 0,
            bit_pos: 0,
        }
    }

    /// Read a single bit.
    #[inline]
    pub fn read_bit(&mut self) -> bool {
        if self.byte_pos >= self.buf.len() {
            return false;
        }
        let bit = (self.buf[self.byte_pos] >> (7 - self.bit_pos)) & 1 == 1;
        self.bit_pos += 1;
        if self.bit_pos == 8 {
            self.byte_pos += 1;
            self.bit_pos = 0;
        }
        bit
    }

    /// Read `n` bits as a u64 (MSB first). `n` must be <= 64.
    pub fn read_bits(&mut self, n: u8) -> u64 {
        debug_assert!(n <= 64);
        let mut value: u64 = 0;
        for _ in 0..n {
            value = (value << 1) | (self.read_bit() as u64);
        }
        value
    }
}

// ============================================================================
// Gorilla Encoder
// ============================================================================

/// Gorilla-style encoder for time-series data.
pub struct GorillaEncoder {
    ts_writer: BitWriter,
    val_writer: BitWriter,
    // Timestamp state
    prev_ts: u64,
    prev_delta: i64,
    ts_count: usize,
    // Value state
    prev_val_bits: u64,
    prev_leading: u8,
    prev_trailing: u8,
    val_count: usize,
}

impl Default for GorillaEncoder {
    fn default() -> Self {
        Self::new()
    }
}

impl GorillaEncoder {
    pub fn new() -> Self {
        Self {
            ts_writer: BitWriter::new(),
            val_writer: BitWriter::new(),
            prev_ts: 0,
            prev_delta: 0,
            ts_count: 0,
            prev_val_bits: 0,
            prev_leading: u8::MAX,
            prev_trailing: 0,
            val_count: 0,
        }
    }

    // -- Timestamp encoding (delta-of-delta) --

    /// Encode a slice of timestamps using delta-of-delta compression.
    pub fn encode_timestamps(&mut self, timestamps: &[u64]) {
        for &ts in timestamps {
            self.encode_one_timestamp(ts);
        }
    }

    fn encode_one_timestamp(&mut self, ts: u64) {
        if self.ts_count == 0 {
            // First timestamp: store raw 64 bits
            self.ts_writer.write_bits(ts, 64);
            self.prev_ts = ts;
            self.ts_count = 1;
            return;
        }

        let delta = ts as i64 - self.prev_ts as i64;

        if self.ts_count == 1 {
            // Second timestamp: store delta raw (64 bits as i64)
            self.ts_writer.write_bits(delta as u64, 64);
            self.prev_ts = ts;
            self.prev_delta = delta;
            self.ts_count = 2;
            return;
        }

        // Third and beyond: store delta-of-delta with variable-length encoding
        let dod = delta - self.prev_delta;

        if dod == 0 {
            // Case 1: same delta → single 0 bit
            self.ts_writer.write_bit(false);
        } else if (-63..=64).contains(&dod) {
            // Case 2: 10 + 7 bits
            self.ts_writer.write_bit(true);
            self.ts_writer.write_bit(false);
            // Encode dod in 7 bits (biased: value + 63 → 0..127)
            self.ts_writer.write_bits((dod + 63) as u64, 7);
        } else if (-255..=256).contains(&dod) {
            // Case 3: 110 + 9 bits
            self.ts_writer.write_bit(true);
            self.ts_writer.write_bit(true);
            self.ts_writer.write_bit(false);
            self.ts_writer.write_bits((dod + 255) as u64, 9);
        } else if (-2047..=2048).contains(&dod) {
            // Case 4: 1110 + 12 bits
            self.ts_writer.write_bit(true);
            self.ts_writer.write_bit(true);
            self.ts_writer.write_bit(true);
            self.ts_writer.write_bit(false);
            self.ts_writer.write_bits((dod + 2047) as u64, 12);
        } else {
            // Case 5: 1111 + 32 bits
            self.ts_writer.write_bit(true);
            self.ts_writer.write_bit(true);
            self.ts_writer.write_bit(true);
            self.ts_writer.write_bit(true);
            self.ts_writer.write_bits(dod as u64, 32);
        }

        self.prev_ts = ts;
        self.prev_delta = delta;
        self.ts_count += 1;
    }

    // -- Value encoding (XOR-based) --

    /// Encode a slice of f64 values using XOR compression.
    pub fn encode_values(&mut self, values: &[f64]) {
        for &val in values {
            self.encode_one_value(val);
        }
    }

    fn encode_one_value(&mut self, val: f64) {
        let bits = val.to_bits();

        if self.val_count == 0 {
            // First value: store raw 64 bits
            self.val_writer.write_bits(bits, 64);
            self.prev_val_bits = bits;
            self.val_count = 1;
            return;
        }

        let xor = bits ^ self.prev_val_bits;

        if xor == 0 {
            // Same value: single 0 bit
            self.val_writer.write_bit(false);
        } else {
            self.val_writer.write_bit(true);

            let leading = xor.leading_zeros() as u8;
            let trailing = xor.trailing_zeros() as u8;

            // Check if the meaningful bits fit within the previous window
            if self.prev_leading != u8::MAX
                && leading >= self.prev_leading
                && trailing >= self.prev_trailing
            {
                // Control bit 0: reuse previous leading/trailing
                self.val_writer.write_bit(false);
                let meaningful_bits = 64 - self.prev_leading - self.prev_trailing;
                let meaningful = (xor >> self.prev_trailing) & ((1u64 << meaningful_bits) - 1);
                self.val_writer.write_bits(meaningful, meaningful_bits);
            } else {
                // Control bit 1: new leading/trailing
                self.val_writer.write_bit(true);
                // 5 bits for leading zeros count (0..31; cap at 31)
                let leading_capped = leading.min(31);
                self.val_writer.write_bits(leading_capped as u64, 5);
                // 6 bits for meaningful bits length (1..64, stored as 0..63)
                let meaningful_bits = 64 - leading_capped - trailing;
                self.val_writer
                    .write_bits((meaningful_bits - 1) as u64, 6);
                // Write the meaningful bits
                let meaningful = (xor >> trailing) & ((1u64 << meaningful_bits) - 1);
                self.val_writer.write_bits(meaningful, meaningful_bits);

                self.prev_leading = leading_capped;
                self.prev_trailing = trailing;
            }
        }

        self.prev_val_bits = bits;
        self.val_count += 1;
    }

    /// Finish encoding and return compressed bytes for timestamps and values.
    pub fn finish(self) -> CompressedBlock {
        CompressedBlock {
            ts_count: self.ts_count,
            val_count: self.val_count,
            ts_data: self.ts_writer.finish(),
            val_data: self.val_writer.finish(),
            ts_bits: 0, // Not needed for decoding, informational only
            val_bits: 0,
        }
    }

    /// Total bits used for timestamps so far.
    pub fn ts_bits(&self) -> usize {
        self.ts_writer.bits_written()
    }

    /// Total bits used for values so far.
    pub fn val_bits(&self) -> usize {
        self.val_writer.bits_written()
    }
}

// ============================================================================
// Compressed Block
// ============================================================================

/// A compressed block of time-series data.
#[derive(Debug, Clone)]
pub struct CompressedBlock {
    pub ts_count: usize,
    pub val_count: usize,
    pub ts_data: Vec<u8>,
    pub val_data: Vec<u8>,
    pub ts_bits: usize,
    pub val_bits: usize,
}

// ============================================================================
// Gorilla Decoder
// ============================================================================

/// Gorilla-style decoder for time-series data.
pub struct GorillaDecoder<'a> {
    ts_reader: BitReader<'a>,
    val_reader: BitReader<'a>,
    // Timestamp state
    prev_ts: u64,
    prev_delta: i64,
    ts_decoded: usize,
    // Value state
    prev_val_bits: u64,
    prev_leading: u8,
    prev_trailing: u8,
    val_decoded: usize,
}

impl<'a> GorillaDecoder<'a> {
    pub fn new(ts_data: &'a [u8], val_data: &'a [u8]) -> Self {
        Self {
            ts_reader: BitReader::new(ts_data),
            val_reader: BitReader::new(val_data),
            prev_ts: 0,
            prev_delta: 0,
            ts_decoded: 0,
            prev_val_bits: 0,
            prev_leading: 0,
            prev_trailing: 0,
            val_decoded: 0,
        }
    }

    /// Decode `count` timestamps.
    pub fn decode_timestamps(&mut self, count: usize) -> Vec<u64> {
        let mut result = Vec::with_capacity(count);
        for _ in 0..count {
            result.push(self.decode_one_timestamp());
        }
        result
    }

    fn decode_one_timestamp(&mut self) -> u64 {
        if self.ts_decoded == 0 {
            // First: raw 64 bits
            let ts = self.ts_reader.read_bits(64);
            self.prev_ts = ts;
            self.ts_decoded = 1;
            return ts;
        }

        if self.ts_decoded == 1 {
            // Second: raw delta (64 bits as i64)
            let delta = self.ts_reader.read_bits(64) as i64;
            let ts = (self.prev_ts as i64 + delta) as u64;
            self.prev_ts = ts;
            self.prev_delta = delta;
            self.ts_decoded = 2;
            return ts;
        }

        // Third and beyond: delta-of-delta
        let dod;
        if !self.ts_reader.read_bit() {
            // 0 → dod = 0
            dod = 0i64;
        } else if !self.ts_reader.read_bit() {
            // 10 + 7 bits
            let raw = self.ts_reader.read_bits(7) as i64;
            dod = raw - 63;
        } else if !self.ts_reader.read_bit() {
            // 110 + 9 bits
            let raw = self.ts_reader.read_bits(9) as i64;
            dod = raw - 255;
        } else if !self.ts_reader.read_bit() {
            // 1110 + 12 bits
            let raw = self.ts_reader.read_bits(12) as i64;
            dod = raw - 2047;
        } else {
            // 1111 + 32 bits
            let raw = self.ts_reader.read_bits(32) as u32;
            dod = raw as i32 as i64;
        }

        let delta = self.prev_delta + dod;
        let ts = (self.prev_ts as i64 + delta) as u64;
        self.prev_ts = ts;
        self.prev_delta = delta;
        self.ts_decoded += 1;
        ts
    }

    /// Decode `count` f64 values.
    pub fn decode_values(&mut self, count: usize) -> Vec<f64> {
        let mut result = Vec::with_capacity(count);
        for _ in 0..count {
            result.push(self.decode_one_value());
        }
        result
    }

    fn decode_one_value(&mut self) -> f64 {
        if self.val_decoded == 0 {
            // First: raw 64 bits
            let bits = self.val_reader.read_bits(64);
            self.prev_val_bits = bits;
            self.val_decoded = 1;
            return f64::from_bits(bits);
        }

        if !self.val_reader.read_bit() {
            // 0 → same value
            self.val_decoded += 1;
            return f64::from_bits(self.prev_val_bits);
        }

        let xor;
        if !self.val_reader.read_bit() {
            // 10 → reuse previous leading/trailing
            let meaningful_bits = 64 - self.prev_leading - self.prev_trailing;
            let meaningful = self.val_reader.read_bits(meaningful_bits);
            xor = meaningful << self.prev_trailing;
        } else {
            // 11 → new leading/trailing
            let leading = self.val_reader.read_bits(5) as u8;
            let meaningful_bits = self.val_reader.read_bits(6) as u8 + 1;
            let trailing = 64 - leading - meaningful_bits;
            let meaningful = self.val_reader.read_bits(meaningful_bits);
            xor = meaningful << trailing;
            self.prev_leading = leading;
            self.prev_trailing = trailing;
        }

        let bits = self.prev_val_bits ^ xor;
        self.prev_val_bits = bits;
        self.val_decoded += 1;
        f64::from_bits(bits)
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bit_writer_reader_roundtrip() {
        let mut w = BitWriter::new();
        w.write_bit(true);
        w.write_bit(false);
        w.write_bit(true);
        w.write_bits(0b1101, 4);
        w.write_bits(42, 8);
        let buf = w.finish();

        let mut r = BitReader::new(&buf);
        assert!(r.read_bit());
        assert!(!r.read_bit());
        assert!(r.read_bit());
        assert_eq!(r.read_bits(4), 0b1101);
        assert_eq!(r.read_bits(8), 42);
    }

    #[test]
    fn gorilla_timestamp_roundtrip_monotonic() {
        // Regular 1-second intervals: should compress very well
        let timestamps: Vec<u64> = (0..1000).map(|i| 1_700_000_000_000 + i * 1000).collect();

        let mut enc = GorillaEncoder::new();
        enc.encode_timestamps(&timestamps);
        let bits = enc.ts_bits();
        let block = enc.finish();

        let mut dec = GorillaDecoder::new(&block.ts_data, &[]);
        let decoded = dec.decode_timestamps(1000);
        assert_eq!(timestamps, decoded);

        // Regular intervals should compress to < 4 bits/point on average
        let bits_per_point = bits as f64 / 1000.0;
        assert!(
            bits_per_point < 4.0,
            "monotonic timestamps: {:.2} bits/point (expected < 4.0)",
            bits_per_point
        );
    }

    #[test]
    fn gorilla_timestamp_roundtrip_irregular() {
        // Irregular intervals
        let timestamps = vec![
            1_000_000, 1_001_000, 1_001_500, 1_005_000, 1_005_001, 1_100_000,
        ];

        let mut enc = GorillaEncoder::new();
        enc.encode_timestamps(&timestamps);
        let block = enc.finish();

        let mut dec = GorillaDecoder::new(&block.ts_data, &[]);
        let decoded = dec.decode_timestamps(6);
        assert_eq!(timestamps, decoded);
    }

    #[test]
    fn gorilla_value_roundtrip_constant() {
        // All same value: should compress to ~1 bit/point after the first
        let values: Vec<f64> = vec![42.5; 1000];

        let mut enc = GorillaEncoder::new();
        enc.encode_values(&values);
        let bits = enc.val_bits();
        let block = enc.finish();

        let mut dec = GorillaDecoder::new(&[], &block.val_data);
        let decoded = dec.decode_values(1000);
        assert_eq!(values, decoded);

        // Constant values: first = 64 bits, rest = 1 bit each → ~1.06 bits/point
        let bits_per_point = bits as f64 / 1000.0;
        assert!(
            bits_per_point < 1.1,
            "constant values: {:.2} bits/point (expected ~1.06)",
            bits_per_point
        );
    }

    #[test]
    fn gorilla_value_roundtrip_varying() {
        let values: Vec<f64> = (0..100).map(|i| (i as f64) * 1.5 + 10.0).collect();

        let mut enc = GorillaEncoder::new();
        enc.encode_values(&values);
        let block = enc.finish();

        let mut dec = GorillaDecoder::new(&[], &block.val_data);
        let decoded = dec.decode_values(100);

        for (i, (orig, dec_val)) in values.iter().zip(decoded.iter()).enumerate() {
            assert!(
                (orig - dec_val).abs() < 1e-15,
                "value mismatch at index {}: {} vs {}",
                i,
                orig,
                dec_val
            );
        }
    }

    #[test]
    fn gorilla_full_roundtrip() {
        let n = 500;
        let timestamps: Vec<u64> = (0..n).map(|i| 1_700_000_000_000 + i * 1000).collect();
        let values: Vec<f64> = (0..n).map(|i| 50.0 + (i as f64) * 0.1).collect();

        let mut enc = GorillaEncoder::new();
        enc.encode_timestamps(&timestamps);
        enc.encode_values(&values);
        let block = enc.finish();

        let mut dec = GorillaDecoder::new(&block.ts_data, &block.val_data);
        let dec_ts = dec.decode_timestamps(n as usize);
        let dec_vals = dec.decode_values(n as usize);

        assert_eq!(timestamps, dec_ts);
        for (orig, decoded) in values.iter().zip(dec_vals.iter()) {
            assert!((orig - decoded).abs() < 1e-15);
        }
    }

    #[test]
    fn gorilla_empty() {
        let enc = GorillaEncoder::new();
        let block = enc.finish();
        assert_eq!(block.ts_count, 0);
        assert_eq!(block.val_count, 0);
    }

    #[test]
    fn gorilla_single_point() {
        let mut enc = GorillaEncoder::new();
        enc.encode_timestamps(&[12345]);
        enc.encode_values(&[99.9]);
        let block = enc.finish();

        let mut dec = GorillaDecoder::new(&block.ts_data, &block.val_data);
        assert_eq!(dec.decode_timestamps(1), vec![12345]);
        let vals = dec.decode_values(1);
        assert!((vals[0] - 99.9).abs() < 1e-15);
    }

    #[test]
    fn gorilla_two_points() {
        let ts = vec![1000, 2000];
        let vals = vec![1.0, 2.0];

        let mut enc = GorillaEncoder::new();
        enc.encode_timestamps(&ts);
        enc.encode_values(&vals);
        let block = enc.finish();

        let mut dec = GorillaDecoder::new(&block.ts_data, &block.val_data);
        assert_eq!(dec.decode_timestamps(2), ts);
        let decoded_vals = dec.decode_values(2);
        assert!((decoded_vals[0] - 1.0).abs() < 1e-15);
        assert!((decoded_vals[1] - 2.0).abs() < 1e-15);
    }

    #[test]
    fn gorilla_special_float_values() {
        let vals = vec![0.0, -0.0, f64::INFINITY, f64::NEG_INFINITY, 1e308, -1e308];
        let ts: Vec<u64> = (0..6).map(|i| 1000 + i * 1000).collect();

        let mut enc = GorillaEncoder::new();
        enc.encode_timestamps(&ts);
        enc.encode_values(&vals);
        let block = enc.finish();

        let mut dec = GorillaDecoder::new(&block.ts_data, &block.val_data);
        let dec_ts = dec.decode_timestamps(6);
        let dec_vals = dec.decode_values(6);

        assert_eq!(ts, dec_ts);
        assert_eq!(vals[0].to_bits(), dec_vals[0].to_bits());
        assert_eq!(vals[1].to_bits(), dec_vals[1].to_bits());
        assert!(dec_vals[2].is_infinite() && dec_vals[2] > 0.0);
        assert!(dec_vals[3].is_infinite() && dec_vals[3] < 0.0);
        assert!((vals[4] - dec_vals[4]).abs() < 1e-15);
        assert!((vals[5] - dec_vals[5]).abs() < 1e-15);
    }
}
