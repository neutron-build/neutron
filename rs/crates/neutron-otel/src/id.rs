use rand::random;

/// Generate a random 128-bit trace ID.
pub fn random_trace_id() -> [u8; 16] {
    [
        random::<u8>(), random::<u8>(), random::<u8>(), random::<u8>(),
        random::<u8>(), random::<u8>(), random::<u8>(), random::<u8>(),
        random::<u8>(), random::<u8>(), random::<u8>(), random::<u8>(),
        random::<u8>(), random::<u8>(), random::<u8>(), random::<u8>(),
    ]
}

/// Generate a random 64-bit span ID.
pub fn random_span_id() -> [u8; 8] {
    [
        random::<u8>(), random::<u8>(), random::<u8>(), random::<u8>(),
        random::<u8>(), random::<u8>(), random::<u8>(), random::<u8>(),
    ]
}

/// Hex-encode a byte slice into a lowercase hex string.
pub fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trace_id_is_16_bytes() {
        let id = random_trace_id();
        assert_eq!(id.len(), 16);
    }

    #[test]
    fn span_id_is_8_bytes() {
        let id = random_span_id();
        assert_eq!(id.len(), 8);
    }

    #[test]
    fn trace_id_hex_is_32_chars() {
        let id = random_trace_id();
        let hex = hex_encode(&id);
        assert_eq!(hex.len(), 32);
    }

    #[test]
    fn span_id_hex_is_16_chars() {
        let id = random_span_id();
        let hex = hex_encode(&id);
        assert_eq!(hex.len(), 16);
    }

    #[test]
    fn hex_encode_known_value() {
        let bytes: [u8; 4] = [0x00, 0xff, 0xab, 0x12];
        assert_eq!(hex_encode(&bytes), "00ffab12");
    }

    #[test]
    fn hex_encode_empty() {
        assert_eq!(hex_encode(&[]), "");
    }

    #[test]
    fn hex_encode_single_byte() {
        assert_eq!(hex_encode(&[0x0a]), "0a");
        assert_eq!(hex_encode(&[0xf0]), "f0");
    }

    #[test]
    fn random_trace_ids_are_unique() {
        let a = random_trace_id();
        let b = random_trace_id();
        // With 128-bit randomness collision probability is astronomically small
        assert_ne!(a, b);
    }

    #[test]
    fn random_span_ids_are_unique() {
        let a = random_span_id();
        let b = random_span_id();
        assert_ne!(a, b);
    }
}
