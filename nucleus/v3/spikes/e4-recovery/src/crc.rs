//! CRC32C (Castagnoli), table-driven, written for this spike.
//!
//! Why hand-written: card X-5 Work 0 — "no crates beyond thiserror:
//! everything on the durability path stays inspectable in the spike". The
//! polynomial is 0x1EDC6F41 (reflected 0x82F63B78), the same one iSCSI,
//! ext4 and NVMe use. The test vector is the one the card names.

/// The reflected Castagnoli polynomial.
const POLY: u32 = 0x82F6_3B78;

/// The 256-entry table, built at first use from the polynomial.
static TABLE: std::sync::OnceLock<[u32; 256]> = std::sync::OnceLock::new();

fn table() -> &'static [u32; 256] {
    TABLE.get_or_init(|| {
        let mut t = [0u32; 256];
        let mut i = 0usize;
        while i < 256 {
            let mut c = i as u32;
            let mut k = 0;
            while k < 8 {
                c = if c & 1 != 0 { (c >> 1) ^ POLY } else { c >> 1 };
                k += 1;
            }
            t[i] = c;
            i += 1;
        }
        t
    })
}

/// CRC32C of `data`, starting from `crc` (pass 0 for a fresh checksum).
pub fn crc32c(crc: u32, data: &[u8]) -> u32 {
    let t = table();
    let mut c = !crc;
    for &b in data {
        c = t[((c ^ b as u32) & 0xFF) as usize] ^ (c >> 8);
    }
    !c
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The card's test vector (Work 0): CRC32C("123456789") = 0xE3069283.
    #[test]
    fn card_test_vector() {
        assert_eq!(crc32c(0, b"123456789"), 0xE306_9283);
    }

    /// Continuation: feeding the data in pieces equals feeding it whole.
    #[test]
    fn continuation_composes() {
        let whole = crc32c(0, b"abcdefgh");
        let split = crc32c(crc32c(0, b"abcd"), b"efgh");
        assert_eq!(whole, split);
    }

    /// The empty input has the standard CRC32C value.
    #[test]
    fn empty() {
        assert_eq!(crc32c(0, b""), 0x0000_0000);
    }

    /// 32 zero bytes (a fresh page tail) pinned against accidental change.
    #[test]
    fn zeros32() {
        assert_eq!(crc32c(0, &[0u8; 32]), 0x8A91_36AA);
    }
}
