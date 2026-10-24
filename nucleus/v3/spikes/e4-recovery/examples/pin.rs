//! Prints the spike's pinned checksum and PRNG reference values.
//!
//! Useful as a smoke check that the in-crate CRC32C table and the
//! splitmix64 seeding are what they were when the campaign digests were
//! produced (card X-5 Work 0).
fn main() {
    println!(
        "crc32c(\"123456789\") = {:#010X}",
        e4_recovery::crc::crc32c(0, b"123456789")
    );
    let mut s = 0u64;
    println!(
        "splitmix64(0) first two = ({}, {})",
        e4_recovery::rng::splitmix64(&mut s),
        e4_recovery::rng::splitmix64(&mut s)
    );
}
