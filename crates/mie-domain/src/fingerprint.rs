//! Stable 64-bit fingerprints over canonical encodings (ADR-029).
//!
//! A fingerprint identifies a value across processes, machines and Rust
//! releases, so it is computed from an explicit byte encoding with FNV-1a 64
//! (offset basis `0xcbf29ce484222325`, prime `0x100000001b3`). It never goes
//! through `std::hash` (`Hash` derives and `DefaultHasher` are not stable
//! across Rust releases), `Debug` output, or enum discriminants that no ADR
//! freezes.
//!
//! The writers are the canonical encoding:
//!
//! | Writer                       | Bytes                                         |
//! |------------------------------|-----------------------------------------------|
//! | [`Fingerprinter::write_u8`]  | the byte                                      |
//! | [`Fingerprinter::write_u32`] | 4 bytes, little-endian                        |
//! | [`Fingerprinter::write_u64`] | 8 bytes, little-endian                        |
//! | [`Fingerprinter::write_i64`] | 8 bytes, little-endian two's complement       |
//! | [`Fingerprinter::write_str`] | byte length as `u32` LE, then the UTF-8 bytes |
//! | [`Fingerprinter::write_len`] | a count or length as `u32` LE                 |
//!
//! The length prefix keeps adjacent strings apart: `("ab", "c")` and
//! `("a", "bc")` encode differently.
//!
//! A fingerprint is an identity, not tamper evidence: 64 non-cryptographic
//! bits separate a few hundred definitions safely but do not resist a chosen
//! collision.

use std::fmt;

/// FNV-1a 64 offset basis.
const OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;

/// FNV-1a 64 prime.
const PRIME: u64 = 0x0000_0100_0000_01b3;

/// A stable 64-bit fingerprint; displays as 16 lowercase hex digits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Fingerprint(u64);

impl Fingerprint {
    /// Wraps a raw fingerprint value, such as one pinned in a lock table.
    pub const fn from_raw(value: u64) -> Self {
        Self(value)
    }

    /// The raw value.
    pub const fn value(self) -> u64 {
        self.0
    }
}

impl fmt::Display for Fingerprint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

/// Computes a [`Fingerprint`] over the canonical encoding (module docs).
#[derive(Debug, Clone)]
pub struct Fingerprinter {
    state: u64,
}

impl Default for Fingerprinter {
    fn default() -> Self {
        Self::new()
    }
}

impl Fingerprinter {
    /// Starts at the FNV-1a 64 offset basis.
    pub const fn new() -> Self {
        Self {
            state: OFFSET_BASIS,
        }
    }

    /// Feeds raw bytes. Private: callers write typed, self-delimiting values.
    fn write_bytes(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            self.state ^= u64::from(byte);
            self.state = self.state.wrapping_mul(PRIME);
        }
    }

    /// Writes one byte.
    pub fn write_u8(&mut self, value: u8) {
        self.write_bytes(&[value]);
    }

    /// Writes 4 bytes, little-endian.
    pub fn write_u32(&mut self, value: u32) {
        self.write_bytes(&value.to_le_bytes());
    }

    /// Writes 8 bytes, little-endian.
    pub fn write_u64(&mut self, value: u64) {
        self.write_bytes(&value.to_le_bytes());
    }

    /// Writes 8 bytes, little-endian two's complement.
    pub fn write_i64(&mut self, value: i64) {
        self.write_bytes(&value.to_le_bytes());
    }

    /// Writes the byte length as a `u32` (little-endian), then the UTF-8
    /// bytes.
    ///
    /// # Panics
    ///
    /// If the string is longer than `u32::MAX` bytes.
    pub fn write_str(&mut self, value: &str) {
        self.write_len(value.len());
        self.write_bytes(value.as_bytes());
    }

    /// Writes a count or length as a `u32` (little-endian).
    ///
    /// # Panics
    ///
    /// If `len` exceeds `u32::MAX`.
    pub fn write_len(&mut self, len: usize) {
        let len = u32::try_from(len).expect("a fingerprinted length fits in u32");
        self.write_u32(len);
    }

    /// The fingerprint of everything written so far.
    pub const fn finish(&self) -> Fingerprint {
        Fingerprint(self.state)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fnv(bytes: &[u8]) -> Fingerprint {
        let mut hasher = Fingerprinter::new();
        hasher.write_bytes(bytes);
        hasher.finish()
    }

    #[test]
    fn matches_the_published_fnv1a_64_vectors() {
        assert_eq!(fnv(b"").value(), 0xcbf2_9ce4_8422_2325);
        assert_eq!(fnv(b"a").value(), 0xaf63_dc4c_8601_ec8c);
        assert_eq!(fnv(b"foobar").value(), 0x8594_4171_f739_67e8);
        assert_eq!(Fingerprinter::default().finish(), fnv(b""));
    }

    #[test]
    fn integers_are_fixed_width_little_endian() {
        let mut hasher = Fingerprinter::new();
        hasher.write_u8(0xab);
        hasher.write_u32(0x0102_0304);
        hasher.write_u64(0x0102_0304_0506_0708);
        hasher.write_i64(-2);
        let mut expected = vec![0xab, 4, 3, 2, 1, 8, 7, 6, 5, 4, 3, 2, 1];
        expected.extend([0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]);
        assert_eq!(hasher.finish(), fnv(&expected));
    }

    #[test]
    fn strings_carry_a_u32_length_prefix() {
        let mut hasher = Fingerprinter::new();
        hasher.write_str("ab");
        assert_eq!(hasher.finish(), fnv(&[2, 0, 0, 0, b'a', b'b']));

        let pair = |a: &str, b: &str| {
            let mut hasher = Fingerprinter::new();
            hasher.write_str(a);
            hasher.write_str(b);
            hasher.finish()
        };
        assert_ne!(pair("ab", "c"), pair("a", "bc"));
        assert_ne!(pair("", "a"), pair("a", ""));
    }

    #[test]
    fn lengths_are_u32_little_endian() {
        let mut hasher = Fingerprinter::new();
        hasher.write_len(258);
        assert_eq!(hasher.finish(), fnv(&[2, 1, 0, 0]));
    }

    #[test]
    fn displays_as_16_lowercase_hex_digits() {
        assert_eq!(Fingerprint::from_raw(0).to_string(), "0000000000000000");
        assert_eq!(Fingerprint::from_raw(0xAB).to_string(), "00000000000000ab");
        assert_eq!(
            Fingerprint::from_raw(u64::MAX).to_string(),
            "ffffffffffffffff"
        );
        assert_eq!(Fingerprint::from_raw(42).value(), 42);
    }
}
