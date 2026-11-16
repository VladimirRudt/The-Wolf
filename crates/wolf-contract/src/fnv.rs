/// FNV-1a 64-bit. Hand-rolled because std's hashers are not guaranteed stable across
/// releases. Also used outside the contract (shard placement hashing in the host), where
/// the same stability guarantee is required.
#[derive(Debug)]
pub struct Fnv1a(u64);

impl Default for Fnv1a {
    fn default() -> Self {
        Self::new()
    }
}

impl Fnv1a {
    const OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;

    pub fn new() -> Self {
        Self(Self::OFFSET_BASIS)
    }

    pub fn write(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            self.0 ^= u64::from(byte);
            self.0 = self.0.wrapping_mul(Self::PRIME);
        }
    }

    pub fn finish(&self) -> u64 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_reference_vectors() {
        // From the FNV reference test suite (draft-eastlake-fnv).
        for (input, expected) in [
            ("", 0xcbf2_9ce4_8422_2325),
            ("a", 0xaf63_dc4c_8601_ec8c),
            ("foobar", 0x8594_4171_f739_67e8),
        ] {
            let mut hash = Fnv1a::new();
            hash.write(input.as_bytes());
            assert_eq!(hash.finish(), expected, "FNV-1a({input:?})");
        }
    }
}
