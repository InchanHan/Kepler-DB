use murmur3::murmur3_x64_128;

use crate::constants::{HASH_COUNT, HASH_SEED};

pub(crate) struct BloomFilter {
    bits: Vec<u8>,
    bit_size: usize,
}

impl BloomFilter {
    pub fn new(key_count: usize) -> Self {
        let bit_size = if key_count == 0 { 0 } else { key_count * 10 };
        let byte_size = (bit_size + 7) / 8;

        Self {
            bits: vec![0u8; byte_size],
            bit_size,
        }
    }

    pub fn options(bits: Vec<u8>, bit_size: usize) -> Self {
        Self { bits, bit_size }
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.bits
    }

    pub fn len(&self) -> usize {
        self.bits.len()
    }

    pub fn bit_size(&self) -> usize {
        self.bit_size
    }

    pub fn add(&mut self, key: &[u8]) {
        if self.bit_size == 0 {
            return;
        }

        let (hi, lo) = hash_key_split(key);
        let bit_size = self.bit_size;

        for i in 0..HASH_COUNT {
            let idx = hi.wrapping_add((i as u64).wrapping_mul(lo)) as usize % bit_size;
            let byte_idx = idx / 8;
            let bit_pos = idx % 8;

            self.bits[byte_idx] |= 1 << bit_pos;
        }
    }

    pub fn contains(&self, key: &[u8]) -> bool {
        if self.bit_size == 0 {
            return false;
        }

        let (hi, lo) = hash_key_split(key);
        let bit_size = self.bit_size;

        for i in 0..HASH_COUNT {
            let idx = hi.wrapping_add((i as u64).wrapping_mul(lo)) as usize % bit_size;
            let byte_idx = idx / 8;
            let bit_pos = idx % 8;

            if (self.bits[byte_idx] & (1 << bit_pos)) == 0 {
                return false;
            }
        }

        true
    }
}

fn hash_key_split(mut key: &[u8]) -> (u64, u64) {
    let hash = murmur3_x64_128(&mut key, HASH_SEED).unwrap();
    let hi = hash as u64;
    let lo = (hash >> 64) as u64;

    (hi, lo)
}
