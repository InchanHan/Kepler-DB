pub fn from_le_to_u64(data: &[u8], start_idx: usize, end_idx: usize) -> u64 {
    u64::from_le_bytes(data[start_idx..end_idx].try_into().unwrap())
}

