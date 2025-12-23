use crate::error::{KeplerResult, KeplerErr};

pub fn from_le_to_u64(data: &[u8], start_idx: usize, end_idx: usize) -> KeplerResult<u64> {
    if data.len() < 8 {
        return Err(KeplerErr::CorruptedSst(0));
    }
    let mut arr = [0u8; 8];
    arr.copy_from_slice(&data[start_idx..end_idx]);
    Ok(u64::from_le_bytes(arr))
}
