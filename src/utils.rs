use memmap2::Mmap;
use std::{fs, path::Path};

pub(crate) fn ensure_dir(path: &Path) -> std::io::Result<()> {
    if !path.exists() {
        fs::create_dir_all(path)?;
    }
    Ok(())
}

pub(crate) fn from_le_to_u64(
    data: &Mmap,
    idx: usize,
    start_idx: usize,
    end_idx: usize,
) -> crate::Result<u64> {
    let mut arr = [0u8; 8];
    arr.copy_from_slice(&data[idx + start_idx..idx + end_idx]);
    Ok(u64::from_le_bytes(arr))
}

pub(crate) fn from_le_to_u32(
    data: &Mmap,
    idx: usize,
    start_idx: usize,
    end_idx: usize,
) -> crate::Result<u32> {
    let mut arr = [0u8; 4];
    arr.copy_from_slice(&data[idx + start_idx..idx + end_idx]);
    Ok(u32::from_le_bytes(arr))
}
