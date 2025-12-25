use crate::{
    error::{KeplerErr, KeplerResult}, memtable::MemTable, utils::{from_le_to_u32, from_le_to_u64}, value::Value, wal_writer::find_latest_file
};
use std::{fs, path::Path};
use bytes::Bytes;


pub fn replay(path: &Path) -> KeplerResult<(u64, u64, MemTable)> {
    let (max_seqno, max_sstno) = replay_sst(path)?;
    if let Some((mem, seq)) = replay_wal(path, max_seqno)? {
        return Ok((seq + 1, max_sstno + 1, mem));
    }

    Ok((max_seqno + 1, max_sstno + 1, MemTable::new()))
}

pub fn replay_sst(path: &Path) -> KeplerResult<(u64, u64)> {
    let manifest_path = path.join("manifest");
    let data: &[u8] = &fs::read(manifest_path)?;
    let data_len = data.len();
    let mut idx: usize = 0;
    let mut max_seqno: u64 = 1;
    let mut max_sstno: u64 = 1;

    while idx + 25 <= data_len {
        let sstno = from_le_to_u64(data, idx + 1, idx + 9)?;
        max_seqno = from_le_to_u64(data, idx + 9, idx + 17)?;
        if max_sstno < sstno {
            max_sstno = sstno;
        }
        idx += 1 + 8 + 8 + 8;
    }

    Ok((max_seqno, max_sstno))
}

pub fn replay_wal(path: &Path, max_seqno: u64) -> KeplerResult<Option<(MemTable, u64)>> {
    let wal_path = path.join("wal");
    let latest_wal = find_latest_file(&wal_path)?;
    let value_slice = match latest_wal {
        None => {
            return Ok(None);
        }
        Some(s) => s,
    };

    let latest_file_id = value_slice.0.0;
    let mut current_file_id: u64 = 1;
    let mut temp_active = MemTable::new();
    let mut seqno_return: u64 = 1;

    while current_file_id <= latest_file_id {
        let current_wal_path = wal_path.join(format!("wal-{:06}.log", current_file_id));
        let data = fs::read(current_wal_path)?;
        let data_len = data.len();
        let mut idx = 0;

        while idx + 17 <= data_len {
            let seqno = from_le_to_u64(&data, idx, idx + 8)?;
            let key_len = from_le_to_u32(&data, idx + 9, idx + 13)? as usize;
            let val_len = from_le_to_u32(&data, idx + 13, idx + 17)? as usize;

            if max_seqno < seqno {
                seqno_return = seqno;
                let type_num: u8 = data[idx + 8];
                let _ = data.get(1..2);
                let new_key = Bytes::copy_from_slice(data
                    .get(idx + 17..idx + 17 + key_len)
                    .ok_or(KeplerErr::Wal(format!(
                        "failed to read Key from WAL, which is fatal! Bytes offset is [{}..{}]",
                        idx + 17,
                        idx + 17 + key_len)))?);
                let new_val = if type_num == 0 {
                    let val = data
                        .get(idx + 17 + key_len..idx + 17 + key_len + val_len)
                        .ok_or(KeplerErr::Wal(format!(
                            "failed to read Value from WAL, which is fatal! Bytes offset is [{}..{}]",
                            idx + 17 + key_len,
                            idx + 17 + key_len + val_len)))?;
                    Value::Data(Bytes::copy_from_slice(val))
                } else {
                    Value::Tombstone
                };

                temp_active.put(seqno, new_key, new_val);
            }

            idx += 8 + 1 + 4 + 4 + key_len + val_len;
        }

        current_file_id += 1;
    }

    Ok(Some((temp_active, seqno_return)))
}
