use std::{fs, path::Path};

use bytes::Bytes;

use crate::{db::Value, memtable::MemTable, wal_writer};

pub fn replay(path: &Path) -> (u64, u64, MemTable) {
    let (max_seqno, max_sstno) = replay_sst(path);
    if let Some((mem, seq)) = replay_wal(path, max_seqno) {
        return (seq + 1, max_sstno + 1, mem);
    }

    (max_seqno, max_sstno, MemTable::new())
}

pub fn replay_sst(path: &Path) -> (u64, u64) {
    /// manifest form = type(1) + sstno(8) + max_seqno(8) + min_seqno(8) 
    let manifest_path = path.join("manifest");
    let data: &[u8] = &fs::read(manifest_path).unwrap();
    let data_len = data.len();
    let mut idx: usize = 0;
    let mut max_seqno: u64 = 1;
    let mut max_sstno: u64 = 1;

    while (idx <= data_len) {
        let sstno = from_le_to_u64(data, idx + 1, idx + 9);
        let max_seqno = from_le_to_u64(data, idx + 9, idx + 17);
        if max_sstno < sstno { max_sstno = sstno; }
        idx += 25;
    }

    (max_seqno, max_sstno)
}

pub fn replay_wal(path: &Path, max_seqno: u64) -> Option<(MemTable, u64)> {
    let wal_path = path.join("wal");
    let latest_wal = wal_writer::find_latest_file(&wal_path).unwrap();
    let value_slice = match latest_wal {
        None => {
            return None;
        },
        Some(s) => s,
    };

    let latest_file_id = value_slice.0.0;
    let mut current_file_id: u64 = 1;
    let mut temp_active = MemTable::new();
    let mut seqno_return = 1;

    while current_file_id <= latest_file_id {
        let current_wal_path = wal_path.join(format!("wal-{:06}.log", current_file_id));
        let data: &[u8] = &fs::read(current_wal_path).unwrap();
        let data_len = data.len();
        let mut idx = 0;
        
        while idx <= data_len {
            let seqno = from_le_to_u64(data, 0, 8);
            let key_len = from_le_to_u64(data, idx + 9, idx + 13) as usize;
            let val_len = from_le_to_u64(data, idx + 13, idx + 17) as usize;

            if max_seqno < seqno {
                seqno_return = seqno;
                let type_num = u8::from_be_bytes(data[idx + 1..idx + 2].try_into().unwrap());
                let new_key = Bytes::copy_from_slice(&data[idx + 17..idx + 17 + key_len]);
                let new_val = if type_num == 0 {
                    let val = &data[(idx + 17 + key_len)..(idx + 17 + key_len + val_len)];
                    Value::Data(Bytes::copy_from_slice(val)) 
                } else { Value::Tombstone };

                temp_active.put(seqno, new_key, new_val);
            }

            idx += 8 + 1 + 4 + 4 + key_len + val_len;
        }

        current_file_id += 1;
    }

    Some((temp_active, seqno_return))
}

pub fn from_le_to_u64(data: &[u8], start_idx: usize, end_idx: usize) -> u64 {
    u64::from_le_bytes(data[start_idx..end_idx].try_into().unwrap())
}

