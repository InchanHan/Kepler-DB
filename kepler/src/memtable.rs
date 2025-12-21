use std::collections::BTreeMap;
use crate::db::Value;
use bytes::Bytes;

pub struct MemTable {
    pub tree: BTreeMap<Bytes, (u64, Value)>,
    pub bytes_written: u64,
}

impl MemTable {
    pub fn new() -> Self {
        Self { tree: BTreeMap::new(), bytes_written: 0 }
    }

    pub fn put(&mut self, seqno: u64, key: Bytes, val: Value) {
        let key_len = key.len() as u64;
        let val_len = match &val {
            Value::Data(b) => b.len() as u64,
            Value::Tombstone => 0,
        };
    
        match self.tree.get(&key) {
            None => {
                self.bytes_written += 8 + key_len + val_len;
            },
            Some((_, v)) => {
                let old_val_len = match v {
                    Value::Data(b) => b.len() as u64,
                    Value::Tombstone => 0,
                };
                self.bytes_written = self.bytes_written + val_len - old_val_len;
            },
        }

        self.tree.insert(key, (seqno, val));
    }
}

