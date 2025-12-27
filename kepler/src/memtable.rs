use crate::value::Value;
use bytes::Bytes;
use std::collections::BTreeMap;

pub struct MemTable {
    pub tree: BTreeMap<Bytes, (u64, Value)>,
    pub bytes_written: usize,
}

impl MemTable {
    pub fn new() -> Self {
        Self {
            tree: BTreeMap::new(),
            bytes_written: 0,
        }
    }

    pub fn put(&mut self, seqno: u64, key: &[u8], val: Option<&[u8]>) {
        let (key_len, key_bytes) = (key.len(), Bytes::copy_from_slice(key));
        let (val_len, val_value) = match val {
            Some(v) => (v.len(), Value::Data(Bytes::copy_from_slice(v))),
            None => (0, Value::Tombstone),
        };

        match self.tree.get(&key_bytes) {
            None => {
                self.bytes_written += 8 + key_len + val_len;
            }
            Some((_, v)) => {
                let old_val_len = match v {
                    Value::Data(b) => b.len(),
                    Value::Tombstone => 0,
                };
                self.bytes_written = self.bytes_written + val_len - old_val_len;
            }
        }

        self.tree.insert(key_bytes, (seqno, val_value));
    }
}
