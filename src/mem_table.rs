use crate::{
    error::{KeplerErr, KeplerResult},
    traits::{Getable, Putable},
    types::{TableMap, Value},
};
use bytes::Bytes;
use std::{
    collections::BTreeMap,
    sync::{
        RwLock,
        atomic::{AtomicUsize, Ordering},
    },
};

impl Getable for MemTable {
    fn get(&self, key: &[u8]) -> KeplerResult<Option<Bytes>> {
        if let Some(v) = self.tree_get(key)? {
            match v {
                Value::Data(b) => return Ok(Some(b)),
                Value::Tombstone => return Ok(None),
            }
        }
        Ok(None)
    }
}

impl Putable for MemTable {
    fn put(&self, seqno: u64, key: &[u8], val: Option<&[u8]>) -> KeplerResult<()> {
        let key_bytes = Bytes::copy_from_slice(key);
        let mut allocated = key.len() + 8;

        let value = match val {
            Some(v) => {
                allocated += v.len();
                Value::Data(Bytes::copy_from_slice(v))
            }
            None => {
                allocated += 1;
                Value::Tombstone
            }
        };

        self.bytes_written.fetch_add(allocated, Ordering::Relaxed);
        self.tree
            .write()
            .map_err(|_| KeplerErr::LockPoisoned)?
            .insert(key_bytes, (seqno, value));
        Ok(())
    }
}

pub struct MemTable {
    pub tree: RwLock<TableMap>,
    pub bytes_written: AtomicUsize,
}

impl MemTable {
    pub fn new() -> Self {
        Self {
            tree: RwLock::new(BTreeMap::new()),
            bytes_written: AtomicUsize::new(0),
        }
    }

    pub fn bytes_written(&self) -> usize {
        self.bytes_written.load(Ordering::Relaxed)
    }

    pub fn take_tree(&self) -> KeplerResult<TableMap> {
        let mut guard = self.tree.write().map_err(|_| KeplerErr::LockPoisoned)?;
        Ok(std::mem::take(&mut *guard))
    }

    pub fn tree_get(&self, key: &[u8]) -> KeplerResult<Option<Value>> {
        let guard = self.tree.read().map_err(|_| KeplerErr::LockPoisoned)?;
        if let Some((_seqno, val)) = guard.get(key) {
            return Ok(Some(val.clone()));
        }
        Ok(None)
    }
}
