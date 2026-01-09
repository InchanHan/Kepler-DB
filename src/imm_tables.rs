use std::{
    collections::VecDeque,
    sync::{Arc, RwLock},
};

use bytes::Bytes;

use crate::{
    Error,
    traits::Getable,
    types::{TableMap, Value},
};

impl Getable for ImmTables {
    fn get(&self, key: &[u8]) -> crate::Result<Option<Bytes>> {
        match self.lookup_latest(key)? {
            Some(Value::Data(v)) => Ok(Some(v)),
            Some(Value::Tombstone) => Ok(None),
            _ => Ok(None),
        }
    }
}

pub struct ImmTables(RwLock<VecDeque<Arc<TableMap>>>);

impl ImmTables {
    pub fn new() -> Self {
        Self(RwLock::new(VecDeque::new()))
    }

    pub fn push_back(&self, tree: Arc<TableMap>) -> crate::Result<()> {
        self.0.write().map_err(|_| Error::Poisoned)?.push_back(tree);
        Ok(())
    }

    pub fn pop_front(&self) -> crate::Result<Option<Arc<TableMap>>> {
        Ok(self.0.write().map_err(|_| Error::Poisoned)?.pop_front())
    }

    fn lookup_latest(&self, key: &[u8]) -> crate::Result<Option<Value>> {
        let tables = self.0.read().map_err(|_| Error::Poisoned)?;

        for table in tables.iter().rev() {
            if let Some((_seqno, val)) = table.get(key) {
                return Ok(Some(val.clone()));
            }
        }
        Ok(None)
    }
}
