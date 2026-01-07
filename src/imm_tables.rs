use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

use bytes::Bytes;

use crate::{
    error::{KeplerErr, KeplerResult},
    traits::Getable,
    types::{TableMap, Value},
};

impl Getable for ImmTables {
    fn get(&self, key: &[u8]) -> KeplerResult<Option<Bytes>> {
        let tables = &self.0.lock().map_err(|_| KeplerErr::CorruptedSst(0))?;

        for table in tables.iter().rev() {
            if let Some((_seqno, Value::Data(v))) = table.get(key) {
                return Ok(Some(Bytes::copy_from_slice(&v)));
            }
        }
        Ok(None)
    }
}

pub struct ImmTables(Mutex<VecDeque<Arc<TableMap>>>);

impl ImmTables {
    pub fn new() -> Self {
        Self(Mutex::new(VecDeque::new()))
    }

    pub fn push_back(&self, tree: Arc<TableMap>) -> KeplerResult<()> {
        self.0
            .lock()
            .map_err(|_| KeplerErr::LockPoisoned)?
            .push_back(tree);
        Ok(())
    }

    pub fn pop_front(&self) -> KeplerResult<()> {
        self.0
            .lock()
            .map_err(|_| KeplerErr::LockPoisoned)?
            .pop_front();
        Ok(())
    }
}
