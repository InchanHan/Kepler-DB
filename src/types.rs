use std::{collections::BTreeMap, sync::Arc};

use bytes::Bytes;

use crate::Error;

#[derive(Clone)]
pub enum Value {
    Tombstone,
    Data(Bytes),
}

pub enum WorkerSignal {
    Flush(Arc<TableMap>),
    #[allow(dead_code)]
    Shutdown,
    Panic(Error),
}

pub type TableMap = BTreeMap<Bytes, (u64, Value)>;
