use std::{collections::BTreeMap, sync::Arc};

use crate::error::KeplerErr;
use bytes::Bytes;

#[derive(Clone)]
pub enum Value {
    Tombstone,
    Data(Bytes),
}

pub enum WorkerSignal {
    Flush(Arc<TableMap>),
    Shutdown,
    Panic(KeplerErr),
}

pub type TableMap = BTreeMap<Bytes, (u64, Value)>;
