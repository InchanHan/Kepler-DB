use std::{collections::VecDeque, sync::{Arc, Mutex}};

use crate::memtable::MemTable;

pub struct ImmTables(pub Mutex<VecDeque<Arc<MemTable>>>);

impl ImmTables {
    pub fn new() -> Self {
        Self(Mutex::new(VecDeque::new()))
    }
}
