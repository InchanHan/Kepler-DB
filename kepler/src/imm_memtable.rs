use std::{collections::VecDeque, sync::{Arc, Mutex}};

use crate::memtable::MemTable;

pub struct ImmTables {
    pub tables: Mutex<VecDeque<Arc<MemTable>>>,
}
impl ImmTables {
    pub fn new() -> Self {
        Self { tables: Mutex::new(VecDeque::new()), }
    }
}
