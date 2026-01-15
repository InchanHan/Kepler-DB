use std::collections::BTreeMap;

pub struct Version {
    #[allow(dead_code)]
    pub sst_list: BTreeMap<u64, SSTInfo>,
    pub next_seqno: u64,
    pub next_sstno: u64,
}

impl Version {
    pub fn new(sst_list: BTreeMap<u64, SSTInfo>, next_seqno: u64, next_sstno: u64) -> Self {
        Self {
            sst_list,
            next_seqno,
            next_sstno,
        }
    }
}

pub struct SSTInfo {
    #[allow(dead_code)]
    pub id: u64,
}

impl SSTInfo {
    pub fn new(id: u64) -> Self {
        Self { id }
    }
}
