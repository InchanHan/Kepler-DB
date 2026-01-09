use std::{
    mem::{self},
    path::Path,
    sync::{Arc, RwLock, mpsc::Sender},
};

use bytes::Bytes;

use crate::{
    Error,
    constants::ACTIVE_CAP_MAX,
    imm_tables::ImmTables,
    manifest::Manifest,
    mem_table::MemTable,
    sst_manager::SSTManager,
    sst_writer::SSTWriter,
    traits::{Getable, Putable},
    types::WorkerSignal,
};

impl Getable for TableSet {
    fn get(&self, key: &[u8]) -> crate::Result<Option<Bytes>> {
        let get_active = self.active.read().map_err(|_| Error::Poisoned)?.get(key);

        if let Some(v) = get_active? {
            return Ok(Some(v));
        }

        if let Some(v) = self.imm_tables.get(key)? {
            return Ok(Some(v));
        }

        if let Some(v) = self.sst_manager.get(key)? {
            return Ok(Some(v));
        }
        Ok(None)
    }
}

impl Putable for TableSet {
    fn put(&self, seqno: u64, key: &[u8], val: Option<&[u8]>) -> crate::Result<()> {
        let mut active_ptr = self.active.write().map_err(|_| Error::Concurrency)?;
        active_ptr.put(seqno, key, val)?;

        if active_ptr.bytes_written() >= ACTIVE_CAP_MAX {
            let old = mem::replace(&mut *active_ptr, MemTable::new());
            let table_map = Arc::new(old.take_tree()?);
            self.imm_tables.push_back(table_map.clone())?;
            self.sst_writer.send(WorkerSignal::Flush(table_map))?;
        }
        Ok(())
    }
}

pub struct TableSet {
    sst_writer: SSTWriter,
    active: RwLock<MemTable>,
    imm_tables: Arc<ImmTables>,
    sst_manager: Arc<SSTManager>,
}

impl TableSet {
    pub(crate) fn new(
        path: &Path,
        sst_manager: SSTManager,
        mem: MemTable,
        manifest: Arc<Manifest>,
        err_tx: Sender<WorkerSignal>,
    ) -> crate::Result<Self> {
        let active = RwLock::new(mem);
        let imm_tables = Arc::new(ImmTables::new());
        let sst_manager = Arc::new(sst_manager);
        let sst_writer = SSTWriter::new(
            path,
            manifest,
            imm_tables.clone(),
            sst_manager.clone(),
            err_tx,
        )?;

        Ok(Self {
            sst_writer,
            active,
            imm_tables,
            sst_manager,
        })
    }
}
