use crate::{
    Error,
    journal::Journal,
    manifest::Manifest,
    mem_table::MemTable,
    sst_manager::SSTManager,
    table_set::TableSet,
    traits::{Getable, Putable},
    types::WorkerSignal,
    utils::ensure_dir,
    version::Version,
};
use bytes::Bytes;
use std::{
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
        mpsc::{Receiver, Sender, channel},
    },
};

impl Clone for Kepler {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

pub struct Kepler(pub(crate) Arc<KeplerInner>);

impl Kepler {
    pub fn new<P: AsRef<Path>>(path: P) -> crate::Result<Self> {
        Ok(Self(Arc::new(KeplerInner::new(path.as_ref())?)))
    }

    pub fn insert(&self, key: &[u8], val: &[u8]) -> crate::Result<()> {
        Ok(self.0.put(key, Some(val))?)
    }

    pub fn remove(&self, key: &[u8]) -> crate::Result<()> {
        Ok(self.0.put(key, None)?)
    }

    pub fn get(&self, key: &[u8]) -> crate::Result<Option<Bytes>> {
        Ok(self.0.get(key)?)
    }
}

pub struct KeplerInner {
    pub seqno: AtomicU64,
    pub tables: TableSet,
    pub journal: Mutex<Journal>,
    #[allow(dead_code)]
    pub manifest: Arc<Manifest>,
    pub(crate) err_rx: Receiver<WorkerSignal>,
}

impl KeplerInner {
    pub fn new(path: &Path) -> crate::Result<Self> {
        ensure_dir(path)?;
        let (err_tx, err_rx) = channel::<WorkerSignal>();
        let (manifest, version) = Self::open_manifest(path, err_tx.clone())?;
        let sst_manager = SSTManager::open(path, version.next_sstno)?;
        let (journal, mem, next_inner_seqno) =
            Self::open_storage_components(path, version.next_seqno)?;
        Ok(Self {
            seqno: AtomicU64::new(next_inner_seqno),
            tables: TableSet::new(path, sst_manager, mem, manifest.clone(), err_tx)?,
            journal: Mutex::new(journal),
            manifest,
            err_rx,
        })
    }

    pub fn put(&self, key: &[u8], val: Option<&[u8]>) -> crate::Result<()> {
        self.check_thread_error()?;
        let seqno = self.seqno.fetch_add(1, Ordering::Relaxed);

        let mut journal = self.journal.lock().map_err(|_| Error::Poisoned)?;

        journal
            .insert(seqno, key, val)
            .map_err(|_| Error::Poisoned)?;

        self.tables.put(seqno, key, val)
    }

    pub fn get(&self, key: &[u8]) -> crate::Result<Option<Bytes>> {
        self.check_thread_error()?;
        self.tables.get(key)
    }

    fn check_thread_error(&self) -> crate::Result<()> {
        match self.err_rx.try_recv() {
            Ok(WorkerSignal::Panic(e)) => Err(e),
            _ => Ok(()),
        }
    }

    fn open_manifest(
        path: &Path,
        err_tx: Sender<WorkerSignal>,
    ) -> crate::Result<(Arc<Manifest>, Version)> {
        Ok(Manifest::new(path, err_tx).map_err(|_| Error::Unrecoverable)?)
    }

    fn open_storage_components(path: &Path, seqno: u64) -> crate::Result<(Journal, MemTable, u64)> {
        Ok(Journal::open(path, seqno).map_err(|_| Error::Unrecoverable)?)
    }
}
