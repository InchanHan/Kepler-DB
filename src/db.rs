use std::{
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
        mpsc::{Receiver, channel},
    },
};

use bytes::Bytes;

use crate::{
    error::{KeplerErr, KeplerResult},
    journal::Journal,
    manifest::Manifest,
    sst_manager::SSTManager,
    table_set::TableSet,
    traits::{Getable, Putable},
    types::WorkerSignal,
    utils::ensure_dir,
};

pub struct Kepler(Arc<KeplerInner>);

impl Kepler {
    pub fn new<P: AsRef<Path>>(path: P) -> KeplerResult<Self> {
        Ok(Self(Arc::new(KeplerInner::new(path.as_ref())?)))
    }

    pub fn insert(&self, key: &[u8], val: &[u8]) -> KeplerResult<()> {
        Ok(self.0.put(key, Some(val))?)
    }

    pub fn remove(&self, key: &[u8]) -> KeplerResult<()> {
        Ok(self.0.put(key, None)?)
    }

    pub fn get(&self, key: &[u8]) -> KeplerResult<Option<Bytes>> {
        Ok(self.0.get(key)?)
    }

    pub fn clone(&self) -> KeplerResult<Self> {
        Ok(Self(self.0.clone()))
    }
}

pub struct KeplerInner {
    seqno: AtomicU64,
    tables: TableSet,
    journal: Mutex<Journal>,
    manifest: Arc<Manifest>,
    err_rx: Receiver<WorkerSignal>,
}

impl KeplerInner {
    fn new(path: &Path) -> KeplerResult<Self> {
        ensure_dir(path)?;
        let (err_tx, err_rx) = channel::<WorkerSignal>();
        let (manifest, version) = Manifest::new(path, err_tx.clone())?;
        let sst_manager = SSTManager::open(path, version.next_sstno)?;
        let (journal, mem, next_inner_seqno) = Journal::open(path, version.next_seqno)?;
        Ok(Self {
            seqno: AtomicU64::new(next_inner_seqno),
            tables: TableSet::new(path, sst_manager, mem, manifest.clone(), err_tx)?,
            journal: Mutex::new(journal),
            manifest,
            err_rx,
        })
    }

    fn put(&self, key: &[u8], val: Option<&[u8]>) -> KeplerResult<()> {
        self.check_thread_error()?;
        let seqno = self.seqno.fetch_add(1, Ordering::SeqCst);
        self.journal
            .lock()
            .map_err(|_| KeplerErr::LockPoisoned)?
            .insert(seqno, key, val)?;
        self.tables.put(seqno, key, val)
    }

    fn get(&self, key: &[u8]) -> KeplerResult<Option<Bytes>> {
        self.check_thread_error()?;
        self.tables.get(key)
    }

    fn check_thread_error(&self) -> KeplerResult<()> {
        match self.err_rx.try_recv() {
            Ok(WorkerSignal::Panic(e)) => Err(e),
            _ => Ok(()),
        }
    }
}
