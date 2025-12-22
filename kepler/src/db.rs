use crate::{flush_worker::{self, FlushConfig, FlushResult, FlushWorker}, memtable::MemTable, wal_writer::WalWriter};
use bytes::Bytes;
use std::{
    collections::{BTreeMap, VecDeque}, fs::{self, File, OpenOptions}, io::{self, BufReader, Read}, mem::{self, replace}, path::{Path, PathBuf}, sync::{Arc, Mutex, RwLock, atomic::{AtomicU64, Ordering}, mpsc}, thread
};

const ACTIVE_MAX_CAP: u64 = 32 * 1024 * 1024;

pub enum Value {
    Data(Bytes),
    Tombstone,
}

struct Kepler(Arc<KeplerInner>);

pub(crate) struct KeplerInner {
    active: RwLock<MemTable>,
    flush_queue: FlushWorker,
    wal: Mutex<WalWriter>,
    seqno: AtomicU64,
    sstno: AtomicU64,
}

impl KeplerInner {
    pub fn new(path: &Path) -> io::Result<Self> {
        let sst_dir = path.join("sst");
        let manifest_path = path.join("manifest");
        fs::create_dir_all(&sst_dir)?;

        let mut manifest = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&manifest_path)?;
        let mut buf_reader = BufReader::new(manifest);
        let mut seqno = String::new();
        buf_reader.read_to_string(&mut seqno)?;

        let seqno: u64 = seqno.parse().unwrap_or(0);
        let mem_table = MemTable::new();
        let wal_writer = WalWriter::new(path)?;
        let (flush_worker, flush_result_rx) = FlushWorker::new(path.to_path_buf());
        manifest_writer(flush_result_rx);
        Ok(Self {
            active: RwLock::new(mem_table),
            flush_queue: flush_worker,
            wal: Mutex::new(wal_writer),
            seqno: AtomicU64::new(seqno),
            sstno: AtomicU64::new(0),
        })
    }

    pub fn insert(&self, key: &[u8], val: &[u8]) -> io::Result<()> {
        let seqno = self.seqno.fetch_add(1, Ordering::Relaxed);

        let mut wal_ptr = self.wal.lock().unwrap();
        wal_ptr.put(seqno, key, Some(val))?;
        drop(wal_ptr);

        let key_bytes = Bytes::copy_from_slice(key);
        let val_bytes = Value::Data(Bytes::copy_from_slice(val));
        let mut active_ptr = self.active.write().unwrap();
        active_ptr.put(seqno, key_bytes, val_bytes);

        let old = if active_ptr.bytes_written >= ACTIVE_MAX_CAP {
            Some(mem::replace(&mut *active_ptr, MemTable::new()))
        } else {
            None
        };
        drop(active_ptr);
        
        if let Some(old) = old {
            let sstno = self.seqno.fetch_add(1,Ordering::Relaxed);
            let cfg = FlushConfig::new(old, sstno);
            let _ = self.flush_queue.sender.send(cfg);
        };
   
        Ok(())
    }
}

pub fn manifest_writer(rx: mpsc::Receiver<FlushResult>) {
    let _ = thread::spawn(move || {
        while let Ok(result) = rx.recv() {
        }// writting to manifest
    });
}


