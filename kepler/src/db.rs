use crate::{flush_worker::{self, FlushConfig, FlushResult, FlushWorker}, recovery::replay, memtable::MemTable, wal_writer::{self, WalWriter}};
use bytes::Bytes;
use std::{
    collections::{BTreeMap, VecDeque}, fs::{self, File, OpenOptions}, io::{self, BufReader, Read, Write}, mem::{self, replace}, os::unix::fs::OpenOptionsExt, path::{Path, PathBuf}, sync::{Arc, Mutex, RwLock, atomic::{AtomicU64, Ordering}, mpsc}, thread, u64
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
        File::create(&manifest_path)?;

        let (seqno, sstno, mem_table) = replay(path);
        let wal_writer = WalWriter::new(path)?;
        let (flush_worker, flush_result_rx) = FlushWorker::new(path.to_path_buf());
        manifest_writer(manifest_path, flush_result_rx);

        Ok(Self {
            active: RwLock::new(mem_table),
            flush_queue: flush_worker,
            wal: Mutex::new(wal_writer),
            seqno: AtomicU64::new(seqno),
            sstno: AtomicU64::new(sstno),
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

pub fn manifest_writer(path: PathBuf, rx: mpsc::Receiver<FlushResult>) {
    let _ = thread::spawn(move || {
        let mut manifest = OpenOptions::new()
            .append(true)
            .open(path)
            .unwrap();
        while let Ok(result) = rx.recv() {
        // type(1) + sstno(8) + max_seqno(8) + min_seqno(8)
            let type_num = result.type_num;
            let sstno = result.sstno;
            let max_seqno = result.max_seqno;
            let min_seqno = result.min_seqno;

            manifest.write_all(&[type_num]);
            manifest.write_all(&sstno.to_le_bytes());
            manifest.write_all(&max_seqno.to_le_bytes());
            manifest.write_all(&min_seqno.to_le_bytes());
            manifest.sync_all().unwrap();
        }
    });
}


