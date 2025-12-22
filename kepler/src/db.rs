use crate::{utils::from_le_to_u64, flush_worker::{self, FlushConfig, FlushResult, FlushWorker}, recovery::replay, memtable::MemTable, wal_writer::{self, WalWriter}};
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
    path: PathBuf,
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
            path: path.to_path_buf(),
        })
    }

    pub fn get(&self, key: &[u8]) -> io::Result<Option<Bytes>> {
        if let Some((_, v)) = self.active.read().unwrap().tree.get(&Bytes::copy_from_slice(key)) {
            match v {
                Value::Data(b) => return Ok(Some(b.clone())),
                Value::Tombstone => return Ok(None),
            }
        };
        // seqno(8) + flag(1) + key_len(4) + val_len(4) + key(?) + val(?)
        let mut current_file_id: u64 = 1;
        let latest_file_id = self.sstno.load(Ordering::Relaxed);
        let mut val_return = None;

        while current_file_id <= latest_file_id {
            let current_sst_path = self.path.as_path().join(format!("sst-{:06}.log", current_file_id));
            let data: &[u8] = &fs::read(current_sst_path).unwrap();
            let data_len = data.len();
            let mut idx = 0;

            while idx <= data_len {
                let flag: u8 = data[idx + 1];
                let key_len = from_le_to_u64(data, idx + 9, idx + 13) as usize;
                let val_len = from_le_to_u64(data, idx + 13, idx + 17) as usize;
                let found_key = &data[idx + 17..idx + 17 + key_len];
                let found_val = &data[(idx + 17 + key_len)..(idx + 17 + key_len + val_len)];
                let val_bytes = Bytes::copy_from_slice(found_val);
                if found_key == key {
                    match flag {
                        0 => val_return = Some(val_bytes),
                        1 => val_return = None,
                        _ => (),
                    }
                }

                idx += 8 + 1 + 4 + 4 + key_len + val_len;
            }

            current_file_id += 1;
        }

        Ok(val_return)
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


