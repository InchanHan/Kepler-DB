use crate::{
    constants::ACTIVE_CAP_MAX, error::{KeplerErr, KeplerResult}, flush_worker::{FlushConfig, FlushResult, FlushWorker}, imm_memtable::ImmTables, memtable::MemTable, recovery::replay, utils::from_le_to_u32, value::Value, wal_writer::WalWriter
};
use bytes::Bytes;
use std::{
    fs::{self, OpenOptions}, io::Write, mem, path::{Path, PathBuf}, sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicU64, Ordering},
        mpsc,
    }, thread
};

pub struct Kepler(Arc<KeplerInner>);

pub(crate) struct KeplerInner {
    active: RwLock<MemTable>,
    imm_tables: Arc<ImmTables>,
    flush_worker: FlushWorker,
    wal: Mutex<WalWriter>,
    seqno: AtomicU64,
    sstno: AtomicU64,
    path: PathBuf,
}

impl Kepler {
    pub fn new<P: Into<PathBuf>>(path: P) -> KeplerResult<Self> {
        Ok(Self(Arc::new(KeplerInner::new(&path.into())?))) 
    }

    pub fn insert(&self, key: &[u8], val: &[u8]) -> KeplerResult<()> {
        Ok(self.0.insert(key, Some(val))?)
    }

    pub fn remove(&self, key: &[u8]) -> KeplerResult<()> {
        Ok(self.0.insert(key, None)?)
    }

    pub fn get(&self, key: &[u8]) -> KeplerResult<Option<Bytes>> {
        self.0.get(key)
    }
}

impl KeplerInner {
    pub fn new(path: &Path) -> KeplerResult<Self> {
        let sst_dir = path.join("sst");
        let manifest_path = path.join("manifest");
        fs::create_dir_all(&sst_dir)?;
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(&manifest_path)?;

        let (seqno, sstno, mem_table) = replay(path)?;
        let wal_writer = WalWriter::new(path)?;
        let imm_tables = Arc::new(ImmTables::new());
        let (flush_worker, flush_result_rx) = FlushWorker::new(path, imm_tables.clone());
        manifest_writer(manifest_path, flush_result_rx);

        Ok(Self {
            active: RwLock::new(mem_table),
            imm_tables,
            flush_worker,
            wal: Mutex::new(wal_writer),
            seqno: AtomicU64::new(seqno),
            sstno: AtomicU64::new(sstno),
            path: path.to_path_buf(),
        })
    }

    pub fn get(&self, key: &[u8]) -> KeplerResult<Option<Bytes>> {
        if let Some((_, v)) = self
            .active
            .read()
            .map_err(|_| KeplerErr::Memory("Lock poisoned".into()))?
            .tree
            .get(&Bytes::copy_from_slice(key))
        {
            match v {
                Value::Data(b) => return Ok(Some(b.clone())),
                Value::Tombstone => return Ok(None),
            }
        };
    
        for table in self.imm_tables.tables.lock().unwrap().iter() {
            if let Some((_, v)) = table.tree.get(&Bytes::copy_from_slice(key)) {
                match v {
                    Value::Data(b) => return Ok(Some(b.clone())),
                    Value::Tombstone => return Ok(None),
                }
            }
        }

        // seqno(8) + flag(1) + key_len(4) + val_len(4) + key(?) + val(?)
        let mut file_id = self.sstno.load(Ordering::Relaxed);
        let mut val_return = None;
        let mut seek_flag = false;

        while file_id >= 1 {
            let current_sst_path = self
                .path
                .as_path()
                .join("sst")
                .join(format!("sst-{:06}.log", file_id));
            let data = match fs::read(current_sst_path) {
                Ok(d) => d,
                Err(ref e) if e.kind() == std::io::ErrorKind::NotFound => {
                    file_id -= 1;
                    continue;
                },
                Err(e) => return Err(e.into()), 
            };
            let data_len = data.len();
            let mut idx = 0;

            while idx + 17 <= data_len && !seek_flag {
                let flag: u8 = data[idx + 8];
                let key_len = from_le_to_u32(&data, idx + 9, idx + 13)? as usize;
                let val_len = from_le_to_u32(&data, idx + 13, idx + 17)? as usize;
                let found_key = data
                    .get(idx + 17..idx + 17 + key_len)
                    .ok_or(KeplerErr::Wal(format!(
                        "failed to read Key from WAL, which is fatal! Bytes offset is [{}..{}]",
                        idx + 17,
                        idx + 17 + key_len)))?;
                let found_val = data
                    .get(idx + 17 + key_len..idx + 17 + key_len + val_len)
                    .ok_or(KeplerErr::Wal(format!(
                        "failed to read Value from WAL, which is fatal! Bytes offset is [{}..{}]",
                        idx + 17 + key_len,
                        idx + 17 + key_len + val_len)))?;
                let val_bytes = Bytes::copy_from_slice(found_val);
                if found_key == key {
                    seek_flag = true;
                    match flag {
                        0 => val_return = Some(val_bytes),
                        1 => val_return = None,
                        _ => return Err(KeplerErr::CorruptedSst(0)),
                    }
                }

                idx += 8 + 1 + 4 + 4 + key_len + val_len;
            }

            file_id -= 1;
        }

        Ok(val_return)
    }

    pub fn insert(&self, key: &[u8], val: Option<&[u8]>) -> KeplerResult<()> {
        let seqno = self.seqno.fetch_add(1, Ordering::Relaxed);

        let mut wal_ptr = self.wal.lock().map_err(|_| KeplerErr::Wal("Lock poisoned. Cannot access to the WalWriter.".into()))?;
        wal_ptr.put(seqno, key, val)?;
        drop(wal_ptr);

        let key_bytes = Bytes::copy_from_slice(key);
        let val_value = match val {
            Some(v) => Value::Data(Bytes::copy_from_slice(v)),
            None => Value::Tombstone,
        };
        let mut active_ptr = self.active.write().map_err(|_| KeplerErr::Memory("Lock posisoned. Can't access to the Memory".into()))?;
        active_ptr.put(seqno, key_bytes, val_value);

        if active_ptr.bytes_written >= ACTIVE_CAP_MAX {
            let old = Arc::new(mem::replace(&mut *active_ptr, MemTable::new()));
            self.imm_tables.tables.lock().unwrap().push_back(old.clone());
            let sstno = self.sstno.fetch_and(1, Ordering::Relaxed);
            let cfg = FlushConfig::new(old.clone(), sstno);
            let _ = self.flush_worker.sender.send(cfg);
        }
        drop(active_ptr);

        Ok(())
    }
}

pub fn manifest_writer(path: PathBuf, rx: mpsc::Receiver<FlushResult>) {
    let _ = thread::spawn(move || {
        let mut manifest = OpenOptions::new()
            .append(true)
            .open(path)
            .expect("Manifest Writer: failed to open manifest filed.");
        while let Ok(result) = rx.recv() {
            // type(1) + sstno(8) + max_seqno(8) + min_seqno(8)
            manifest.write_all(&[result.type_num])
                .expect("Manifest Writer: failed to write type byte");
            manifest.write_all(&result.sstno.to_le_bytes())
                .expect("Manifest Writer: failed to write sstno bytes");
            manifest.write_all(&result.max_seqno.to_le_bytes())
                .expect("Manifest Writer: failed to write max_seqno bytes");
            manifest.write_all(&result.min_seqno.to_le_bytes())
                .expect("Manifest Writer: failed to write min_seqno bytes");
            manifest.sync_all()
                .expect("Manifest Writer: failed to fsync while writting");
        }
    });
}
