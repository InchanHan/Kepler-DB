use std::{fs::{File, OpenOptions}, io::Write, path::{Path, PathBuf}, sync::{Arc, mpsc::{self, SyncSender, sync_channel}}, thread};
use bytes::Bytes;

use crate::{db::Value, memtable::MemTable};

pub struct FlushConfig {
    memtable: Arc<MemTable>,
    sstno: u64,
}

impl FlushConfig {
    pub fn new(mem: MemTable, sstno: u64) -> Self {
        Self { memtable: Arc::new(mem), sstno, }
    }
}

pub struct FlushWorker {
    pub sender: mpsc::SyncSender<FlushConfig>,
    sst_path: PathBuf,
}

impl FlushWorker {
    pub(crate) fn new(path: &Path) -> Self {
        let (sender, rx) = mpsc::sync_channel::<FlushConfig>(4);
        let sst_path = path.join("sst");
        let _ = thread::spawn(move || {
            while let Ok(mem) = rx.recv() {
            }
        });
        Self { sender, sst_path }
    }

    pub(crate) fn send(&self, cfg: FlushConfig) {
        let _ = self.sender.send(cfg).unwrap();
    }

    pub(crate) fn flush_one(&self, cfg: FlushConfig) {
        let sst_file_path = self.sst_path.join(format!("sst-{:06}.log", cfg.sstno));
        
        let mut sst = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&sst_file_path)
            .unwrap();

        let tree = &cfg.memtable.tree;    
        for (key, (seqno, val)) in tree.iter() {
            let (flag, val): (u8, &[u8]) = match val {
                Value::Data(b) => (0, b.as_ref()),
                Value::Tombstone => (1, &[]),
            };

            let key_len = key.len() as u32;
            let val_len = val.len() as u32;
            sst.write_all(&seqno.to_le_bytes());
            sst.write_all(&[flag]);
            sst.write_all(&key_len.to_le_bytes());
            sst.write_all(&val_len.to_le_bytes());
            sst.write_all(key);
            sst.write_all(val);
            sst.sync_all().unwrap();
        }


    }
}
