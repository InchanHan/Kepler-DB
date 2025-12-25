use std::{
    fs::OpenOptions,
    io::Write,
    path::{Path, PathBuf},
    sync::{
        Arc,
        mpsc,
    },
    thread,
};
use crate::{
    value::Value,
    memtable::MemTable,
    error::KeplerResult,
};

pub struct FlushConfig {
    memtable: Arc<MemTable>,
    sstno: u64,
}

pub struct FlushResult {
    pub type_num: u8,
    pub sstno: u64,
    pub max_seqno: u64,
    pub min_seqno: u64,
}

impl FlushResult {
    pub fn new(type_num: u8, sstno: u64, max_seqno: u64, min_seqno: u64) -> Self {
        Self {
            type_num,
            sstno,
            max_seqno,
            min_seqno,
        }
    }
}

impl FlushConfig {
    pub fn new(mem: MemTable, sstno: u64) -> Self {
        Self {
            memtable: Arc::new(mem),
            sstno,
        }
    }
}

pub struct FlushWorker {
    pub sender: mpsc::SyncSender<FlushConfig>,
}

impl FlushWorker {
    pub fn new(path: PathBuf) -> (Self, mpsc::Receiver<FlushResult>) {
        let (sender, rx) = mpsc::sync_channel::<FlushConfig>(4);
        let (result_tx, result_rx) = mpsc::sync_channel::<FlushResult>(4);
        let _ = thread::spawn(move || {
            while let Ok(cfg) = rx.recv() {
                match flush_one(&path, cfg) {
                    Ok(result) => { let _ = result_tx.send(result); },
                    Err(_) => panic!("Flush Worker: failed to flush data in memory, which is fatal!"),
                }
            }
        });
        (Self { sender }, result_rx)
    }

    pub fn send(&self, cfg: FlushConfig) {
        let _ = self.sender.send(cfg);
    }
}

pub fn flush_one(path: &Path, cfg: FlushConfig) -> KeplerResult<FlushResult> {
    let sst_file_path = path.join("sst").join(format!("sst-{:06}.log", cfg.sstno));

    let mut sst = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&sst_file_path)?;

    let tree = &cfg.memtable.tree;
    let mut max_seqno: u64 = 0;
    let mut min_seqno: u64 = 0;
    for (key, (seqno, val)) in tree.iter() {
        if max_seqno < *seqno {
            max_seqno = *seqno;
        } else if *seqno < min_seqno {
            min_seqno = *seqno;
        }

        let (flag, val): (u8, &[u8]) = match val {
            Value::Data(b) => (0, b.as_ref()),
            Value::Tombstone => (1, &[]),
        };

        // seqno(8) + flag(1) + key_len(4) + val_len(4) + key(?) + val(?)
        let key_len = key.len() as u32;
        let val_len = val.len() as u32;
        sst.write_all(&seqno.to_le_bytes())?;
        sst.write_all(&[flag])?;
        sst.write_all(&key_len.to_le_bytes())?;
        sst.write_all(&val_len.to_le_bytes())?;
        sst.write_all(key)?;
        sst.write_all(val)?;
        sst.sync_all()?;
    }

    Ok(FlushResult::new(0, cfg.sstno, max_seqno, min_seqno))
}
