use std::{
    fs::OpenOptions,
    io::{BufWriter, Write},
    path::Path,
    sync::{
        Arc,
        mpsc::{Receiver, Sender, SyncSender, sync_channel},
    },
    thread,
};

use memmap2::Mmap;

use crate::{
    bloom::BloomFilter,
    constants::{BUF_SIZE, LEN_SIZE, MAGIC, OFFSET_SIZE, PAGE_4KB},
    error::{KeplerErr, KeplerResult},
    imm_tables::ImmTables,
    manifest::Manifest,
    sst_manager::SSTManager,
    sstable::{SSTable, SparseIndex},
    types::{TableMap, Value, WorkerSignal},
};

pub struct FlushResult {
    pub t: u8,
    pub sstno: u64,
    pub max_seqno: u64,
    pub min_seqno: u64,
}

impl FlushResult {
    fn new(t: u8, sstno: u64, max_seqno: u64, min_seqno: u64) -> Self {
        Self {
            t,
            sstno,
            max_seqno,
            min_seqno,
        }
    }
}

pub(crate) struct SSTWriter {
    sender: SyncSender<WorkerSignal>,
}

impl SSTWriter {
    pub(crate) fn new(
        path: &Path,
        manifest: Arc<Manifest>,
        imm_tables: Arc<ImmTables>,
        sst_manager: Arc<SSTManager>,
        err_tx: Sender<WorkerSignal>,
    ) -> KeplerResult<Self> {
        let (flush_tx, flush_rx) = sync_channel::<WorkerSignal>(4);

        start_sst_writer_thread(path, manifest, imm_tables, sst_manager, flush_rx, err_tx)?;

        Ok(Self { sender: flush_tx })
    }

    pub(crate) fn send(&self, signal: WorkerSignal) -> KeplerResult<()> {
        self.sender
            .send(signal)
            .map_err(|_| KeplerErr::ManifestCorrupted(0))?;
        Ok(())
    }
}

fn start_sst_writer_thread(
    path: &Path,
    manifest: Arc<Manifest>,
    imm_tables: Arc<ImmTables>,
    sst_manager: Arc<SSTManager>,
    flush_rx: Receiver<WorkerSignal>,
    err_tx: Sender<WorkerSignal>,
) -> KeplerResult<()> {
    let sst_dir_path = path.join("sst");

    thread::spawn(move || {
        let process = || -> KeplerResult<()> {
            while let Ok(WorkerSignal::Flush(table_map)) = flush_rx.recv() {
                let sstno = sst_manager.get_id();

                let (sstable, result) = flush_one(&sst_dir_path, sstno, table_map)?;
                sst_manager.push(sstable)?;
                manifest.send(result)?;
                imm_tables.pop_front()?;
            }
            Ok(())
        };

        if let Err(_) = process() {
            let _ = err_tx.send(WorkerSignal::Panic(KeplerErr::CorruptedSst(0)));
        }
    });

    Ok(())
}

/// SST Format
///
/// Data Block
///     - Values(?)
///
/// Sparse Index
///     - index_count(4) + key_len(4) + key(key_len) + key_block_offset(8)
///         + block_len(8)
///
/// Key Block
///     - key_len(4) + key(key_len) + val_block_offset(8)
///
/// Bloom filter
///     - filter_len(4) + bit_size(4) + BloomFilter(filter_len)
///
/// Footer
///     - sparse_idx_offset(8) + bloom_filter_offset(8) + max_seqno(8)
///         + min_seqno(8) + sstno(8) + magic_number(8)
fn flush_one(
    sst_path: &Path,
    sstno: u64,
    table_map: Arc<TableMap>,
) -> KeplerResult<(SSTable, FlushResult)> {
    let sst_path = sst_path.join(format!("sst-{:06}.log", sstno));
    let sst = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&sst_path)?;

    let mut buf = BufWriter::new(&sst);
    let mut buf_2: Vec<u8> = Vec::with_capacity(BUF_SIZE);

    let mut filter = BloomFilter::new(table_map.len());
    let mut sparse_index: Vec<SparseIndex> = Vec::new();
    let mut index_set: Vec<(&[u8], usize)> = Vec::new();

    let mut sparse_key = None;
    let mut key_block_idx = LEN_SIZE;
    let mut block_len = 0;
    let mut val_offset = 0;
    let (mut max_seqno, mut min_seqno) = (0, u64::MAX);

    for (key, (seqno, val)) in table_map.iter() {
        max_seqno = max_seqno.max(*seqno);
        min_seqno = min_seqno.min(*seqno);

        let val: &[u8] = match val {
            Value::Data(b) => b.as_ref(),
            Value::Tombstone => &[],
        };

        if sparse_key.is_none() {
            sparse_key = Some(key);
        }

        let key_len = key.len();
        let val_len = val.len();

        buf.write_all(val)?;
        buf_2.write_all(&(key_len as u32).to_le_bytes())?;
        buf_2.write_all(key)?;
        buf_2.write_all(&(val_offset as u64).to_le_bytes())?;
        filter.add(key);

        val_offset += val_len;
        block_len += LEN_SIZE + key_len + OFFSET_SIZE;

        if block_len + LEN_SIZE + OFFSET_SIZE >= PAGE_4KB {
            if let Some(s_key) = sparse_key {
                key_block_idx += LEN_SIZE + s_key.len() + OFFSET_SIZE;
                index_set.push((s_key, block_len));
                block_len = 0;
            }
            sparse_key = None;
        }
    }

    key_block_idx += val_offset;
    buf.write_all(&(index_set.len() as u32).to_le_bytes())?;

    for idx in index_set {
        buf.write_all(&(idx.0.len() as u32).to_le_bytes())?;
        buf.write_all(idx.0)?;
        buf.write_all(&(key_block_idx as u64).to_le_bytes())?;
        buf.write_all(&(idx.1 as u64).to_le_bytes())?;

        let new_idx = SparseIndex::new(idx.0, key_block_idx, idx.1);
        sparse_index.push(new_idx);
        key_block_idx += idx.1;
    }

    buf.write_all(&buf_2)?;
    buf.write_all(&(filter.len() as u32).to_le_bytes())?;
    buf.write_all(&(filter.bit_size() as u32).to_le_bytes())?;
    buf.write_all(filter.as_slice())?;

    buf.write_all(&(val_offset as u64).to_le_bytes())?;
    buf.write_all(&(key_block_idx as u64).to_le_bytes())?;
    buf.write_all(&max_seqno.to_le_bytes())?;
    buf.write_all(&min_seqno.to_le_bytes())?;
    buf.write_all(&sstno.to_le_bytes())?;
    buf.write_all(&MAGIC.to_le_bytes())?;

    buf.flush()?;
    buf.get_mut().sync_all()?;

    let mmap = unsafe { Mmap::map(&sst)? };
    let sstable = SSTable::new(sstno, mmap, sparse_index, filter);
    let result = FlushResult::new(0, sstno, max_seqno, min_seqno);

    Ok((sstable, result))
}
