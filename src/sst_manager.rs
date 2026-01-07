use bytes::Bytes;
use memmap2::Mmap;
use std::{
    fs::{self, File},
    io::{Read, Seek, SeekFrom},
    path::Path,
    sync::{
        Arc, RwLock,
        atomic::{AtomicU64, Ordering},
    },
};

use crate::{
    bloom::BloomFilter,
    constants::{LEN_SIZE, MAGIC, OFFSET_SIZE},
    error::{KeplerErr, KeplerResult},
    sstable::{SSTable, SparseIndex},
    traits::Getable,
    utils::{ensure_dir, from_le_to_u32, from_le_to_u64},
};

impl Getable for SSTManager {
    fn get(&self, key: &[u8]) -> KeplerResult<Option<Bytes>> {
        let tables = &self.tables.read().map_err(|_| KeplerErr::CorruptedSst(0))?;

        for table in tables.iter().rev() {
            if table.contains(key) {
                if let Ok(Some(v)) = table.get(key) {
                    return Ok(Some(v));
                }
            }
        }
        Ok(None)
    }
}

pub struct SSTManager {
    tables: RwLock<Vec<Arc<SSTable>>>,
    id: AtomicU64,
}

impl SSTManager {
    pub(crate) fn open(path: &Path, next_sstno: u64) -> KeplerResult<Self> {
        let tables = recovery_sst(path)?;

        Ok(Self {
            tables: RwLock::new(tables),
            id: AtomicU64::new(next_sstno),
        })
    }

    pub(crate) fn get_id(&self) -> u64 {
        self.id.fetch_add(1, Ordering::Relaxed)
    }

    pub(crate) fn push(&self, table: SSTable) -> KeplerResult<()> {
        self.tables
            .write()
            .map_err(|_| KeplerErr::CorruptedSst(0))?
            .push(Arc::new(table));

        Ok(())
    }
}

fn recovery_sst(path: &Path) -> KeplerResult<Vec<Arc<SSTable>>> {
    let mut tables: Vec<Arc<SSTable>> = Vec::new();
    let sst_dir_path = path.join("sst");
    ensure_dir(&sst_dir_path)?;

    let mut entries: Vec<_> = fs::read_dir(&sst_dir_path)?
        .filter_map(|read| read.ok())
        .collect();
    entries.sort_by_key(|e| e.path());

    for entry in entries {
        let file_path = entry.path();
        let mut file = File::open(file_path)?;

        let mut footer = [0u8; 48];
        file.seek(SeekFrom::End(-48))?;
        file.read_exact(&mut footer)?;

        if u64::from_le_bytes(footer[40..48].try_into().unwrap()) != MAGIC {
            return Err(KeplerErr::CorruptedSst(0));
        }

        // Footer
        //      -sparse_idx_offset(8) + bloom_filter_offset(8)
        //          + max_seqno(8) + min_seqno(8) + sstno(8)
        //          + magic_number(8)
        //
        let mmap = unsafe { Mmap::map(&file)? };
        let sparse_offset = u64::from_le_bytes(footer[0..8].try_into().unwrap()) as usize;
        let bloom_offset = u64::from_le_bytes(footer[8..16].try_into().unwrap()) as usize;
        let sstno = u64::from_le_bytes(footer[32..40].try_into().unwrap());

        let index = sparse_idx_from_offset(sparse_offset, &mmap)?;
        let bloomfilter = bloom_filter_from_offset(bloom_offset, &mmap)?;

        tables.push(Arc::new(SSTable::new(sstno, mmap, index, bloomfilter)));
    }
    Ok(tables)
}

fn sparse_idx_from_offset(offset: usize, mmap: &Mmap) -> KeplerResult<Vec<SparseIndex>> {
    let mut sparse_index: Vec<SparseIndex> = Vec::new();
    let mut idx_count = from_le_to_u32(mmap, offset, 0, LEN_SIZE)?;
    let mut idx = offset + LEN_SIZE;

    while idx_count > 0 {
        let key_len = from_le_to_u32(mmap, idx, 0, LEN_SIZE)? as usize;
        let key_start = idx + LEN_SIZE;
        let key_end = key_start + key_len;

        let key = &mmap[key_start..key_end];
        let key_block_offset = from_le_to_u64(mmap, key_end, 0, OFFSET_SIZE)? as usize;
        let block_len = from_le_to_u64(mmap, key_end + OFFSET_SIZE, 0, OFFSET_SIZE)? as usize;

        let new_idx = SparseIndex::new(key, key_block_offset, block_len);
        sparse_index.push(new_idx);
        idx += LEN_SIZE + key_len + OFFSET_SIZE + OFFSET_SIZE;
        idx_count -= 1;
    }
    Ok(sparse_index)
}

fn bloom_filter_from_offset(offset: usize, mmap: &Mmap) -> KeplerResult<BloomFilter> {
    let filter_len = from_le_to_u32(mmap, offset, 0, LEN_SIZE)? as usize;
    let bit_size = from_le_to_u32(mmap, offset + LEN_SIZE, 0, LEN_SIZE)? as usize;
    let filter_start = offset + LEN_SIZE + LEN_SIZE;
    let filter_end = filter_start + filter_len;
    let bloom_filter: Vec<u8> = mmap[filter_start..filter_end].to_vec();
    Ok(BloomFilter::options(bloom_filter, bit_size))
}
