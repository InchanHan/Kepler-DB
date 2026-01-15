use crate::{
    bloom::BloomFilter,
    constants::{LEN_SIZE, OFFSET_SIZE},
    traits::Getable,
    utils::{from_le_to_u32, from_le_to_u64},
};
use bytes::Bytes;
use memmap2::Mmap;

pub(crate) struct SparseIndex {
    first_key: Bytes,
    offset: usize,
    len: usize,
}

impl SparseIndex {
    pub(crate) fn new(key: &[u8], offset: usize, len: usize) -> Self {
        Self {
            first_key: Bytes::copy_from_slice(key),
            offset,
            len,
        }
    }
}

impl Getable for SSTable {
    fn get(&self, key: &[u8]) -> crate::Result<Option<Bytes>> {
        let i = self.index.partition_point(|x| x.first_key <= key);
        if i == 0 {
            return Ok(None);
        }

        let target = &self.index[i - 1];
        match self.search(key, target.offset, target.len)? {
            Some(b) => Ok(Some(b)),
            None => Ok(None),
        }
    }
}

pub(crate) struct SSTable {
    #[allow(dead_code)]
    pub(crate) id: u64,
    mmap: Mmap,
    index: Vec<SparseIndex>,
    bloomfilter: BloomFilter,
}

impl SSTable {
    pub(crate) fn new(
        id: u64,
        mmap: Mmap,
        index: Vec<SparseIndex>,
        bloomfilter: BloomFilter,
    ) -> Self {
        Self {
            id,
            mmap,
            index,
            bloomfilter,
        }
    }

    pub(crate) fn contains(&self, key: &[u8]) -> bool {
        self.bloomfilter.contains(key)
    }

    fn search(
        &self,
        key: &[u8],
        target_offset: usize,
        block_len: usize,
    ) -> crate::Result<Option<Bytes>> {
        let mut idx = target_offset;
        let mmap = &self.mmap;
        let end_bound = target_offset + block_len;

        while idx + LEN_SIZE + OFFSET_SIZE <= end_bound {
            let key_len = from_le_to_u32(mmap, idx, 0, LEN_SIZE)? as usize;
            let key_start = idx + LEN_SIZE;
            let key_end = key_start + key_len;
            let found_key = &mmap[key_start..key_end];

            if found_key == key {
                let val_offset = from_le_to_u64(
                    mmap,
                    idx,
                    LEN_SIZE + key_len,
                    LEN_SIZE + key_len + OFFSET_SIZE,
                )? as usize;
                let val_len = from_le_to_u32(mmap, val_offset, 0, LEN_SIZE)? as usize;
                let val = &mmap[LEN_SIZE + val_offset..LEN_SIZE + val_offset + val_len];

                return Ok(Some(Bytes::copy_from_slice(val)));
            }

            if found_key > key {
                break;
            }

            idx += LEN_SIZE + key_len + OFFSET_SIZE;
        }

        Ok(None)
    }
}
