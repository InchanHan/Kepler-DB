use crate::{
    Error,
    constants::{WAL_CAP_LIMIT, WAL_HEADER_SIZE},
    mem_table::MemTable,
    traits::Putable,
    utils::ensure_dir,
};
use std::{
    fs::{self, File, OpenOptions},
    io::{self, BufRead, BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
};

pub(crate) struct FileId(u64);

pub(crate) struct Journal {
    id: FileId,
    wal: BufWriter<File>,
    wal_dir_path: PathBuf,
    bytes_written: usize,
    sync_count: usize,
}

impl Journal {
    pub(crate) fn open(path: &Path, seqno: u64) -> crate::Result<(Self, MemTable, u64)> {
        let wal_dir_path = path.join("wal");
        ensure_dir(&wal_dir_path).map_err(|e| Error::Io(e))?;
        let (mem, next_seqno, latest_id) = recovery_wal(&wal_dir_path, seqno)?;
        let next_id = latest_id.0 + 1;
        let wal = OpenOptions::new()
            .create(true)
            .append(true)
            .open(create_wal_path(&wal_dir_path, next_id))?;

        Ok((
            Self {
                id: FileId(next_id),
                wal: BufWriter::new(wal),
                wal_dir_path,
                bytes_written: 0,
                sync_count: 0,
            },
            mem,
            next_seqno,
        ))
    }

    pub(crate) fn insert(&mut self, seqno: u64, key: &[u8], val: Option<&[u8]>) -> io::Result<()> {
        let key_len = key.len() as u32;
        let (val_len, t) = match val {
            None => (0, 1),
            Some(v) => (v.len() as u32, 0),
        };

        self.wal.write_all(&seqno.to_le_bytes())?;
        self.wal.write_all(&[t])?;
        self.wal.write_all(&key_len.to_le_bytes())?;
        self.wal.write_all(&val_len.to_le_bytes())?;
        self.wal.write_all(key)?;

        if let Some(v) = val {
            self.wal.write_all(v)?;
        };

        self.wal.flush()?;

        // seqno(8) + type(1) + key_len(4) + val_len(4) + key(?) + val(?)
        let written = WAL_HEADER_SIZE + (key_len + val_len) as usize;
        self.bytes_written += written;
        self.sync_count += written;

        if self.bytes_written >= WAL_CAP_LIMIT {
            self.rotate()?;
        }

        Ok(())
    }

    fn rotate(&mut self) -> io::Result<()> {
        if self.sync_count >= WAL_CAP_LIMIT * 4 {
            self.fsync()?;
            self.sync_count = 0;
        }
        let id = self.id.0 + 1;
        let wal = OpenOptions::new()
            .create(true)
            .append(true)
            .open(create_wal_path(&self.wal_dir_path, id))?;

        self.id = FileId(id);
        self.wal = BufWriter::new(wal);
        self.bytes_written = 0;

        Ok(())
    }

    fn fsync(&mut self) -> io::Result<()> {
        self.wal.get_mut().sync_all()?;
        Ok(())
    }
}

fn create_wal_path(path: &Path, id: u64) -> PathBuf {
    path.join(format!("wal-{:06}.log", id))
}

fn recovery_wal(
    wal_dir_path: &Path,
    next_wal_seqno: u64,
) -> crate::Result<(MemTable, u64, FileId)> {
    let table = MemTable::new();
    let mut max_seqno = next_wal_seqno;
    let mut entries: Vec<_> = fs::read_dir(wal_dir_path)?
        .filter_map(|read| read.ok())
        .collect();
    entries.sort_by_key(|e| e.path());
    let latest_id = entries
        .last()
        .and_then(|e| parse_file_name(&e.path()))
        .unwrap_or(FileId(0));
    // NOTE:
    // WAL recovery must replay files in ascending order.
    // Reverse replay breaks record-level ordering guarantees.
    for entry in entries.iter() {
        let file_path = entry.path();
        let file = File::open(file_path)?;
        let mut reader = BufReader::with_capacity(64 * 1024, file);

        // seqno(8) + type(1) + key_len(4) + val_len(4)
        loop {
            let mut header = [0u8; WAL_HEADER_SIZE];

            if let Err(e) = reader.read_exact(&mut header) {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    break;
                }
                return Err(e.into());
            }

            let seqno = u64::from_le_bytes(header[0..8].try_into().unwrap());
            let t = header[8];
            let key_len = u32::from_le_bytes(header[9..13].try_into().unwrap()) as usize;
            let val_len = u32::from_le_bytes(header[13..17].try_into().unwrap()) as usize;

            if next_wal_seqno <= seqno {
                let mut key = vec![0u8; key_len];
                let mut val = vec![0u8; val_len];

                reader.read_exact(&mut key)?;
                let val = match t {
                    0 => {
                        reader.read_exact(&mut val)?;
                        Some(val.as_slice())
                    }
                    1 => None,
                    _ => unreachable!(),
                };

                table.put(seqno, &key, val)?;
                max_seqno = max_seqno.max(seqno);
            } else {
                reader.consume(key_len + val_len);
            }
        }
    }
    Ok((table, max_seqno + 1, latest_id))
}

fn parse_file_name(path: &Path) -> Option<FileId> {
    let name = path.file_name()?.to_string_lossy();

    if !name.starts_with("wal-") || !name.ends_with(".log") {
        return None;
    }

    let num_str = &name[4..name.len() - 4];
    num_str.parse().ok().map(FileId)
}

#[cfg(test)]
mod tests {
    use crate::traits::Getable;

    use bytes::Bytes;
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn wal_replay() -> crate::Result<()> {
        let dir = tempdir()?;

        {
            let (mut journal, _, _) = Journal::open(dir.path(), 0)?;
            journal.insert(1, b"a", Some(b"1"))?;
            journal.insert(2, b"b", Some(b"2"))?;
        }

        let (_, mem, _) = Journal::open(dir.path(), 0)?;
        assert_eq!(mem.get(b"a")?, Some(Bytes::from("1")));
        assert_eq!(mem.get(b"b")?, Some(Bytes::from("2")));
        Ok(())
    }

    #[test]
    fn wal_replay_multiple_files() -> crate::Result<()> {
        let dir = tempdir()?;
        let mut n = 0;
        let mut rotate_cnt = 0;

        {
            let (mut journal, _, _) = Journal::open(dir.path(), 0)?;
            let mut last_id = journal.id.0;

            while rotate_cnt < 2 {
                journal.insert(n, b"k", Some(&[n as u8]))?;
                n += 1;

                if journal.id.0 != last_id {
                    rotate_cnt += 1;
                    last_id = journal.id.0;
                }
            }
        }

        let (_, mem, _) = Journal::open(dir.path(), 0)?;
        assert_eq!(
            mem.get(b"k")?,
            Some(Bytes::copy_from_slice(&[(n - 1) as u8]))
        );

        Ok(())
    }
}
