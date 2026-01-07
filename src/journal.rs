use crate::{
    constants::{WAL_CAP_LIMIT, WAL_HEADER_SIZE},
    error::{KeplerErr, KeplerResult},
    mem_table::MemTable,
    traits::Putable,
    utils::ensure_dir,
};
use std::{
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
};

pub struct FileId(u64);

pub struct Journal {
    id: FileId,
    wal: BufWriter<File>,
    wal_dir_path: PathBuf,
    bytes_written: usize,
}

impl Journal {
    pub(crate) fn open(path: &Path, seqno: u64) -> KeplerResult<(Self, MemTable, u64)> {
        let wal_dir_path = path.join("wal");
        ensure_dir(&wal_dir_path)?;
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
            },
            mem,
            next_seqno,
        ))
    }

    pub(crate) fn insert(
        &mut self,
        seqno: u64,
        key: &[u8],
        val: Option<&[u8]>,
    ) -> KeplerResult<()> {
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

        self.fsync()?;

        // seqno(8) + type(1) + key_len(4) + val_len(4) + key(?) + val(?)
        self.bytes_written += WAL_HEADER_SIZE + (key_len + val_len) as usize;

        if self.bytes_written >= WAL_CAP_LIMIT {
            self.rotate()?;
        }

        Ok(())
    }

    fn rotate(&mut self) -> KeplerResult<()> {
        self.fsync()?;

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

    fn fsync(&mut self) -> KeplerResult<()> {
        self.wal.flush()?;
        self.wal.get_mut().sync_all()?;
        Ok(())
    }
}

fn create_wal_path(path: &Path, id: u64) -> PathBuf {
    path.join(format!("wal-{:06}.log", id))
}

fn recovery_wal(wal_dir_path: &Path, next_wal_seqno: u64) -> KeplerResult<(MemTable, u64, FileId)> {
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

    for entry in entries.iter().rev() {
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
                return Err(KeplerErr::Io(e));
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
