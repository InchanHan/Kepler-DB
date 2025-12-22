use crate::db::Value;
use bytes::Bytes;
use std::{
    fmt,
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct FileId(pub u64);

pub struct WalWriter {
    wal: File,
    id: FileId,
    path: PathBuf,
    bytes_written: u64,
}

const WAL_SEGEMNETS_MAX: u64 = 64 * 1024 * 1024;

impl WalWriter {
    pub(crate) fn new(root: &Path) -> io::Result<Self> {
        let wal_dir = root.join("wal");
        fs::create_dir_all(&wal_dir)?;

        let (id, path) = find_latest_file(&wal_dir)?.unwrap_or_else(|| {
            let id = FileId(1);
            let path = wal_dir.join(format!("wal-{:06}.log", id.0));
            (id, path)
        });

        let wal = OpenOptions::new().create(true).append(true).open(&path)?;

        Ok(Self {
            wal,
            id,
            path,
            bytes_written: 0,
        })
    }

    pub(crate) fn put(&mut self, seqno: u64, key: &[u8], val: Option<&[u8]>) -> io::Result<()> {
        let type_num: u8 = if val.is_some() { 0 } else { 1 };
        let key_len = key.len() as u32;
        let val_len = val.map(|v| v.len() as u32).unwrap_or(0);

        self.wal.write_all(&seqno.to_le_bytes())?;
        self.wal.write_all(&[type_num])?;
        self.wal.write_all(&key_len.to_le_bytes())?;
        self.wal.write_all(&val_len.to_le_bytes())?;
        self.wal.write_all(key)?;
        if let Some(v) = val {
            self.wal.write_all(v)?;
        }
        self.wal.sync_all()?;
        self.bytes_written += 8 + 1 + 4 + 4 + key_len as u64 + val_len as u64;

        if self.bytes_written >= WAL_SEGEMNETS_MAX {
            self.rotate()?;
        }

        Ok(())
    }

    pub fn rotate(&mut self) -> io::Result<()> {
        self.wal.sync_all()?;

        let mut path = self.path.clone();
        path.pop();

        let next_id = FileId(self.id.0 + 1);
        let next_path = path.join(format!("wal-{:06}.log", self.id.0));

        let next_wal = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;

        self.id = next_id;
        self.path = next_path;
        self.wal = next_wal;
        self.bytes_written = 0;

        Ok(())
    }
}

pub fn find_latest_file(wal_dir: &Path) -> io::Result<Option<(FileId, PathBuf)>> {
    let mut file_set: Vec<(FileId, PathBuf)> = Vec::new();

    if !wal_dir.exists() {
        return Ok(None);
    }

    for read in fs::read_dir(wal_dir)? {
        let entry = read?;
        if !entry.file_type()?.is_file() {
            continue;
        }

        let file_path = entry.path();
        let Some(name) = file_path.file_name().and_then(|s| s.to_str()) else {
            continue;
        };

        if let Some(id) = parse_file_name(name) {
            file_set.push((id, file_path));
        }
    }

    file_set.sort_by_key(|(id, _)| *id);
    Ok(file_set.pop())
}

fn parse_file_name(name: &str) -> Option<FileId> {
    let prefix = "wal-";
    let suffix = ".log";

    if !name.starts_with(prefix) || !name.ends_with(suffix) {
        return None;
    }

    let num_str = &name[prefix.len()..name.len() - suffix.len()];
    let n: u64 = num_str.parse().ok()?;
    Some(FileId(n))
}
