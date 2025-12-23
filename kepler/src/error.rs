use std::io;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum KeplerErr {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    #[error("WAL error: {0}")]
    Wal(String),

    #[error("Manifest error: {0}")]
    Manifest(String),

    #[error("Corrupted SST format at byte offset {0}")]
    CorruptedSst(usize),

    #[error("Unknown error: {0}")]
    Unknown(String),
}

pub type KeplerResult<T> = Result<T, KeplerErr>;
