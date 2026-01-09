use std::io;
use thiserror::Error;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    #[error("I/O error")]
    Io(#[from] io::Error),

    #[error("Concurrency failure")]
    Concurrency,

    #[error("Wal or Manifest corruption")]
    Corrupted,

    #[error("Previous write failed; engine poisoned")]
    Poisoned,

    #[error("engine is unrecoverable")]
    Unrecoverable,
}

pub type Result<T> = std::result::Result<T, Error>;
