use std::io;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum KeplerErr {
    #[error("Index out of Bounds!")]
    IndexOutOfBounds,

    #[error("lock poisoned")]
    LockPoisoned,

    #[error("I/O error")]
    Io(#[from] io::Error),

    #[error("WAL write failed")]
    WalWrite {
        #[source]
        source: io::Error,
    },

    #[error("Corrupted Manifest format at byte offset {0}")]
    ManifestCorrupted(usize),

    #[error("Corrupted SST format at byte offset {0}")]
    CorruptedSst(usize),
}

pub type KeplerResult<T> = Result<T, KeplerErr>;
