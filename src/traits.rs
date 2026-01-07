use bytes::Bytes;

use crate::error::KeplerResult;

pub trait Getable {
    fn get(&self, key: &[u8]) -> KeplerResult<Option<Bytes>>;
}

pub trait Putable {
    fn put(&self, seqno: u64, key: &[u8], val: Option<&[u8]>) -> KeplerResult<()>;
}
