use bytes::Bytes;

pub trait Getable {
    fn get(&self, key: &[u8]) -> crate::Result<Option<Bytes>>;
}

pub trait Putable {
    fn put(&self, seqno: u64, key: &[u8], val: Option<&[u8]>) -> crate::Result<()>;
}
