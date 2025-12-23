use bytes::Bytes;

pub enum Value {
    Data(Bytes),
    Tombstone,
}

