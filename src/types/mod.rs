use bytes::Bytes;

#[derive(Clone, Debug, PartialEq)]
pub enum ValueKind {
    Put(Bytes),
    Delete,
}
