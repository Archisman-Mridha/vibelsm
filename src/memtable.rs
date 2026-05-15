use bytes::Bytes;
use crossbeam_skiplist::SkipMap;

use crate::types::ValueKind;

pub struct WalWriter;

impl WalWriter {
    pub fn write_put(&self, _key: &Bytes, _value: &Bytes) {
        // Stub — WAL internals are out of scope.
    }

    pub fn write_delete(&self, _key: &Bytes) {
        // Stub — WAL internals are out of scope.
    }
}

pub struct Memtable {
    map: SkipMap<Bytes, ValueKind>,
    wal_writer: Option<WalWriter>,
}

impl Memtable {
    pub fn new(wal_writer: Option<WalWriter>) -> Self {
        Self {
            map: SkipMap::new(),
            wal_writer,
        }
    }

    pub fn put(&self, key: Bytes, value: Bytes) {
        if let Some(ref wal) = self.wal_writer {
            wal.write_put(&key, &value);
        }
        self.map.insert(key, ValueKind::Put(value));
    }

    pub fn delete(&self, key: Bytes) {
        if let Some(ref wal) = self.wal_writer {
            wal.write_delete(&key);
        }
        self.map.insert(key, ValueKind::Delete);
    }

    pub fn get(&self, key: &Bytes) -> Option<ValueKind> {
        self.map.get(key).map(|entry| entry.value().clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_then_get_returns_put_value() {
        let mt = Memtable::new(None);
        let key = Bytes::from("key1");
        let value = Bytes::from("value1");
        mt.put(key.clone(), value.clone());
        assert_eq!(mt.get(&key), Some(ValueKind::Put(value)));
    }

    #[test]
    fn delete_then_get_returns_delete() {
        let mt = Memtable::new(None);
        let key = Bytes::from("key1");
        mt.delete(key.clone());
        assert_eq!(mt.get(&key), Some(ValueKind::Delete));
    }

    #[test]
    fn get_on_missing_key_returns_none() {
        let mt = Memtable::new(None);
        assert_eq!(mt.get(&Bytes::from("nonexistent")), None);
    }

    #[test]
    fn put_then_delete_then_get_returns_delete() {
        let mt = Memtable::new(None);
        let key = Bytes::from("key1");
        mt.put(key.clone(), Bytes::from("value1"));
        mt.delete(key.clone());
        assert_eq!(mt.get(&key), Some(ValueKind::Delete));
    }

    #[test]
    fn put_overwrites_previous_value() {
        let mt = Memtable::new(None);
        let key = Bytes::from("key1");
        mt.put(key.clone(), Bytes::from("v1"));
        mt.put(key.clone(), Bytes::from("v2"));
        assert_eq!(mt.get(&key), Some(ValueKind::Put(Bytes::from("v2"))));
    }

    #[test]
    fn delete_then_put_restores_value() {
        let mt = Memtable::new(None);
        let key = Bytes::from("key1");
        mt.delete(key.clone());
        mt.put(key.clone(), Bytes::from("restored"));
        assert_eq!(mt.get(&key), Some(ValueKind::Put(Bytes::from("restored"))));
    }

    #[test]
    fn new_with_wal_writer_accepts_writes() {
        let mt = Memtable::new(Some(WalWriter));
        let key = Bytes::from("k");
        mt.put(key.clone(), Bytes::from("v"));
        assert_eq!(mt.get(&key), Some(ValueKind::Put(Bytes::from("v"))));
        mt.delete(key.clone());
        assert_eq!(mt.get(&key), Some(ValueKind::Delete));
    }

    #[test]
    fn multiple_keys_are_independent() {
        let mt = Memtable::new(None);
        mt.put(Bytes::from("a"), Bytes::from("1"));
        mt.put(Bytes::from("b"), Bytes::from("2"));
        mt.delete(Bytes::from("c"));
        assert_eq!(mt.get(&Bytes::from("a")), Some(ValueKind::Put(Bytes::from("1"))));
        assert_eq!(mt.get(&Bytes::from("b")), Some(ValueKind::Put(Bytes::from("2"))));
        assert_eq!(mt.get(&Bytes::from("c")), Some(ValueKind::Delete));
        assert_eq!(mt.get(&Bytes::from("d")), None);
    }
}
