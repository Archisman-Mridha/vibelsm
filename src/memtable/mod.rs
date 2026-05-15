use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use crate::types::ValueKind;

pub struct Memtable {
    map: SkipMap<Bytes, ValueKind>,
    approximate_size: AtomicUsize,
}

impl Memtable {
    pub fn new() -> Self {
        Memtable {
            map: SkipMap::new(),
            approximate_size: AtomicUsize::new(0),
        }
    }

    pub fn put(&self, key: Bytes, value: Bytes) {
        let size = key.len() + value.len();
        self.map.insert(key, ValueKind::Put(value));
        self.approximate_size.fetch_add(size, Ordering::Relaxed);
    }

    pub fn delete(&self, key: Bytes) {
        let size = key.len();
        self.map.insert(key, ValueKind::Delete);
        self.approximate_size.fetch_add(size, Ordering::Relaxed);
    }

    pub fn get(&self, key: &Bytes) -> Option<ValueKind> {
        self.map.get(key).map(|entry| entry.value().clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use crate::types::ValueKind;

    #[test]
    fn put_then_delete_then_get_returns_tombstone() {
        let memtable = Memtable::new();
        let key = Bytes::from("key");

        memtable.put(key.clone(), Bytes::from("value"));
        memtable.delete(key.clone());

        assert_eq!(memtable.get(&key), Some(ValueKind::Delete));
    }

    #[test]
    fn get_on_unseen_key_returns_none() {
        let memtable = Memtable::new();
        assert_eq!(memtable.get(&Bytes::from("ghost")), None);
    }

    #[test]
    fn delete_then_get_returns_tombstone() {
        let memtable = Memtable::new();
        let key = Bytes::from("key");

        memtable.delete(key.clone());

        assert_eq!(memtable.get(&key), Some(ValueKind::Delete));
    }

    #[test]
    fn put_then_get_returns_value() {
        let memtable = Memtable::new();
        let key = Bytes::from("key");
        let value = Bytes::from("value");

        memtable.put(key.clone(), value.clone());

        assert_eq!(memtable.get(&key), Some(ValueKind::Put(value)));
    }
}
