use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

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
    wal_writer: Mutex<Option<WalWriter>>,
    approximate_size: AtomicUsize,
}

impl Memtable {
    pub fn new(wal_writer: Option<WalWriter>) -> Self {
        Self {
            map: SkipMap::new(),
            wal_writer: Mutex::new(wal_writer),
            approximate_size: AtomicUsize::new(0),
        }
    }

    pub fn has_wal_writer(&self) -> bool {
        self.wal_writer.lock().unwrap().is_some()
    }

    pub fn take_wal_writer(&self) -> Option<WalWriter> {
        self.wal_writer.lock().unwrap().take()
    }

    pub fn approximate_size(&self) -> usize {
        self.approximate_size.load(Ordering::Relaxed)
    }

    pub fn put(&self, key: Bytes, value: Bytes) {
        if let Some(ref wal) = *self.wal_writer.lock().unwrap() {
            wal.write_put(&key, &value);
        }
        self.approximate_size
            .fetch_add(key.len() + value.len(), Ordering::Relaxed);
        self.map.insert(key, ValueKind::Put(value));
    }

    pub fn delete(&self, key: Bytes) {
        if let Some(ref wal) = *self.wal_writer.lock().unwrap() {
            wal.write_delete(&key);
        }
        self.approximate_size
            .fetch_add(key.len(), Ordering::Relaxed);
        self.map.insert(key, ValueKind::Delete);
    }

    pub fn get(&self, key: &Bytes) -> Option<ValueKind> {
        self.map.get(key).map(|entry| entry.value().clone())
    }

    pub fn iter(&self) -> impl Iterator<Item = (Bytes, ValueKind)> + '_ {
        self.map
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
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
    fn approximate_size_starts_at_zero() {
        let mt = Memtable::new(None);
        assert_eq!(mt.approximate_size(), 0);
    }

    #[test]
    fn put_increases_approximate_size_by_key_plus_value_len() {
        let mt = Memtable::new(None);
        let key = Bytes::from("abc");    // 3 bytes
        let value = Bytes::from("defgh"); // 5 bytes
        mt.put(key, value);
        assert_eq!(mt.approximate_size(), 8);
    }

    #[test]
    fn delete_increases_approximate_size_by_key_len_only() {
        let mt = Memtable::new(None);
        let key = Bytes::from("abcde"); // 5 bytes
        mt.delete(key);
        assert_eq!(mt.approximate_size(), 5);
    }

    #[test]
    fn approximate_size_grows_monotonically() {
        let mt = Memtable::new(None);
        mt.put(Bytes::from("k1"), Bytes::from("v1"));
        let size_after_first = mt.approximate_size();
        assert_eq!(size_after_first, 4); // 2 + 2

        mt.put(Bytes::from("k2"), Bytes::from("val2"));
        let size_after_second = mt.approximate_size();
        assert_eq!(size_after_second, 10); // 4 + (2 + 4)
        assert!(size_after_second > size_after_first);

        mt.delete(Bytes::from("k3"));
        let size_after_delete = mt.approximate_size();
        assert_eq!(size_after_delete, 12); // 10 + 2
        assert!(size_after_delete > size_after_second);

        // Overwrite k1 — size still grows, never decreases
        mt.put(Bytes::from("k1"), Bytes::from("new"));
        let size_after_overwrite = mt.approximate_size();
        assert_eq!(size_after_overwrite, 17); // 12 + (2 + 3)
        assert!(size_after_overwrite > size_after_delete);
    }

    #[test]
    fn iter_yields_entries_in_lexicographic_key_order() {
        let mt = Memtable::new(None);
        mt.put(Bytes::from("cherry"), Bytes::from("3"));
        mt.put(Bytes::from("apple"), Bytes::from("1"));
        mt.put(Bytes::from("banana"), Bytes::from("2"));
        mt.delete(Bytes::from("date"));

        let entries: Vec<(Bytes, ValueKind)> = mt.iter().collect();
        assert_eq!(entries.len(), 4);
        assert_eq!(entries[0], (Bytes::from("apple"), ValueKind::Put(Bytes::from("1"))));
        assert_eq!(entries[1], (Bytes::from("banana"), ValueKind::Put(Bytes::from("2"))));
        assert_eq!(entries[2], (Bytes::from("cherry"), ValueKind::Put(Bytes::from("3"))));
        assert_eq!(entries[3], (Bytes::from("date"), ValueKind::Delete));
    }

    #[test]
    fn iter_on_empty_memtable_yields_nothing() {
        let mt = Memtable::new(None);
        let entries: Vec<(Bytes, ValueKind)> = mt.iter().collect();
        assert!(entries.is_empty());
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
