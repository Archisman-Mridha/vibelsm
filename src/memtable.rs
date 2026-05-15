use std::ops::RangeBounds;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use crossbeam_skiplist::SkipMap;

use crate::types::ValueKind;

/// Seam between the Memtable and the WAL implementation. The real WAL module will provide a
/// concrete adapter; tests inject a spy. Keeping this a trait prevents Memtable from being
/// coupled to the WAL's file-I/O internals before that module is built.
pub trait WalWrite: Send + Sync {
    fn write(&self, key: &Bytes, value: &ValueKind);
}

pub struct Memtable {
    // Lock-free ordered map; concurrent reads and writes need no external synchronization.
    map: SkipMap<Bytes, ValueKind>,
    // Wrapped in a Mutex so take_wal_writer() can mutate through an &self reference —
    // necessary because Memtable is always shared behind Arc during Freeze.
    wal_writer: Mutex<Option<Box<dyn WalWrite>>>,
    // Heuristic byte counter used to decide when to Freeze. Relaxed ordering is intentional:
    // this is not a synchronization point, and the size limit is approximate by design.
    approximate_size: AtomicUsize,
}

impl Memtable {
    pub fn new(wal_writer: Option<Box<dyn WalWrite>>) -> Self {
        Self {
            map: SkipMap::new(),
            wal_writer: Mutex::new(wal_writer),
            approximate_size: AtomicUsize::new(0),
        }
    }

    /// Transfers ownership of the WAL Writer out of this Memtable. Called on Freeze to strip
    /// the writer from the old Active Memtable before it becomes Immutable.
    pub fn take_wal_writer(&self) -> Option<Box<dyn WalWrite>> {
        self.wal_writer.lock().unwrap().take()
    }

    pub fn approximate_size(&self) -> usize {
        self.approximate_size.load(Ordering::Relaxed)
    }

    pub fn put(&self, key: Bytes, value: Bytes) {
        let value_kind = ValueKind::Put(value);
        // WAL write must happen before the SkipMap insert. If the process crashes between
        // the two, the WAL replay on recovery will re-insert the entry.
        if let Some(ref wal) = *self.wal_writer.lock().unwrap() {
            wal.write(&key, &value_kind);
        }
        let size = key.len()
            + match &value_kind {
                ValueKind::Put(v) => v.len(),
                ValueKind::Delete => 0,
            };
        self.approximate_size.fetch_add(size, Ordering::Relaxed);
        self.map.insert(key, value_kind);
    }

    pub fn delete(&self, key: Bytes) {
        // WAL write before SkipMap insert — same durability invariant as put().
        if let Some(ref wal) = *self.wal_writer.lock().unwrap() {
            wal.write(&key, &ValueKind::Delete);
        }
        // Tombstones carry no value bytes; only the key length counts toward Approximate Size.
        self.approximate_size
            .fetch_add(key.len(), Ordering::Relaxed);
        self.map.insert(key, ValueKind::Delete);
    }

    pub fn get(&self, key: &Bytes) -> Option<ValueKind> {
        self.map.get(key).map(|entry| entry.value().clone())
    }

    pub fn range<'a>(&'a self, range: impl RangeBounds<Bytes> + 'a) -> impl Iterator<Item = (Bytes, ValueKind)> + 'a {
        self.map
            .range(range)
            .map(|entry| (entry.key().clone(), entry.value().clone()))
    }

}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;

    // --- WalSpy ---

    struct WalSpy {
        writes: Mutex<Vec<(Bytes, ValueKind)>>,
    }

    impl WalSpy {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                writes: Mutex::new(vec![]),
            })
        }

        fn recorded(&self) -> Vec<(Bytes, ValueKind)> {
            self.writes.lock().unwrap().clone()
        }
    }

    // WalWrite is implemented on Arc<WalSpy> so the test can hold a clone of the Arc
    // to inspect recorded calls after passing the other clone into the Memtable.
    impl WalWrite for Arc<WalSpy> {
        fn write(&self, key: &Bytes, value: &ValueKind) {
            self.writes
                .lock()
                .unwrap()
                .push((key.clone(), value.clone()));
        }
    }

    // --- Memtable behaviour ---

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
        assert_eq!(
            mt.get(&key),
            Some(ValueKind::Put(Bytes::from("restored")))
        );
    }

    #[test]
    fn approximate_size_starts_at_zero() {
        let mt = Memtable::new(None);
        assert_eq!(mt.approximate_size(), 0);
    }

    #[test]
    fn put_increases_approximate_size_by_key_plus_value_len() {
        let mt = Memtable::new(None);
        mt.put(Bytes::from("abc"), Bytes::from("defgh")); // 3 + 5
        assert_eq!(mt.approximate_size(), 8);
    }

    #[test]
    fn delete_increases_approximate_size_by_key_len_only() {
        let mt = Memtable::new(None);
        mt.delete(Bytes::from("abcde")); // 5
        assert_eq!(mt.approximate_size(), 5);
    }

    #[test]
    fn approximate_size_grows_monotonically() {
        let mt = Memtable::new(None);
        mt.put(Bytes::from("k1"), Bytes::from("v1"));
        let s1 = mt.approximate_size();
        mt.put(Bytes::from("k2"), Bytes::from("val2"));
        let s2 = mt.approximate_size();
        mt.delete(Bytes::from("k3"));
        let s3 = mt.approximate_size();
        mt.put(Bytes::from("k1"), Bytes::from("new"));
        let s4 = mt.approximate_size();
        assert!(s1 < s2 && s2 < s3 && s3 < s4);
    }

    #[test]
    fn range_all_yields_entries_in_lexicographic_key_order() {
        let mt = Memtable::new(None);
        mt.put(Bytes::from("cherry"), Bytes::from("3"));
        mt.put(Bytes::from("apple"), Bytes::from("1"));
        mt.put(Bytes::from("banana"), Bytes::from("2"));
        mt.delete(Bytes::from("date"));

        let entries: Vec<(Bytes, ValueKind)> = mt.range(..).collect();
        assert_eq!(entries[0].0, Bytes::from("apple"));
        assert_eq!(entries[1].0, Bytes::from("banana"));
        assert_eq!(entries[2].0, Bytes::from("cherry"));
        assert_eq!(entries[3], (Bytes::from("date"), ValueKind::Delete));
    }

    #[test]
    fn range_all_on_empty_memtable_yields_nothing() {
        let mt = Memtable::new(None);
        assert!(mt.range(..).collect::<Vec<_>>().is_empty());
    }

    #[test]
    fn multiple_keys_are_independent() {
        let mt = Memtable::new(None);
        mt.put(Bytes::from("a"), Bytes::from("1"));
        mt.put(Bytes::from("b"), Bytes::from("2"));
        mt.delete(Bytes::from("c"));
        assert_eq!(
            mt.get(&Bytes::from("a")),
            Some(ValueKind::Put(Bytes::from("1")))
        );
        assert_eq!(
            mt.get(&Bytes::from("b")),
            Some(ValueKind::Put(Bytes::from("2")))
        );
        assert_eq!(mt.get(&Bytes::from("c")), Some(ValueKind::Delete));
        assert_eq!(mt.get(&Bytes::from("d")), None);
    }

    // --- WAL seam tests ---

    #[test]
    fn put_calls_wal_write_with_put_value_kind() {
        let spy = WalSpy::new();
        let mt = Memtable::new(Some(Box::new(Arc::clone(&spy))));

        mt.put(Bytes::from("k"), Bytes::from("v"));

        let recorded = spy.recorded();
        assert_eq!(recorded.len(), 1);
        assert_eq!(
            recorded[0],
            (Bytes::from("k"), ValueKind::Put(Bytes::from("v")))
        );
    }

    #[test]
    fn delete_calls_wal_write_with_delete_value_kind() {
        let spy = WalSpy::new();
        let mt = Memtable::new(Some(Box::new(Arc::clone(&spy))));

        mt.delete(Bytes::from("k"));

        let recorded = spy.recorded();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0], (Bytes::from("k"), ValueKind::Delete));
    }

    #[test]
    fn wal_write_is_called_for_every_operation() {
        let spy = WalSpy::new();
        let mt = Memtable::new(Some(Box::new(Arc::clone(&spy))));

        mt.put(Bytes::from("a"), Bytes::from("1"));
        mt.delete(Bytes::from("b"));
        mt.put(Bytes::from("c"), Bytes::from("3"));

        assert_eq!(spy.recorded().len(), 3);
    }

    #[test]
    fn no_wal_calls_when_writer_is_none() {
        let mt = Memtable::new(None);
        mt.put(Bytes::from("k"), Bytes::from("v"));
        mt.delete(Bytes::from("k2"));
        assert_eq!(
            mt.get(&Bytes::from("k")),
            Some(ValueKind::Put(Bytes::from("v")))
        );
    }

    // --- range() tests ---

    #[test]
    fn range_returns_only_entries_within_inclusive_bounds() {
        let mt = Memtable::new(None);
        mt.put(Bytes::from("a"), Bytes::from("1"));
        mt.put(Bytes::from("b"), Bytes::from("2"));
        mt.put(Bytes::from("c"), Bytes::from("3"));
        mt.put(Bytes::from("d"), Bytes::from("4"));

        let entries: Vec<(Bytes, ValueKind)> = mt.range(Bytes::from("b")..=Bytes::from("c")).collect();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0], (Bytes::from("b"), ValueKind::Put(Bytes::from("2"))));
        assert_eq!(entries[1], (Bytes::from("c"), ValueKind::Put(Bytes::from("3"))));
    }

    #[test]
    fn range_with_exclusive_lower_bound_excludes_boundary_key() {
        use std::ops::Bound;
        let mt = Memtable::new(None);
        mt.put(Bytes::from("a"), Bytes::from("1"));
        mt.put(Bytes::from("b"), Bytes::from("2"));
        mt.put(Bytes::from("c"), Bytes::from("3"));

        let entries: Vec<(Bytes, ValueKind)> =
            mt.range((Bound::Excluded(Bytes::from("a")), Bound::Included(Bytes::from("c")))).collect();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].0, Bytes::from("b"));
        assert_eq!(entries[1].0, Bytes::from("c"));
    }

    #[test]
    fn range_with_exclusive_upper_bound_excludes_boundary_key() {
        let mt = Memtable::new(None);
        mt.put(Bytes::from("a"), Bytes::from("1"));
        mt.put(Bytes::from("b"), Bytes::from("2"));
        mt.put(Bytes::from("c"), Bytes::from("3"));

        let entries: Vec<(Bytes, ValueKind)> = mt.range(Bytes::from("a")..Bytes::from("c")).collect();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].0, Bytes::from("a"));
        assert_eq!(entries[1].0, Bytes::from("b"));
    }

    #[test]
    fn range_with_unbounded_lower_returns_entries_up_to_upper() {
        let mt = Memtable::new(None);
        mt.put(Bytes::from("a"), Bytes::from("1"));
        mt.put(Bytes::from("b"), Bytes::from("2"));
        mt.put(Bytes::from("c"), Bytes::from("3"));

        let entries: Vec<(Bytes, ValueKind)> = mt.range(..=Bytes::from("b")).collect();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].0, Bytes::from("a"));
        assert_eq!(entries[1].0, Bytes::from("b"));
    }

    #[test]
    fn range_with_unbounded_upper_returns_entries_from_lower() {
        let mt = Memtable::new(None);
        mt.put(Bytes::from("a"), Bytes::from("1"));
        mt.put(Bytes::from("b"), Bytes::from("2"));
        mt.put(Bytes::from("c"), Bytes::from("3"));

        let entries: Vec<(Bytes, ValueKind)> = mt.range(Bytes::from("b")..).collect();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].0, Bytes::from("b"));
        assert_eq!(entries[1].0, Bytes::from("c"));
    }

    #[test]
    fn range_returns_entries_in_lexicographic_key_order() {
        let mt = Memtable::new(None);
        mt.put(Bytes::from("cherry"), Bytes::from("3"));
        mt.put(Bytes::from("apple"), Bytes::from("1"));
        mt.put(Bytes::from("banana"), Bytes::from("2"));
        mt.delete(Bytes::from("date"));

        let entries: Vec<(Bytes, ValueKind)> = mt.range(Bytes::from("apple")..=Bytes::from("date")).collect();
        assert_eq!(entries.len(), 4);
        assert_eq!(entries[0].0, Bytes::from("apple"));
        assert_eq!(entries[1].0, Bytes::from("banana"));
        assert_eq!(entries[2].0, Bytes::from("cherry"));
        assert_eq!(entries[3], (Bytes::from("date"), ValueKind::Delete));
    }

    #[test]
    fn range_on_empty_memtable_yields_nothing() {
        let mt = Memtable::new(None);
        assert!(mt.range(Bytes::from("a")..Bytes::from("z")).collect::<Vec<_>>().is_empty());
    }

    #[test]
    fn range_with_no_matching_keys_yields_nothing() {
        let mt = Memtable::new(None);
        mt.put(Bytes::from("a"), Bytes::from("1"));
        mt.put(Bytes::from("z"), Bytes::from("2"));

        let entries: Vec<(Bytes, ValueKind)> = mt.range(Bytes::from("m")..Bytes::from("n")).collect();
        assert!(entries.is_empty());
    }

    #[test]
    fn take_wal_writer_returns_writer_and_leaves_none() {
        let spy = WalSpy::new();
        let mt = Memtable::new(Some(Box::new(Arc::clone(&spy))));

        assert!(mt.take_wal_writer().is_some()); // writer was present
        assert!(mt.take_wal_writer().is_none());  // writer was consumed
    }
}
