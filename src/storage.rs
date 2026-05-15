use std::collections::{BTreeMap, VecDeque};
use std::ops::Bound;
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, RwLock};

use bytes::Bytes;

use crate::memtable::Memtable;
use crate::types::ValueKind;

// Only the fields that change on Freeze are grouped here. size_limit and flush_notifier are
// immutable after construction so they live directly on StorageState, outside the lock.
struct Inner {
    active: Arc<Memtable>,
    // FIFO: oldest frozen Memtable at the front, most recently frozen at the back.
    // The read path walks this in reverse (newest-to-oldest) so recent writes shadow older ones.
    immutable_queue: VecDeque<Arc<Memtable>>,
}

// A point-in-time view of all readable Memtable layers, captured under the read lock.
// Layers are ordered by priority: index 0 is the Active Memtable (highest), followed by
// Immutable Memtables newest-to-oldest. Both get() and scan() use this ordering so the
// "Active wins over Immutable, newest Immutable wins over older" rule is defined once.
struct ReadSnapshot {
    layers: Vec<Arc<Memtable>>,
}

impl ReadSnapshot {
    /// Point lookup: iterate layers in priority order, return the first match.
    /// A Tombstone is a valid match — callers that need live-only values must filter.
    fn get(&self, key: &Bytes) -> Option<ValueKind> {
        self.layers.iter().find_map(|m| m.get(key))
    }

    /// Range read: merge all layers into a BTreeMap (oldest-to-newest so newer entries
    /// overwrite stale ones), then collect live entries in sorted key order.
    fn scan(&self, lower: Bound<Bytes>, upper: Bound<Bytes>) -> Vec<(Bytes, Bytes)> {
        let mut merged = BTreeMap::new();
        // Reverse = oldest layer first; later inserts (newer layers) overwrite stale values.
        for memtable in self.layers.iter().rev() {
            for (key, value_kind) in memtable.range((lower.clone(), upper.clone())) {
                merged.insert(key, value_kind);
            }
        }
        merged
            .into_iter()
            .filter_map(|(key, vk)| match vk {
                ValueKind::Put(value) => Some((key, value)),
                ValueKind::Delete => None,
            })
            .collect()
    }
}

pub struct StorageState {
    // RwLock lives inside StorageState so put() and delete() can take &self while still
    // acquiring the write lock on Freeze without requiring callers to manage locking.
    inner: RwLock<Inner>,
    size_limit: usize,
    flush_notifier: Option<Sender<()>>,
}

impl StorageState {
    pub fn new(size_limit: usize) -> Self {
        Self {
            inner: RwLock::new(Inner {
                active: Arc::new(Memtable::new(None)),
                immutable_queue: VecDeque::new(),
            }),
            size_limit,
            flush_notifier: None,
        }
    }

    /// Like `new`, but also returns a receiver that fires once each time a Freeze occurs.
    /// The flush background thread listens on this to know when a new Immutable Memtable
    /// is ready to be written to disk as an SSTable.
    pub fn with_flush_notifier(size_limit: usize) -> (Self, Receiver<()>) {
        let (tx, rx) = mpsc::channel();
        let mut state = Self::new(size_limit);
        state.flush_notifier = Some(tx);
        (state, rx)
    }

    /// Captures a point-in-time view of all readable layers under the read lock,
    /// then releases the lock. Freeze operations on the write path are not blocked
    /// for the duration of any subsequent read.
    fn snapshot(&self) -> ReadSnapshot {
        let inner = self.inner.read().unwrap();
        let mut layers = Vec::with_capacity(1 + inner.immutable_queue.len());
        layers.push(inner.active.clone());
        // Push immutables newest-to-oldest to match the priority order defined on ReadSnapshot.
        layers.extend(inner.immutable_queue.iter().rev().cloned());
        ReadSnapshot { layers }
    }

    pub fn get(&self, key: &Bytes) -> Option<ValueKind> {
        self.snapshot().get(key)
    }

    pub fn scan(&self, lower: Bound<Bytes>, upper: Bound<Bytes>) -> Vec<(Bytes, Bytes)> {
        self.snapshot().scan(lower, upper)
    }

    pub fn put(&self, key: Bytes, value: Bytes) {
        // Clone the Arc under the read lock, then release it. The SkipMap inside Memtable is
        // lock-free, so no lock needs to be held for the actual write.
        let active = self.inner.read().unwrap().active.clone();
        active.put(key, value);
        self.freeze_if_needed(&active);
    }

    pub fn delete(&self, key: Bytes) {
        let active = self.inner.read().unwrap().active.clone();
        active.delete(key);
        self.freeze_if_needed(&active);
    }

    pub fn active_approximate_size(&self) -> usize {
        self.inner.read().unwrap().active.approximate_size()
    }

    pub fn immutable_count(&self) -> usize {
        self.inner.read().unwrap().immutable_queue.len()
    }

    fn freeze_if_needed(&self, written_to: &Arc<Memtable>) {
        if written_to.approximate_size() >= self.size_limit {
            let mut inner = self.inner.write().unwrap();
            // Re-check under the write lock: two threads can both observe size >= limit, both
            // reach this point, but only the first one should actually Freeze. By the time the
            // second thread acquires the write lock, the active will already be a fresh empty
            // Memtable whose size is below the limit.
            if inner.active.approximate_size() >= self.size_limit {
                let old = std::mem::replace(
                    &mut inner.active,
                    Arc::new(Memtable::new(None)),
                );
                // Strip the WAL Writer from the frozen Memtable — Immutable Memtables must not
                // hold a writer (CONTEXT.md invariant). The WAL file is finalized separately.
                let _ = old.take_wal_writer();
                inner.immutable_queue.push_back(old);
                // Ignore send errors: if the flush receiver was dropped the Freeze still
                // completes correctly — data is safe in the Immutable queue.
                if let Some(ref tx) = self.flush_notifier {
                    let _ = tx.send(());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Bound;
    use super::*;

    fn new_memtable() -> Arc<Memtable> {
        Arc::new(Memtable::new(None))
    }

    // Constructs StorageState with specific active + immutable entries for read-path tests.
    fn make_state(active: Arc<Memtable>, immutables: Vec<Arc<Memtable>>) -> StorageState {
        StorageState {
            inner: RwLock::new(Inner {
                active,
                immutable_queue: VecDeque::from(immutables),
            }),
            size_limit: usize::MAX, // prevents Freeze from firing during read-path tests
            flush_notifier: None,
        }
    }

    // --- Read-path tests ---

    #[test]
    fn get_finds_key_in_active_memtable() {
        let active = new_memtable();
        active.put(Bytes::from("k1"), Bytes::from("v1"));
        let state = make_state(active, vec![]);
        assert_eq!(
            state.get(&Bytes::from("k1")),
            Some(ValueKind::Put(Bytes::from("v1")))
        );
    }

    #[test]
    fn get_falls_through_to_immutable_when_active_returns_none() {
        let imm = new_memtable();
        imm.put(Bytes::from("k1"), Bytes::from("v1"));
        let state = make_state(new_memtable(), vec![imm]);
        assert_eq!(
            state.get(&Bytes::from("k1")),
            Some(ValueKind::Put(Bytes::from("v1")))
        );
    }

    #[test]
    fn get_returns_active_value_when_key_exists_in_both() {
        let imm = new_memtable();
        imm.put(Bytes::from("k1"), Bytes::from("old"));
        let active = new_memtable();
        active.put(Bytes::from("k1"), Bytes::from("new"));
        let state = make_state(active, vec![imm]);
        assert_eq!(
            state.get(&Bytes::from("k1")),
            Some(ValueKind::Put(Bytes::from("new")))
        );
    }

    #[test]
    fn get_stops_at_tombstone_in_active_without_checking_immutable() {
        let imm = new_memtable();
        imm.put(Bytes::from("k1"), Bytes::from("v1"));
        let active = new_memtable();
        active.delete(Bytes::from("k1"));
        let state = make_state(active, vec![imm]);
        assert_eq!(state.get(&Bytes::from("k1")), Some(ValueKind::Delete));
    }

    #[test]
    fn get_stops_at_tombstone_in_immutable_without_checking_older() {
        let older = new_memtable();
        older.put(Bytes::from("k1"), Bytes::from("v1"));
        let newer = new_memtable();
        newer.delete(Bytes::from("k1"));
        let state = make_state(new_memtable(), vec![older, newer]);
        assert_eq!(state.get(&Bytes::from("k1")), Some(ValueKind::Delete));
    }

    #[test]
    fn get_returns_none_when_key_not_in_any_memtable() {
        let imm = new_memtable();
        imm.put(Bytes::from("other"), Bytes::from("v"));
        let state = make_state(new_memtable(), vec![imm]);
        assert_eq!(state.get(&Bytes::from("missing")), None);
    }

    #[test]
    fn get_walks_immutable_queue_newest_to_oldest() {
        let oldest = new_memtable();
        oldest.put(Bytes::from("k1"), Bytes::from("v_oldest"));
        let middle = new_memtable();
        middle.put(Bytes::from("k1"), Bytes::from("v_middle"));
        let newest = new_memtable();
        // newest doesn't have k1 — falls through to middle
        let state = make_state(new_memtable(), vec![oldest, middle, newest]);
        assert_eq!(
            state.get(&Bytes::from("k1")),
            Some(ValueKind::Put(Bytes::from("v_middle")))
        );
    }

    // --- Write + Freeze tests ---

    #[test]
    fn new_state_has_empty_active_and_no_immutables() {
        let state = StorageState::new(1024);
        assert_eq!(state.active_approximate_size(), 0);
        assert_eq!(state.immutable_count(), 0);
    }

    #[test]
    fn put_writes_to_active_memtable() {
        let state = StorageState::new(1024);
        state.put(Bytes::from("k1"), Bytes::from("v1"));
        assert_eq!(
            state.get(&Bytes::from("k1")),
            Some(ValueKind::Put(Bytes::from("v1")))
        );
    }

    #[test]
    fn delete_writes_to_active_memtable() {
        let state = StorageState::new(1024);
        state.delete(Bytes::from("k1"));
        assert_eq!(state.get(&Bytes::from("k1")), Some(ValueKind::Delete));
    }

    #[test]
    fn no_freeze_when_size_below_limit() {
        let state = StorageState::new(100);
        state.put(Bytes::from("k"), Bytes::from("v")); // 2 bytes, well below 100
        assert_eq!(state.immutable_count(), 0);
    }

    #[test]
    fn put_triggers_freeze_when_size_reaches_limit() {
        // "aaaa"(4) + "bbbb"(4) = 8; then "cc"(2) + "dd"(2) = 4 → total 12 >= 10
        let state = StorageState::new(10);
        state.put(Bytes::from("aaaa"), Bytes::from("bbbb")); // size = 8, no freeze
        assert_eq!(state.immutable_count(), 0);
        state.put(Bytes::from("cc"), Bytes::from("dd")); // size = 12, triggers freeze
        assert_eq!(state.immutable_count(), 1);
        assert_eq!(state.active_approximate_size(), 0);
    }

    #[test]
    fn delete_triggers_freeze_when_size_reaches_limit() {
        let state = StorageState::new(10);
        state.put(Bytes::from("aaaa"), Bytes::from("bbbb")); // size = 8
        assert_eq!(state.immutable_count(), 0);
        state.delete(Bytes::from("cc")); // size = 10 >= 10, triggers freeze
        assert_eq!(state.immutable_count(), 1);
    }

    #[test]
    fn freeze_strips_wal_writer_from_old_memtable() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use crate::memtable::WalWrite;

        // Spy that counts WAL write calls.
        struct CountingWal(AtomicUsize);
        impl WalWrite for Arc<CountingWal> {
            fn write(&self, _: &Bytes, _: &ValueKind) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }

        let wal = Arc::new(CountingWal(AtomicUsize::new(0)));
        let active = Arc::new(Memtable::new(Some(Box::new(Arc::clone(&wal)))));
        let state = StorageState {
            inner: RwLock::new(Inner {
                active,
                immutable_queue: VecDeque::new(),
            }),
            size_limit: 5,
            flush_notifier: None,
        };

        // This write goes through the WAL Writer and triggers Freeze.
        state.put(Bytes::from("ab"), Bytes::from("cde"));
        assert_eq!(wal.0.load(Ordering::Relaxed), 1);

        // The frozen Memtable is now in the immutable queue. Write to it directly —
        // the WAL Writer was stripped on Freeze so the count must not increase.
        let frozen = state.inner.read().unwrap()
            .immutable_queue.back().unwrap().clone();
        frozen.put(Bytes::from("x"), Bytes::from("y"));
        assert_eq!(wal.0.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn freeze_signals_flush_notifier() {
        let (state, rx) = StorageState::with_flush_notifier(5);
        state.put(Bytes::from("ab"), Bytes::from("cde")); // size = 5, triggers freeze
        assert!(rx.try_recv().is_ok());
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn freeze_without_notifier_does_not_panic() {
        let state = StorageState::new(5);
        state.put(Bytes::from("ab"), Bytes::from("cde")); // triggers freeze
        assert_eq!(state.immutable_count(), 1);
    }

    #[test]
    fn freeze_at_exact_size_limit() {
        let state = StorageState::new(4);
        state.put(Bytes::from("ab"), Bytes::from("cd")); // exactly 4 >= 4
        assert_eq!(state.immutable_count(), 1);
        assert_eq!(state.active_approximate_size(), 0);
    }

    #[test]
    fn triggering_write_is_findable_after_freeze() {
        let state = StorageState::new(10);
        state.put(Bytes::from("aaaa"), Bytes::from("bbbbbb")); // 10 >= 10, triggers freeze
        assert_eq!(state.active_approximate_size(), 0);
        assert_eq!(
            state.get(&Bytes::from("aaaa")),
            Some(ValueKind::Put(Bytes::from("bbbbbb")))
        );
    }

    #[test]
    fn fresh_active_after_freeze_has_zero_size() {
        let state = StorageState::new(5);
        state.put(Bytes::from("abc"), Bytes::from("de")); // 5 >= 5
        assert_eq!(state.active_approximate_size(), 0);
    }

    #[test]
    fn multiple_freezes_accumulate_immutable_queue() {
        let state = StorageState::new(5);
        state.put(Bytes::from("a"), Bytes::from("1111")); // 5 >= 5, first freeze
        assert_eq!(state.immutable_count(), 1);
        state.put(Bytes::from("b"), Bytes::from("2222")); // 5 >= 5, second freeze
        assert_eq!(state.immutable_count(), 2);
        assert_eq!(
            state.get(&Bytes::from("a")),
            Some(ValueKind::Put(Bytes::from("1111")))
        );
        assert_eq!(
            state.get(&Bytes::from("b")),
            Some(ValueKind::Put(Bytes::from("2222")))
        );
    }

    // --- Scan tests ---

    #[test]
    fn scan_returns_live_keys_from_active_memtable() {
        let active = new_memtable();
        active.put(Bytes::from("a"), Bytes::from("1"));
        active.put(Bytes::from("b"), Bytes::from("2"));
        active.put(Bytes::from("c"), Bytes::from("3"));
        let state = make_state(active, vec![]);

        let result = state.scan(Bound::Included(Bytes::from("a")), Bound::Included(Bytes::from("c")));
        assert_eq!(result, vec![
            (Bytes::from("a"), Bytes::from("1")),
            (Bytes::from("b"), Bytes::from("2")),
            (Bytes::from("c"), Bytes::from("3")),
        ]);
    }

    #[test]
    fn scan_falls_through_to_immutable_when_active_has_no_matching_keys() {
        let imm = new_memtable();
        imm.put(Bytes::from("a"), Bytes::from("1"));
        imm.put(Bytes::from("b"), Bytes::from("2"));
        let state = make_state(new_memtable(), vec![imm]);

        let result = state.scan(Bound::Included(Bytes::from("a")), Bound::Included(Bytes::from("b")));
        assert_eq!(result, vec![
            (Bytes::from("a"), Bytes::from("1")),
            (Bytes::from("b"), Bytes::from("2")),
        ]);
    }

    #[test]
    fn scan_returns_active_value_when_key_exists_in_both() {
        let imm = new_memtable();
        imm.put(Bytes::from("k"), Bytes::from("old"));
        let active = new_memtable();
        active.put(Bytes::from("k"), Bytes::from("new"));
        let state = make_state(active, vec![imm]);

        let result = state.scan(Bound::Included(Bytes::from("k")), Bound::Included(Bytes::from("k")));
        assert_eq!(result, vec![(Bytes::from("k"), Bytes::from("new"))]);
    }

    #[test]
    fn scan_excludes_key_tombstoned_in_active() {
        let imm = new_memtable();
        imm.put(Bytes::from("a"), Bytes::from("1"));
        let active = new_memtable();
        active.delete(Bytes::from("a"));
        let state = make_state(active, vec![imm]);

        let result = state.scan(Bound::Included(Bytes::from("a")), Bound::Included(Bytes::from("a")));
        assert!(result.is_empty());
    }

    #[test]
    fn scan_excludes_key_tombstoned_in_newer_immutable() {
        let older = new_memtable();
        older.put(Bytes::from("a"), Bytes::from("1"));
        let newer = new_memtable();
        newer.delete(Bytes::from("a"));
        let state = make_state(new_memtable(), vec![older, newer]);

        let result = state.scan(Bound::Included(Bytes::from("a")), Bound::Included(Bytes::from("a")));
        assert!(result.is_empty());
    }

    #[test]
    fn scan_result_is_sorted_in_lexicographic_order() {
        let active = new_memtable();
        active.put(Bytes::from("cherry"), Bytes::from("3"));
        active.put(Bytes::from("apple"), Bytes::from("1"));
        active.put(Bytes::from("banana"), Bytes::from("2"));
        let state = make_state(active, vec![]);

        let result = state.scan(Bound::Unbounded, Bound::Unbounded);
        let keys: Vec<Bytes> = result.iter().map(|(k, _)| k.clone()).collect();
        assert_eq!(keys, vec![Bytes::from("apple"), Bytes::from("banana"), Bytes::from("cherry")]);
    }

    #[test]
    fn scan_returns_empty_vec_when_no_live_keys_in_range() {
        let active = new_memtable();
        active.put(Bytes::from("a"), Bytes::from("1"));
        active.put(Bytes::from("z"), Bytes::from("2"));
        let state = make_state(active, vec![]);

        let result = state.scan(Bound::Included(Bytes::from("m")), Bound::Excluded(Bytes::from("n")));
        assert!(result.is_empty());
    }

    #[test]
    fn scan_with_unbounded_lower() {
        let active = new_memtable();
        active.put(Bytes::from("a"), Bytes::from("1"));
        active.put(Bytes::from("b"), Bytes::from("2"));
        active.put(Bytes::from("c"), Bytes::from("3"));
        let state = make_state(active, vec![]);

        let result = state.scan(Bound::Unbounded, Bound::Included(Bytes::from("b")));
        assert_eq!(result, vec![
            (Bytes::from("a"), Bytes::from("1")),
            (Bytes::from("b"), Bytes::from("2")),
        ]);
    }

    #[test]
    fn scan_with_unbounded_upper() {
        let active = new_memtable();
        active.put(Bytes::from("a"), Bytes::from("1"));
        active.put(Bytes::from("b"), Bytes::from("2"));
        active.put(Bytes::from("c"), Bytes::from("3"));
        let state = make_state(active, vec![]);

        let result = state.scan(Bound::Included(Bytes::from("b")), Bound::Unbounded);
        assert_eq!(result, vec![
            (Bytes::from("b"), Bytes::from("2")),
            (Bytes::from("c"), Bytes::from("3")),
        ]);
    }

    #[test]
    fn scan_returns_newest_value_across_multiple_immutables() {
        let oldest = new_memtable();
        oldest.put(Bytes::from("k"), Bytes::from("v_oldest"));
        let middle = new_memtable();
        middle.put(Bytes::from("k"), Bytes::from("v_middle"));
        let newest = new_memtable();
        // newest doesn't have k
        let state = make_state(new_memtable(), vec![oldest, middle, newest]);

        let result = state.scan(Bound::Included(Bytes::from("k")), Bound::Included(Bytes::from("k")));
        assert_eq!(result, vec![(Bytes::from("k"), Bytes::from("v_middle"))]);
    }

    #[test]
    fn concurrent_puts_on_shared_state_both_land() {
        use std::thread;
        let state = Arc::new(StorageState::new(1024));
        let s1 = Arc::clone(&state);
        let t1 = thread::spawn(move || s1.put(Bytes::from("k1"), Bytes::from("v1")));
        let s2 = Arc::clone(&state);
        let t2 = thread::spawn(move || s2.put(Bytes::from("k2"), Bytes::from("v2")));
        t1.join().unwrap();
        t2.join().unwrap();
        assert_eq!(
            state.get(&Bytes::from("k1")),
            Some(ValueKind::Put(Bytes::from("v1")))
        );
        assert_eq!(
            state.get(&Bytes::from("k2")),
            Some(ValueKind::Put(Bytes::from("v2")))
        );
    }
}
