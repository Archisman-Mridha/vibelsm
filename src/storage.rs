use std::collections::VecDeque;
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, RwLock};

use bytes::Bytes;

use crate::memtable::Memtable;
use crate::types::ValueKind;

struct Inner {
    active: Arc<Memtable>,
    immutable_queue: VecDeque<Arc<Memtable>>,
}

pub struct StorageState {
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

    pub fn with_flush_notifier(size_limit: usize) -> (Self, Receiver<()>) {
        let (tx, rx) = mpsc::channel();
        let mut state = Self::new(size_limit);
        state.flush_notifier = Some(tx);
        (state, rx)
    }

    pub fn get(&self, key: &Bytes) -> Option<ValueKind> {
        let inner = self.inner.read().unwrap();
        inner.active.get(key).or_else(|| {
            inner
                .immutable_queue
                .iter()
                .rev()
                .find_map(|m| m.get(key))
        })
    }

    pub fn put(&self, key: Bytes, value: Bytes) {
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
            // Re-check under write lock: another thread may have already frozen this active.
            if inner.active.approximate_size() >= self.size_limit {
                let old = std::mem::replace(
                    &mut inner.active,
                    Arc::new(Memtable::new(None)),
                );
                let _ = old.take_wal_writer();
                inner.immutable_queue.push_back(old);
                if let Some(ref tx) = self.flush_notifier {
                    let _ = tx.send(());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
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
            size_limit: usize::MAX,
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
        // WAL not yet wired — both active and immutable have no writer.
        // Verifies take_wal_writer() is called on freeze, leaving the immutable without a writer.
        let state = StorageState::new(5);
        state.put(Bytes::from("ab"), Bytes::from("cde")); // size = 5 >= 5, triggers freeze
        let inner = state.inner.read().unwrap();
        assert!(!inner.active.has_wal_writer());
        assert!(!inner.immutable_queue.back().unwrap().has_wal_writer());
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
