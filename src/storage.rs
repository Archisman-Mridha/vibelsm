use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver, Sender};

use bytes::Bytes;

use crate::memtable::{Memtable, WalWriter};
use crate::types::ValueKind;

pub struct StorageState {
    pub(crate) active: Arc<Memtable>,
    pub(crate) immutable_queue: VecDeque<Arc<Memtable>>,
    size_limit: usize,
    flush_notifier: Option<Sender<()>>,
}

impl StorageState {
    pub fn new(size_limit: usize) -> Self {
        Self {
            active: Arc::new(Memtable::new(Some(WalWriter))),
            immutable_queue: VecDeque::new(),
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
        self.active.get(key).or_else(|| {
            self.immutable_queue
                .iter()
                .rev()
                .find_map(|memtable| memtable.get(key))
        })
    }

    pub fn put(&self, key: Bytes, value: Bytes) {
        self.active.put(key, value);
    }

    pub fn delete(&self, key: Bytes) {
        self.active.delete(key);
    }

    pub fn should_freeze(&self) -> bool {
        self.active.approximate_size() >= self.size_limit
    }

    pub fn freeze(&mut self) {
        let old_active = std::mem::replace(
            &mut self.active,
            Arc::new(Memtable::new(Some(WalWriter))),
        );
        old_active.take_wal_writer();
        self.immutable_queue.push_back(old_active);
        if let Some(ref tx) = self.flush_notifier {
            let _ = tx.send(());
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::RwLock;

    use super::*;

    fn new_memtable() -> Arc<Memtable> {
        Arc::new(Memtable::new(None))
    }

    fn make_state(active: Arc<Memtable>, immutables: Vec<Arc<Memtable>>) -> StorageState {
        StorageState {
            active,
            immutable_queue: VecDeque::from(immutables),
            size_limit: usize::MAX,
            flush_notifier: None,
        }
    }

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

        // immutable_queue is FIFO: older is pushed first, newer second
        // iter().rev() walks newest first
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
        // newest doesn't have k1

        // Queue order: oldest first, newest last (FIFO push order)
        let state = make_state(new_memtable(), vec![oldest, middle, newest]);

        // Should find v_middle (middle is newer than oldest, newest has no k1)
        assert_eq!(
            state.get(&Bytes::from("k1")),
            Some(ValueKind::Put(Bytes::from("v_middle")))
        );
    }

    #[test]
    fn read_operations_use_shared_lock() {
        let active = new_memtable();
        active.put(Bytes::from("k1"), Bytes::from("v1"));

        let state = Arc::new(RwLock::new(make_state(active, vec![])));

        // Two concurrent readers can hold shared locks
        let read1 = state.read().unwrap();
        let read2 = state.read().unwrap();

        assert_eq!(
            read1.get(&Bytes::from("k1")),
            Some(ValueKind::Put(Bytes::from("v1")))
        );
        assert_eq!(
            read2.get(&Bytes::from("k1")),
            Some(ValueKind::Put(Bytes::from("v1")))
        );
    }

    // --- Freeze write-path tests (issue #6) ---

    fn new_state(size_limit: usize) -> StorageState {
        StorageState::new(size_limit)
    }

    #[test]
    fn new_creates_state_with_size_limit_and_empty_active() {
        let state = new_state(1024);
        assert_eq!(state.active.approximate_size(), 0);
        assert!(state.immutable_queue.is_empty());
    }

    #[test]
    fn put_writes_to_active_memtable() {
        let state = new_state(1024);
        state.put(Bytes::from("k1"), Bytes::from("v1"));
        assert_eq!(
            state.get(&Bytes::from("k1")),
            Some(ValueKind::Put(Bytes::from("v1")))
        );
    }

    #[test]
    fn delete_writes_to_active_memtable() {
        let state = new_state(1024);
        state.delete(Bytes::from("k1"));
        assert_eq!(state.get(&Bytes::from("k1")), Some(ValueKind::Delete));
    }

    #[test]
    fn put_triggers_freeze_when_size_exceeds_limit() {
        // size_limit = 10; "aaaa" + "bbbb" = 8, then "cc" + "dd" = 4 → total 12 >= 10
        let state = Arc::new(RwLock::new(new_state(10)));

        {
            let s = state.read().unwrap();
            s.put(Bytes::from("aaaa"), Bytes::from("bbbb")); // size = 8
        }
        assert!(state.read().unwrap().immutable_queue.is_empty());

        {
            let s = state.read().unwrap();
            s.put(Bytes::from("cc"), Bytes::from("dd")); // size = 12 >= 10
        }
        if state.read().unwrap().should_freeze() {
            state.write().unwrap().freeze();
        }

        let s = state.read().unwrap();
        assert_eq!(s.immutable_queue.len(), 1);
        assert_eq!(s.active.approximate_size(), 0);
    }

    #[test]
    fn delete_triggers_freeze_when_size_exceeds_limit() {
        let state = Arc::new(RwLock::new(new_state(10)));

        {
            let s = state.read().unwrap();
            s.put(Bytes::from("aaaa"), Bytes::from("bbbb")); // size = 8
        }
        assert!(!state.read().unwrap().should_freeze());

        {
            let s = state.read().unwrap();
            s.delete(Bytes::from("cc")); // size = 10 >= 10
        }
        if state.read().unwrap().should_freeze() {
            state.write().unwrap().freeze();
        }

        let s = state.read().unwrap();
        assert_eq!(s.immutable_queue.len(), 1);
    }

    #[test]
    fn triggering_write_lands_in_old_memtable() {
        let state = Arc::new(RwLock::new(new_state(10)));

        {
            let s = state.read().unwrap();
            s.put(Bytes::from("aaaa"), Bytes::from("bbbbbb")); // size = 10, triggers freeze
        }
        if state.read().unwrap().should_freeze() {
            state.write().unwrap().freeze();
        }

        let s = state.read().unwrap();
        // The triggering write is in the immutable (old active), not the new active
        let old = s.immutable_queue.back().unwrap();
        assert_eq!(
            old.get(&Bytes::from("aaaa")),
            Some(ValueKind::Put(Bytes::from("bbbbbb")))
        );
        // New active doesn't have the key
        assert_eq!(s.active.get(&Bytes::from("aaaa")), None);
    }

    #[test]
    fn fresh_active_after_freeze_has_zero_size() {
        let state = Arc::new(RwLock::new(new_state(5)));

        {
            let s = state.read().unwrap();
            s.put(Bytes::from("abc"), Bytes::from("de")); // 5 >= 5
        }
        if state.read().unwrap().should_freeze() {
            state.write().unwrap().freeze();
        }

        assert_eq!(state.read().unwrap().active.approximate_size(), 0);
    }

    #[test]
    fn old_active_is_newest_in_immutable_queue() {
        let state = Arc::new(RwLock::new(new_state(5)));

        // First freeze
        {
            let s = state.read().unwrap();
            s.put(Bytes::from("a"), Bytes::from("1111")); // 5 >= 5
        }
        if state.read().unwrap().should_freeze() {
            state.write().unwrap().freeze();
        }

        // Second freeze
        {
            let s = state.read().unwrap();
            s.put(Bytes::from("b"), Bytes::from("2222")); // 5 >= 5
        }
        if state.read().unwrap().should_freeze() {
            state.write().unwrap().freeze();
        }

        let s = state.read().unwrap();
        assert_eq!(s.immutable_queue.len(), 2);
        // newest (most recently frozen) is at the back
        let newest = s.immutable_queue.back().unwrap();
        assert_eq!(
            newest.get(&Bytes::from("b")),
            Some(ValueKind::Put(Bytes::from("2222")))
        );
        let oldest = s.immutable_queue.front().unwrap();
        assert_eq!(
            oldest.get(&Bytes::from("a")),
            Some(ValueKind::Put(Bytes::from("1111")))
        );
    }

    #[test]
    fn get_finds_triggering_write_after_freeze() {
        let state = Arc::new(RwLock::new(new_state(5)));

        {
            let s = state.read().unwrap();
            s.put(Bytes::from("k"), Bytes::from("val1")); // 5 >= 5
        }
        if state.read().unwrap().should_freeze() {
            state.write().unwrap().freeze();
        }

        // get on the state finds the key through immutable queue
        assert_eq!(
            state.read().unwrap().get(&Bytes::from("k")),
            Some(ValueKind::Put(Bytes::from("val1")))
        );
    }

    #[test]
    fn write_operations_use_shared_lock() {
        let state = Arc::new(RwLock::new(new_state(1024)));

        // Two concurrent writers can hold shared (read) locks
        let read1 = state.read().unwrap();
        let read2 = state.read().unwrap();

        read1.put(Bytes::from("k1"), Bytes::from("v1"));
        read2.put(Bytes::from("k2"), Bytes::from("v2"));

        drop(read1);
        drop(read2);

        let s = state.read().unwrap();
        assert_eq!(
            s.get(&Bytes::from("k1")),
            Some(ValueKind::Put(Bytes::from("v1")))
        );
        assert_eq!(
            s.get(&Bytes::from("k2")),
            Some(ValueKind::Put(Bytes::from("v2")))
        );
    }

    #[test]
    fn no_freeze_when_size_below_limit() {
        let state = new_state(100);
        state.put(Bytes::from("k"), Bytes::from("v")); // size = 2, well below 100
        assert!(!state.should_freeze());
        assert!(state.immutable_queue.is_empty());
    }

    #[test]
    fn freeze_strips_wal_writer_from_old_memtable() {
        let mut state = new_state(5);
        state.put(Bytes::from("ab"), Bytes::from("cde")); // size = 5 >= 5
        state.freeze();

        assert!(state.active.has_wal_writer());
        assert!(!state.immutable_queue.back().unwrap().has_wal_writer());
    }

    #[test]
    fn freeze_signals_flush_notifier() {
        let (mut state, rx) = StorageState::with_flush_notifier(5);
        state.put(Bytes::from("ab"), Bytes::from("cde")); // size = 5
        state.freeze();

        assert!(rx.try_recv().is_ok());
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn freeze_without_notifier_does_not_panic() {
        let mut state = new_state(5);
        state.put(Bytes::from("ab"), Bytes::from("cde"));
        state.freeze(); // should not panic even without notifier
        assert_eq!(state.immutable_queue.len(), 1);
    }

    #[test]
    fn freeze_at_exact_size_limit() {
        let state = Arc::new(RwLock::new(new_state(4)));
        {
            let s = state.read().unwrap();
            s.put(Bytes::from("ab"), Bytes::from("cd")); // size = 4, exactly at limit
        }
        assert!(state.read().unwrap().should_freeze());
        state.write().unwrap().freeze();
        assert_eq!(state.read().unwrap().immutable_queue.len(), 1);
        assert_eq!(state.read().unwrap().active.approximate_size(), 0);
    }
}
