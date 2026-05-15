use std::collections::VecDeque;
use std::sync::Arc;

use bytes::Bytes;

use crate::memtable::Memtable;
use crate::types::ValueKind;

pub struct StorageState {
    pub(crate) active: Arc<Memtable>,
    pub(crate) immutable_queue: VecDeque<Arc<Memtable>>,
}

impl StorageState {
    pub fn get(&self, key: &Bytes) -> Option<ValueKind> {
        if let Some(value) = self.active.get(key) {
            return Some(value);
        }

        for memtable in self.immutable_queue.iter().rev() {
            if let Some(value) = memtable.get(key) {
                return Some(value);
            }
        }

        None
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
}
