# vibelsm

An LSM (Log-Structured Merge Tree) storage engine written in Rust.

## Language

**Memtable**:
The in-memory write buffer that absorbs all incoming writes before they are flushed to disk.
_Avoid_: write buffer, memory table

**Key**:
An arbitrary byte string used to identify a record, compared lexicographically.
_Avoid_: identifier, record key

**ValueKind**:
The discriminated type stored alongside a Key — either `Put(Bytes)` for a live value or `Delete` for a tombstone.
_Avoid_: value, entry value, record value

**Tombstone**:
A `ValueKind::Delete` entry written on user delete to shadow older versions of the same Key in on-disk storage.
_Avoid_: delete marker, deletion record

**Active Memtable**:
The single mutable Memtable currently accepting writes.
_Avoid_: current memtable, write memtable, mutable memtable

**Immutable Memtable**:
A frozen Memtable that is no longer accepting writes and is queued for flush to disk.
_Avoid_: read-only memtable, frozen memtable

**Freeze**:
The act of converting the Active Memtable into an Immutable Memtable when its Approximate Size reaches the size limit.
_Avoid_: flush trigger, rotate, seal

**Approximate Size**:
A running byte count of all Key + ValueKind bytes inserted into a Memtable, used to determine when to Freeze it.
_Avoid_: memory usage, size estimate

**WAL Writer**:
A write handle to the WAL file, owned exclusively by the Active Memtable. Every insert writes and flushes a record through the WAL Writer before the entry enters the SkipMap.
_Avoid_: log writer, WAL handle

## Relationships

- A **Memtable** holds zero or more **Key** → **ValueKind** entries
- A **Tombstone** is a **ValueKind**, not an absence of a **Key**
- There is exactly one **Active Memtable** at any time; it becomes an **Immutable Memtable** when **Frozen**
- **Approximate Size** is tracked per **Memtable** and drives the **Freeze** decision
- **Immutable Memtables** are held in a FIFO queue and flushed to disk in order
- Only the **Active Memtable** holds a **WAL Writer**; **Immutable Memtables** do not

## Example dialogue

> **Dev:** "When a user calls `delete(key)`, do we remove the entry from the Memtable?"
> **Domain expert:** "No — we insert a Tombstone. The Key stays present in the Memtable; its ValueKind becomes `Delete`. The actual removal happens during compaction when the Tombstone has propagated past all older versions."

> **Dev:** "What happens when a `put()` fills the Active Memtable?"
> **Domain expert:** "The write thread writes to WAL first, inserts the entry into the Active Memtable's SkipMap, then checks the Approximate Size. If it's over the limit, the thread Freezes inline: it takes the write lock, swaps in a new Active Memtable, and pushes the old one onto the Immutable Memtable queue. The triggering write stays in the old memtable."
