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

**Scan**:
A point-in-time range read over all Memtables that returns all live Key → value pairs whose Keys fall within the specified bounds. Keys whose latest ValueKind is a Tombstone are silently excluded. The newest ValueKind for each Key takes precedence.
_Avoid_: range query, range scan, key range

**SSTable** (Sorted String Table):
An immutable, sorted on-disk file produced by flushing an Immutable Memtable. Contains Data Blocks, an Index Block, and a Footer.
_Avoid_: segment file, sorted file, on-disk table

**Data Block**:
A fixed-size (configurable, default 4 KB) chunk of sorted Key → ValueKind records within an SSTable. The last Data Block is zero-padded to the full block size. Records never straddle Data Block boundaries.
_Avoid_: block, page, chunk

**Index Block**:
A variable-length structure at the end of an SSTable, after all Data Blocks, containing one entry per Data Block. Each entry maps the first Key of a Data Block to that block's byte offset in the file. Used to binary-search for the relevant Data Block during a point lookup.
_Avoid_: sparse index, block index, key index

**Footer**:
A fixed 28-byte structure at the very end of an SSTable file. Contains the Index Block offset (`u64`), Index Block size (`u64`), Data Block size (`u32`), and a magic number (`u64`). A reader always reads the Footer first to locate the Index Block.
_Avoid_: trailer, file header

**Flush**:
The act of writing an Immutable Memtable to disk as a new SSTable. Produces a file named `<id>.sst` in the configured data directory.
_Avoid_: persist, compact, drain

**SSTable ID**:
A monotonically incrementing `u32` assigned to each SSTable at Flush time. Determines the filename: zero-padded to 6 digits with a `.sst` extension (e.g. `000001.sst`).
_Avoid_: file number, sequence number

## Relationships

- A **Memtable** holds zero or more **Key** → **ValueKind** entries
- A **Tombstone** is a **ValueKind**, not an absence of a **Key**
- There is exactly one **Active Memtable** at any time; it becomes an **Immutable Memtable** when **Frozen**
- **Approximate Size** is tracked per **Memtable** and drives the **Freeze** decision
- **Immutable Memtables** are held in a FIFO queue and flushed to disk in order
- Only the **Active Memtable** holds a **WAL Writer**; **Immutable Memtables** do not
- A **Flush** converts one **Immutable Memtable** into one **SSTable**
- An **SSTable** contains one or more **Data Blocks**, one **Index Block**, and one **Footer**
- The **Index Block** has one entry per **Data Block**, in the same lexicographic key order
- Each **SSTable** is identified by a unique **SSTable ID** and named `<id>.sst`

## Example dialogue

> **Dev:** "When a user calls `delete(key)`, do we remove the entry from the Memtable?"
> **Domain expert:** "No — we insert a Tombstone. The Key stays present in the Memtable; its ValueKind becomes `Delete`. The actual removal happens during compaction when the Tombstone has propagated past all older versions."

> **Dev:** "What happens when a `put()` fills the Active Memtable?"
> **Domain expert:** "The write thread writes to WAL first, inserts the entry into the Active Memtable's SkipMap, then checks the Approximate Size. If it's over the limit, the thread Freezes inline: it takes the write lock, swaps in a new Active Memtable, and pushes the old one onto the Immutable Memtable queue. The triggering write stays in the old memtable."
