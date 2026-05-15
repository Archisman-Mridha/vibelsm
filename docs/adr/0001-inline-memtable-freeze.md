# Inline Memtable Freeze on the Write Path

When a `put()` or `delete()` causes the Active Memtable's Approximate Size to exceed the configured limit, the calling thread performs the Freeze inline: it takes the write lock on `StorageState`, swaps in a fresh Active Memtable, pushes the full one onto the Immutable Memtable queue, and signals the flush background thread. The triggering write lands in the old memtable before the Freeze.

We chose inline over a background poller because Freeze is a fast in-memory operation (no disk I/O — that belongs to the flush thread). A background poller would let the memtable grow past its limit between polls and adds unnecessary coordination complexity. The occasional write-lock acquisition on `put()` is the right cost to pay to keep the size budget honest.
