# Metadata store on RocksDB

`libs/rocksdb` implements `MetaStore` and `ConfigStore` for a single machine; `realmucketd` uses it for all
object metadata (`<data_dir>/meta`, or `RMK_META_DIR`). The key layout is in `metadata-layout.md`; this page is
about how the database is driven. The engine was chosen by measurement (`tools/metabench`, results in
`PERFORMANCE.md`).

## How it works

- **One writer.** Every mutating call becomes a transaction body handed to a single thread. It gathers whatever
  queued up while the previous commit was running, runs the bodies one after the other in one
  `WriteBatchWithIndex` (each in a savepoint, so a body that fails rolls back only its own writes) and commits
  the batch with one `Write`. Nothing needs conflict detection because nothing runs concurrently.
- **Group commit for the log.** The write-ahead log is flushed by a second thread. The writer never waits for it:
  a lazy transaction is released as soon as it is applied; a durable one waits until a flush that started after
  its commit has finished. One flush serves every transaction committed before it began.
- **Reads** run on the caller's thread against a snapshot and never queue behind writes.
- **Which calls are durable.** `begin_upload`, `put_part`, `put_inline_part`, `abort_upload`, `reclaim_expired` and `ack_garbage`
  are lazy: after a crash the worst outcome is an upload that has to be redone, or garbage that is counted twice.
  Everything else flushes before it returns, in particular `commit_upload` (the object exists), `remove` and
  `remove_version` (the Data Key is gone for good), `relocate_part` (compaction may then delete the old chunk)
  and `rewrap`. `put_inline_part` is lazy like `put_part`, and its bytes ride in the same write-ahead log as
  the `commit_upload` that follows, so that one flush makes the bytes and the object durable together.

## Notes on the layout

- A version is named by a stamp of 10 bytes whose first 8 are the Generation. The Generation comes from a
  counter under key `Z gen`, taken by the first stamped write of a transaction and stored in the same batch, so a
  transaction that rolls back gives its number back, and Generations only ever grow, also across restarts.
- A commit is never in doubt, so there are no commit receipts.
- `clear_range` deletes the keys one by one (the batch index does not see range deletions).
- Usage counters are read, changed and written inside the single writer.
- Small Parts are stored as values under `I` (inline Parts). The values go through the write-ahead log once and
  through the LSM levels like any other value; `docs/PERFORMANCE.md` records what that cost in a sustained run.

## Operating notes

- The directory is opened by one process at a time (RocksDB's own `LOCK`; `realmucketd` also locks
  `<data_dir>/LOCK`).
- If the database is missing but chunk files exist, `realmucketd` refuses to start instead of treating the chunks
  as garbage.
- To copy it while running, use RocksDB's checkpoint facility; a plain file copy of a live database is not safe.
