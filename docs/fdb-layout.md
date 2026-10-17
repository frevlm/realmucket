# FoundationDB key layout

Implementation: `libs/fdb/include/rmk/fdb/fdb_stores.hpp`, `libs/fdb/src/`. FDB API version 730. Changing anything
below after deployment needs a migration (ADR 0010: the layout is permanent).

## Encoding

Every key is `P tag fields...`:

- `P` = the configured `key_prefix` (`RMK_FDB_PREFIX`, default `rmk`) encoded as a string field,
  so prefix `rmk` never overlaps prefix `rmk2`.
- `tag` = one byte naming the subspace. MetaStore, ConfigStore and ClusterStore may share a
  prefix; the tags are unique across all three.
- String field: the bytes with `00` escaped as `00 FF`, then a `00` terminator. Order-preserving,
  and `str(a)` is never a prefix of `str(b)` for `a != b`.
- Integers: fixed-width big-endian (`u32`, `u64`). Times: nanoseconds since the epoch as
  `u64` with the sign bit flipped.
- `stamp` = the 10-byte FDB versionstamp of the committing transaction (8-byte commit version +
  2-byte batch order). **Generation = the first 8 bytes, big-endian.**

Values are big-endian integers and `u32`-length-prefixed strings, except the counters (`u`, `g`),
which are 8-byte little-endian for atomic `ADD`.

## Subspaces

| Key | Value | Notes |
|---|---|---|
| `T str(tenant)` | max_bytes, max_objects | Tenant |
| `u str(tenant) 'b'` / `'o'` | LE u64 | usage bytes / object versions, atomic ADD only |
| `B str(tenant) str(bucket)` | versioning, data_units, parity_units | Bucket |
| `H str(t) str(b) str(key)` | stamp(10), delete_marker(1) | head: the newest version of an Object |
| `V str(t) str(b) str(key) stamp` | delete_marker, size, checksum, created, master_key_id, wrapped Data Key | version header |
| `Y stamp u32(part)` | size, checksum, extent | one Part of the version `stamp` |
| `W stamp` | tenant, bucket, key | the Object of the version `stamp` |
| `X u64(chunk) stamp u32(part)` | extent | Chunk → committed Parts (`extents_in_chunk`) |
| `P u64(upload) 00` | tenant, bucket, key, master_key_id, wrapped Data Key | Pending Upload header |
| `P u64(upload) 01` | deadline | written by every `put_part` |
| `P u64(upload) 02 u32(part)` | size, checksum, extent | registered Part |
| `Q u64(chunk) u64(upload) u32(part)` | — | Chunk → registered Parts of Pending Uploads (`has_pending_parts`) |
| `D time(deadline) u64(upload)` | — | deadline index (`reclaim_expired`) |
| `p str(t) str(b) u64(upload)` | — | Pending Uploads per Bucket (`delete_bucket`) |
| `R time(now) u64(token)` | stamp, returns_generation(1) | commit receipt, see below |
| `G u64(id)` | extent | garbage queue |
| `K` | min_sdk_version | ClusterConfig |
| `N str(node id)` | NodeInfo | Data Nodes |
| `C u64(chunk)` | ChunkInfo (with sealed_at) without garbage_bytes | Chunk |
| `S u8(state) u64(chunk)` | — | Chunks by state |
| `g u64(chunk)` | LE u64 | garbage_bytes, atomic ADD only |
| `A` | u64 | last allocated Chunk id |
| `L` | holder, expiry, epoch | Controller lease |

Objects sort by tenant, bucket, key (ADR 0010), and the versions of one Object by Generation, so
`list` is a range read over `H` and `scan_versions` a range read over `V`; the cursor is the last
returned header key relative to `P 'V'`.

A transaction writes at most one version, so its versionstamp names that version cluster-wide.
Parts (`Y`), the chunk index (`X`) and the back reference (`W`) are keyed by the stamp alone: the
Object key, up to 1024 bytes, is stored once per version, not once per Part.

**Transaction size (10 MB).** `put_part` rejects Part numbers above `kMaxParts` (4096). The
largest commit, 4096 Parts under a 1024-byte key that replaces an equally large version, writes
4096 `Y` and `X` keys (~45 bytes each), clears the Pending Upload (one range clear plus 4096 `Q`
clears), and for the replaced version clears its `Y` range, 4096 `X` keys and queues 4096
garbage entries. Counting conflict ranges this is about 3 MB.
`LargestCommitAndOverwriteFitInOneTransaction` in `tests/fdb` runs exactly this case.

**`list`** reads `H` entries and, for each visible one, only its version header (a point read),
never Parts; a listed ObjectMeta has no `parts`. Every examined `H` entry counts against `limit`,
delete markers included, so a page may be short or empty while `next_start_after` is set.

## Transactions

Every method is one transaction run by `fdb::run` (`src/fdb/fdb.hpp`): the standard
`fdb_transaction_on_error` retry loop with a 10 s timeout (`FDB_TR_OPTION_TIMEOUT`, it survives
retries). An rmk error from the body aborts without committing; FDB errors that are not retried
map to `unavailable` (`invalid_argument` for too large transactions, keys or values).

**Commits.** `commit_upload` reads `H` of the Object (conflict range), checks the Precondition
against it, writes the header, Parts and `X` entries with `SET_VERSIONSTAMPED_KEY` and the head
with `SET_VERSIONSTAMPED_VALUE`, and adds an explicit write conflict on `H` (versionstamped
mutations add no conflict ranges of their own). Two concurrent commits to one Object therefore
conflict on `H`; the loser retries and re-checks its Precondition. The Generation is read with
`fdb_transaction_get_versionstamp` after the commit. Without Versioning the replaced version is
destroyed in the same transaction.

**Soft quota (ADR 0010).** Usage is read with snapshot reads (no conflict range) and changed with
atomic `ADD`, so concurrent commits of one Tenant do not conflict; usage can overshoot by what is
in flight.

**Deletion (ADR 0016).** The wrapped Data Key exists only in the `V` header and the `P` header.
Permanent deletion clears the whole version range (header + Parts), its `X` entries, and queues
its Extents as garbage; nothing else holds a copy. Receipts and the garbage queue hold no key
material. (Cleared data still lives in FDB's storage files and backups until they are
compacted/expired; ADR 0016 already requires bounded backup retention.)

**Pending Uploads.** `put_part` reads the header with a conflict range (so it conflicts with
commit/abort/reclaim) but the deadline without one, so parallel Parts of one upload do not abort
each other. Racing `put_part` calls may each leave a `D` entry; `reclaim_expired` reads at most
`limit` due `D` entries, drops entries whose upload is gone or whose current deadline is later,
and reclaims the rest (Parts → garbage, all keys cleared). All due uploads are read with
concurrent range reads; the transaction stops early once it has taken in 4 × `kMaxParts` Parts
(always at least one upload), and the next call continues. `put_part` keeps one `Q` entry per
registered Part (a replaced Part's entry moves with it); commit, abort and reclaim clear the
upload's entries together with its `P` keys, so `has_pending_parts` is a one-key range read. Receipts older than one hour are cleared first,
in a transaction of their own (one range clear).

**Garbage ids** are `seconds since epoch << 24 | random 24 bits`: roughly FIFO, no shared counter
(no hot key). Each new id is checked for existence in the same transaction and redrawn if taken,
so ids are unique.

**Chunk ids** come from the counter `A`. Only the leading Controller allocates, so it is never
contended; ids already taken by `put_chunk` are skipped. `garbage_bytes` is its own counter `g`:
`add_garbage` is a pure atomic add, and `update_chunk` ignores `next.garbage_bytes`, so a stale
copy cannot erase an add (this differs from the in-memory store, which stores whatever `next`
says). `claim_open_chunk` scans `S open` with snapshot reads and adds a read conflict only on the
Chunk it claims, so Gateways claiming different Chunks do not conflict.

**Controller lease.** One record `L`: the holder renews while `now < expiry`; anyone takes over
once `expiry <= now`, incrementing the epoch.

**Fencing.** `put_chunk`, `update_chunk`, `delete_chunk` and `remove_node` with a `Fence` read `L`
in the same transaction, with a read conflict range, and fail `aborted` unless it names that
holder and epoch. A takeover that commits while the fenced write is in flight invalidates its
read of `L`, so the write retries and fails; a fenced write that commits first is ordered before
the takeover.

## commit_unknown_result

`fdb_transaction_on_error` retries after `commit_unknown_result`, so a transaction may run again
although its first attempt committed. The retry loop marks such attempts
(`Txn::maybe_committed`, sticky for the rest of the call) and each operation is either idempotent
or detects its own commit:

| Operation | Retry after an unknown commit |
|---|---|
| `commit_upload`, `remove` | Write a receipt `R time(now) token` (token drawn once per call) in the same transaction. A maybe-committed retry reads it first and returns the recorded result (the versionstamp's Generation, or `kNoObject`). Without it a retried `commit_upload` would see its upload gone (`not_found`) and a retried `remove` could delete a second version or add a second delete marker. |
| `put_part` | Idempotent: the same Part number with the same Extent is not garbage; only the deadline moves. |
| `begin_upload` | The upload id is drawn once per call, outside the retry loop; an existing id on retry is ours. |
| `relocate_part`, `rewrap` | On retry, a Part already at `to` / a key already equal to `to` is success. |
| `update_chunk` | On retry, a stored record equal to `next` with revision + 1 is success. |
| `create_tenant`, `create_bucket`, `put_chunk` | On retry, an identical existing record is success. |
| `abort_upload`, `remove_version`, `delete_bucket`, `delete_chunk`, `remove_node` | On retry, `not_found` is success. |
| `add_garbage` (ClusterStore) | May count twice; the interface already allows double counting. |
| `allocate_chunk_id` | May skip an id (ids may be sparse). |
| `claim_open_chunk`, `try_lead`, setters, `ack_garbage`, `reclaim_expired`, `heartbeat` | Idempotent. A retried claim may pick another Chunk, leaving the first one leased to us until the lease expires. |

Tests reproduce this deterministically: `Database::simulate_unknown_commit_results(n)` reports
the next `n` successful commits as `commit_unknown_result` to the retry loop
(`tests/fdb/fdb_stores_test.cpp`).
