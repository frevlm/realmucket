# Metadata key layout

Implementation: `libs/rocksdb/src/` (`kv.hpp` has the encoding, `meta_store.cpp` the subspaces). How the
database is driven (single writer, group commit, durability) is in `metadata-store.md`. Changing anything
below after deployment needs a migration (ADR 0010: the layout is permanent).

## Encoding

Every key is `tag fields...`:

- `tag` = one byte naming the subspace. MetaStore and ConfigStore share one database; the tags are unique
  across both.
- String field: the bytes with `00` escaped as `00 FF`, then a `00` terminator. Order-preserving, and `str(a)`
  is never a prefix of `str(b)` for `a != b`.
- Integers: fixed-width big-endian (`u32`, `u64`). Times: nanoseconds since the epoch as `u64` with the sign
  bit flipped.
- `stamp` = 10 bytes: the Generation (8 bytes, big-endian, from the counter under `Z`) and 2 spare bytes.
  **Generation = the first 8 bytes.**

Values are big-endian integers and `u32`-length-prefixed strings, except the counters (`u`, `Z`, `J`), which
are 8-byte little-endian.

## Subspaces

| Key | Value | Notes |
|---|---|---|
| `T str(tenant)` | max_bytes, max_objects | Tenant |
| `u str(tenant) 'b'` / `'o'` | LE u64 | usage bytes / object versions |
| `B str(tenant) str(bucket)` | versioning, 2 reserved bytes | Bucket (the reserved bytes held a storage policy once) |
| `H str(t) str(b) str(key)` | stamp(10), delete_marker(1) | head: the newest version of an Object |
| `V str(t) str(b) str(key) stamp` | delete_marker, size, checksum, created, master_key_id, wrapped Data Key | version header |
| `Y stamp u32(part)` | size, checksum, extent | one Part of the version `stamp` |
| `W stamp` | tenant, bucket, key | the Object of the version `stamp` |
| `X u64(chunk) stamp u32(part)` | extent | Chunk → committed Parts (`extents_in_chunk`); inline Parts are not listed |
| `P u64(upload) 00` | tenant, bucket, key, master_key_id, wrapped Data Key | Pending Upload header |
| `P u64(upload) 01` | deadline | written by every `put_part` |
| `P u64(upload) 02 u32(part)` | size, checksum, extent | registered Part |
| `Q u64(chunk) u64(upload) u32(part)` | — | Chunk → registered Parts of Pending Uploads (`has_pending_parts`); inline Parts are not listed |
| `D time(deadline) u64(upload)` | — | deadline index (`reclaim_expired`) |
| `p str(t) str(b) u64(upload)` | — | Pending Uploads per Bucket (`delete_bucket`) |
| `G u64(id)` | extent | garbage queue (Chunk extents only) |
| `I u64(blob)` | the stored (encrypted) bytes of one inline Part | see below |
| `J` | LE u64 | the last blob id handed out |
| `K` | min_sdk_version | ClusterConfig |
| `Z gen` | LE u64 | the last Generation |

Objects sort by tenant, bucket, key (ADR 0010), and the versions of one Object by Generation, so `list` is a
range read over `H` and `scan_versions` a range read over `V`; the cursor is the last returned header key.

A transaction writes at most one version, so its stamp names that version. Parts (`Y`), the chunk index (`X`)
and the back reference (`W`) are keyed by the stamp alone: the Object key, up to 1024 bytes, is stored once per
version, not once per Part.

**Inline Parts.** A Part whose stored bytes fit `RMK_INLINE_MAX_BYTES` is not written to a Chunk. Its Extent has
`chunk_id == 0` (`kInlineChunk`), `offset` is the blob id and `length`/`crc32c` describe the stored bytes, which
live under `I`. The blob is written in the same transaction as the Part's registration, so one flush of the
write-ahead log covers both. Blobs have no garbage entry: every place that would queue an Extent as garbage
(replaced Part, abort, reclaim, permanent deletion) clears the blob key in its own transaction instead. Blob ids
only grow (`J`), so an id is never reused.

**Soft quota (ADR 0010).** Usage is read and changed inside the single writer, with no conflict range; usage can
overshoot by what is in flight.

**Deletion (ADR 0016).** The wrapped Data Key exists only in the `V` header and the `P` header. Permanent
deletion clears the whole version range (header + Parts), its `X` entries and its inline blobs, and queues its
Chunk Extents as garbage; nothing else holds a copy. The garbage queue holds no key material.

**Pending Uploads.** `put_part` keeps one `Q` entry per registered Chunk Part (a replaced Part's entry moves with
it); commit, abort and reclaim clear the upload's entries together with its `P` keys, so `has_pending_parts` is a
one-key range read. `reclaim_expired` reads at most `limit` due `D` entries, drops entries whose upload is gone or
whose current deadline is later, and reclaims the rest; it stops early once it has taken in 4 × `kMaxParts` Parts
(always at least one upload), and the next call continues.

**Garbage ids** are `seconds since epoch << 24 | random 24 bits`: roughly FIFO, no shared counter. Each new id is
checked for existence in the same transaction and redrawn if taken, so ids are unique.
