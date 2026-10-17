# Implementation guide

Engineering contract for the single-node `realmucketd` (see the note at the top of `docs/PLAN.md`). Decisions live in `docs/adr/`,
vocabulary in `CONTEXT.md`. Interfaces are the headers in `libs/*/include/` and `services/*/include/` and the protos in
`proto/rmk/v1/`; this document pins down the behaviour those signatures cannot express.

## 1. Build and test

Native first. On macOS (Homebrew `grpc nlohmann-json`, AppleClang) and Linux alike:

```
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Debug
cmake --build build
ctest --test-dir build --output-on-failure
```

`CMakeLists.txt` builds a module from every `.cpp` in its `src/` directory and its tests from
every `.cpp` in its `test/` directory into `rmk_<module>_tests`. A module owns its headers,
sources and tests; adding files never requires editing CMake.

```
libs/<module>/{include/rmk/<module>/, src/, test/}
services/<name>/{include/rmk/<name>/, src/, test/, main.cpp}
apps/<name>/main.cpp
tests/e2e/            cross-module tests, through the Client SDK against realmucketd
tests/store_contract/ contract suites shared by the memory and RocksDB stores
```

| Module | Directory | Depends on |
|---|---|---|
| core (types, errors, names, config, env, crc32c, chunk file, store interfaces) | `libs/core` | — |
| stores (in-memory) | `libs/stores` | core |
| local (ChunkStore: object bytes on this disk) | `libs/local` | core |
| rocksdb (MetaStore, ConfigStore) | `libs/rocksdb` | RocksDB |
| crypto | `libs/crypto` | OpenSSL |
| http (+metrics) | `libs/http` | libcurl |
| rpc (status mapping, TLS) | `libs/rpc` | gRPC |
| client (SDK, public header `rmk/client.hpp`) | `libs/client` | gRPC |
| gateway (the `rmk.v1.Storage` service) | `services/gateway` | rpc |
| realmucketd (the daemon) | `services/realmucketd` | gateway, local, rocksdb |
| rmkctl | `apps/rmkctl` | client |
| realmucket-bench | `apps/bench` | client |

Rules for all code: C++23, `-Wall -Wextra -Wpedantic -Werror` on AppleClang and GCC 13,
errors as `rmk::Result`/`rmk::Status` (no exceptions across module boundaries; catch library
exceptions at the edge), no global mutable state except metrics, comments say what, not history.

## 2. Names and limits

- Tenant and bucket names: 1–63 chars of `[a-z0-9._-]`, starting with a letter or digit.
- Object keys: 1–1024 bytes of valid UTF-8, no NUL.
- `list` limit: 1–1000.
- Violations are `invalid_argument`.

## 3. Store semantics (MetaStore, ConfigStore)

Both have an in-memory implementation (`libs/stores/include/rmk/stores/memory_stores.hpp`) and a
RocksDB implementation (`libs/rocksdb/include/rmk/rocksdb/rocks_stores.hpp`, layout in
`docs/metadata-store.md`). Both must pass the shared contract suite in `tests/store_contract/`,
which is instantiated once per implementation.

MetaStore details:

- `create_bucket` needs the Tenant (`not_found` otherwise). Bucket config validation:
  `1 <= data_units`, `data_units + parity_units <= 20`.
- Quota (soft, ADR 0010): `commit_upload` fails `quota_exceeded` when
  `usage.bytes + size > max_bytes` or `usage.objects + 1 > max_objects`, reading usage inside
  the commit transaction. Usage counts every stored
  version (not delete markers). Replacing a version without Versioning subtracts the old one.
- Generations: a counter persisted with the metadata — unique per Object, non-decreasing.
- Versioning off: commit replaces the version; `remove` deletes it permanently.
  Versioning on: commit adds a version; `remove` adds a delete marker (size 0, no parts, no
  encryption); `head`/`list` show the newest version unless it is a delete marker.
  `remove_version` deletes exactly one version, marker or not. Turning Versioning off later
  keeps existing versions; new writes then replace only the newest.
- Garbage (Extents no longer referenced) comes from: a replaced Part (`put_part` with an
  existing number), `abort_upload`, `reclaim_expired`, a replaced version (Versioning off), and
  any permanent deletion. Never from `relocate_part`.
- `remove` with a Precondition compares against the newest version's Generation, where a delete
  marker or nothing counts as `kNoObject`. Removing something absent is `not_found`.
- `list`: keys in byte order, `start_after` exclusive, `next_start_after` set iff more remain.
  Every examined key, delete markers included, counts toward `limit`, so a page may be short or
  even empty while `next_start_after` is set: callers keep paging until it is absent.
- `extents_in_chunk` covers every committed version (current and noncurrent).
- `scan_versions` order: tenant, bucket, key, generation. The cursor is opaque.
- `delete_bucket`: `not_empty` while any version, marker or Pending Upload exists.
- The quota check is net of what a commit replaces: with Versioning off, overwriting at the
  object limit, or growing within the byte limit, succeeds.
- `relocate_part` and `rewrap` return `aborted` for every mismatch, including a vanished version.
- `scan_versions` includes delete markers (they have no Data Key; rotation skips them);
  `head_version` returns a delete marker when asked for its Generation.
- A commit with zero Parts creates an empty Object. Re-putting a Part with an identical Extent
  produces no garbage. `add_garbage` does not change `revision`.
- `list_versions` of an unknown Object is empty. `remove` when the newest version is already a
  delete marker is `not_found`; a failing Precondition is reported first.

## 4. Formats

- Stored Part = `nonce(12) || AES-256-GCM ciphertext || tag(16)`, associated data = part number
  as 4 bytes big-endian. A fresh random nonce per write, so re-sending a Part never reuses one.
- Wrapped Data Key = `nonce(12) || ciphertext(32) || tag(16)` under the Master Key, associated
  data `"rmk-data-key-v1"`.
- `Part.checksum` = crc32c of plaintext; `ObjectMeta.checksum` = `crc32c_combine` over parts.
  `Extent.crc32c` = crc32c of the stored bytes.
- Chunk files: `<RMK_DATA_DIR>/chunks/<chunk id as 16 hex>.chunk`, one header then appended Parts.

## 5. Access control (ADR 0018)

The daemon does not authenticate or authorize callers; whatever is in front of it does (k8s NetworkPolicy, mTLS, mesh, or a firewall).
There is no caller identity: every request names its Tenant explicitly and an empty Tenant is
`invalid_argument`. The only per-request check is the minimum SDK version (`x-rmk-sdk-version`,
`SetClusterConfig`). Tenant and Quota administration RPCs are open to whoever can reach the
listening port.

## 6. Data path

### Writing

1. Check the Tenant is named and the minimum SDK version, load the Bucket.
2. `crypto::new_data_key`, then `begin_upload` with the wrapped key.
3. Cut the content into Parts of `RMK_PART_SIZE` (single `PutObject` streams too). Per Part:
   encrypt, then `local::ChunkStore::write`, which appends to the one open Chunk, starts a new
   Chunk when the Part does not fit, and returns only after the bytes are flushed. Writers in
   flight together share flushes (group commit, `docs/PERFORMANCE.md`).
4. `put_part` with the resulting Extent; finally `commit_upload` with the Precondition. The
   metadata commit is flushed to disk before it returns (only tests turn that off).

The order is fixed: metadata never points at bytes that are not on disk.

### Reading

`ChunkStore::read` verifies `Extent.crc32c`; a mismatch is `corruption`. A ranged GET decrypts every
Part it overlaps in full (GCM authenticates whole Parts).

### Background (realmucketd maintenance thread)

Each pass (`RMK_MAINTENANCE_INTERVAL_SECONDS`, default 5):
1. Garbage: `peek_garbage` → `add_garbage` per Chunk → `ack_garbage`.
2. `reclaim_expired` Pending Uploads.
3. Compaction (ADR 0016): skip any Chunk for which `has_pending_parts` is true. Otherwise a sealed
   Chunk with `garbage_bytes * 100 >= (size - 64) * RMK_COMPACTION_GARBAGE_PERCENT` (default 50)
   and sealed at least `RMK_COMPACTION_GRACE_SECONDS` ago. No live Extents → delete the file.
   Otherwise copy each live Extent (stored bytes, unchanged) through the ChunkStore, then
   `relocate_part`; an `aborted` relocation turns the copy into garbage. Throttled by
   `RMK_COMPACTION_BYTES_PER_SECOND` (default 50 MiB/s).
4. Master Key rotation: `scan_versions` and `rewrap` versions whose `master_key_id` is not active
   (`RMK_REWRAP_BATCH`, default 1000 per pass).

### Safety rules

1. **Compaction grace.** The ChunkStore stamps `sealed_at` when a Chunk stops taking appends
   (the start time for Chunks found at open). Compaction considers only Chunks sealed at least
   `RMK_COMPACTION_GRACE_SECONDS` (default 600) ago. The Gateway registers an Extent (`put_part`)
   only if less than half that grace has passed since the bytes were flushed (steady clock);
   otherwise it writes the Part again. Together: every Extent of a Chunk is registered before the
   Chunk can be compacted.
2. **Bounded memory and parts.** A Part is at most `RMK_MAX_PART_BYTES` (default 256 MiB, must be
   below the chunk capacity minus 92 bytes); larger is `invalid_argument`. The Gateway buffers at
   most `RMK_MAX_BUFFERED_BYTES` (default 2 GiB) across requests and answers `unavailable` beyond.
   At most `kMaxParts` (4096) Parts per upload.
3. **Bounded transactions.** `reclaim_expired` stops early when its transaction grows large
   (cumulative Parts), clearing receipts in a separate transaction; `list` reads only version
   headers and counts skipped delete markers against its limit.
4. **No silent leaks.** If `put_part` or `relocate_part` fails after bytes were written, the
   Gateway or maintenance thread reports those bytes with `add_garbage`.
5. **Reads survive relocation.** If a read of an Extent fails with `not_found`, the Gateway
   re-reads the version's metadata once and retries with the new Extent.
6. **One owner per directory.** The daemon holds a lock on `RMK_DATA_DIR` and refuses to start when
   chunk files exist without the metadata database that describes them.

## 7. Process and configuration

`realmucketd` is the only server process: the Storage gRPC API, chunk files in
`<RMK_DATA_DIR>/chunks`, metadata in RocksDB, and the maintenance thread. Nothing else has to run.
Every process logs one line per event to stderr: `<RFC3339 time> <level> <component> <message>`.

| Variable | Default |
|---|---|
| `RMK_DATA_DIR`, `RMK_MASTER_KEY_DIR` | required; Master Keys are read from the directory |
| `RMK_META_DIR` | `<RMK_DATA_DIR>/meta`; the RocksDB database with all object metadata (docs/metadata-store.md) |
| `RMK_LISTEN`, `RMK_METRICS_PORT` | `0.0.0.0:7000`, `9100` (`/metrics`, `/healthz`) |
| `RMK_CHUNK_CAPACITY_BYTES` | 1 GiB; a Part plus encryption overhead must fit in one chunk |
| `RMK_TLS_CERT`, `RMK_TLS_KEY`, `RMK_TLS_CA`, `RMK_INSECURE` | TLS required unless `RMK_INSECURE=true` (`rpc::load_tls`) |
| `RMK_PART_SIZE`, `RMK_MAX_PART_BYTES`, `RMK_MAX_BUFFERED_BYTES` | 64 MiB, 256 MiB, 2 GiB |
| `RMK_PENDING_UPLOAD_RETENTION_SECONDS` | 7 days |
| `RMK_MAINTENANCE_INTERVAL_SECONDS`, `RMK_COMPACTION_GARBAGE_PERCENT`, `RMK_COMPACTION_BYTES_PER_SECOND`, `RMK_COMPACTION_GRACE_SECONDS`, `RMK_REWRAP_BATCH` | `5`, `50`, 50 MiB, `600`, `1000` |

## 8. Errors on the wire

`rmk::Errc` ↔ gRPC: not_found→NOT_FOUND, already_exists→ALREADY_EXISTS,
precondition_failed / not_empty / expired→FAILED_PRECONDITION, invalid_argument→INVALID_ARGUMENT,
no_space / quota_exceeded→RESOURCE_EXHAUSTED, permission_denied→PERMISSION_DENIED,
unauthenticated→UNAUTHENTICATED, unavailable→UNAVAILABLE, aborted→ABORTED, corruption→DATA_LOSS,
io_error→INTERNAL. The status message is `"<errc name>: <message>"` so the exact Errc survives the
trip; `rpc::from_status` parses it back.
