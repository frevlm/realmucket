# Implementation guide

Engineering contract for phases 2–4 of `docs/PLAN.md`. Decisions live in `docs/adr/`,
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
tests/e2e/            cross-module tests
tests/store_contract/ contract suites shared by the memory and FoundationDB stores
```

| Module | Directory | Depends on |
|---|---|---|
| core (types, errors, names, config, env, crc32c, chunk file, store interfaces) | `libs/core` | — |
| stores (in-memory) | `libs/stores` | core |
| ec | `libs/ec` | core |
| crypto | `libs/crypto` | OpenSSL |
| http (+metrics) | `libs/http` | libcurl |
| fdb | `libs/fdb` | libfdb_c (Linux image / CI only) |
| rpc | `libs/rpc` | gRPC, all of the above |
| client (SDK, public header `rmk/client.hpp`) | `libs/client` | gRPC |
| datanode | `services/datanode` | rpc |
| gateway | `services/gateway` | rpc |
| controller | `services/controller` | rpc |
| rmk-dev | `apps/dev` | the three services |
| rmkctl | `apps/rmkctl` | client |

FoundationDB tests run where libfdb_c exists (the Linux dev image `docker/dev.Dockerfile`)
against a server started with `docker run -d --name rmk-fdb -p 4500:4500 foundationdb/foundationdb:7.3.69`
(then `fdbcli --exec "configure new single memory"` once) and `RMK_FDB_CLUSTER_FILE` pointing
at a cluster file `docker:docker@<host>:4500`. Without that variable the fdb tests skip.

Rules for all code: C++23, `-Wall -Wextra -Wpedantic -Werror` on AppleClang and GCC 13,
errors as `rmk::Result`/`rmk::Status` (no exceptions across module boundaries; catch library
exceptions at the edge), no global mutable state except metrics, comments say what, not history.

## 2. Names and limits

- Tenant and bucket names: 1–63 chars of `[a-z0-9._-]`, starting with a letter or digit.
- Object keys: 1–1024 bytes of valid UTF-8, no NUL.
- `list` limit: 1–1000.
- Violations are `invalid_argument`.

## 3. Store semantics (MetaStore, IdentityStore, ClusterStore)

All three have an in-memory implementation (`libs/stores/include/rmk/stores/memory_stores.hpp`) and a
FoundationDB implementation (`libs/fdb/include/rmk/fdb/fdb_stores.hpp`). Both must pass the shared
contract suite in `tests/store_contract/`, which is instantiated once per implementation.

MetaStore details:

- `create_bucket` needs the Tenant (`not_found` otherwise). Bucket config validation:
  `1 <= data_units`, `data_units + parity_units <= 20`.
- Quota (soft, ADR 0010): `commit_upload` fails `quota_exceeded` when
  `usage.bytes + size > max_bytes` or `usage.objects + 1 > max_objects`, reading usage without
  conflict (FDB snapshot read); usage is updated with atomic adds. Usage counts every stored
  version (not delete markers). Replacing a version without Versioning subtracts the old one.
- Generations: the memory store uses a counter; FDB uses the transaction's commit version
  (versionstamp bytes 0–7, big-endian) — unique per Object, non-decreasing cluster-wide.
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

ClusterStore:
- `update_chunk` is the only way to change a Chunk; it is a compare-and-set on `revision`.
- `claim_open_chunk` picks the qualifying Chunk with the lowest id; it sets `writer`,
  `writer_lease_expiry` and bumps `revision` atomically. Free space = `capacity - size`.
- `try_lead`: one record; the holder may renew while its lease lives; anyone may take over once
  it has expired, which increments the epoch.

## 4. Formats

- Stored Part = `nonce(12) || AES-256-GCM ciphertext || tag(16)`, associated data = part number
  as 4 bytes big-endian. A fresh random nonce per write, so re-sending a Part never reuses one.
- Wrapped Data Key = `nonce(12) || ciphertext(32) || tag(16)` under the Master Key, associated
  data `"rmk-data-key-v1"`.
- `Part.checksum` = crc32c of plaintext; `ObjectMeta.checksum` = `crc32c_combine` over parts.
  `Extent.crc32c` = crc32c of the stored bytes.
- Chunk files on a Data Node: `<data dir>/<chunk id as 16 hex>.r` for replicas and
  `<data dir>/<chunk id as 16 hex>.s<shard index>` for shards.

## 5. Access control (ADR 0018)

The Gateway does not authenticate or authorize callers; k8s does (NetworkPolicy, mTLS, mesh).
There is no caller identity: every request names its Tenant explicitly and an empty Tenant is
`invalid_argument`. The only per-request check is the minimum SDK version (`x-rmk-sdk-version`,
`SetClusterConfig`). Tenant and Quota administration RPCs are open to whoever can reach the
Gateway port.

## 6. Data path

### Writing (Gateway)

1. Check the Tenant is named and the minimum SDK version, load the Bucket.
2. `crypto::new_data_key`, then `begin_upload` with the wrapped key.
3. Cut the content into Parts of `RMK_PART_SIZE` (single `PutObject` streams too). Per Part:
   encrypt, then hand it to the **ChunkWriter** (`libs/rpc`), which:
   - holds writer leases on up to `RMK_WRITERS_PER_POLICY` open Chunks per Storage Policy
     (`claim_open_chunk`, lease `RMK_WRITER_LEASE_SECONDS`, renewed in the background);
   - on claiming, `Stat`s every replica and starts at the **largest** size seen (bytes a crashed
     writer left behind are never overwritten); any unreachable replica → seal and claim another;
   - writes the bytes to all replicas at the same offset (`Write`), requires every replica to
     answer with the same crc32c, then advances its offset and records it with `update_chunk`
     (size = offset, lease renewed) before the Extent is handed back — so ChunkInfo.size covers
     every registered Extent even if the Controller seals the Chunk meanwhile (a failed CAS
     retries the Part elsewhere). Each pool slot claims under its own name `<writer id>/<n>`;
   - when the next Part does not fit, or any replica fails, seals the Chunk
     (`update_chunk`: state sealed, size = its offset, writer cleared) and retries on another
     Chunk (3 attempts); `unavailable` when no open Chunk can be claimed.
4. `put_part` with the resulting Extent; finally `commit_upload` with the Precondition.

Only after every replica has acknowledged (fsynced) is an Extent registered; so registered bytes
never exceed any replica's size, and sealing at the writer's offset is safe.

### Reading (Gateway, Controller)

The **ChunkReader** (`libs/rpc`) reads an Extent given its ChunkInfo:
- replicated (open or sealed): try replicas in random order, verify `Extent.crc32c`, fall back
  on error or mismatch;
- erasure coded: `ec::map_range` → read pieces from shard nodes; if a shard is unavailable,
  read the same shard range from any `k` other shards and `Codec::reconstruct` it; verify crc.

A ranged GET decrypts every Part it overlaps in full (GCM authenticates whole Parts).

### Background (Controller, leader only)

Each loop (`RMK_CONTROLLER_INTERVAL_SECONDS`, default 5):
1. `try_lead` (TTL 15 s). Not leader → sleep.
2. Health: a node whose heartbeat is older than 30 s is *suspect*: seal every open Chunk on it.
   Older than `RMK_REPAIR_DELAY_SECONDS` (default 900, ADR 0009) it is *dead*: repair every
   Chunk listing it — replicas via `CopyChunkFrom` from a live replica onto a new node, shards
   via `RebuildShard` — then `update_chunk`; once nothing lists it, `remove_node`.
3. Open Chunk pool: for every Storage Policy in use (all Buckets' policies plus 1+2), keep
   `RMK_OPEN_CHUNKS_PER_POLICY` (default 4) open Chunks with free space ≥ half their capacity.
   New Chunk: `allocate_chunk_id`, three healthy nodes in distinct Failure Domains with most free
   space, `CreateChunk` on each, `put_chunk`.
   Failure Domain (ADR 0005): zone if healthy nodes span ≥ 3 zones, else k8s node if ≥ 3 distinct,
   else node id.
4. Garbage: `peek_garbage` → `add_garbage` per Chunk → `ack_garbage`.
5. `reclaim_expired`.
6. Erasure coding: a Sealed Chunk whose `target_policy` differs from 1+2 and whose replicas are all
   healthy → pick `k+m` nodes in distinct Failure Domains (fewer domains: allow reuse, logged) →
   `EncodeChunk` on one replica holder → `update_chunk` (erasure_coded, k, m, nodes) → delete the
   replica files.
7. Compaction (ADR 0016): skip any Chunk for which `has_pending_parts` is true. Otherwise an immutable Chunk with `garbage_bytes * 100 >= (size - 64) *
   RMK_COMPACTION_GARBAGE_PERCENT` (default 50). No live Extents → delete it. Otherwise copy each
   live Extent (stored bytes, unchanged) through a ChunkWriter owned by the Controller, then
   `relocate_part`; an `aborted` relocation turns the copy into garbage. Throttled by
   `RMK_COMPACTION_BYTES_PER_SECOND` (default 50 MiB/s).
8. Master Key rotation: `scan_versions` and `rewrap` versions whose `master_key_id` is not active
   (`RMK_REWRAP_BATCH`, default 1000 per loop).

### Safety rules (from the correctness review)

1. **Controller fencing.** The Controller renews its lease on its own thread every `lead_ttl/3`
   using the real clock, and every ClusterStore write it makes (`put_chunk`, `update_chunk`,
   `delete_chunk`, `remove_node`) carries `Fence{holder, epoch}` from its last successful
   `try_lead`. Before any destructive Data Node call (`DeleteChunk`, encoding, rebuild) it
   re-confirms leadership with `try_lead(now = Clock::now())`. After a lost CAS it re-reads the
   Chunk and deletes a file only if no current ChunkInfo lists that node for that file.
2. **Compaction grace.** Whoever seals a Chunk sets `sealed_at`. Compaction considers only
   Chunks sealed at least `RMK_COMPACTION_GRACE_SECONDS` (default 600) ago. A Gateway registers an
   Extent (`put_part`) only if less than half that grace has passed since the Data Nodes
   acknowledged it (measured with the steady clock); otherwise it writes the Part again. Together:
   every Extent of a Chunk is registered before the Chunk can be compacted.
3. **Lease hygiene.** A writer slot idle for one lease period releases its Chunk (recording its
   size). The Controller counts toward the pool only open Chunks whose lease is free.
4. **Internal mTLS.** With TLS configured, Data Nodes require and verify a client certificate
   signed by `RMK_TLS_CA`; Gateways and Controllers present `RMK_TLS_CERT`/`RMK_TLS_KEY` as
   their client identity. The internal CA must issue certificates only to realmucket components.
   k8s also ships a NetworkPolicy restricting the Data Node port to those components.
5. **Bounded memory and parts.** A Part is at most `RMK_MAX_PART_BYTES` (default 256 MiB, must be
   below the chunk capacity minus 92 bytes); larger is `invalid_argument`. A Gateway buffers at
   most `RMK_MAX_BUFFERED_BYTES` (default 2 GiB) across requests and answers `unavailable` beyond.
   At most `kMaxParts` (4096) Parts per upload. A writer seals non-fitting slots only when the
   bytes would fit an empty Chunk.
6. **Data Node file safety.** EncodeChunk works only on Sealed Chunks and writes every shard to a
   temporary file renamed into place — it never deletes an existing shard first. Deleting a file
   fsyncs its directory.
7. **Scrub.** Each Controller pass `Stat`s a bounded batch of Chunk holders (rotating cursor);
   a missing file or one shorter than ChunkInfo.size is treated like a dead holder and repaired.
8. **Bounded transactions.** `reclaim_expired` stops early when its transaction grows large
   (cumulative Parts), clearing receipts in a separate transaction; `list` reads only version
   headers and counts skipped delete markers against its limit.
9. **No silent leaks.** If `put_part` or `relocate_part` fails after bytes were written, the Gateway
   or Controller reports those bytes with `add_garbage`. A writer never shrinks a Chunk's size
   when tolerating a revision change (`size = max(stored, offset)`).
10. **Reads survive relocation.** If a read of an Extent fails with `not_found`, the Gateway
    re-reads the version's metadata once and retries with the new Extent.

## 7. Processes and configuration

Store selection (all processes): `RMK_FDB_CLUSTER_FILE` set → FoundationDB stores under key
prefix `RMK_FDB_PREFIX` (default `rmk`); otherwise in-memory stores, which only make sense
inside one process (`rmk-dev`).

| Variable | Process | Default |
|---|---|---|
| `RMK_LISTEN` | all | gateway `0.0.0.0:7000`, datanode `0.0.0.0:7001` |
| `RMK_METRICS_PORT` | all | 9100 (`/metrics`, `/healthz`) |
| `RMK_TLS_CERT`, `RMK_TLS_KEY` | gateway | none; required unless `RMK_INSECURE=true` |
| `RMK_GATEWAY_ID` | gateway | hostname |
| `RMK_MASTER_KEY_DIR` | gateway, controller | required (DirectoryKeyProvider) |
| `RMK_PART_SIZE` | gateway | 64 MiB |
| `RMK_WRITERS_PER_POLICY` | gateway | 4 |
| `RMK_WRITER_LEASE_SECONDS` | gateway, controller | 30 |
| `RMK_NODE_ID`, `RMK_ADVERTISE_ADDRESS`, `RMK_DATA_DIR` | datanode | required |
| `RMK_ZONE`, `RMK_K8S_NODE` | datanode | empty; with `RMK_K8S_ZONE_LOOKUP=true` the zone is read from the k8s Node label `topology.kubernetes.io/zone` |
| `RMK_CAPACITY_BYTES` | datanode | 0 = size of the filesystem |
| `RMK_CHUNK_CAPACITY_BYTES` | controller | 1 GiB |
| `RMK_PENDING_UPLOAD_RETENTION_SECONDS` | gateway | 7 days |
| `RMK_CONTROLLER_ID` | controller | hostname |
| others in §6 | controller | as listed |

Every process logs one line per event to stderr: `<RFC3339 time> <level> <component> <message>`.

- All three processes read `RMK_TLS_CERT`, `RMK_TLS_KEY`, `RMK_TLS_CA`, `RMK_INSECURE`
  (`rpc::load_tls`): servers present the certificate, outgoing internal calls trust the CA.
- `RMK_FDB_CLUSTER_FILE` may be read-only (a ConfigMap); `open_stores` copies it to a writable
  temporary file before opening, because the FDB client rewrites it when coordinators change.

## 8. Errors on the wire

`rmk::Errc` ↔ gRPC: not_found→NOT_FOUND, already_exists→ALREADY_EXISTS,
precondition_failed / not_empty / expired→FAILED_PRECONDITION, invalid_argument→INVALID_ARGUMENT,
no_space / quota_exceeded→RESOURCE_EXHAUSTED, permission_denied→PERMISSION_DENIED,
unauthenticated→UNAUTHENTICATED, unavailable→UNAVAILABLE, aborted→ABORTED, corruption→DATA_LOSS,
io_error→INTERNAL. The status message is `"<errc name>: <message>"` so the exact Errc survives the
trip; `rpc::from_status` parses it back.
