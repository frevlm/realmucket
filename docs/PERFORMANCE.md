# Performance

`scripts/bench-local.sh` builds an optimized `realmucketd`, runs it on a scratch directory and drives it
with `realmucket-bench` through the Client SDK (gRPC over loopback, plaintext). Defaults: small objects
of 16 KiB for 5 s per phase (set `--duration`), large objects of 32 MiB, concurrency 1, 16 and 256.
Every number below comes from that script unless noted; each row is one run.

**Read the numbers with the machine in mind.** The data files are flushed with `F_FULLFSYNC` on macOS
(about 3.4 ms here) and `fdatasync` on Linux (tens of microseconds to a millisecond, depending on the
device), so absolute PUT rates differ widely between the two. Re-run on the target hardware before
sizing anything. Metadata is still held in memory at this point, so a durable metadata write is not yet
part of any PUT figure.

Machine A: Apple M5, 10 cores, 16 GiB, macOS, APFS, loopback.

## Baseline (before any optimization)

Small objects, 16 KiB:

| conc | PUT ops/s | PUT p50 ms | PUT p99 ms | GET ops/s | GET p50 ms | GET p99 ms |
|---|---|---|---|---|---|---|
| 1 | 245 | 4.01 | 5.36 | 6322 | 0.15 | 0.34 |
| 16 | 283 | 53.19 | 142.29 | 20423 | 0.76 | 1.54 |
| 256 | 358 | 478.21 | 2968.48 | 14754 | 16.16 | 32.88 |

Large objects, 32 MiB each, one object per worker (256 workers used 4 MiB Parts, one in flight):

| conc | PUT MiB/s | GET MiB/s |
|---|---|---|
| 1 | 284 | 289 |
| 16 | 246 | 1087 |
| 256 | 202 | 530 (1 request refused: "gateway is buffering too much data") |

What the baseline says:

- Small PUT does not scale with concurrency (245 to 358 ops/s for 256 times the workers) and its
  latency at one worker equals one `F_FULLFSYNC`: every Part pays its own flush and the flushes queue.
- Large PUT is capped near 260 MiB/s. The software `crc32c` runs at 525 MiB/s and every stored byte is
  checksummed twice (plaintext checksum, Extent checksum), against 7.3 GiB/s for AES-256-GCM.

Not a regression, but worth knowing: 256 workers each uploading with 4 Parts of 8 MiB in flight ask the
Gateway to buffer 8 GiB, more than its 2 GiB budget (`RMK_MAX_BUFFERED_BYTES`), so most uploads are
refused with "gateway is buffering too much data". The 256-worker large rows therefore use
`--parallel-parts 1 --part-mib 4`.

## Step 1: hardware CRC32C

`crc32c()` now uses the ARMv8 CRC32 / x86 SSE4.2 instructions when the CPU has them
(`libs/core/src/crc32c.cpp`); the table version stays as `crc32c_portable()` and as the fallback.
Raw speed on machine A: 545 MiB/s before, 11258 MiB/s after.

Small objects, 16 KiB:

| conc | PUT ops/s | PUT p50 ms | GET ops/s | GET p50 ms | GET p99 ms |
|---|---|---|---|---|---|
| 1 | 240 | 4.02 | 11162 | 0.08 | 0.19 |
| 16 | 286 | 51.94 | 21991 | 0.70 | 1.25 |
| 256 | 343 | 506.99 | 19508 | 13.11 | 20.26 |

Large objects, 32 MiB:

| conc | PUT MiB/s | GET MiB/s |
|---|---|---|
| 1 | 1002 | 2374 |
| 16 | 708 | 4158 |
| 256 | 534 | 1741 |

Reading it: large transfers are 2 to 4 times faster and no longer limited by the checksum; small GET
at one worker nearly doubled (the Extent and plaintext checksums were a large share of a 0.15 ms read).
Small PUT is unchanged, as expected: it waits for the flush, not for the CPU. Single runs; the one-object
rows at one worker are noisy.

## Step 2: no lock held while copying bytes

`ChunkFile::reserve()` claims a range of the open Chunk with one atomic step and
`write_reserved()` fills it with no lock held; `ChunkStore::write` takes the shared lock for the claim and
the exclusive lock only to start a new Chunk, and `ChunkStore::read` releases its lock before the disk
read. Writers now copy their bytes in parallel and readers never wait for writers.

Small objects, 16 KiB:

| conc | PUT ops/s | PUT p50 ms | PUT p99 ms | GET ops/s | GET p50 ms | GET p99 ms |
|---|---|---|---|---|---|---|
| 1 | 251 | 4.00 | 4.98 | 11784 | 0.08 | 0.12 |
| 16 | 553 | 27.07 | 59.28 | 24253 | 0.66 | 1.01 |
| 256 | 871 | 285.71 | 663.06 | 21229 | 12.17 | 16.48 |

Large objects, 32 MiB:

| conc | PUT MiB/s | GET MiB/s |
|---|---|---|
| 1 | 1260 | 2506 |
| 16 | 1335 | 5129 |
| 256 | 792 | 3666 |

Reading it: with the lock gone, concurrent PUTs overlap their flushes and the operating system folds
some of them together, so small PUT roughly doubled at 16 and 256 workers (and p99 at 256 fell from
2.9 s to 0.66 s), and large PUT at 16 workers nearly doubled. One worker is unchanged: it still waits
for one flush. The flush is still issued once per Part; step 3 makes the batching explicit.

## Step 3: group commit

`ChunkStore` now lets writers that are in flight together share a flush. A writer claims a range,
copies its bytes in, and then waits until a flush covers it; the first waiting writer becomes the leader
and flushes once for everything that finished copying before the flush began (never past a range that is
still being copied), then wakes the others. `write()` still returns only after a flush that covers its
bytes, so durability is unchanged. The number of flushes is exported as `rmk_chunk_syncs_total`.

Small objects, 16 KiB:

| conc | PUT ops/s | PUT p50 ms | PUT p99 ms | GET ops/s | GET p50 ms | GET p99 ms |
|---|---|---|---|---|---|---|
| 1 | 258 | 3.97 | 6.40 | 11542 | 0.08 | 0.17 |
| 16 | 1283 | 11.86 | 20.67 | 24856 | 0.64 | 1.00 |
| 256 | 9645 | 24.78 | 61.14 | 20965 | 12.59 | 16.41 |

Large objects, 32 MiB (the 1 and 16 worker PUT rows are the median of three runs; each phase lasts
only 0.2 to 0.5 s, so single runs vary by a factor of two):

| conc | PUT MiB/s | GET MiB/s |
|---|---|---|
| 1 | 1136 | 2358 |
| 16 | 2765 | 3354 |
| 256 | 818 | 3151 |

Reading it: with 256 writers, small PUT went from 871 to 9645 ops/s and p99 from 663 ms to 61 ms. One
worker still waits one full flush (about 4 ms here), which is the floor for any durable write.

## Summary on machine A

| | baseline | step 1 CRC | step 2 no lock | step 3 group commit |
|---|---|---|---|---|
| small PUT ops/s, 1 / 16 / 256 workers | 245 / 283 / 358 | 240 / 286 / 343 | 251 / 553 / 871 | 258 / 1283 / 9645 |
| small GET ops/s, 1 / 16 / 256 workers | 6322 / 20423 / 14754 | 11162 / 21991 / 19508 | 11784 / 24253 / 21229 | 11542 / 24856 / 20965 |
| large PUT MiB/s, 1 / 16 / 256 workers | 284 / 246 / 202 | 1002 / 708 / 534 | 1260 / 1335 / 792 | 1136 / 2765 / 818 |
| large GET MiB/s, 1 / 16 / 256 workers | 289 / 1087 / 530 | 2374 / 4158 / 1741 | 2506 / 5129 / 3666 | 2358 / 3354 / 3151 |

All of this still runs with metadata in memory. A durable metadata commit adds a second flush to every
PUT, so it must be batched the same way; that is the requirement for the metadata engine.

## Metadata engines (tools/metabench)

`tools/metabench` (CMake option `RMK_BUILD_METABENCH`) commits the transactions of one PUT to candidate
engines, with the key layout and sizes of `docs/metadata-layout.md`, into a database that already holds
1,000,000 objects. A PUT is the three `MetaStore` calls the Gateway makes: `begin_upload` and `put_part`
(which need not be on disk when they return) and `commit_upload` (which must). All engines sit behind the
same single writer thread that gathers whatever queued up into one commit (group commit). For engines
whose flush is a separate step (LMDB, RocksDB) the flush runs on its own thread, so the writer never waits
for a flush; SQLite's flush is part of its commit.

Honest limits of the numbers:

- Machine A, macOS; flushes cost about 3.2 to 3.8 ms (`F_FULLFSYNC`). A durable commit with one writer
  measured 6.1 ms (SQLite), 5.4 ms (LMDB) and 7.2 ms (RocksDB); each is checked to reach the disk
  before its numbers count. Apple's own `libsqlite3` does not (0.4 ms): the build must use its own SQLite.
- The shared writer thread tops out near 9,000 to 30,000 PUT/s (the in-memory engine's own figure). At 256
  workers RocksDB (7754) and LMDB (5878) are near that ceiling, so they cannot be told apart there.
- Single 5 s runs; tail figures (p99.9, max) move a lot between runs.

Durable PUT (only `commit_upload` flushed), full PUTs per second and p99 in ms:

| workers | memory (ceiling) | SQLite | LMDB | RocksDB |
|---|---|---|---|---|
| 1 | 42410 | 195 (10.5) | 170 (12.0) | 120 (12.0) |
| 16 | 32023 | 849 (34.0) | 1690 (16.2) | 565 (35.1) |
| 256 | 8733 | 2870 (397.7) | 5878 (84.0, p99.9 978) | 7754 (50.1, p99.9 55.9) |

Reads, 1,000,000 objects, operations per second (p99 ms) at 16 and 256 workers:

| | SQLite | LMDB | RocksDB |
|---|---|---|---|
| GET 16 / 256 | 93033 / 58062 (0.91 / 66.9) | 2479611 / 2587185 (0.01 / 0.00) | 553597 / 698342 (0.02 / 3.40) |
| LIST 100 keys, 16 / 256 | 128593 / 71260 | 2082651 / 2225224 | 356412 / 367785 |

Mixed, 80 % GET and 20 % PUT, 256 workers: PUT/s (GET/s) SQLite 3357 (13452), LMDB 12396 (49659),
RocksDB 10271 (41201).

Database size on disk after loading the 1,000,000 objects: SQLite 395 MiB, RocksDB 382 MiB, LMDB 655 MiB.
Loading took 25.7 s, 4.2 s and 7.8 s.

Two more sets of cells, run separately (so the figures differ a little from the table above; runs of the
same cell moved by up to 2x):

Flush inside the commit, the mode that is safe for LMDB (its `MDB_NOSYNC` mode, needed to flush on another
thread, can corrupt the database if the machine crashes), durable PUT/s with p99 in ms:

| workers | LMDB | RocksDB |
|---|---|---|
| 1 | 189 (6.1) | 283 (5.0) |
| 16 | 717 (26.0) | 1177 (24.1) |
| 256 | 8012 (35.0) | 10298 (37.3) |

No group commit (every transaction commits and flushes on its own), durable PUT/s: SQLite 233 / 232,
LMDB 198 / 199, RocksDB 251 / 252 at 16 / 256 workers. A flush costs 3 to 4 ms whatever the engine, so
without batching every engine is stuck near 250 PUT/s however many workers there are; with it,
256 workers reach 2870 (SQLite) to 10298 (RocksDB), 11 to 41 times more.

### What it means

- SQLite fails the rule set in advance (at least half of the best durable PUT rate at 256 workers and no
  worse read tail): 37 % of the best, and GET falls from 93 thousand to 58 thousand per second between 16 and
  256 workers with a p99 of 67 ms.
- Write side, safe modes: RocksDB is ahead of LMDB at every concurrency and keeps the flattest tail.
- Read side: LMDB is 4 to 6 times faster than RocksDB, but RocksDB still serves 550 to 700 thousand GET/s,
  far more than the data path can feed it (11 to 25 thousand objects per second in the benchmark above).
- Space: RocksDB 382 MiB against 655 MiB for LMDB at 1,000,000 objects; RocksDB loads 2 to 6 times faster.
- LMDB 1.0.2 accepts keys up to 8122 bytes; the 0.9.x packages most Linux distributions ship stop at 511,
  which is below our longest keys.
- Not covered: all of this fits in memory (a 1,000,000 object database is under 1 GB). Random inserts into a
  B-tree (LMDB, SQLite) get slower once the database outgrows memory while an LSM tree (RocksDB) holds its
  write rate, so the gap should widen in RocksDB's favour; that still has to be measured with 20 to 50
  million objects.

## Step 4: durable metadata in the daemon (RocksDB)

`realmucketd` now keeps all object metadata in RocksDB (`docs/metadata-store.md`), flushed to disk before
`commit_upload` returns; the numbers above ran with metadata in memory. Same script, same machine.

Small objects, 16 KiB:

| conc | PUT ops/s | PUT p50 ms | PUT p99 ms | GET ops/s | GET p50 ms | GET p99 ms |
|---|---|---|---|---|---|---|
| 1 | 128 | 7.90 | 10.09 | 11087 | 0.09 | 0.13 |
| 16 | 401 | 40.73 | 55.97 | 23748 | 0.67 | 1.04 |
| 256 | 4359 | 57.57 | 88.69 | 21118 | 12.25 | 17.19 |

Large objects, 32 MiB: PUT 686 / 541 / 829 MiB/s and GET 1584 / 4553 / 3112 MiB/s at 1 / 16 / 256 workers
(single short runs, they vary a lot).

Reading it: GET and large transfers are untouched. A small PUT now waits for two flushes one after the other,
the chunk bytes first and then the metadata commit, so one worker takes about 8 ms (twice the 4 ms before) and
the rates at 16 and 256 workers are roughly 0.4 to 0.5 of the in-memory-metadata figures (1283 and 9645). The
order is not negotiable (metadata must never point at bytes that are not on disk), so the next step would be
to make one flush serve both, for example by writing the commit record into the chunk the bytes went to;
on a Linux server where a flush costs a fraction of a millisecond this matters much less.

Process-crash check: 40 objects written one by one, daemon killed with `kill -9`, restarted: all 40 intact.
(A power cut is not simulated.)

## Step 5: small Parts inside the metadata database (inline Parts)

A Part up to `RMK_INLINE_MAX_BYTES` is written into the RocksDB write-ahead log together with its
registration instead of into a Chunk, so a small PUT needs one flush (the commit) instead of two (chunk, then
commit). Same script, same machine, 16 KiB objects, 8 s per phase, `RMK_INLINE_MAX_BYTES=0` against 32768 in
the same session:

| conc | PUT ops/s off | PUT ops/s on | PUT p50 ms off | PUT p50 ms on | GET ops/s off | GET ops/s on | GET p99 ms off | GET p99 ms on |
|---|---|---|---|---|---|---|---|---|
| 1 | 121 | 227 | 8.07 | 4.15 | 10865 | 10946 | 0.17 | 0.16 |
| 16 | 378 | 837 | 42.33 | 19.24 | 23328 | 23375 | 1.16 | 1.10 |
| 256 | 4280 | 7360 | 57.52 | 33.66 | 20026 | 17904 | 24.98 | 27.65 |

PUT is 1.7 to 2.2 times faster; GET is unchanged up to 16 workers and about 10 % lower with a 10 % worse p99 at
256 (single short runs; they vary by that much on their own). The decision rule set beforehand (at least 1.6
times at 1 and 16 workers, GET p99 no more than 20 % worse, no loss after `kill -9`) is met.

Where it stops paying (6 s runs, PUT ops/s inline against Chunk, 1 / 16 / 256 workers):

| object size | inline | Chunk |
|---|---|---|
| 4 KiB | 248 / 936 / 7788 | 125 / 369 / 4166 |
| 64 KiB | 220 / 853 / 5488 | 132 / 408 / 4001 |
| 128 KiB | 211 / 860 / 3677 | 143 / 436 / 3703 |

At 128 KiB with 256 writers the disk bandwidth is the limit and the two are equal, so the default stays at
32 KiB; 64 KiB would also pay.

Sustained 25 s at 256 workers, 16 KiB objects: inline plain values 6142 PUT/s, database 1889 MiB; inline with
RocksDB BlobDB (`min_blob_size` 4096) 5479 PUT/s with a 1.36 s p99.9 and 2209 MiB; no inline 4219 PUT/s. BlobDB
was rejected. Not measured: write amplification over hours of sustained writing, when compaction of the LSM
levels has to rewrite the inline values; watch disk write volume against payload on a long run before raising
the threshold.
