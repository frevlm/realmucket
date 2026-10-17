---
status: accepted
---

# 条件写比较 Generation，完整性靠 Checksum；Versioning 按 Bucket 开关，默认关闭

状态：部分作废（2026-10-17）：Generation、Checksum、Versioning 的语义不变；“直接使用 FoundationDB 事务的提交版本号（versionstamp 前 8 字节）”和“由 FDB 分配”这两处作废，现在 Generation 来自 RocksDB MetaStore 中持久化的计数器，同一 Object 内唯一，只增不减。见 docs/metadata-store.md。

每次 Object 提交时，系统分配一个 Generation：直接使用 FoundationDB 事务的提交版本号（versionstamp 前 8 字节）：同一 Object 内唯一且严格递增，全集群按提交顺序不减。条件写只比较 Generation：`if-generation-match` 要求当前 Generation 等于给定值，`if-not-exists` 等价于要求当前 Generation 为 0。内容完整性由单独的 Checksum（crc32c）负责，它不参与条件判断。这样就避免了用内容哈希做条件写时的 ABA 问题：内容被改走又改回，Generation 也一定不同。

Versioning 按 Bucket 单独开关，默认关闭。开启后，每个历史版本由它的 Generation 唯一标识，删除标记也有自己的 Generation。关闭时，覆盖和删除都会回收旧数据。

## Considered Options

- **内容哈希（ETag）作为条件写依据**：客户端只需理解一种标识，但存在 ABA 问题；开启 Versioning 后，内容相同的历史版本无法区分，删除标记也没有内容哈希。被否决。

## Consequences

- Generation 在第 ③ 步提交事务时由 FDB 分配，`if-generation-match` 的比较也在同一个事务内完成（见 ADR 0007）。
- crc32c 只用于发现意外损坏，不防恶意篡改。
