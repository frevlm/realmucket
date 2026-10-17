---
status: accepted
---

# Object 聚合进追加写 Chunk，冗余按 Storage Policy 混合三副本与 EC

状态：部分作废（2026-10-17）：“Object 追加写入大 Chunk，位置记为 Extent，删除留下空洞由 Compaction 回收”仍然有效；三副本、纠删码、Storage Policy、Failure Domain 分散全部作废，Chunk 现在是本地盘上的单份文件，写满后封存为只读，由每个 Extent 的 crc32c 发现损坏。见 docs/IMPLEMENTATION.md。

Data Node 不按“一个 Object 一个文件”存储，而是把 Object 追加写入大的 Chunk 文件，Object 在 Chunk 里的位置记为 `(chunk_id, offset, length)`，登记在 Metadata Store。Chunk 写入期间用三副本；写满并封存后，后台按所属 Bucket 的 Storage Policy 决定保持三副本，还是转成纠删码。这一做法沿用 Meta Haystack/f4/Tectonic 的思路：既控制盘上文件数，又让 EC 以 Chunk 为单位编码，绕开小对象 EC 的碎片开销。

## Considered Options

- **全部三副本**：路径最简单，但存储成本是 3 倍。
- **全部 EC、每个 Object 单独编码**：成本最低，但小对象的碎片和元数据开销太大，写延迟高。

## Consequences

- 删除 Object 只是在 Metadata Store 里删掉索引，Chunk 中留下空洞；必须有后台压缩（compaction）来回收空间，而对已转成 EC 的 Chunk 做压缩代价很高。
- 同一个 Chunk 的副本或 EC 碎片必须分布在不同的故障域。故障域的粒度从 k8s 拓扑标签自动识别：集群中有 3 个或更多可用区（`topology.kubernetes.io/zone`）时按可用区分散，否则按节点分散，不需要人工配置。
