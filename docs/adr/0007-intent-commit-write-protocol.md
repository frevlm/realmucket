---
status: accepted
---

# 写入协议：意图记录 + 提交，支持条件写

一次写入分三步：① Gateway 在 Metadata Store 写入 Pending Upload（包含 upload id、预分配的 Extent、deadline）；② 把字节写入 Data Node 上的 Chunk；③ 用一个 Metadata Store 事务删除 Pending Upload 并写入正式的 Object 索引。Object 只有在第 ③ 步提交后才可见，提交后立即全局可读。崩溃留下的孤儿数据，靠扫描已经超过 deadline 的 Pending Upload 来精确回收，回收代价只和崩溃次数成正比，不和数据总量成正比。

条件写（`if-generation-match`、`if-not-exists`）的判断放在第 ③ 步的提交事务里，不在第 ① 步做，否则两个并发写入都可能通过检查。没有附加条件的并发覆盖，按提交顺序由最后提交者生效；被覆盖的旧 Extent 在同一个事务里登记为待回收。

## Considered Options

- **先写数据、后提交索引，不记录意图**：写路径最短，但孤儿只能靠全量对比 Chunk 和 Metadata Store 来找出，百亿对象时不可接受。
- **由 Data Node 发起提交**：Data Node 需要访问 Metadata Store，违反了 ADR 0001 中“Data Node 只认字节、不认名字”的边界。

## Consequences

- 每次写入多一次元数据事务，小对象的写延迟增加约一个 FDB 提交的时间。
- 大对象上传期间，每写完一个 Extent 就续期一次 Pending Upload 的 deadline，避免长时间上传被误当成孤儿回收。
- 条件写不满足时，第 ② 步已经写入的字节由 Pending Upload 的回收流程清理。
