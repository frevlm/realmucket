---
status: accepted
---

# 元数据与数据分离：无状态 Gateway + Metadata Store + Data Node

系统要在 k8s 上支持高并发、按节点弹性扩容，并且要高效执行 Lifecycle Rule 和 Bucket 级操作（统计、清空、删除），所以选择三层分离：无状态 Gateway（Deployment + HPA）、强一致事务的 Metadata Store、每个节点独占 RWO 卷的 Data Node（StatefulSet）。Object 索引全部放在 Metadata Store，列举和生命周期匹配只做有序范围扫描，不需要扇出到 Data Node。

## Considered Options

- **对等节点、元数据与数据同处（MinIO 式）**：只有一种组件，运维最简单；但没有全局索引，列举要扇出再归并，生命周期要爬遍全部盘，对象到亿级时代价不可接受；扩容也只能按池整组进行。
- **多个无状态 Pod 共享 RWX 卷（NFS/CephFS）**：SQLite 在网络文件系统上锁不可靠，`rename` 原子性和 `flock` 语义取决于具体实现，等于把分布式的难题交给 NFS。否决。

## Consequences

- 每块盘同一时刻只归一个 Data Node 进程所有，单盘引擎不需要跨进程并发控制。
- 一次写入要跨 Metadata Store 与 Data Node 两个系统，必须单独设计两者之间的一致性协议（包括无主数据的回收）。
