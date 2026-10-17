---
status: accepted
---

# 放置与修复由无状态 Controller 负责，状态存 FDB，不在数据路径上

状态：已被取代（2026-10-17）：单机化后不再有 Controller、Open Chunk 池、心跳、leader 租约和修复；压缩与 Pending Upload 回收由 realmucketd 内的维护线程完成。见 docs/IMPLEMENTATION.md。

Data Node 成员管理、Chunk 放置、副本修复、EC 转换、压缩和 Pending Upload 回收，统一由 Controller 负责。Controller 是一个 k8s Deployment（多副本，只有 leader 工作），自身不保存任何状态，全部状态都存在 FDB 中。leader 租约也记录在 FDB 中而不是 k8s Lease：租约与它保护的状态在同一个数据库里，换主时的纪元（epoch）递增，便于在同一事务内判断自己是否仍是 leader。Controller 不在数据路径上：它提前为 Gateway 分配一批 Open Chunk，Gateway 从 FDB 读取并直接写入 Data Node；读请求只经过 Gateway、FDB 和 Data Node。因此 Controller 的工作量只和集群规模、故障次数成正比，和请求量无关。这和 kube-scheduler 与 etcd 的关系是同一种结构。

## Considered Options

- **专用有状态 Master（HDFS NameNode / SeaweedFS master 式）**：放置决策可以全在内存中完成，但多出一个需要自己做主备的有状态组件，而它的状态本来就已经在 FDB 里了。
- **去中心化 gossip + 一致性哈希（Dynamo 式）**：回到了 ADR 0001 否决的对等节点路线，并发修复时多个节点可能同时修同一个 Chunk。

## Consequences

- Data Node 定期向 FDB 写心跳（存活状态、剩余容量、从 k8s 拓扑标签读到的 Failure Domain）。
- 节点失联超过修复延迟才开始修复。修复延迟默认 15 分钟，可通过环境变量覆盖，用来避免 Pod 重启或滚动升级时触发大量无谓的修复流量。
- Controller 不可用期间，读写不受影响；新 Chunk 分配和修复暂停，直到新的 leader 接任。所以 Open Chunk 池的余量必须能覆盖 leader 切换的时间。
