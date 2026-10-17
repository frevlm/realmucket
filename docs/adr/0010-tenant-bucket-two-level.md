---
status: accepted
---

# Tenant → Bucket 两级命名空间，配额挂在 Tenant 上

状态：部分作废（2026-10-17）：Tenant → Bucket 模型、key 前缀布局和软配额仍然有效；“用量计数用 FDB 原子加法维护”作废，现在由 MetaStore 的单写线程读改写计数器。见 docs/metadata-store.md。

Bucket 名字只在所属 Tenant 内唯一，不是全局唯一。Tenant 是独立存储在 Metadata Store 中的概念，和 k8s Namespace 没有绑定关系。第一期就提供 Tenant 级配额。Metadata Store 中所有 Object 索引的 key 以 `tenant/bucket/object_key` 为前缀，所以统计、迁移、删除一个 Tenant，都是对一段连续 key 区间的操作。

## Considered Options

- **Bucket 全局唯一（S3/GCS 式）**：模型最简单，但名字需要抢，配额和权限没有地方批量挂载。
- **直接把 k8s Namespace 当作 Tenant**：零配置，但把组织结构绑死在 Namespace 划分上，非 k8s 调用方无法归属。

## Consequences

- 每个请求都显式指明 Tenant，Tenant 为空的请求无效（见 ADR 0018）。
- key 前缀布局上线后无法修改。
- 配额是软配额。用量计数用 FDB 原子加法维护（写入加、删除减），判断时用快照读，不参与冲突检测，因此同一 Tenant 的并发写入不会争抢同一个热点 key。代价是用量可能超出配额，超出量约等于同一时刻在途的写入量。开启 Versioning 时，历史版本也计入用量。
