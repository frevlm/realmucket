---
status: accepted
---

# Metadata Store 先用 FoundationDB，并封装在 MetaStore 接口后面

状态：已被取代（2026-10-17）：单机化后 Metadata Store 改用单机 RocksDB，不再使用 FoundationDB，也不再规划自研 Multi-Raft；仍然有效的只有“元数据访问只经过 `MetaStore` 接口”。见 docs/metadata-store.md。

项目长期投入，最终目标是像 Azure Partition Layer、Meta ZippyDB 那样自研分片事务 KV。但共识层最容易出现丢数据级别的 bug，所以第一阶段用 FoundationDB（严格可串行化事务、原生 C API、有 k8s operator）尽快得到可靠的 Metadata Store。所有元数据访问都只经过项目自有的 `MetaStore` 接口，FDB 的类型和语义不能泄漏到接口之外，以便以后替换成自研 Multi-Raft 实现而不影响上层。

## Considered Options

- **TiKV**：国内生产案例多，但 C++ 客户端不成熟，等于要自己维护一个客户端。
- **一开始就自研 Raft + RocksDB**：没有外部依赖，但要先花一两年把共识、分片调度、成员变更写对，才能交付第一个可用版本。
- **分库分表 MySQL**：团队熟悉，但跨分片事务难做，大 Bucket 列举性能会退化，扩容需要迁移数据。

## Consequences

- `MetaStore` 接口的事务语义必须按“严格可串行化 + 有界事务大小”来设计；大 Bucket 删除这类操作在接口层面就要支持分批完成。
- 前提是系统只作为自家 k8s 上的内部服务运营；FDB 由运维团队用 `fdb-kubernetes-operator` 管理。
