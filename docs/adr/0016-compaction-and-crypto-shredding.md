---
status: accepted
---

# 删除即永久不可读：每个 Object 单独加密，删除时销毁钥匙；空间按废弃比例异步回收

删除必须立刻、永久生效，不能让内容在磁盘上残留到下一次整理。但在 Chunk 模型中，删除 Object 只会留下空洞，物理空间要等 Compaction 才能回收，而冷数据所在的 Chunk 可能几个月都不会被整理。在 SSD 上原地覆写也无法保证物理擦除：磨损均衡会把旧数据留在其他闪存页上。因此每个 Object 写入时都用一把独立随机生成的 Data Key 加密，Data Key 存在 Metadata Store 的 Object 索引中；删除 Object 时，在同一个事务里删掉 Data Key。事务提交后，盘上残留的密文就永久不可解密。

物理空间回收和删除语义因此彻底分开：一个 Chunk 中已删除内容的占比超过阈值（默认 50%，可配置）时，Controller 安排 Compaction，把仍然有效的 Extent 复制到新的 Chunk，再释放旧 Chunk。每块盘的 Compaction 速度有上限（可配置），避免影响在线读写。

## Considered Options

- **删除时原地覆写数据**：对纠删码 Chunk 会破坏校验块；在 SSD 上也无法保证物理擦除。
- **删除后限期强制 Compaction**：冷数据和纠删码 Chunk 的重写代价极高，而且 SSD 上同样无法保证物理擦除。
- **定时 Compaction**：k8s 上的业务不分昼夜，所有盘在同一时段整理还会造成压力集中。

## Consequences

- 每次读写都需要加解密，Gateway 要承担这部分 CPU 开销（AES-GCM 有硬件指令支持）。
- 开启 Versioning 的 Bucket 中，普通删除只会生成删除标记，历史版本及其 Data Key 仍然保留；只有删除指定版本，或者在 Versioning 关闭时删除，才会销毁 Data Key。
- 销毁的效果依赖 Data Key 在 Metadata Store 中被真正清除，所以 Metadata Store 的备份不能长期保留已删除的 Data Key；Data Key 本身也必须再经过主密钥加密后才能存储，主密钥的管理方式另行决定。
