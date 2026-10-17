---
status: accepted
---

# 大对象分段上传，支持断点续传与并行

状态：部分作废（2026-10-17）：分段上传与断点续传仍然有效；“由 Controller 回收其已写入的 Extent”现在由 realmucketd 的维护线程完成，“Metadata Store 单个事务的大小上限”不再来自 FDB。

在 k8s 中，客户端重启、网络闪断、Gateway 滚动升级每天都会发生，从头重传 GB 级对象不可接受。因此超过阈值（默认 64MB，可配置）的对象，由 Client SDK 自动切成 Part 上传。每个 Part 写入完成后，就登记到对应的 Pending Upload 中。上传中断后，客户端查询已经登记的 Part，只补传缺少的部分；多个 Part 也可以并行上传。所有 Part 到齐后，客户端发起完成请求，系统在一个事务中把 Pending Upload 提交为 Object（沿用 ADR 0007 的第 ③ 步），在此之前 Object 不可见。

## Considered Options

- **中断即从头重传**：没有额外设计，但大对象在不稳定的网络下可能永远传不完。
- **只支持并行、不支持续传**：速度快，但中断后仍要重传全部内容。

## Consequences

- Pending Upload 的过期期限改为“距离最后一次登记 Part 已满保留期”，保留期默认 7 天，可配置；到期后由 Controller 回收其已写入的 Extent。
- 单个 Pending Upload 登记的 Part 数量可能很多，完成提交时必须控制在 Metadata Store 单个事务的大小上限内（见 ADR 0002）。
