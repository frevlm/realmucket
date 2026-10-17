---
status: accepted
---

# 对外只提供自定义 gRPC API，不兼容 S3

Gateway 对外只暴露自定义的 gRPC API，Client SDK 是它唯一的官方客户端。选择 gRPC 是为了流式传输、强类型接口和多路复用，同时让 Object 语义（一致性、版本、错误模型）完全由本项目定义，不背 S3 的历史包袱（SigV4、XML、分片上传 ETag 规则、几十种错误码）。

## Considered Options

- **S3 兼容 HTTP API**：能直接接入 aws cli、rclone、Spark/Flink、Velero、Loki/Thanos 等现成生态；代价是长期维护 S3 语义兼容。被否决。
- **S3 + gRPC 双协议**：两套协议，语义必须保持一致，维护成本翻倍。被否决。

## Consequences

- 只认 S3 的第三方工具和框架都无法直接使用本系统，需要接入时只能经由 Client SDK 或另写适配层。
- 将来如果要补一层 S3 兼容，它只能作为 Gateway 之外的翻译层，不能反向改变 gRPC API 的语义。
