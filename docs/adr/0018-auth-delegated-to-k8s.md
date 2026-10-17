---
status: accepted
---

# 认证与授权整体委托给 k8s，Gateway 不识别调用方

Gateway 不再认证调用方，也没有授权判断：谁能连上 Gateway，由 k8s 的 NetworkPolicy、mTLS 或服务网格决定。系统内没有 Principal、Policy、Bucket Policy、Access Key、ServiceAccount Token、JWKS 和 bootstrap token 这些概念，也没有“调用方身份”。每个请求都显式指明它操作的 Tenant，Tenant 为空的请求是 `invalid_argument`。保留的只有 TLS（`RMK_TLS_*`）、SDK 最低版本检查（`x-rmk-sdk-version`、`SetClusterConfig`）以及 Tenant 与 Quota。

本决定取代 ADR 0011（认证）和 ADR 0012（授权）。已删除的 RPC：RegisterServiceAccount、CreateAccessKey、SetPolicies、DeletePrincipal、ListPrincipals、SetBucketPolicy。已删除的环境变量：`RMK_BOOTSTRAP_TOKEN`、`RMK_K8S_ISSUER`、`RMK_TOKEN_AUDIENCE`、`RMK_K8S_JWKS_URL`。

## Considered Options

- **保留 ADR 0011/0012 的 Principal + Policy 体系**：粒度最细，但自研认证与策略求值器是热路径上的一大块代码，正确性全靠本项目测试；而 k8s 已经提供网络层的准入控制。被否决。
- **只保留一个共享 Token**：仍然要分发和轮换密钥，却没有任何隔离能力。被否决。

## Consequences

- 没有调用方身份：Gateway 无法区分同一个 Tenant 下的不同调用方，也无法按调用方审计或限制操作。
- 每个请求必须带 Tenant；Tenant 隔离靠调用方自觉，不由 Gateway 强制。能连上 Gateway 的任何工作负载都能读写任何 Tenant 的数据。
- Tenant 与 Quota 的管理 RPC 对所有能连上 Gateway 的调用方开放，因此必须用 NetworkPolicy 或服务网格限制能访问 Gateway 端口（7000）的 Pod。
- 不再有跨 Tenant 共享的授权模型；跨 Tenant 访问就是在请求里指明另一个 Tenant。
- 部署上不再需要 bootstrap Secret、投影 ServiceAccount Token、Gateway 读取 API server JWKS 的权限。
