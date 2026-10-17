---
status: superseded by ADR 0018
---

> 已被 [ADR 0018](0018-auth-delegated-to-k8s.md) 取代：鉴权整体委托给 k8s，本文不再描述现状。

# 认证：集群内用 ServiceAccount Token，集群外用 Access Key；两者都必须先注册为 Principal

公司没有服务网格，也没有统一身份系统。集群内的 Pod 使用 k8s 自动挂载的、绑定 audience 的短期 ServiceAccount Token，Gateway 用集群公钥离线验证签名，不需要每次请求都调用 k8s API。集群外的程序和人使用 Access Key。

两种凭证都必须先注册才能使用：管理员注册一个 ServiceAccount（例如 `rec-prod/trainer`），或者创建一把 Access Key 时，必须同时指定它归属的 Tenant 和附带的 Policy。认证通过后，请求只会映射到已注册的 Principal；签名合法但未注册的 ServiceAccount 一律拒绝。授权只看 Principal，不关心它是怎么认证来的。所有连接都必须使用 TLS。

## Considered Options

- **mTLS + 服务网格证书（SPIFFE）**：公司没有服务网格，为了这个项目部署一套网格的代价太大。
- **只用 Access Key**：可以覆盖所有调用方，但集群内的大量服务也要分发、保管、轮换密钥，泄露的风险最大。
- **按 Namespace 推出 Tenant 并自动授权**：新服务部署后即可使用，但任何能在该 Namespace 创建 Pod 的人或程序都会自动获得该 Tenant 的数据权限，而且这条推导规则在 Policy 列表中看不到。

## Consequences

- 每上线一个新服务，都要先注册它的 ServiceAccount，这和创建 Access Key 是同一种操作。
- Access Key 是 Bearer Token：客户端在 TLS 连接上直接发送，服务端只存储它的 SHA-256 哈希，并在收到请求时比对。没有选择 HMAC 请求签名，因为 TLS 已覆盖传输安全，而 HMAC 需要服务端保存可还原的原始密钥，又没有现成的 KMS 来管理主密钥。
- Access Key 带固定前缀 `rmk_`，便于代码扫描工具发现泄露在仓库中的密钥；明文只在创建时显示一次。
- 一个 Tenant 可以同时持有多把 Access Key，以便轮换时新旧并存；Access Key 可以随时吊销。
- 以后如果公司有了统一身份系统，只需要新增一种“认证后转换为 Principal”的方式，授权逻辑不需要改动。
