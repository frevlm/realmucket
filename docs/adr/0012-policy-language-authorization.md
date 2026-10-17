---
status: superseded by ADR 0018
---

> 已被 [ADR 0018](0018-auth-delegated-to-k8s.md) 取代：鉴权整体委托给 k8s，本文不再描述现状。

# 授权使用策略语言（Allow/Deny、通配符资源、条件）

授权不采用固定角色，而是使用策略语言：每条 Policy 声明对哪些资源（Tenant、Bucket、Object Key，支持通配符）允许或拒绝哪些操作，并且可以附加条件。授权判断的输入只有 Principal、操作、资源和请求上下文，与认证方式无关（见 ADR 0011）。

策略格式和求值器自研：使用 JSON 格式，语义照搬 AWS IAM，即显式 Deny 优先于一切，其次是显式 Allow，其余默认拒绝。这样使用者不需要学习一套新规则，求值也能在 C++ 进程内完成，构建链不引入其他语言。

Policy 有两种挂载位置：绑定在 Principal 上（注册时指定，见 ADR 0011），以及挂在 Bucket 上的 Bucket Policy。同一 Tenant 内的访问，只要 Principal 的 Policy 允许即可。跨 Tenant 访问必须双方都允许：Principal 的 Policy 允许访问对方的资源（调用方的 Tenant 同意），并且目标 Bucket 的 Bucket Policy 也允许这个 Principal（数据所属的 Tenant 同意）。任何一方撤销，共享立即停止。Bucket Policy 只能由该 Bucket 所属 Tenant 的管理员编写。跨 Tenant 写入的数据计入 Bucket 所属 Tenant 的配额。

## Considered Options

- **只有 Tenant 级固定角色**：最简单，但无法做到“只读某一个 Bucket”，也无法跨 Tenant 共享。
- **固定角色，作用范围为 Tenant 或 Bucket（GCS IAM / k8s RBAC 式）**：可以覆盖常见场景，规则只有 Allow；但表达不了 Deny、Object Key 前缀级的权限以及条件。被否决。
- **Cedar**：求值器经过形式化验证，还有策略变更分析工具；但用 Rust 编写，没有官方 C++ 绑定，需要在构建链中引入 Rust 工具链。
- **跨 Tenant 只看 Bucket Policy**：调用方的 Tenant 无法限制自己的服务能读谁的数据，服务被入侵后无法从自己这一侧收紧。
- **禁止跨 Tenant 访问，需要时复制数据**：隔离最彻底，但数据和配额都会重复，副本之间也不会同步。
- **OPA (Rego)**：通常以 sidecar 方式部署，每个请求多一次网络调用，热路径延迟不可接受。

## Consequences

- 授权判断位于每个请求的热路径上，策略求值必须在 Gateway 进程内完成，并且要缓存从 Metadata Store 读取的 Policy。
- 求值器的正确性完全由本项目的测试保证，测试必须覆盖 Deny 优先、通配符匹配、条件运算符的边界情况。
- Deny 与通配符同时存在时，“为什么这个请求被拒绝”很难靠人工推断，所以必须提供一个解释某次判断结果的工具或 API。
