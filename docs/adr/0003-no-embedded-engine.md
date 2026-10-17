---
status: accepted
---

# 不提供嵌入式存储引擎，hpp 接口只做 Client SDK

最初的需求是“对外提供 hpp 和网络接口两种方式”。但系统作为自家 k8s 上的内部服务运营，并且 ADR 0001 规定每块盘只归一个 Data Node 进程所有，所以业务进程不能直接打开存储。hpp 接口因此只是 Client SDK：业务进程链接它，通过网络访问 Gateway。两种接入方式背后是同一套集群和同一套语义。

## Consequences

- 不存在“单机嵌入模式”。本地开发和测试用一个单副本的最小集群（Gateway + FDB + 单个 Data Node），而不是一个进程内引擎。
