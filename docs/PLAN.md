# realmucket 分阶段实现计划

依据 `docs/adr/` 中的 17 份决策和 `CONTEXT.md` 中的术语制定。每个阶段结束时，系统都处于“可以运行、可以验收”的状态，后一阶段只在前一阶段的基础上增加能力，不推翻前一阶段。

## 阶段 1：核心库与单盘存储引擎

不依赖网络、FoundationDB 和 k8s，只用单元测试验证最底层的数据格式和写入语义是否正确。后面所有阶段都建在这一层之上，所以它最先做、也最需要做对。

| 内容 | 依据 |
|---|---|
| 统一的错误类型、核心类型（Bucket、Object、Extent、Generation 等） | CONTEXT.md |
| 环境变量配置：非法值拒绝启动，未设置时使用默认值 | ADR 0006 |
| Chunk 文件格式 v1：头部自描述（格式版本、冗余参数）、追加写、按 Extent 读取并校验 crc32c；能读取所有不高于当前版本的格式 | ADR 0005、0006、0015 |
| `MetaStore` 接口，外加一个内存实现：Bucket 管理、三步写入协议、Generation 与条件写、列举分页、Pending Upload 过期回收、待回收 Extent 登记 | ADR 0002、0007、0008、0010 |

**验收**：`ctest` 全部通过。

## 阶段 2：单副本端到端可用

本地起一个最小集群（Gateway + FDB + 单个 Data Node），业务程序可以通过 Client SDK 完成上传、下载、删除、列举。

| 内容 | 依据 |
|---|---|
| FDB 版 `MetaStore`，与内存版跑同一套接口测试 | ADR 0002 |
| Data Node 服务：内部 gRPC，提供追加、读取、落盘 | ADR 0001 |
| Gateway 对外 gRPC API（Bucket、Object 基本操作）与 Client SDK（hpp） | ADR 0003、0004 |
| 每个 Object 用 Data Key 加密，Master Key 通过 `KeyProvider` 从 k8s Secret 读取 | ADR 0016、0017 |
| 鉴权委托给 k8s（NetworkPolicy/mTLS/服务网格），每个请求显式指明 Tenant | ADR 0018 |
| Tenant 与软配额 | ADR 0010 |

**验收**：端到端测试覆盖上传、下载、条件写、删除后内容不可读、未指明 Tenant 的请求被拒绝。

## 阶段 3：分布式与容错

| 内容 | 依据 |
|---|---|
| Controller：Data Node 心跳、Open Chunk 池、按 Failure Domain 放置（可用区自动识别）、修复（默认延迟 15 分钟） | ADR 0005、0009 |
| 三副本写入与读取时的副本切换 | ADR 0005 |
| 分段上传与断点续传（默认阈值 64MB，半成品保留 7 天） | ADR 0013 |
| k8s 部署清单：Gateway（Deployment + HPA）、Data Node（StatefulSet）、Controller（Deployment + Lease） | ADR 0001、0009 |

**验收**：杀掉任意一个 Data Node 时读写不中断；节点失联 15 分钟后，副本自动补齐。

## 阶段 4：成本与运维

| 内容 | 依据 |
|---|---|
| Storage Policy；Sealed Chunk 转为纠删码 | ADR 0005 |
| Compaction（废弃比例超过 50% 触发，限速） | ADR 0016 |
| Versioning（按 Bucket 开关，默认关闭） | ADR 0008 |
| Master Key 轮换 | ADR 0017 |
| 监控指标；按 SDK 版本统计调用量；配置最低支持的 SDK 版本 | ADR 0015 |

## 明确推迟

Lifecycle Rule、异地复制（ADR 0014）、自研元数据存储（ADR 0002）、钥匙保管服务（ADR 0017）。

## 现状（2026-10-16）

阶段 1–4 已实现。验证情况：

- macOS 本机（AppleClang）：439 个测试全部通过。
- Linux（GCC 13，Release）：652 个测试全部通过，其中包括 228 个针对真实 FoundationDB 7.3.69 的测试。
- `scripts/e2e-compose.sh` 通过：用真实二进制和 FoundationDB 起一个 4 节点集群，跑通 100 MiB 分段上传，并在停掉一个 Data Node 后仍能读回。
- `kubectl kustomize deploy/k8s` 能正常渲染。
- 在本机 kind 集群上做了一次真实 k8s 部署（见下一节），所有检查项都通过。

### kind 端到端验证（2026-10-16）

环境：kind v0.33 / k8s v1.37，1 个 control-plane + 3 个 worker，worker 分别打了 `topology.kubernetes.io/zone` = z1/z2/z3；Docker Desktop 只有 4 核、约 4 GiB 内存。官方 fdb-kubernetes-operator v2.37.0（helm chart，装在 `realmucket` 命名空间），cert-manager v1.21.2。Master Key 用 `openssl rand` 现生成，直接 `kubectl create secret` 建在集群里，没有写进仓库。

为了在 4 GiB 的机器上装得下，测试时多加了一层 kustomize overlay（只在本机，不进仓库）：删掉 `secrets.yaml` 里的两个占位 Secret（真 Secret 已经另外建好，不能被占位值覆盖）；把三个组件的 requests 降到 100m CPU / 128Mi；`RMK_REPAIR_DELAY_SECONDS` 设为 60。FoundationDB 也打了补丁：requests 100m / 256Mi、limits 1Gi，`memory=1GiB`、`cache_memory=128MiB`（operator 默认每个进程要 1 核 8 GiB）。

检查结果：

| 检查项 | 结果 |
|---|---|
| 所有 pod 就绪 | 通过（FDB 3 storage + 3 log，2 gateway，2 controller，3 datanode） |
| Data Node 从 Node 标签读到 zone | 通过：FDB 里的节点记录是 worker→z1、worker2→z2、worker3→z3 |
| 内部 mTLS | 通过：pod 里没有 `RMK_INSECURE`；不带客户端证书调用 Data Node 的 gRPC 返回 `UNAUTHENTICATED: a client certificate signed by the cluster CA is required`，带集群证书则返回 OK |
| NetworkPolicy | 通过：没有组件标签的 pod 连 7001 超时，9100（metrics/healthz）仍可访问 |
| put / get / 分段上传 | 通过：140 MiB 文件分 3 段上传，读回的 sha256 一致 |
| 删掉一个 Data Node 的 pod 和 PVC | 通过：(a) 让它离线超过修复延迟（先 cordon 所有 worker）：心跳停 30 秒后，Controller 封存它参与的 open Chunk；60 秒后把副本修复到别的节点，并删除这个死节点；期间 43 次读全部成功。(b) 让它带着空盘立刻回来（StatefulSet 默认行为，约 10 秒）：scrub 发现文件丢失，修复前的代码修不了（见下面第 3 条修复），修复后 33 秒内把 7 个副本拷了回来 |

发现并已修复的问题：

1. `deploy/k8s/fdb/foundationdbcluster.yaml` 缺少 FDB pod 的 ServiceAccount 和 RBAC。operator 默认使用 unified image（`fdb-kubernetes-monitor`），它要靠修改自己 Pod 的 annotation 来上报 locality；用 default ServiceAccount 时没有权限，operator 一直报 `Could only select 0 processes, but 3 are required`，集群永远初始化不了。已加上 ServiceAccount、Role、RoleBinding（pods 的 get/list/watch/update/patch，与 operator 官方示例一致），并在 `processes.general.podTemplate` 里指定这个 ServiceAccount。
2. Gateway 和 Controller 读不到 Master Key：Secret 以 0400 挂载，文件属主是 root，而这两个 pod 没有设置 `fsGroup`（Data Node 设置了），所以 uid 10001 读取失败，启动时报 `cannot read /etc/realmucket/master-keys/active`。已在 `gateway.yaml` 和 `controller.yaml` 的 pod securityContext 里加上 `fsGroup: 10001`，文件变成 `0440 root:10001`。
3. Data Node 带着空盘以同一个 ID 回来后，丢失的副本修不回来。scrub 能发现文件丢失，但 `replacement()` 会排除这个 Chunk 的所有持有者，其中也包括丢文件的那个节点本身。只有 3 个 Data Node 时就再也找不到目标节点，日志不停地报 `no healthy node with room`，副本数一直停在 2/3。已修改 `src/controller/controller.cpp`：其他节点都放不下时，把这个槽位原来的持有者作为最后的选择。新增测试 `ControllerTest.ScrubRestoresAMissingReplicaOnItsOwnHolderWhenNoOtherNodeFits`，确认它在修复前失败、修复后通过。

观察到但没有修改的问题：

- 故障期间个别读会卡大约 22 秒。Data Node 的 pod 被删后，Gateway 到它旧 IP 的连接会一直等到 gRPC 默认的 20 秒最小连接超时，然后才换下一个副本。读最终都成功了，健康时一次读约 10 毫秒。机器整台宕掉时也会出现同样的情况，以后可以考虑调小 Data Node 通道的连接超时，或者对读请求做 hedging。
- 本机的 DNS 代理用了 fake-IP（198.18.0.0/15）。pod 重建的那几秒里，headless 名字查不到，查询被转发到上游，返回了一个假 IP，Controller 短暂连到 198.18.x.x，报 `Socket closed`，随后自行恢复。这是本机环境造成的，真实集群的上游 DNS 会直接返回 NXDOMAIN。
- 在同一台 Docker VM 里一边跑 kind 一边编译镜像时，内存被吃满，CoreDNS 的存活探针失败、重启了约 10 次，这段时间读请求报 `DNS resolution failed`。这是环境问题，不是 realmucket 的问题。
- `topology.kubernetes.io/zone` 上的反亲和只是 preferred（软约束）。PVC 被删掉、重新调度后，datanode-0 和 datanode-1 落到了同一个 zone，这时 Controller 按 ADR 0005 退化成按节点 ID 划分故障域。如果需要硬性跨 zone，应该改成 required，或者使用 topologySpreadConstraints。
- `kind load docker-image` 在 Docker Desktop 的 containerd 镜像存储下会报 `content digest ... not found`，要改用 `docker save --platform linux/arm64` 加 `kind load image-archive`。
- 客户端如果要信任 Gateway 的自签名 CA，需要把 `ca.crt` 挂进 pod（测试时用的是 ConfigMap）。

两轮独立审查（安全审查、数据面正确性审查）发现的 critical 和 high 问题都已修复，修复后的规则写在 `docs/IMPLEMENTATION.md` §6 的 "Safety rules"。

已知限制，尚未验证或未处理：

- 只在 kind 上做过 k8s 部署，还没有在多机的真实集群、真实存储类和真实故障域下跑过。
- 纠删码转换完成后，如果删除旧副本文件失败，残留的文件不会被重试清理。
- 没有逐块校验和：复制或重建时无法发现源端的静默损坏。读取时会按 Extent 的 crc 校验，校验失败就换副本读，所以不会返回错误的数据。
- 范围读取要解密整个 Part（最大 `RMK_MAX_PART_BYTES`）。单个 Object 最大为 4096 × Part 大小。
