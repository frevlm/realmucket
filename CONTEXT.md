# realmucket

自研的单机对象存储系统：一个进程 `realmucketd` 独占一台服务器的本地盘；运行在 k8s 上（每台存储服务器一个 realmucketd，由另一个仓库部署），同时以 C++ 头文件 SDK 和 gRPC 接口两种方式对外提供服务。

## Language

### 用户可见的概念

**Tenant**:
拥有一组 **Bucket** 的使用方（通常是一个团队或业务），是配额与用量统计的归属单位。
_Avoid_: 账号, account, 用户, namespace

**Quota**:
对一个 **Tenant** 可使用的容量与对象数的软上限，包含历史版本，可能被并发写入少量超出。
_Avoid_: 限额, limit

**Bucket**:
对象的命名空间容器，名字只在所属 **Tenant** 内唯一，是生命周期配置的挂载点。
_Avoid_: 容器, 仓库, 目录

**Object**:
由 **Object Key** 唯一标识的一段不可原地修改的字节数据及其元数据。
_Avoid_: 文件, file, blob

**Object Key**:
一个 **Object** 在所属 **Bucket** 内的唯一名称，是扁平字符串，`/` 不具备层级语义。
_Avoid_: 路径, path, 文件名

**Generation**:
一个 **Object** 每次提交时由系统分配的编号：同一 **Object** 内唯一且严格递增，全局按提交顺序不减；是条件写的唯一比较依据，也用于标识历史版本。
_Avoid_: ETag, 版本号, revision

**Checksum**:
一个 **Object** 内容的校验值，只用于发现数据损坏，不参与条件写。
_Avoid_: ETag, 哈希, digest

**Versioning**:
按 **Bucket** 设置的开关；开启后，覆盖与删除都会把旧内容保留为由 **Generation** 标识的历史版本，而不是回收。
_Avoid_: 多版本, 快照, backup

**Lifecycle Rule**:
挂在 **Bucket** 上、按 **Object Key** 前缀和对象年龄自动处置 **Object** 的规则。
_Avoid_: 过期策略, TTL

### 系统组件

**Gateway**:
`realmucketd` 内的 Storage gRPC 服务，负责协议解析，并协调 **Metadata Store** 与本机的 **Chunk** 存储完成一次请求；不是独立进程。
_Avoid_: 前端, proxy, server

**Metadata Store**:
保存全部 **Bucket**、**Object** 索引与 **Lifecycle Rule** 的事务存储，本机 RocksDB 实现。
_Avoid_: 元数据库, DB, index server

### 数据布局

**Chunk**:
本地盘上的一个文件，持续追加写入直到写满，之后封存为只读；容纳多个 **Object** 的字节数据，只有一份。每个 **Extent** 带 crc32c，用来发现损坏。
_Avoid_: block, volume, 段

**Extent**:
一个 **Object** 的字节数据在某个 **Chunk** 中占据的一段连续区间。
_Avoid_: 片段, segment, piece

**Pending Upload**:
记录在 **Metadata Store** 中、已开始但尚未提交的一次写入，带有过期期限。
_Avoid_: 临时对象, 草稿, multipart session

**Data Key**:
专门用于加密某一个 **Object** 内容的随机钥匙；**Object** 被删除时一并销毁，盘上残留的内容随即永久不可读。
_Avoid_: 密钥, object key, encryption key

**Master Key**:
用来加密所有 **Data Key** 的上层钥匙，可以有多把并存以便轮换；丢失即所有数据永久不可读。
_Avoid_: 根密钥, root key, KEK

**Compaction**:
把 **Sealed Chunk** 中仍然有效的 **Extent** 搬到新 **Chunk**、再释放旧 **Chunk** 的空间回收过程，由 `realmucketd` 内的维护线程执行。
_Avoid_: 整理, GC, 垃圾回收

**Part**:
大 **Object** 上传时被切分出的一段，可以单独上传、单独重传，全部到齐后才组成 **Object**。
_Avoid_: 分片, shard, block

**Sealed Chunk**:
已写满并封存、不再追加的 **Chunk**，只有它才会被 **Compaction** 处理。
_Avoid_: 冷 chunk, 只读 chunk

### 接入

**Client SDK**:
业务进程链接的 C++ 头文件库，是 **Gateway** 对外 gRPC API 的官方客户端，不包含任何存储能力。
_Avoid_: 嵌入式接口, embedded mode, hpp 接口

## Relationships

- 一个 **Tenant** 拥有零或多个 **Bucket**，并恰好有一份 **Quota**
- 一个 **Bucket** 恰好属于一个 **Tenant**，由 (**Tenant**, Bucket 名) 唯一确定
- 一个 **Bucket** 包含零或多个 **Object**
- 一个 **Object** 恰好属于一个 **Bucket**，由 (**Bucket**, **Object Key**) 唯一确定
- 一个 **Bucket** 挂载零或多条 **Lifecycle Rule**
- **Object** 的索引存于 **Metadata Store**，字节数据存于本机的一个或多个 **Chunk**
- 每个 **Object** 恰好有一把 **Data Key**；每把 **Data Key** 由一把 **Master Key** 加密保存
- 一个 **Chunk** 容纳多个 **Object**；一个大 **Object** 的字节数据可以跨越多个 **Chunk**
- 一个 **Object** 由一个或多个 **Extent** 组成，每个 **Extent** 恰好位于一个 **Chunk** 中
- 一个 **Pending Upload** 登记零或多个 **Part**；提交后成为一个 **Object**；过期未提交的，其 **Extent** 被回收
- **Gateway** 自己不持有持久状态，状态在 **Metadata Store** 与 **Chunk** 中
- 写入只追加到未满的 **Chunk**；**Chunk** 写满后成为 **Sealed Chunk**
- 每个请求都显式指明所属 **Tenant**；Tenant 为空的请求无效
- **Client SDK** 只与 **Gateway** 通信，从不直接访问 **Metadata Store** 或 **Chunk**

## Example dialogue

> **Dev:** "用户上传 `photos/2026/a.jpg`，要先在 **Bucket** 里建 `photos/2026/` 目录吗？"
> **Domain expert:** "不用。`photos/2026/a.jpg` 整体就是一个 **Object Key**，没有目录这回事；列举时按前缀和分隔符模拟出来。"

> **Dev:** "列举一个 **Bucket** 下的 **Object**，要扫描 **Chunk** 吗？"
> **Domain expert:** "不用。**Chunk** 只认字节不认名字，列举和 **Lifecycle Rule** 的匹配都只查 **Metadata Store**。"

> **Dev:** "删除一个 **Object** 后，要等 **Compaction** 跑完它才算删掉吗？"
> **Domain expert:** "不用。删除时它的 **Data Key** 就被销毁了，那一刻起内容就永久读不出来；**Compaction** 只负责把空间收回来。"

## Flagged ambiguities

- 需求中的「文件管理」指的是 **Object** 管理 —— 系统内没有文件和目录的概念。
- 「key」同时可能指 **Object Key**（对象的名字）和 **Data Key**（加密钥匙）—— 已确定：单独说 key 时只指 **Object Key**，加密钥匙一律称 **Data Key**。
- 「hpp 嵌入式接口」曾被理解为把存储引擎嵌入业务进程 —— 已确定：它指的是 **Client SDK**，存储引擎只运行在 `realmucketd` 进程内。
- 「鉴权」—— 由 k8s（NetworkPolicy/mTLS/服务网格）负责，系统内没有 Principal，也没有 Policy。
