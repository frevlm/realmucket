---
status: accepted
---

# Master Key 第一期存放在 k8s Secret，获取方式隔离在单独接口后面

Data Key 存入 Metadata Store 之前，必须先用 Master Key 加密。公司没有现成的钥匙管理系统，所以第一期把 Master Key 放在 k8s Secret 中，并要求集群开启 Secret 的静态加密（etcd encryption at rest）。获取 Master Key 只能经过一个单独的 `KeyProvider` 接口，以后迁移到专门的钥匙保管服务（Vault/OpenBao）或硬件加密设备时，只需替换这一个实现。

## Considered Options

- **专门部署钥匙保管服务（Vault/OpenBao）**：有权限控制和访问审计，但多一个必须高可用的服务，它不可用时整个存储都无法读写。
- **硬件加密设备（HSM）**：最安全，但成本和部署复杂度过高。

## Consequences

- 可以同时存在多把 Master Key，每份加密后的 Data Key 都记录自己用的是哪一把：新数据用最新的 Master Key，旧数据仍能用旧的解开，并由后台逐步改用新钥匙重新加密。这样怀疑泄露时可以立即轮换。
- Master Key 必须另外离线备份。Master Key 一旦丢失，所有数据都将永久不可读，这比数据泄露更严重。
- k8s 集群管理员能够读取 Master Key，这是第一期接受的风险。
