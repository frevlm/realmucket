# Deployment

One image (`docker/runtime.Dockerfile`) contains `rmk-gateway`, `rmk-datanode`, `rmk-controller` and `rmkctl`; the container command picks the binary. It runs as uid 10001.

## Build the image

```
docker build -f docker/dev.Dockerfile -t realmucket-dev .         # once
docker build -f docker/runtime.Dockerfile -t realmucket:dev .      # from the repo root
```

## Local cluster (docker compose)

```
docker compose -f deploy/compose/docker-compose.yaml up --build
```

Starts FoundationDB 7.3.69 (single process, configured once by `fdb-init`, idempotent), 4 datanodes in zones z1,z2,z3,z1, 1 controller and a gateway on `localhost:7000`. Everything runs with `RMK_INSECURE=true`, 256 MiB chunks (a Part, 64 MiB by default, must fit in one) and a 60 s repair delay. `deploy/compose/master-keys/` holds development keys only; never reuse them.

## Kubernetes

Manifests in `deploy/k8s/` (namespace `realmucket`), rendered with `kubectl kustomize deploy/k8s`.

1. **FoundationDB.** Install the official [fdb-kubernetes-operator](https://github.com/FoundationDB/fdb-kubernetes-operator) first, then apply `deploy/k8s/fdb/foundationdbcluster.yaml` (3 storage processes). The operator creates ConfigMap `realmucket-fdb-config` (key `cluster-file`), which our pods mount at `/etc/realmucket/fdb`. If you name the cluster differently, change the ConfigMap name in the three workload files.
2. **Internal TLS.** Secret `realmucket-tls` with `tls.crt`, `tls.key`, `ca.crt` must exist (`RMK_TLS_CERT/KEY/CA`). The certificate needs SANs for the gateway Service and `*.realmucket-datanode.realmucket.svc`. With cert-manager installed: `kubectl apply -k deploy/k8s/cert-manager` (self-signed CA; swap the Issuer for your own CA in production).
3. **Master keys.** One file per key named by decimal id holding 64 hex chars, plus `active` holding the active id:
   ```
   kubectl -n realmucket create secret generic realmucket-master-keys \
     --from-literal=1="$(openssl rand -hex 32)" --from-literal=active=1
   ```
   Enable etcd encryption at rest, and **back every key up offline** (ADR 0017): losing a Master Key makes the data wrapped by it permanently unreadable. `secrets.yaml` contains only "REPLACE ME" placeholders; prefer creating real secrets out of band, then drop `secrets.yaml` from `kustomization.yaml`.
4. **Image and storage.** Set the registry/tag under `images:` in `kustomization.yaml`; set `storageClassName` and size in the datanode `volumeClaimTemplates`.
5. **Storage node pool.** Data Nodes run only on nodes of type `storage` (label and taint `realmubernetes.io/node-type=storage`, see `~/realmubernetes/docs/node-pools.md`). Label and taint those nodes **before** applying, or the Data Node Pods stay Pending. `deploy/k8s` pulls `node-type-storage` and `pod-baseline` from the sibling `realmubernetes` checkout by relative path; switch to a git URL with a tag once that repository is published.
6. `kubectl apply -k deploy/k8s`.

Notes: datanodes prefer spreading across zones (`topology.kubernetes.io/zone`) and read their zone from the Node label via `RMK_K8S_ZONE_LOOKUP` (ClusterRole: `get nodes`). The controller runs 2 replicas, only the leader works (ADR 0009). All pods expose `/healthz` and `/metrics` on 9100 with Prometheus scrape annotations. The FDB client may try to rewrite the mounted cluster file when coordinators change; the read-only ConfigMap mount is kept up to date by the operator.

The Gateway does not authenticate callers (ADR 0018): every request names its Tenant, and anyone who can reach port 7000 can use every Tenant and the Tenant/Quota admin RPCs. Restrict the port with NetworkPolicy or the mesh; see `deploy/k8s/examples/gateway-networkpolicy.yaml`.

## Local development

Without FoundationDB or Docker, on a Mac or Linux box:

```
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Debug && cmake --build build --target rmk-dev rmkctl
./build/rmk-dev                      # 4 Data Nodes (zones z1,z2,z3,z1), Controller and Gateway on 127.0.0.1:7000
export RMK_ENDPOINT=127.0.0.1:7000
./build/rmkctl --insecure tenant create demo
./build/rmkctl --insecure bucket create photos --tenant demo
./build/rmkctl --insecure object put photos hello.txt ./hello.txt --tenant demo
```

`rmk-dev` runs everything in one process on in-memory stores with plaintext gRPC, a random Master Key per start and 16 MiB chunks; stopping it (Ctrl-C) discards all metadata. Flags: `--nodes N`, `--listen HOST:PORT`, `--data-dir DIR` (default `./.rmk-dev`; its `node-*` subdirectories are wiped at start), `--chunk-mib N`. A Part must fit in a chunk: `rmkctl` uploads files above 64 MiB in 64 MiB Parts, so start with `--chunk-mib 128` or more before putting such files.

Tests that need no external services: `cmake --build build --target rmk_e2e_tests && ./build/rmk_e2e_tests` (the whole system in one process, through the Client SDK). The Docker variant against the real compose deployment, needing Docker and building the image: `scripts/e2e-compose.sh`.
