#pragma once

#include <atomic>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>

#include <grpcpp/grpcpp.h>

#include "rmk/core/cluster_store.hpp"
#include "rmk/core/env.hpp"
#include "rmk/core/error.hpp"
#include "rmk/rpc/rpc.hpp"

namespace rmk::datanode {

struct Options {
    NodeInfo identity;  // id, address, k8s_node, zone, capacity_bytes (0 = filesystem size)
    std::filesystem::path data_dir;
    std::chrono::seconds heartbeat_interval{10};
};

// RMK_NODE_ID, RMK_ADVERTISE_ADDRESS, RMK_DATA_DIR, RMK_ZONE, RMK_K8S_NODE,
// RMK_K8S_ZONE_LOOKUP, RMK_CAPACITY_BYTES (docs/IMPLEMENTATION.md §7).
Result<Options> load_options(const EnvLookup& env);

// The DataNode gRPC service plus its heartbeat loop. Serves files under data_dir only.
class Node {
public:
    static Result<std::unique_ptr<Node>> create(Options options, std::shared_ptr<ClusterStore> cluster,
                                                std::shared_ptr<rpc::Channels> channels);
    ~Node();  // stops the heartbeat loop

    grpc::Service* service();
    // Sends one heartbeat now (also done every heartbeat_interval once start_heartbeats ran).
    Status heartbeat();
    void start_heartbeats();

private:
    Node() = default;
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace rmk::datanode
