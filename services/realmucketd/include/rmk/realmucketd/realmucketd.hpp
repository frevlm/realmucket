#pragma once

#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>

#include <grpcpp/grpcpp.h>

#include "rmk/core/env.hpp"
#include "rmk/core/error.hpp"
#include "rmk/crypto/crypto.hpp"
#include "rmk/gateway/gateway.hpp"
#include "rmk/realmucketd/maintenance.hpp"
#include "rmk/rpc/rpc.hpp"

namespace rmk::realmucketd {

// One machine, one process: the Storage gRPC API (the Gateway) writing object bytes straight
// to this machine's disk. Nothing else has to run for it to work.
struct Options {
    std::string listen = "0.0.0.0:7000";
    std::filesystem::path data_dir;
    std::filesystem::path master_key_dir;
    std::filesystem::path meta_dir;  // object metadata; empty: <data_dir>/meta
    bool sync_metadata = true;       // flush metadata commits to disk; only tests turn it off
    std::uint64_t chunk_capacity = std::uint64_t{1} << 30;
    std::uint64_t metrics_port = 9100;
    rpc::TlsConfig tls;
    gateway::Options gateway;
    MaintenanceOptions maintenance;
};

// RMK_LISTEN, RMK_DATA_DIR, RMK_MASTER_KEY_DIR, RMK_META_DIR, RMK_CHUNK_CAPACITY_BYTES, RMK_METRICS_PORT,
// RMK_INSECURE / RMK_TLS_CERT / RMK_TLS_KEY, RMK_MAINTENANCE_INTERVAL_SECONDS,
// RMK_COMPACTION_GARBAGE_PERCENT, RMK_COMPACTION_BYTES_PER_SECOND, RMK_REWRAP_BATCH, plus the
// Gateway's own variables.
Result<Options> load_options(const EnvLookup& env);

class Daemon {
public:
    // Opens the data directory and starts serving on options.listen. Refuses to start if another
    // process holds the directory, or if there are chunk files but no metadata database to go with them.
    static Result<std::unique_ptr<Daemon>> start(Options options, std::shared_ptr<crypto::KeyProvider> keys);
    ~Daemon();  // stops serving, then closes the store

    int port() const;

private:
    Daemon() = default;
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace rmk::realmucketd
