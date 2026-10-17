#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>

#include <grpcpp/grpcpp.h>

#include "rmk/crypto/crypto.hpp"
#include "rmk/core/env.hpp"
#include "rmk/rpc/rpc.hpp"

namespace rmk::gateway {

struct Options {
    std::uint64_t part_size = std::uint64_t{64} << 20;
    std::uint32_t list_limit_max = 1000;
    std::chrono::seconds pending_upload_retention = std::chrono::days{7};
    std::chrono::seconds cluster_config_cache{30};
    // An Extent is registered only within compaction_grace / 2 of being written (§6 safety rule 2).
    std::chrono::seconds compaction_grace{600};
    std::uint64_t max_part_bytes = std::uint64_t{256} << 20;
    // Content buffered across all requests; beyond it a request waits buffer_wait, then unavailable.
    std::uint64_t max_buffered_bytes = std::uint64_t{2} << 30;
    std::chrono::milliseconds buffer_wait{2000};
};

// Everything the service needs, built by main() (or a test) from the environment.
struct Dependencies {
    rpc::Stores stores;
    std::shared_ptr<crypto::KeyProvider> keys;
    std::shared_ptr<ExtentWriter> writer;
    std::shared_ptr<ExtentReader> reader;
    std::shared_ptr<GarbageSink> garbage;
};

// RMK_PART_SIZE,
// RMK_PENDING_UPLOAD_RETENTION_SECONDS, RMK_COMPACTION_GRACE_SECONDS, RMK_MAX_PART_BYTES,
// RMK_MAX_BUFFERED_BYTES (docs/IMPLEMENTATION.md §6, §7).
Result<Options> load_options(const EnvLookup& env);

// The rmk.v1.Storage service (proto/rmk/v1/storage.proto): minimum SDK version (ADR 0015),
// quotas (ADR 0010), encryption
// (ADR 0016), the write protocol (ADR 0007, 0013) and reads (docs/IMPLEMENTATION.md §6).
class Gateway {
public:
    static Result<std::unique_ptr<Gateway>> create(Options options, Dependencies deps);
    ~Gateway();
    grpc::Service* service();

private:
    Gateway() = default;
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace rmk::gateway
