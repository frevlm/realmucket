#pragma once

#include <chrono>
#include <cstdint>

#include "rmk/core/env.hpp"
#include "rmk/core/error.hpp"

namespace rmk {

// Per-process tunables read from environment variables (ADR 0006).
// Cluster-wide settings live in the Metadata Store, never here.
struct Config {
    std::uint64_t chunk_capacity_bytes = std::uint64_t{1} << 30;          // RMK_CHUNK_CAPACITY_BYTES
    std::chrono::seconds pending_upload_retention = std::chrono::days{7};  // RMK_PENDING_UPLOAD_RETENTION_SECONDS
};

// Unset variables keep their defaults; any invalid value is an error, never a silent fallback.
Result<Config> load_config(const EnvLookup& lookup);
Result<Config> load_config_from_env();

}  // namespace rmk
