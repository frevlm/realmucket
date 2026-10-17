#include "rmk/realmucketd/realmucketd.hpp"

#include <limits>

#include "rmk/core/chunk_file.hpp"

namespace rmk::realmucketd {

Result<Options> load_options(const EnvLookup& env) {
    Options options;
    std::string data_dir, key_dir;
    if (auto s = env_required(env, "RMK_DATA_DIR", data_dir); !s)
        return std::unexpected(s.error());
    if (auto s = env_required(env, "RMK_MASTER_KEY_DIR", key_dir); !s)
        return std::unexpected(s.error());
    options.data_dir = data_dir;
    options.master_key_dir = key_dir;
    std::string meta_dir;
    if (auto s = env_string(env, "RMK_META_DIR", false, meta_dir); !s)
        return std::unexpected(s.error());
    options.meta_dir = meta_dir;
    if (auto s = env_string(env, "RMK_LISTEN", false, options.listen); !s)
        return std::unexpected(s.error());
    if (auto s = env_u64(env, "RMK_CHUNK_CAPACITY_BYTES", std::uint64_t{1} << 20,
                         std::numeric_limits<std::uint64_t>::max(), options.chunk_capacity);
        !s)
        return std::unexpected(s.error());
    if (auto s = env_u64(env, "RMK_METRICS_PORT", 0, 65535, options.metrics_port); !s)
        return std::unexpected(s.error());

    auto tls = rpc::load_tls(env);
    if (!tls)
        return std::unexpected(tls.error());
    options.tls = std::move(*tls);

    auto gateway = gateway::load_options(env);
    if (!gateway)
        return std::unexpected(gateway.error());
    options.gateway = std::move(*gateway);
    MaintenanceOptions& m = options.maintenance;
    m.compaction_grace = options.gateway.compaction_grace;
    std::uint64_t percent = m.compaction_garbage_percent, batch = m.rewrap_batch;
    for (Status s : {env_seconds(env, "RMK_MAINTENANCE_INTERVAL_SECONDS", std::chrono::seconds(1), m.interval),
                     env_u64(env, "RMK_COMPACTION_GARBAGE_PERCENT", 1, 100, percent),
                     env_u64(env, "RMK_COMPACTION_BYTES_PER_SECOND", 1, std::numeric_limits<std::uint64_t>::max() >> 1,
                             m.compaction_bytes_per_second),
                     env_u64(env, "RMK_REWRAP_BATCH", 1, 1000000, batch)})
        if (!s)
            return std::unexpected(s.error());
    m.compaction_garbage_percent = static_cast<std::uint32_t>(percent);
    m.rewrap_batch = static_cast<std::uint32_t>(batch);
    // A Part plus its encryption overhead must fit into one Chunk.
    if (options.gateway.max_part_bytes + crypto::kPartOverhead + kChunkHeaderSize > options.chunk_capacity)
        return fail(Errc::invalid_argument, "RMK_MAX_PART_BYTES plus overhead must fit in RMK_CHUNK_CAPACITY_BYTES");
    return options;
}

}  // namespace rmk::realmucketd
