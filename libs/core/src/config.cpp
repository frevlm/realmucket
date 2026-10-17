#include "rmk/core/config.hpp"

#include "rmk/core/chunk_file.hpp"
#include "rmk/core/env.hpp"

namespace rmk {

Result<Config> load_config(const EnvLookup& lookup) {
    Config config;
    if (auto s = env_u64(lookup, "RMK_CHUNK_CAPACITY_BYTES", kChunkHeaderSize + 1, UINT64_MAX,
                         config.chunk_capacity_bytes);
        !s)
        return std::unexpected(s.error());
    if (auto s = env_seconds(lookup, "RMK_PENDING_UPLOAD_RETENTION_SECONDS", std::chrono::seconds{1},
                             config.pending_upload_retention);
        !s)
        return std::unexpected(s.error());
    return config;
}

Result<Config> load_config_from_env() {
    return load_config(process_env());
}

}  // namespace rmk
