#include "rmk/gateway.hpp"

#include <limits>

namespace rmk::gateway {

Result<Options> load_options(const EnvLookup& env) {
    Options options;
    // A Part plus its encryption overhead must fit into one Chunk (1 GiB by default).
    if (auto s = env_u64(env, "RMK_PART_SIZE", std::uint64_t{64} << 10, std::uint64_t{512} << 20,
                         options.part_size);
        !s)
        return std::unexpected(s.error());
    if (auto s = env_seconds(env, "RMK_PENDING_UPLOAD_RETENTION_SECONDS", std::chrono::seconds{60},
                             options.pending_upload_retention);
        !s)
        return std::unexpected(s.error());
    if (auto s = env_seconds(env, "RMK_COMPACTION_GRACE_SECONDS", std::chrono::seconds{60}, options.compaction_grace);
        !s)
        return std::unexpected(s.error());
    if (auto s = env_u64(env, "RMK_MAX_PART_BYTES", std::uint64_t{64} << 10, std::uint64_t{4} << 30,
                         options.max_part_bytes);
        !s)
        return std::unexpected(s.error());
    if (auto s = env_u64(env, "RMK_MAX_BUFFERED_BYTES", std::uint64_t{1} << 20, std::numeric_limits<std::uint64_t>::max(),
                         options.max_buffered_bytes);
        !s)
        return std::unexpected(s.error());
    return options;
}

}  // namespace rmk::gateway
