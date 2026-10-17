#include <unistd.h>

#include <array>
#include <cstdint>
#include <string>

#include "rmk/chunk_file.hpp"
#include "rmk/controller.hpp"

namespace rmk::controller {
namespace {

std::string hostname() {
    std::array<char, 256> name{};
    if (::gethostname(name.data(), name.size() - 1) != 0 || name[0] == '\0')
        return "controller";
    return name.data();
}

Status read_u32(const EnvLookup& env, const char* name, std::uint32_t min, std::uint32_t max,
                std::uint32_t& out) {
    std::uint64_t value = out;
    if (auto s = env_u64(env, name, min, max, value); !s)
        return s;
    out = static_cast<std::uint32_t>(value);
    return {};
}

}  // namespace

Result<Options> load_options(const EnvLookup& env) {
    Options o;
    o.id = hostname();
    Status checks[] = {
        env_string(env, "RMK_CONTROLLER_ID", false, o.id),
        env_seconds(env, "RMK_CONTROLLER_INTERVAL_SECONDS", std::chrono::seconds(1), o.interval),
        env_seconds(env, "RMK_REPAIR_DELAY_SECONDS", std::chrono::seconds(1), o.repair_delay),
        env_u64(env, "RMK_CHUNK_CAPACITY_BYTES", 2 * kChunkHeaderSize, std::uint64_t{1} << 40, o.chunk_capacity),
        read_u32(env, "RMK_OPEN_CHUNKS_PER_POLICY", 1, 10000, o.open_chunks_per_policy),
        read_u32(env, "RMK_COMPACTION_GARBAGE_PERCENT", 1, 100, o.compaction_garbage_percent),
        env_u64(env, "RMK_COMPACTION_BYTES_PER_SECOND", 1, UINT64_MAX >> 1, o.compaction_bytes_per_second),
        env_seconds(env, "RMK_COMPACTION_GRACE_SECONDS", std::chrono::seconds(0), o.compaction_grace),
        read_u32(env, "RMK_REWRAP_BATCH", 1, 1000000, o.rewrap_batch),
        env_seconds(env, "RMK_WRITER_LEASE_SECONDS", std::chrono::seconds(1), o.writer_lease),
    };
    for (Status& s : checks)
        if (!s)
            return std::unexpected(s.error());
    return o;
}

}  // namespace rmk::controller
