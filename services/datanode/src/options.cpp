#include <limits>
#include <string>

#include "rmk/datanode/datanode.hpp"

namespace rmk::datanode {

Result<Options> load_options(const EnvLookup& env) {
    Options options;
    std::string data_dir;
    for (auto [name, out] : {std::pair{"RMK_NODE_ID", &options.identity.id},
                             std::pair{"RMK_ADVERTISE_ADDRESS", &options.identity.address},
                             std::pair{"RMK_DATA_DIR", &data_dir}})
        if (auto s = env_required(env, name, *out); !s)
            return std::unexpected(s.error());
    if (auto s = env_string(env, "RMK_ZONE", true, options.identity.zone); !s)
        return std::unexpected(s.error());
    if (auto s = env_string(env, "RMK_HOST", true, options.identity.host); !s)
        return std::unexpected(s.error());
    if (auto s = env_u64(env, "RMK_CAPACITY_BYTES", 0, std::numeric_limits<std::uint64_t>::max(),
                         options.identity.capacity_bytes);
        !s)
        return std::unexpected(s.error());
    options.data_dir = data_dir;
    return options;
}

}  // namespace rmk::datanode
