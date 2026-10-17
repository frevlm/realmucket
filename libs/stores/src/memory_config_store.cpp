#include "rmk/stores/memory_stores.hpp"

namespace rmk {

Result<ClusterConfig> MemoryConfigStore::get_cluster_config() {
    std::lock_guard lock(mutex_);
    return config_;
}

Status MemoryConfigStore::set_cluster_config(const ClusterConfig& config) {
    std::lock_guard lock(mutex_);
    config_ = config;
    return {};
}

}  // namespace rmk
