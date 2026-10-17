#pragma once

#include "rmk/core/error.hpp"
#include "rmk/core/types.hpp"

namespace rmk {

// Cluster-wide settings (ADR 0006, 0015).
class ConfigStore {
public:
    virtual ~ConfigStore() = default;

    virtual Result<ClusterConfig> get_cluster_config() = 0;  // defaults when never set
    virtual Status set_cluster_config(const ClusterConfig& config) = 0;
};

}  // namespace rmk
