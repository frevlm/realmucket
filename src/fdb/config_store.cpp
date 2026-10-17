#include "fdb.hpp"

namespace rmk::fdb {

FdbConfigStore::FdbConfigStore(std::shared_ptr<Database> db, std::string key_prefix)
    : db_(std::move(db)), prefix_(encode_prefix(key_prefix)) {}

Result<ClusterConfig> FdbConfigStore::get_cluster_config() {
    return run(*db_, [&](Txn& tx) -> Result<ClusterConfig> {
        auto v = tx.get(prefix_ + static_cast<char>(kClusterConfig));
        if (!v) return std::unexpected(v.error());
        if (!*v) return ClusterConfig{};
        Reader r(**v);
        ClusterConfig config{r.str()};
        if (!r.ok()) return corrupt("cluster config");
        return config;
    });
}

Status FdbConfigStore::set_cluster_config(const ClusterConfig& config) {
    return run(*db_, [&](Txn& tx) -> Status {
        tx.set(prefix_ + static_cast<char>(kClusterConfig), Writer().str(config.min_sdk_version).take());
        return {};
    });
}

}  // namespace rmk::fdb
