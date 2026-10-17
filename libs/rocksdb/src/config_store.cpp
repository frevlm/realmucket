#include "kv.hpp"

namespace rmk::rocks {
namespace {

const std::string kKey(1, kClusterConfig);

class RocksConfigStore final : public ConfigStore {
public:
    explicit RocksConfigStore(std::shared_ptr<Database> db) : db_(std::move(db)) {}

    Result<ClusterConfig> get_cluster_config() override {
        return read(*db_, [&](Txn& tx) -> Result<ClusterConfig> {
            auto v = tx.get(kKey);
            if (!v) return std::unexpected(v.error());
            if (!*v) return ClusterConfig{};
            Reader r(**v);
            ClusterConfig config{r.str()};
            if (!r.ok()) return corrupt("cluster config");
            return config;
        });
    }

    Status set_cluster_config(const ClusterConfig& config) override {
        return write(*db_, Database::Durability::durable, [&](Txn& tx) -> Status {
            tx.set(kKey, Writer().str(config.min_sdk_version).take());
            return {};
        });
    }

private:
    std::shared_ptr<Database> db_;
};

}  // namespace

std::shared_ptr<ConfigStore> make_config_store(std::shared_ptr<Database> db) {
    return std::make_shared<RocksConfigStore>(std::move(db));
}

}  // namespace rmk::rocks
