#include <cstdio>
#include <cstdlib>
#include <memory>
#include <random>
#include <string>

#include "rmk/fdb/fdb_stores.hpp"
#include "store_contract/contract.hpp"

// Shared with fdb_stores_test.cpp.
std::shared_ptr<rmk::fdb::Database> fdb_test_database();
std::string fdb_test_prefix();
rmk::contract::StoreSet make_fdb_stores(std::chrono::seconds retention);

// The test database, or null when RMK_FDB_CLUSTER_FILE is unset (every FDB test then skips).
std::shared_ptr<rmk::fdb::Database> fdb_test_database() {
    static std::shared_ptr<rmk::fdb::Database> db = []() -> std::shared_ptr<rmk::fdb::Database> {
        const char* cluster_file = std::getenv("RMK_FDB_CLUSTER_FILE");
        if (!cluster_file || !*cluster_file) return nullptr;
        auto opened = rmk::fdb::open_database(cluster_file);
        if (!opened) {
            std::fprintf(stderr, "open_database: %s\n", opened.error().message.c_str());
            return nullptr;
        }
        return *opened;
    }();
    return db;
}

// A key prefix no other test uses.
std::string fdb_test_prefix() {
    static std::mt19937_64 rng{std::random_device{}()};
    char buf[32];
    std::snprintf(buf, sizeof buf, "rmk-test-%016llx", static_cast<unsigned long long>(rng()));
    return buf;
}

// Fresh stores under a fresh prefix; the prefix is cleared once the last store is released.
rmk::contract::StoreSet make_fdb_stores(std::chrono::seconds retention) {
    auto db = fdb_test_database();
    if (!db) return {};
    std::string prefix = fdb_test_prefix();
    std::shared_ptr<void> cleanup(nullptr, [db, prefix](void*) { (void)rmk::fdb::clear_prefix(*db, prefix); });
    auto meta = new rmk::fdb::FdbMetaStore(db, prefix, retention);
    auto config = new rmk::fdb::FdbConfigStore(db, prefix);
    auto cluster = new rmk::fdb::FdbClusterStore(db, prefix);
    return {std::shared_ptr<rmk::MetaStore>(meta, [cleanup](rmk::MetaStore* p) { delete p; }),
            std::shared_ptr<rmk::ConfigStore>(config, [cleanup](rmk::ConfigStore* p) { delete p; }),
            std::shared_ptr<rmk::ClusterStore>(cluster, [cleanup](rmk::ClusterStore* p) { delete p; })};
}

RMK_INSTANTIATE_STORE_CONTRACT(Fdb, make_fdb_stores);
