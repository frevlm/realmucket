#include "rmk/rocksdb/rocks_stores.hpp"

#include <unistd.h>

#include <filesystem>

#include "store_contract/contract.hpp"

namespace {

namespace fs = std::filesystem;

// A fresh database in a directory of its own for every store set the contract asks for.
rmk::contract::StoreSet make_stores(std::chrono::seconds retention) {
    static std::atomic<int> counter{0};
    const fs::path dir = fs::temp_directory_path() /
                         ("rmk-rocks-contract-" + std::to_string(::getpid()) + "-" + std::to_string(counter++));
    fs::remove_all(dir);
    auto db = rmk::rocks::Database::open({dir, /*sync=*/false});
    if (!db) return {};
    return {rmk::rocks::make_meta_store(*db, retention), rmk::rocks::make_config_store(*db), nullptr};
}

RMK_INSTANTIATE_STORE_CONTRACT(Rocks, make_stores);

}  // namespace
