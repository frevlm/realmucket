#pragma once

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>

#include "rmk/core/config_store.hpp"
#include "rmk/core/meta_store.hpp"

namespace rmk::rocks {

class Txn;

struct Options {
    std::filesystem::path dir;
    // false: nothing is ever flushed (tests that do not care about crashes).
    bool sync = true;
};

// One RocksDB database that holds the MetaStore and the ConfigStore. Writes go through a single
// thread that gathers whatever has queued up into one commit (docs/PERFORMANCE.md); the write-ahead
// log is flushed on a thread of its own, so the writer never waits for the disk and one flush serves
// every transaction that was committed before it started (group commit). Reads run on the caller's
// thread against a snapshot.
class Database {
public:
    static Result<std::shared_ptr<Database>> open(const Options& options);
    ~Database();
    Database(const Database&) = delete;
    Database& operator=(const Database&) = delete;

    // True if the directory held no database before open(): a brand new, empty one.
    bool created() const { return created_; }

    struct Stats {
        std::uint64_t batches = 0;       // commits to the database
        std::uint64_t flushes = 0;       // write-ahead log flushes
        std::uint64_t transactions = 0;  // logical transactions committed
    };
    Stats stats() const;

    // Used by the stores. A transaction body returns false to roll back its own writes only.
    // A `lazy` write returns once it is applied; a durable one once the log holding it is flushed.
    enum class Durability { durable, lazy };
    Status write(const std::function<bool(Txn&)>& body, Durability durability);
    void read(const std::function<void(Txn&)>& body);

private:
    struct Impl;
    Database() = default;
    std::unique_ptr<Impl> impl_;
    bool created_ = false;
};

std::shared_ptr<MetaStore> make_meta_store(std::shared_ptr<Database> db, std::chrono::seconds pending_upload_retention);
std::shared_ptr<ConfigStore> make_config_store(std::shared_ptr<Database> db);

}  // namespace rmk::rocks
