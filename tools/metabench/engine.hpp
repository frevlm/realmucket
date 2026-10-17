// Metadata engine micro-benchmark: the engines under test share one interface and one batching
// thread, so they differ only in how a batch of transactions is stored and made durable.
#pragma once

#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace metabench {

// A write transaction as the engine's executor presents it. Reads see the transaction's own writes.
class Tx {
public:
    virtual ~Tx() = default;
    virtual std::optional<std::string> get(std::string_view key) = 0;
    virtual void put(std::string_view key, std::string_view value) = 0;
    virtual void del(std::string_view key) = 0;
};

// Returns false to abort just this transaction (its writes are rolled back, the batch goes on).
using TxBody = std::function<bool(Tx&)>;

struct Options {
    std::filesystem::path dir;
    bool sync = true;  // false: nothing is ever flushed; measures the engine itself
};

class Engine {
public:
    virtual ~Engine() = default;
    virtual std::string name() const = 0;

    // Runs the bodies in order inside one engine transaction, each isolated from the others, and
    // commits once. With `durable` the data is flushed to disk before this returns.
    // ok[i] is set to the result of bodies[i]. Called from one thread at a time.
    virtual void apply_batch(const std::vector<const TxBody*>& bodies, std::vector<char>& ok, bool durable) = 0;

    // Engines whose commit and flush are separate steps let the writer carry on while a flush runs:
    // apply_unsynced() commits without flushing, flush() makes everything committed before the call
    // durable and may run on another thread at the same time as apply_unsynced().
    virtual bool can_decouple() const { return false; }
    virtual void apply_unsynced(const std::vector<const TxBody*>&, std::vector<char>&) {}
    virtual void flush() {}

    // Concurrent readers.
    virtual std::optional<std::string> read_get(std::string_view key) = 0;
    // Keys in [from, to), at most `limit`; returns how many were visited.
    virtual std::size_t read_scan(std::string_view from, std::string_view to, std::size_t limit) = 0;

    virtual std::uint64_t disk_bytes() const = 0;
};

std::unique_ptr<Engine> make_memory(const Options&);
std::unique_ptr<Engine> make_sqlite(const Options&);
#ifdef MB_HAVE_LMDB
std::unique_ptr<Engine> make_lmdb(const Options&);
#endif
#ifdef MB_HAVE_ROCKSDB
std::unique_ptr<Engine> make_rocksdb(const Options&);
#endif

std::uint64_t directory_bytes(const std::filesystem::path& dir);

}  // namespace metabench
