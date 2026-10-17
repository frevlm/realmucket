#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <filesystem>
#include <map>
#include <memory>
#include <shared_mutex>
#include <vector>

#include "rmk/core/chunk_file.hpp"
#include "rmk/core/extent_io.hpp"

namespace rmk::local {

struct Options {
    std::filesystem::path dir;  // chunk files live in <dir>/chunks
    std::uint64_t chunk_capacity = std::uint64_t{1} << 30;  // file size limit, header included
    // Tests only: replaces ChunkFile::sync() (to count, delay or fail the flushes). `through` is the
    // end of the range the flush is meant to cover: every byte below it has been written.
    std::function<Status(ChunkFile&, std::uint64_t through)> sync = nullptr;
    // Tests only: called by a writer after it claimed its range at `offset` and before it copies
    // its bytes in, to hold a copy half-way.
    std::function<void(std::uint64_t offset)> after_claim = nullptr;
};

struct ChunkStat {
    ChunkId id = 0;
    std::uint64_t size = 0;  // file size, header included
    std::uint64_t capacity = 0;
    std::uint64_t garbage_bytes = 0;
    bool open = false;  // the one Chunk that still takes appends
    Clock::time_point sealed_at;  // when it stopped taking appends; the epoch for Chunks found at open()
};

// Byte storage on this machine's disk: appends to one open Chunk, starts a new one when it is
// full, and keeps every earlier Chunk read-only. write() returns only after the bytes are flushed
// to disk; writers that are in flight together share flushes (group commit). No replicas, no placement; the crc32c in each
// Extent is what notices a damaged disk.
//
// Chunks found on disk at open() are all sealed (sealed_at is the epoch: nothing is in flight
// after a restart), and their garbage counters start at zero until set_garbage().
// Thread-safe.
class ChunkStore final : public ExtentWriter, public ExtentReader, public GarbageSink {
public:
    static Result<std::unique_ptr<ChunkStore>> open(Options options);
    ~ChunkStore() override;

    // `policy` is ignored: there is one copy of everything.
    Result<Extent> write(const StoragePolicy& policy, std::span<const std::byte> bytes) override;
    Result<std::vector<std::byte>> read(const Extent& extent) override;
    Status add_garbage(ChunkId id, std::uint64_t bytes) override;
    Status set_garbage(ChunkId id, std::uint64_t bytes);

    // Stops appending to the open Chunk; the next write starts a new one. No-op without one.
    void seal_open();
    // Deletes a sealed Chunk's file. invalid_argument for the open Chunk, not_found if unknown.
    // Later reads of its Extents are not_found.
    Status remove(ChunkId id);

    std::vector<ChunkStat> chunks() const;
    // Flushes issued so far; fewer than the writes when writers share them.
    std::uint64_t sync_calls() const { return syncs_.load(); }

private:
    struct Slot;  // a ChunkFile plus the state that lets concurrent writers share one flush
    struct Entry {
        std::shared_ptr<Slot> slot;  // shared: a writer may still be syncing it after a remove()
        std::uint64_t garbage_bytes = 0;
        Clock::time_point sealed_at;
    };

    explicit ChunkStore(Options options) : options_(std::move(options)) {}
    Status start_chunk();     // callers hold mu_ exclusively
    void seal_unlocked();     // likewise
    void stop_appending(ChunkId id);  // seal_open(), but only if `id` is still the open Chunk

    Options options_;
    mutable std::shared_mutex mu_;
    std::map<ChunkId, Entry> chunks_;
    ChunkId open_ = 0;  // 0: none yet
    ChunkId next_id_ = 1;
    std::atomic<std::uint64_t> syncs_{0};
};

}  // namespace rmk::local
