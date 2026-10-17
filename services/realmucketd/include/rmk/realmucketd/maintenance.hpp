#pragma once

#include <chrono>
#include <cstdint>
#include <memory>

#include "rmk/core/error.hpp"
#include "rmk/core/meta_store.hpp"
#include "rmk/crypto/crypto.hpp"
#include "rmk/local/chunk_store.hpp"

namespace rmk::realmucketd {

struct MaintenanceOptions {
    std::chrono::seconds interval{5};               // RMK_MAINTENANCE_INTERVAL_SECONDS
    std::uint32_t compaction_garbage_percent = 50;  // RMK_COMPACTION_GARBAGE_PERCENT (ADR 0016)
    std::uint64_t compaction_bytes_per_second = std::uint64_t{50} << 20;  // RMK_COMPACTION_BYTES_PER_SECOND
    // Only a Chunk sealed at least this long ago is compacted: every Extent written to it has
    // been registered by then (docs/IMPLEMENTATION.md §6 safety rule 2). The Gateway's
    // RMK_COMPACTION_GRACE_SECONDS.
    std::chrono::seconds compaction_grace{600};
    std::uint32_t rewrap_batch = 1000;  // RMK_REWRAP_BATCH
};

// What one pass did; tests and metrics read it.
struct MaintenanceReport {
    std::uint32_t garbage_entries = 0;
    std::uint32_t uploads_reclaimed = 0;
    std::uint32_t chunks_reconciled = 0;
    std::uint32_t chunks_sealed = 0;
    std::uint32_t chunks_compacted = 0;  // had live Extents moved away
    std::uint32_t chunks_deleted = 0;    // includes the compacted ones
    std::uint32_t versions_rewrapped = 0;
};

// The housekeeping that keeps a single machine's disk and keys in order: counts garbage,
// reclaims expired uploads, compacts and deletes Chunks, and rewraps Data Keys under the active
// Master Key (ADR 0016, 0017). One thread; there is exactly one instance, so no election.
class Maintenance {
public:
    static Result<std::unique_ptr<Maintenance>> create(MaintenanceOptions options, std::shared_ptr<MetaStore> meta,
                                                       std::shared_ptr<local::ChunkStore> store,
                                                       std::shared_ptr<crypto::KeyProvider> keys);
    ~Maintenance();  // stops the thread

    // One pass of every duty, as of `now`. Tests drive this directly.
    MaintenanceReport run_once(Clock::time_point now);
    // run_once every interval until destroyed.
    void start();

private:
    Maintenance() = default;
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace rmk::realmucketd
