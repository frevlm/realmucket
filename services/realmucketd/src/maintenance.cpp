#include "rmk/realmucketd/maintenance.hpp"

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstdio>
#include <ctime>
#include <format>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "rmk/core/chunk_file.hpp"
#include "rmk/http/http.hpp"

namespace rmk::realmucketd {
namespace {

constexpr std::size_t kPage = 1000;
// Reconciling a Chunk needs all its live Extents at once; one with more is left as it is.
constexpr std::size_t kReconcileLimit = 1'000'000;

void log(const char* level, const std::string& message) {
    std::time_t now = std::time(nullptr);
    std::tm tm{};
    ::gmtime_r(&now, &tm);
    char stamp[32];
    std::strftime(stamp, sizeof stamp, "%Y-%m-%dT%H:%M:%SZ", &tm);
    std::fprintf(stderr, "%s %s maintenance %s\n", stamp, level, message.c_str());
}

std::string describe(const Error& error) {
    return std::format("{}: {}", errc_name(error.code), error.message);
}

struct Metrics {
    metrics::Counter& garbage;
    metrics::Counter& reclaimed;
    metrics::Counter& compacted;
    metrics::Counter& deleted;
    metrics::Counter& rewrapped;
    metrics::Gauge& open_chunks;
    metrics::Gauge& sealed_chunks;
    metrics::Gauge& chunk_syncs;

    static Metrics make() {
        auto& r = metrics::global();
        return Metrics{
            r.counter("rmk_maintenance_garbage_entries_total", "Garbage entries added to Chunk garbage counts"),
            r.counter("rmk_maintenance_uploads_reclaimed_total", "Expired Pending Uploads reclaimed"),
            r.counter("rmk_maintenance_chunks_compacted_total", "Chunks whose live Extents were moved away"),
            r.counter("rmk_maintenance_chunks_deleted_total", "Chunks deleted"),
            r.counter("rmk_maintenance_versions_rewrapped_total", "Data Keys rewrapped under the active Master Key"),
            r.gauge("rmk_maintenance_chunks", "Chunks by state", {{"state", "open"}}),
            r.gauge("rmk_maintenance_chunks", "Chunks by state", {{"state", "sealed"}}),
            r.gauge("rmk_chunk_syncs_total", "Flushes of chunk files so far (fewer than writes when writers share them)"),
        };
    }

    void record(const MaintenanceReport& report) {
        garbage.inc(report.garbage_entries);
        reclaimed.inc(report.uploads_reclaimed);
        compacted.inc(report.chunks_compacted);
        deleted.inc(report.chunks_deleted);
        rewrapped.inc(report.versions_rewrapped);
    }
};

enum class Relocation { moved, raced, failed };

std::uint64_t payload_of(const local::ChunkStat& c) {
    return c.size - std::min<std::uint64_t>(c.size, kChunkHeaderSize);
}

}  // namespace

struct Maintenance::Impl {
    MaintenanceOptions options;
    std::shared_ptr<MetaStore> meta_owner;
    std::shared_ptr<local::ChunkStore> store;
    std::shared_ptr<crypto::KeyProvider> keys;
    MetaStore& meta;
    Metrics metrics = Metrics::make();

    std::mutex pass_mu;  // one pass at a time; guards the fields below
    std::set<ChunkId> unreconciled;  // Chunks found on disk at start whose garbage count is unknown
    std::optional<std::string> rewrap_cursor;
    double compaction_budget = 0;
    std::optional<Clock::time_point> budget_refilled;

    std::mutex stop_mu;
    std::condition_variable stop_cv;
    std::atomic<bool> stopping{false};
    std::thread loop;

    Impl(MaintenanceOptions o, std::shared_ptr<MetaStore> m, std::shared_ptr<local::ChunkStore> s,
         std::shared_ptr<crypto::KeyProvider> k)
        : options(o), meta_owner(std::move(m)), store(std::move(s)), keys(std::move(k)), meta(*meta_owner) {
        for (const auto& chunk : store->chunks())
            unreconciled.insert(chunk.id);
    }

    MaintenanceReport run_once(Clock::time_point now) {
        std::lock_guard lock(pass_mu);
        MaintenanceReport report;
        // Garbage first: reconciling afterwards counts whatever it already registered.
        collect_garbage(report);
        reclaim_uploads(now, report);
        reconcile(report);
        seal_garbage_heavy_open_chunk(report);
        compact(now, report);
        rotate_master_key(report);
        count_chunks();
        metrics.record(report);
        return report;
    }

    // ----- Garbage and expired Pending Uploads

    void collect_garbage(MaintenanceReport& report) {
        for (;;) {
            auto batch = meta.peek_garbage(kPage);
            if (!batch) {
                log("error", "peek garbage: " + describe(batch.error()));
                return;
            }
            std::vector<std::uint64_t> done;
            for (const GarbageEntry& entry : *batch) {
                auto s = store->add_garbage(entry.extent.chunk_id, entry.extent.length);
                if (!s && s.error().code != Errc::not_found) {  // a deleted Chunk needs no count
                    log("error", std::format("add garbage to chunk {}: {}", entry.extent.chunk_id, describe(s.error())));
                    break;
                }
                done.push_back(entry.id);
            }
            if (done.empty())
                return;
            if (auto s = meta.ack_garbage(done); !s) {
                log("error", "ack garbage: " + describe(s.error()));
                return;
            }
            report.garbage_entries += static_cast<std::uint32_t>(done.size());
            if (done.size() < kPage)
                return;
        }
    }

    void reclaim_uploads(Clock::time_point now, MaintenanceReport& report) {
        auto reclaimed = meta.reclaim_expired(now, kPage);
        if (!reclaimed) {
            log("error", "reclaim expired uploads: " + describe(reclaimed.error()));
            return;
        }
        report.uploads_reclaimed += static_cast<std::uint32_t>(*reclaimed);
    }

    // ----- Garbage counts after a restart

    // A Chunk found on disk at start has lost its garbage count. Everything in it that no live
    // Extent covers is garbage; with Pending Uploads in it that cannot be told, so it waits.
    void reconcile(MaintenanceReport& report) {
        for (const auto& chunk : store->chunks()) {
            if (!unreconciled.contains(chunk.id))
                continue;
            auto pending = meta.has_pending_parts(chunk.id);
            if (!pending) {
                log("error", std::format("pending parts of chunk {}: {}", chunk.id, describe(pending.error())));
                continue;
            }
            if (*pending)
                continue;
            auto live = meta.extents_in_chunk(chunk.id, kReconcileLimit);
            if (!live) {
                log("error", std::format("live extents of chunk {}: {}", chunk.id, describe(live.error())));
                continue;
            }
            if (live->size() >= kReconcileLimit)
                continue;
            std::uint64_t live_bytes = 0;
            for (const ExtentRef& ref : *live)
                live_bytes += ref.extent.length;
            const std::uint64_t garbage = payload_of(chunk) - std::min(payload_of(chunk), live_bytes);
            if (auto s = store->set_garbage(chunk.id, garbage); !s && s.error().code != Errc::not_found) {
                log("error", std::format("set garbage of chunk {}: {}", chunk.id, describe(s.error())));
                continue;
            }
            unreconciled.erase(chunk.id);
            ++report.chunks_reconciled;
        }
    }

    // ----- Compaction (ADR 0016)

    bool over_threshold(const local::ChunkStat& c) const {
        return c.garbage_bytes * 100 >= payload_of(c) * options.compaction_garbage_percent;
    }

    // The open Chunk is never compacted; once it holds a lot of garbage, close it so it can be.
    void seal_garbage_heavy_open_chunk(MaintenanceReport& report) {
        for (const auto& chunk : store->chunks()) {
            if (!chunk.open || chunk.garbage_bytes == 0 || !over_threshold(chunk) || chunk.size < chunk.capacity / 8)
                continue;
            store->seal_open();
            ++report.chunks_sealed;
            log("info", std::format("sealed chunk {} ({} of {} bytes garbage)", chunk.id, chunk.garbage_bytes, chunk.size));
        }
    }

    void compact(Clock::time_point now, MaintenanceReport& report) {
        const double rate = static_cast<double>(options.compaction_bytes_per_second);
        const double full = rate * static_cast<double>(std::max<std::int64_t>(options.interval.count(), 1));
        const double elapsed =
            budget_refilled ? std::max(0.0, std::chrono::duration<double>(now - *budget_refilled).count()) : full / rate;
        compaction_budget = std::min(full, compaction_budget + rate * elapsed);
        budget_refilled = now;

        for (const auto& chunk : store->chunks())
            if (!chunk.open && !unreconciled.contains(chunk.id) && over_threshold(chunk) &&
                now - chunk.sealed_at >= options.compaction_grace && !has_pending_parts(chunk.id))
                compact_chunk(chunk, report);
    }

    // Parts of Pending Uploads are invisible to extents_in_chunk; such a Chunk must wait.
    bool has_pending_parts(ChunkId id) {
        auto pending = meta.has_pending_parts(id);
        if (!pending)
            log("error", std::format("pending parts of chunk {}: {}", id, describe(pending.error())));
        return pending.value_or(true);
    }

    void compact_chunk(const local::ChunkStat& chunk, MaintenanceReport& report) {
        std::size_t moved = 0;
        for (;;) {
            auto live = meta.extents_in_chunk(chunk.id, kPage);
            if (!live) {
                log("error", std::format("live extents of chunk {}: {}", chunk.id, describe(live.error())));
                return;
            }
            if (live->empty())
                break;
            std::size_t progress = 0;
            for (const ExtentRef& ref : *live) {
                if (compaction_budget <= 0)
                    return;  // resumes next pass
                const Relocation r = relocate(ref);
                if (r == Relocation::failed)
                    return;
                if (r == Relocation::moved) {
                    compaction_budget -= static_cast<double>(ref.extent.length);
                    ++moved;
                    ++progress;
                }
            }
            if (progress == 0)
                return;
        }
        if (has_pending_parts(chunk.id))
            return;
        if (auto s = store->remove(chunk.id); !s) {
            log("error", std::format("delete chunk {}: {}", chunk.id, describe(s.error())));
            return;
        }
        ++report.chunks_deleted;
        if (moved > 0)
            ++report.chunks_compacted;
        log("info", std::format("{} chunk {} ({} of {} bytes garbage, {} extents moved)",
                                moved > 0 ? "compacted" : "deleted", chunk.id, chunk.garbage_bytes, chunk.size, moved));
    }

    // Copies one live Extent, stored bytes unchanged, into the open Chunk.
    Relocation relocate(const ExtentRef& ref) {
        auto bytes = store->read(ref.extent);
        if (!bytes) {
            log("error", std::format("compaction read from chunk {}: {}", ref.extent.chunk_id, describe(bytes.error())));
            return Relocation::failed;
        }
        auto copy = store->write(kReplicated3, *bytes);
        if (!copy) {
            log("error", "compaction write: " + describe(copy.error()));
            return Relocation::failed;
        }
        auto s = meta.relocate_part(ref.ref, ref.generation, ref.part_number, ref.extent, *copy);
        if (s)
            return Relocation::moved;
        if (s.error().code != Errc::aborted)
            log("error", std::format("relocate part {} of {}/{}/{}: {}", ref.part_number, ref.ref.bucket.tenant,
                                     ref.ref.bucket.bucket, ref.ref.key, describe(s.error())));
        // The copy is referenced by nothing (§6 safety rule 9).
        if (auto g = store->add_garbage(copy->chunk_id, copy->length); !g)
            log("warn", std::format("add garbage to chunk {}: {}", copy->chunk_id, describe(g.error())));
        return s.error().code == Errc::aborted ? Relocation::raced : Relocation::failed;
    }

    // ----- Master Key rotation (ADR 0017)

    void rotate_master_key(MaintenanceReport& report) {
        const std::uint32_t active = keys->active_id();
        auto page = meta.scan_versions(rewrap_cursor, options.rewrap_batch);
        if (!page) {
            log("error", "scan versions: " + describe(page.error()));
            rewrap_cursor.reset();
            return;
        }
        for (const ObjectMeta& version : page->versions) {
            const Encryption& from = version.encryption;
            if (version.delete_marker || from.wrapped_data_key.empty() || from.master_key_id == active)
                continue;
            auto to = crypto::rewrap(*keys, from, active);
            if (!to) {
                log("error", std::format("rewrap {}/{}/{}@{}: {}", version.ref.bucket.tenant, version.ref.bucket.bucket,
                                         version.ref.key, version.generation, describe(to.error())));
                continue;
            }
            auto s = meta.rewrap(version.ref, version.generation, from, *to);
            if (s)
                ++report.versions_rewrapped;
            else if (s.error().code != Errc::aborted)  // aborted: changed or deleted meanwhile
                log("error", "store rewrapped data key: " + describe(s.error()));
        }
        rewrap_cursor = page->next_cursor;
    }

    void count_chunks() {
        double open = 0, sealed = 0;
        for (const auto& chunk : store->chunks())
            (chunk.open ? open : sealed) += 1;
        metrics.open_chunks.set(open);
        metrics.sealed_chunks.set(sealed);
        metrics.chunk_syncs.set(static_cast<double>(store->sync_calls()));
    }

    void run_loop() {
        std::unique_lock lock(stop_mu);
        while (!stopping) {
            lock.unlock();
            run_once(Clock::now());
            lock.lock();
            stop_cv.wait_for(lock, options.interval, [&] { return stopping.load(); });
        }
    }
};

Result<std::unique_ptr<Maintenance>> Maintenance::create(MaintenanceOptions options, std::shared_ptr<MetaStore> meta,
                                                         std::shared_ptr<local::ChunkStore> store,
                                                         std::shared_ptr<crypto::KeyProvider> keys) {
    if (!meta || !store || !keys)
        return fail(Errc::invalid_argument, "maintenance dependencies are incomplete");
    if (options.interval.count() <= 0)
        return fail(Errc::invalid_argument, "maintenance interval must be positive");
    if (options.compaction_garbage_percent < 1 || options.compaction_garbage_percent > 100)
        return fail(Errc::invalid_argument, "compaction garbage percent must be 1-100");
    if (options.rewrap_batch == 0 || options.compaction_bytes_per_second == 0)
        return fail(Errc::invalid_argument, "rewrap batch and compaction rate must be positive");
    std::unique_ptr<Maintenance> maintenance(new Maintenance());
    maintenance->impl_ = std::make_unique<Impl>(options, std::move(meta), std::move(store), std::move(keys));
    return maintenance;
}

Maintenance::~Maintenance() {
    if (!impl_)
        return;
    {
        std::lock_guard lock(impl_->stop_mu);
        impl_->stopping = true;
    }
    impl_->stop_cv.notify_all();
    if (impl_->loop.joinable())
        impl_->loop.join();
}

MaintenanceReport Maintenance::run_once(Clock::time_point now) {
    return impl_->run_once(now);
}

void Maintenance::start() {
    std::lock_guard lock(impl_->stop_mu);
    if (impl_->loop.joinable())
        return;
    impl_->loop = std::thread([impl = impl_.get()] { impl->run_loop(); });
}

}  // namespace rmk::realmucketd
