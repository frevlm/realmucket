#include "rmk/controller.hpp"

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstdio>
#include <ctime>
#include <format>
#include <functional>
#include <map>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <grpcpp/grpcpp.h>

#include "rmk/chunk_file.hpp"
#include "rmk/ec.hpp"
#include "rmk/http.hpp"
#include "rmk/v1/datanode.grpc.pb.h"

namespace rmk::controller {
namespace {

namespace pb = rmk::internal::v1;

constexpr std::size_t kPage = 1000;
constexpr auto kStatTimeout = std::chrono::seconds(5);
constexpr auto kCallTimeout = std::chrono::seconds(10);

void log(const char* level, const std::string& message) {
    std::time_t now = std::time(nullptr);
    std::tm tm{};
    ::gmtime_r(&now, &tm);
    char stamp[32];
    std::strftime(stamp, sizeof stamp, "%Y-%m-%dT%H:%M:%SZ", &tm);
    std::fprintf(stderr, "%s %s controller %s\n", stamp, level, message.c_str());
}

std::string describe(const Error& error) {
    return std::format("{}: {}", errc_name(error.code), error.message);
}

const char* state_name(ChunkState state) {
    switch (state) {
        case ChunkState::open: return "open";
        case ChunkState::sealed: return "sealed";
        case ChunkState::erasure_coded: return "erasure_coded";
    }
    return "unknown";
}

bool contains(const std::vector<NodeId>& ids, const NodeId& id) {
    return std::ranges::find(ids, id) != ids.end();
}

// Copying a whole Chunk or shard: a minute plus one second per MiB.
std::chrono::seconds bulk_timeout(std::uint64_t bytes) {
    return std::chrono::seconds(60) + std::chrono::seconds(bytes >> 20);
}

pb::FileRef replica_file(ChunkId id) {
    pb::FileRef file;
    file.set_chunk_id(id);
    return file;
}

pb::FileRef shard_file(ChunkId id, std::size_t index) {
    pb::FileRef file = replica_file(id);
    file.set_shard(true);
    file.set_shard_index(static_cast<std::uint32_t>(index));
    return file;
}

// The file of `chunk` that nodes[index] holds.
pb::FileRef file_of(const ChunkInfo& chunk, std::size_t index) {
    return chunk.state == ChunkState::erasure_coded ? shard_file(chunk.id, index) : replica_file(chunk.id);
}

// ----- Data Node calls (proto/rmk/v1/datanode.proto)

class DataNodes {
public:
    explicit DataNodes(std::shared_ptr<rpc::Channels> channels) : channels_(std::move(channels)) {}

    Result<pb::StatResponse> stat(const std::string& address, const pb::FileRef& file) {
        pb::StatResponse response;
        auto s = call(address, kStatTimeout,
                      [&](pb::DataNode::Stub& stub, grpc::ClientContext* ctx) { return stub.Stat(ctx, file, &response); });
        if (!s)
            return std::unexpected(s.error());
        return response;
    }

    Status create(const std::string& address, ChunkId id, std::uint64_t capacity) {
        pb::CreateChunkRequest request;
        *request.mutable_file() = replica_file(id);
        request.set_capacity(capacity);
        request.set_data_units(kReplicated3.data_units);
        request.set_parity_units(kReplicated3.parity_units);
        return unary(address, kCallTimeout, &pb::DataNode::Stub::CreateChunk, request);
    }

    // Deleting a file that does not exist succeeds.
    Status remove(const std::string& address, const pb::FileRef& file) {
        return unary(address, kCallTimeout, &pb::DataNode::Stub::DeleteChunk, file);
    }

    Status copy(const std::string& address, ChunkId id, const std::string& source, std::uint64_t size) {
        pb::CopyChunkRequest request;
        *request.mutable_file() = replica_file(id);
        request.set_source_address(source);
        return unary(address, bulk_timeout(size), &pb::DataNode::Stub::CopyChunkFrom, request);
    }

    Status encode(const std::string& address, const ChunkInfo& chunk, const StoragePolicy& policy,
                  const std::vector<std::string>& targets) {
        pb::EncodeChunkRequest request;
        request.set_chunk_id(chunk.id);
        request.set_data_units(policy.data_units);
        request.set_parity_units(policy.parity_units);
        for (const std::string& target : targets)
            request.add_target_addresses(target);
        return unary(address, bulk_timeout(chunk.size), &pb::DataNode::Stub::EncodeChunk, request);
    }

    Status rebuild(const std::string& address, const ChunkInfo& chunk, std::size_t index,
                   const std::vector<std::string>& sources) {
        pb::RebuildShardRequest request;
        request.set_chunk_id(chunk.id);
        request.set_shard_index(static_cast<std::uint32_t>(index));
        request.set_logical_size(chunk.size);
        request.set_data_units(chunk.data_units);
        request.set_parity_units(chunk.parity_units);
        for (const std::string& source : sources)
            request.add_source_addresses(source);
        return unary(address, bulk_timeout(chunk.size), &pb::DataNode::Stub::RebuildShard, request);
    }

private:
    template <class Fn>
    Status call(const std::string& address, std::chrono::seconds timeout, Fn fn) {
        auto stub = pb::DataNode::NewStub(channels_->get(address));
        grpc::ClientContext ctx;
        ctx.set_deadline(std::chrono::system_clock::now() + timeout);
        grpc::Status status = fn(*stub, &ctx);
        if (!status.ok())
            return std::unexpected(rpc::from_status(status));
        return {};
    }

    template <class Method, class Request>
    Status unary(const std::string& address, std::chrono::seconds timeout, Method method, const Request& request) {
        pb::Empty response;
        return call(address, timeout, [&](pb::DataNode::Stub& stub, grpc::ClientContext* ctx) {
            return (stub.*method)(ctx, request, &response);
        });
    }

    std::shared_ptr<rpc::Channels> channels_;
};

// ----- Data Nodes as one pass sees them

struct Topology {
    std::map<NodeId, NodeInfo> nodes;
    std::set<NodeId> healthy;
    std::set<NodeId> suspect;  // no heartbeat for suspect_after; includes the dead
    std::set<NodeId> dead;     // no heartbeat for repair_delay
    std::map<NodeId, std::string> domain;  // Failure Domain of every known node
    std::map<NodeId, std::uint64_t> free;  // healthy nodes, less what this pass placed

    std::string address(const NodeId& id) const {
        auto it = nodes.find(id);
        return it == nodes.end() ? std::string() : it->second.address;
    }

    bool all_healthy(const ChunkInfo& chunk) const {
        return std::ranges::all_of(chunk.nodes, [&](const NodeId& id) { return healthy.contains(id); });
    }

    void reserve(const NodeId& id, std::uint64_t bytes) {
        if (auto it = free.find(id); it != free.end())
            it->second -= std::min(it->second, bytes);
    }
};

Topology make_topology(const std::vector<NodeInfo>& list, const Options& options, Clock::time_point now) {
    Topology t;
    for (const NodeInfo& node : list) {
        t.nodes[node.id] = node;
        const auto age = now - node.last_heartbeat;
        if (age < options.suspect_after) {
            t.healthy.insert(node.id);
            t.free[node.id] = node.capacity_bytes - std::min(node.capacity_bytes, node.used_bytes);
            continue;
        }
        t.suspect.insert(node.id);
        if (age >= options.repair_delay)
            t.dead.insert(node.id);
    }
    // ADR 0005: zone if the healthy nodes span >= 3 zones, else k8s node if >= 3, else node id.
    auto spans_three = [&](std::string NodeInfo::*field) {
        std::set<std::string> values;
        for (const NodeId& id : t.healthy)
            if (const std::string& v = t.nodes[id].*field; !v.empty())
                values.insert(v);
        return values.size() >= 3;
    };
    std::string NodeInfo::*field = spans_three(&NodeInfo::zone)       ? &NodeInfo::zone
                                   : spans_three(&NodeInfo::k8s_node) ? &NodeInfo::k8s_node
                                                                      : &NodeInfo::id;
    for (const auto& [id, node] : t.nodes)
        t.domain[id] = node.*field;
    return t;
}

// Up to `count` healthy nodes outside `exclude` with room for `bytes`, most free space first,
// each in a Failure Domain not in `used`. With allow_reuse, a short pick is filled from
// domains already taken.
std::vector<NodeId> pick(const Topology& t, std::size_t count, const std::vector<NodeId>& exclude,
                         std::set<std::string> used, std::uint64_t bytes, bool allow_reuse) {
    std::vector<NodeId> candidates;
    for (const auto& [id, free] : t.free)
        if (free >= bytes && !contains(exclude, id))
            candidates.push_back(id);
    std::ranges::stable_sort(candidates, std::greater{}, [&](const NodeId& id) { return t.free.at(id); });

    std::vector<NodeId> out;
    for (const NodeId& id : candidates)
        if (out.size() < count && used.insert(t.domain.at(id)).second)
            out.push_back(id);
    for (const NodeId& id : candidates)
        if (allow_reuse && out.size() < count && !contains(out, id))
            out.push_back(id);
    return out;
}

Result<std::vector<ChunkInfo>> list_all(ClusterStore& cluster, std::optional<ChunkState> state) {
    std::vector<ChunkInfo> out;
    std::optional<ChunkId> after;
    for (;;) {
        auto page = cluster.list_chunks(state, after, kPage);
        if (!page)
            return std::unexpected(page.error());
        out.insert(out.end(), page->begin(), page->end());
        if (page->size() < kPage)
            return out;
        after = page->back().id;
    }
}

// ----- Metrics

struct Metrics {
    metrics::Gauge& leader;
    std::map<ChunkState, metrics::Gauge*> chunks;
    metrics::Counter& created;
    metrics::Counter& sealed;
    metrics::Counter& repaired;
    metrics::Counter& encoded;
    metrics::Counter& compacted;
    metrics::Counter& deleted;
    metrics::Counter& reclaimed;
    metrics::Counter& garbage;
    metrics::Counter& rewrapped;
    metrics::Counter& removed;
    metrics::Gauge& unrecoverable;

    static Metrics make() {
        auto& r = metrics::global();
        auto counter = [&](const char* name, const char* help) -> metrics::Counter& { return r.counter(name, help); };
        Metrics m{
            r.gauge("rmk_controller_leader", "1 while this Controller is the leader"),
            {},
            counter("rmk_controller_chunks_created_total", "Open Chunks created"),
            counter("rmk_controller_chunks_sealed_total", "Open Chunks sealed because a node turned suspect"),
            counter("rmk_controller_chunks_repaired_total", "Chunks whose copies on dead nodes were recreated"),
            counter("rmk_controller_chunks_encoded_total", "Sealed Chunks converted to erasure coding"),
            counter("rmk_controller_chunks_compacted_total", "Chunks whose live Extents were moved away"),
            counter("rmk_controller_chunks_deleted_total", "Chunks deleted"),
            counter("rmk_controller_uploads_reclaimed_total", "Expired Pending Uploads reclaimed"),
            counter("rmk_controller_garbage_entries_total", "Garbage entries added to Chunk garbage counts"),
            counter("rmk_controller_versions_rewrapped_total", "Data Keys rewrapped under the active Master Key"),
            counter("rmk_controller_nodes_removed_total", "Dead Data Nodes removed"),
            r.gauge("rmk_controller_unrecoverable_chunks", "Chunks on dead nodes with no surviving source to repair from"),
        };
        for (ChunkState state : {ChunkState::open, ChunkState::sealed, ChunkState::erasure_coded})
            m.chunks[state] = &r.gauge("rmk_controller_chunks", "Chunks by state", {{"state", state_name(state)}});
        return m;
    }

    void record(const Report& report) {
        created.inc(report.chunks_created);
        sealed.inc(report.chunks_sealed);
        repaired.inc(report.chunks_repaired);
        encoded.inc(report.chunks_encoded);
        compacted.inc(report.chunks_compacted);
        deleted.inc(report.chunks_deleted);
        reclaimed.inc(report.uploads_reclaimed);
        garbage.inc(report.garbage_entries);
        rewrapped.inc(report.versions_rewrapped);
        removed.inc(report.nodes_removed);
    }
};

enum class Relocation { moved, raced, failed };

}  // namespace

// ================================================================================ Controller

struct Controller::Impl {
    static constexpr std::size_t kScrubBatch = 64;

    Options options;
    Dependencies deps;
    MetaStore& meta;
    ClusterStore& cluster;
    DataNodes data_nodes;
    rpc::ChunkReader reader;
    rpc::ChunkWriter compactor;
    Metrics metrics = Metrics::make();

    // Leadership, renewed on its own thread with the real clock (§6 safety rule 1).
    std::mutex lease_mu;
    std::atomic<std::uint64_t> epoch{0};  // last epoch confirmed by try_lead, 0 when not leading

    std::mutex pass_mu;  // one pass at a time; guards the fields below
    std::uint64_t pass_epoch = 0;   // the epoch this pass acts under
    std::size_t unrecoverable = 0;  // Chunks found beyond repair in the current pass
    std::optional<ChunkId> scrub_after;
    std::optional<std::string> rewrap_cursor;
    double compaction_budget = 0;
    std::optional<Clock::time_point> budget_refilled;

    std::mutex stop_mu;
    std::condition_variable stop_cv;
    std::atomic<bool> stopping{false};
    std::thread loop;
    std::thread renewer;

    Impl(Options o, Dependencies d)
        : options(std::move(o)),
          deps(std::move(d)),
          meta(*deps.stores.meta),
          cluster(*deps.stores.cluster),
          data_nodes(deps.channels),
          reader(deps.stores.cluster, deps.channels),
          compactor(deps.stores.cluster, deps.channels,
                    rpc::WriterOptions{options.id + "-compactor", options.writer_lease, 1, 3}) {}

    // ----- 1. Leadership

    // Acquires or renews the lease as of the real clock. The epoch held, or nothing.
    std::optional<std::uint64_t> lead() {
        std::lock_guard lock(lease_mu);
        auto held = cluster.try_lead(options.id, options.lead_ttl, Clock::now());
        if (!held) {
            if (held.error().code != Errc::aborted) {
                log("warn", "leadership check failed: " + describe(held.error()));
                return std::nullopt;  // unknown; fenced writes stay safe meanwhile
            }
            if (epoch.exchange(0) != 0)
                log("info", "no longer the leader");
            metrics.leader.set(0);
            return std::nullopt;
        }
        if (epoch.exchange(*held) != *held)
            log("info", std::format("leading as {} (epoch {})", options.id, *held));
        metrics.leader.set(1);
        return *held;
    }

    // Still the leader this pass started as, as far as the last renewal knows.
    bool leading() const { return !stopping && pass_epoch != 0 && epoch == pass_epoch; }

    // Asks the store again; required before every destructive Data Node call.
    bool confirm() {
        auto held = lead();
        return held && *held == pass_epoch && !stopping;
    }

    Fence fence() const { return Fence{options.id, pass_epoch}; }

    void renew_loop() {
        const auto period = std::max<std::chrono::milliseconds>(
            std::chrono::duration_cast<std::chrono::milliseconds>(options.lead_ttl) / 3, std::chrono::milliseconds(100));
        std::unique_lock lock(stop_mu);
        while (!stop_cv.wait_for(lock, period, [&] { return stopping.load(); })) {
            lock.unlock();
            if (epoch != 0)
                (void)lead();
            lock.lock();
        }
    }

    Report run_once(Clock::time_point now) {
        std::lock_guard lock(pass_mu);
        Report report;
        auto held = lead();
        if (!held) {
            metrics.unrecoverable.set(0);
            return report;
        }
        pass_epoch = *held;
        report.leader = true;
        unrecoverable = 0;

        auto nodes = cluster.list_nodes();
        if (!nodes) {
            log("error", "list nodes: " + describe(nodes.error()));
            return report;
        }
        Topology t = make_topology(*nodes, options, now);

        const std::function<void()> duties[] = {
            [&] { check_health(t, now, report); },
            [&] { scrub(t, report); },
            [&] { fill_pools(t, now, report); },
            [&] { collect_garbage(report); },
            [&] { reclaim_uploads(now, report); },
            [&] { encode_sealed(t, report); },
            [&] { compact(t, now, report); },
            [&] { rotate_master_key(report); },
        };
        for (const auto& duty : duties) {
            if (!leading()) {
                log("warn", "leadership lost; stopping this pass");
                break;
            }
            duty();
        }
        count_chunks();
        metrics.unrecoverable.set(static_cast<double>(unrecoverable));
        metrics.record(report);
        return report;
    }

    // ----- File deletion

    // Whether `chunk` as stored lists `node` as holder of `file`.
    static bool references(const ChunkInfo& chunk, const NodeId& node, const pb::FileRef& file) {
        for (std::size_t i = 0; i < chunk.nodes.size(); ++i) {
            if (chunk.nodes[i] != node)
                continue;
            if (file.shard() ? chunk.state == ChunkState::erasure_coded && i == file.shard_index()
                             : chunk.state != ChunkState::erasure_coded)
                return true;
        }
        return false;
    }

    // Deletes a file left over by this Controller unless the current ChunkInfo still lists it.
    void remove_unreferenced(const Topology& t, ChunkId id, const NodeId& node, const pb::FileRef& file) {
        const std::string address = t.address(node);
        if (address.empty())
            return;
        auto current = cluster.get_chunk(id);
        if (!current && current.error().code != Errc::not_found) {
            log("warn", std::format("keep chunk {} file on {}: {}", id, node, describe(current.error())));
            return;
        }
        if ((current && references(*current, node, file)) || !confirm())
            return;
        if (auto s = data_nodes.remove(address, file); !s)
            log("warn", std::format("delete leftover file of chunk {} on {}: {}", id, node, describe(s.error())));
    }

    // ----- 2. Health

    void check_health(Topology& t, Clock::time_point now, Report& report) {
        if (t.suspect.empty())
            return;
        if (auto open = list_all(cluster, ChunkState::open); !open) {
            log("error", "list open chunks: " + describe(open.error()));
        } else {
            for (const ChunkInfo& chunk : *open) {
                if (!leading())
                    return;
                if (std::ranges::any_of(chunk.nodes, [&](const NodeId& id) { return t.suspect.contains(id); }) &&
                    seal(t, chunk, now))
                    ++report.chunks_sealed;
            }
        }
        if (t.dead.empty())
            return;

        auto chunks = list_all(cluster, std::nullopt);
        if (!chunks) {
            log("error", "list chunks: " + describe(chunks.error()));
            return;
        }
        std::set<NodeId> referenced;
        for (const ChunkInfo& chunk : *chunks) {
            if (!leading())
                return;
            std::set<std::size_t> bad;
            for (std::size_t i = 0; i < chunk.nodes.size(); ++i)
                if (t.dead.contains(chunk.nodes[i]))
                    bad.insert(i);
            const ChunkInfo* stored = &chunk;
            Result<ChunkInfo> repaired = fail(Errc::aborted, "not repaired");
            if (!bad.empty() && chunk.state != ChunkState::open) {
                repaired = repair(t, chunk, bad);
                if (repaired) {
                    ++report.chunks_repaired;
                    stored = &*repaired;
                }
            }
            referenced.insert(stored->nodes.begin(), stored->nodes.end());
        }
        for (const NodeId& id : t.dead) {
            if (referenced.contains(id))
                continue;
            if (auto s = cluster.remove_node(id, fence()); !s && s.error().code != Errc::not_found) {
                log("error", "remove node " + id + ": " + describe(s.error()));
                continue;
            }
            log("info", "removed dead node " + id);
            ++report.nodes_removed;
        }
    }

    // Seals an open Chunk at the largest size any reachable replica reports.
    bool seal(const Topology& t, const ChunkInfo& chunk, Clock::time_point now) {
        ChunkInfo next = chunk;
        for (const NodeId& id : chunk.nodes) {
            const std::string address = t.address(id);
            if (address.empty())
                continue;
            if (auto st = data_nodes.stat(address, replica_file(chunk.id)); st && st->exists())
                next.size = std::max<std::uint64_t>(next.size, st->size());
        }
        next.state = ChunkState::sealed;
        next.sealed_at = now;
        next.writer.clear();
        next.writer_lease_expiry = {};
        if (auto stored = cluster.update_chunk(next, fence()); !stored) {
            log("warn", std::format("seal chunk {}: {}", chunk.id, describe(stored.error())));
            return false;
        }
        log("info", std::format("sealed chunk {} at {} bytes: a replica node is suspect", chunk.id, next.size));
        return true;
    }

    // ----- Scrub (§6 safety rule 7): a rotating batch of immutable Chunks per pass.

    static std::uint64_t expected_size(const ChunkInfo& chunk) {
        return chunk.state == ChunkState::erasure_coded
                   ? kChunkHeaderSize + ec::shard_length(chunk.size, chunk.data_units)
                   : chunk.size;
    }

    void scrub(Topology& t, Report& report) {
        auto page = cluster.list_chunks(std::nullopt, scrub_after, kScrubBatch);
        if (!page) {
            log("error", "scrub: list chunks: " + describe(page.error()));
            return;
        }
        scrub_after = page->size() < kScrubBatch ? std::nullopt : std::optional(page->back().id);
        for (const ChunkInfo& chunk : *page) {
            if (!leading())
                return;
            if (chunk.state == ChunkState::open)
                continue;
            std::set<std::size_t> bad;
            for (std::size_t i = 0; i < chunk.nodes.size(); ++i) {
                if (!t.healthy.contains(chunk.nodes[i]))
                    continue;  // unreachable holders are the health check's business
                auto st = data_nodes.stat(t.address(chunk.nodes[i]), file_of(chunk, i));
                if (st && (!st->exists() || st->size() < expected_size(chunk))) {
                    log("warn", std::format("scrub: chunk {} file on {} is {}", chunk.id, chunk.nodes[i],
                                            st->exists() ? "short" : "missing"));
                    bad.insert(i);
                }
            }
            if (!bad.empty() && repair(t, chunk, bad))
                ++report.chunks_repaired;
        }
    }

    // ----- Repair

    // Moves every slot in `bad` (dead holder, or missing or short file) to a new healthy node.
    // Returns the stored Chunk.
    Result<ChunkInfo> repair(Topology& t, ChunkInfo chunk, std::set<std::size_t> bad) {
        while (!bad.empty()) {
            if (!leading())
                return fail(Errc::aborted, "leadership lost");
            const std::size_t i = *bad.begin();
            const NodeId old = chunk.nodes[i];
            auto next = chunk.state == ChunkState::erasure_coded ? rebuild_shard(t, chunk, i, bad)
                                                                 : copy_replica(t, chunk, i, bad);
            if (!next) {
                log("error", std::format("repair chunk {} on {}: {}", chunk.id, old, describe(next.error())));
                return std::unexpected(next.error());
            }
            log("info", std::format("repaired chunk {}: {} replaced by {}", chunk.id, old, next->nodes[i]));
            if (t.healthy.contains(old))
                remove_unreferenced(t, chunk.id, old, file_of(chunk, i));
            chunk = *next;
            bad.erase(i);
        }
        return chunk;
    }

    // A healthy node for slot `index` of `chunk`, outside the domains of the other slots if possible.
    // The slot's own holder, when healthy (its file went missing or short), is the last resort.
    Result<NodeId> replacement(const Topology& t, const ChunkInfo& chunk, std::size_t index, std::uint64_t bytes) {
        std::set<std::string> used;
        std::vector<NodeId> others;
        for (std::size_t j = 0; j < chunk.nodes.size(); ++j)
            if (j != index) {
                used.insert(t.domain.contains(chunk.nodes[j]) ? t.domain.at(chunk.nodes[j]) : chunk.nodes[j]);
                others.push_back(chunk.nodes[j]);
            }
        auto target = pick(t, 1, chunk.nodes, used, bytes, false);
        if (target.empty())
            target = pick(t, 1, chunk.nodes, used, bytes, true);
        if (target.empty())
            target = pick(t, 1, others, used, bytes, true);
        if (target.empty())
            return fail(Errc::unavailable, "no healthy node with room");
        return target.front();
    }

    Result<ChunkInfo> copy_replica(Topology& t, const ChunkInfo& chunk, std::size_t index,
                                   const std::set<std::size_t>& bad) {
        std::vector<std::string> sources;
        for (std::size_t j = 0; j < chunk.nodes.size(); ++j)
            if (!bad.contains(j) && t.healthy.contains(chunk.nodes[j]))
                sources.push_back(t.address(chunk.nodes[j]));
        if (sources.empty()) {
            ++unrecoverable;
            return fail(Errc::unavailable, "no surviving replica");
        }
        auto target = replacement(t, chunk, index, chunk.size);
        if (!target)
            return std::unexpected(target.error());
        const std::string address = t.address(*target);

        Status copied = fail(Errc::unavailable, "no source tried");
        for (const std::string& source : sources) {
            if (!confirm())
                return fail(Errc::aborted, "leadership lost");
            if (copied = data_nodes.copy(address, chunk.id, source, chunk.size); copied)
                break;
        }
        if (!copied)
            return std::unexpected(copied.error());
        return place(t, chunk, index, *target, replica_file(chunk.id));
    }

    Result<ChunkInfo> rebuild_shard(Topology& t, const ChunkInfo& chunk, std::size_t index,
                                    const std::set<std::size_t>& bad) {
        std::vector<std::string> sources(chunk.nodes.size());
        std::size_t available = 0;
        for (std::size_t j = 0; j < chunk.nodes.size(); ++j)
            if (!bad.contains(j) && t.healthy.contains(chunk.nodes[j])) {
                sources[j] = t.address(chunk.nodes[j]);
                ++available;
            }
        if (available < chunk.data_units) {
            ++unrecoverable;
            return fail(Errc::unavailable, std::format("only {} of {} needed shards survive", available, chunk.data_units));
        }
        auto target = replacement(t, chunk, index, expected_size(chunk));
        if (!target)
            return std::unexpected(target.error());
        if (!confirm())
            return fail(Errc::aborted, "leadership lost");
        if (auto s = data_nodes.rebuild(t.address(*target), chunk, index, sources); !s)
            return std::unexpected(s.error());
        return place(t, chunk, index, *target, shard_file(chunk.id, index));
    }

    // Records `target` as holder of slot `index`, whose file it now has.
    Result<ChunkInfo> place(Topology& t, const ChunkInfo& chunk, std::size_t index, const NodeId& target,
                            const pb::FileRef& file) {
        ChunkInfo next = chunk;
        next.nodes[index] = target;
        auto stored = cluster.update_chunk(next, fence());
        if (!stored) {
            if (stored.error().code == Errc::aborted)
                remove_unreferenced(t, chunk.id, target, file);
            return stored;
        }
        t.reserve(target, expected_size(chunk));
        return stored;
    }

    // ----- 3. Open Chunk pool

    // Bucket policies, 1+2, and the target policy of every Chunk due for Compaction, whose
    // live Extents need an open Chunk of that policy even if no Bucket uses it any more.
    std::set<StoragePolicy> policies_in_use(const Topology& t, Clock::time_point now) {
        std::set<StoragePolicy> policies{kReplicated3};
        for (const ChunkInfo& chunk : due_for_compaction(t, now))
            policies.insert(chunk.target_policy);
        auto tenants = meta.list_tenants();
        if (!tenants) {
            log("error", "list tenants: " + describe(tenants.error()));
            return policies;
        }
        for (const std::string& tenant : *tenants) {
            auto buckets = meta.list_buckets(tenant);
            if (!buckets) {
                log("error", "list buckets of " + tenant + ": " + describe(buckets.error()));
                continue;
            }
            for (const BucketInfo& bucket : *buckets)
                policies.insert(bucket.config.storage_policy);
        }
        return policies;
    }

    void fill_pools(Topology& t, Clock::time_point now, Report& report) {
        auto open = list_all(cluster, ChunkState::open);
        if (!open) {
            log("error", "list open chunks: " + describe(open.error()));
            return;
        }
        // Only Chunks a writer can claim count: room left and no live lease (§6 safety rule 3).
        for (const StoragePolicy& policy : policies_in_use(t, now)) {
            auto usable = std::ranges::count_if(*open, [&](const ChunkInfo& c) {
                return c.target_policy == policy && c.capacity - std::min(c.capacity, c.size) >= c.capacity / 2 &&
                       (c.writer.empty() || c.writer_lease_expiry <= now);
            });
            for (; usable < options.open_chunks_per_policy && leading(); ++usable) {
                if (!create_chunk(t, policy))
                    break;
                ++report.chunks_created;
            }
        }
    }

    bool create_chunk(Topology& t, const StoragePolicy& policy) {
        const std::size_t copies = kReplicated3.data_units + kReplicated3.parity_units;
        auto nodes = pick(t, copies, {}, {}, options.chunk_capacity, false);
        if (nodes.size() < copies) {
            log("warn", std::format("cannot create an open chunk: {} healthy nodes with room in distinct failure "
                                    "domains, {} needed",
                                    nodes.size(), copies));
            return false;
        }
        auto id = cluster.allocate_chunk_id();
        if (!id) {
            log("error", "allocate chunk id: " + describe(id.error()));
            return false;
        }
        ChunkInfo chunk;
        chunk.id = *id;
        chunk.state = ChunkState::open;
        chunk.target_policy = policy;
        chunk.data_units = kReplicated3.data_units;
        chunk.parity_units = kReplicated3.parity_units;
        chunk.nodes = nodes;
        chunk.capacity = options.chunk_capacity;
        chunk.size = kChunkHeaderSize;

        Status s;
        for (const NodeId& node : nodes)
            if (s = data_nodes.create(t.address(node), chunk.id, chunk.capacity); !s) {
                log("error", std::format("create chunk {} on {}: {}", chunk.id, node, describe(s.error())));
                break;
            }
        if (s)
            if (s = cluster.put_chunk(chunk, fence()); !s)
                log("error", std::format("record chunk {}: {}", chunk.id, describe(s.error())));
        if (!s) {
            for (const NodeId& node : nodes)
                remove_unreferenced(t, chunk.id, node, replica_file(chunk.id));
            return false;
        }
        for (const NodeId& node : nodes)
            t.reserve(node, chunk.capacity);
        log("info", std::format("created open chunk {} for {}+{} on {}, {}, {}", chunk.id, policy.data_units,
                                policy.parity_units, nodes[0], nodes[1], nodes[2]));
        return true;
    }

    // ----- 4. Garbage, 5. expired Pending Uploads

    void collect_garbage(Report& report) {
        for (;;) {
            auto batch = meta.peek_garbage(kPage);
            if (!batch) {
                log("error", "peek garbage: " + describe(batch.error()));
                return;
            }
            std::vector<std::uint64_t> done;
            for (const GarbageEntry& entry : *batch) {
                auto s = cluster.add_garbage(entry.extent.chunk_id, entry.extent.length);
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

    void reclaim_uploads(Clock::time_point now, Report& report) {
        auto reclaimed = meta.reclaim_expired(now, kPage);
        if (!reclaimed) {
            log("error", "reclaim expired uploads: " + describe(reclaimed.error()));
            return;
        }
        report.uploads_reclaimed += static_cast<std::uint32_t>(*reclaimed);
    }

    // ----- 6. Erasure coding

    void encode_sealed(Topology& t, Report& report) {
        auto sealed = list_all(cluster, ChunkState::sealed);
        if (!sealed) {
            log("error", "list sealed chunks: " + describe(sealed.error()));
            return;
        }
        // A Chunk over the garbage threshold is rewritten soon; encoding it first would be wasted work.
        for (const ChunkInfo& chunk : *sealed) {
            if (!leading())
                return;
            if (chunk.target_policy != kReplicated3 && !over_threshold(chunk) && t.all_healthy(chunk) &&
                encode(t, chunk))
                ++report.chunks_encoded;
        }
    }

    bool encode(Topology& t, const ChunkInfo& chunk) {
        const StoragePolicy policy = chunk.target_policy;
        const std::size_t width = policy.data_units + policy.parity_units;
        // The Data Node encodes the first ChunkInfo.size bytes of its replica.
        std::string encoder;
        for (const NodeId& id : chunk.nodes)
            if (auto st = data_nodes.stat(t.address(id), replica_file(chunk.id)); st && st->exists() &&
                                                                                 st->size() >= chunk.size) {
                encoder = t.address(id);
                break;
            }
        if (encoder.empty()) {
            log("warn", std::format("encode chunk {}: no replica holds all {} bytes", chunk.id, chunk.size));
            return false;
        }
        const std::uint64_t shard_bytes = kChunkHeaderSize + ec::shard_length(chunk.size, policy.data_units);
        auto targets = pick(t, width, {}, {}, shard_bytes, false);
        if (targets.size() < width) {
            targets = pick(t, width, {}, {}, shard_bytes, true);
            if (targets.size() < width) {
                log("warn", std::format("encode chunk {}: {} healthy nodes with room, {} needed", chunk.id,
                                        targets.size(), width));
                return false;
            }
            log("warn", std::format("encode chunk {}: fewer than {} failure domains, some share one", chunk.id, width));
        }
        std::vector<std::string> addresses;
        for (const NodeId& id : targets)
            addresses.push_back(t.address(id));
        if (!confirm())
            return false;
        if (auto s = data_nodes.encode(encoder, chunk, policy, addresses); !s) {
            log("error", std::format("encode chunk {}: {}", chunk.id, describe(s.error())));
            return false;
        }

        ChunkInfo next = chunk;
        next.state = ChunkState::erasure_coded;
        next.data_units = policy.data_units;
        next.parity_units = policy.parity_units;
        next.nodes = targets;
        if (auto stored = cluster.update_chunk(next, fence()); !stored) {
            log("warn", std::format("record encoded chunk {}: {}", chunk.id, describe(stored.error())));
            if (stored.error().code == Errc::aborted)
                for (std::size_t i = 0; i < width; ++i)
                    remove_unreferenced(t, chunk.id, targets[i], shard_file(chunk.id, i));
            return false;
        }
        for (const NodeId& id : targets)
            t.reserve(id, shard_bytes);
        for (const NodeId& id : chunk.nodes)
            remove_unreferenced(t, chunk.id, id, replica_file(chunk.id));
        log("info", std::format("encoded chunk {} as {}+{}", chunk.id, policy.data_units, policy.parity_units));
        return true;
    }

    // ----- 7. Compaction (ADR 0016)

    bool over_threshold(const ChunkInfo& c) const {
        const std::uint64_t payload = c.size - std::min<std::uint64_t>(c.size, kChunkHeaderSize);
        return c.garbage_bytes * 100 >= payload * options.compaction_garbage_percent;
    }

    void compact(const Topology& t, Clock::time_point now, Report& report) {
        const double rate = static_cast<double>(options.compaction_bytes_per_second);
        const double full = rate * static_cast<double>(std::max<std::int64_t>(options.interval.count(), 1));
        const double elapsed =
            budget_refilled ? std::max(0.0, std::chrono::duration<double>(now - *budget_refilled).count()) : full / rate;
        compaction_budget = std::min(full, compaction_budget + rate * elapsed);
        budget_refilled = now;

        for (const ChunkInfo& chunk : due_for_compaction(t, now)) {
            if (!leading())
                return;
            compact_chunk(t, chunk, report);
        }
    }

    // Immutable Chunks over the garbage threshold, sealed at least compaction_grace ago (every
    // Extent written to them is registered by then, §6 safety rule 2), on healthy nodes only
    // (repair comes first) and without Parts of Pending Uploads.
    std::vector<ChunkInfo> due_for_compaction(const Topology& t, Clock::time_point now) {
        std::vector<ChunkInfo> due;
        for (ChunkState state : {ChunkState::sealed, ChunkState::erasure_coded}) {
            auto chunks = list_all(cluster, state);
            if (!chunks) {
                log("error", std::format("list {} chunks: {}", state_name(state), describe(chunks.error())));
                continue;
            }
            for (const ChunkInfo& chunk : *chunks)
                if (over_threshold(chunk) && now - chunk.sealed_at >= options.compaction_grace &&
                    t.all_healthy(chunk) && !has_pending_parts(chunk))
                    due.push_back(chunk);
        }
        return due;
    }

    // Parts of Pending Uploads are invisible to extents_in_chunk; such a Chunk must wait.
    bool has_pending_parts(const ChunkInfo& chunk) {
        auto pending = meta.has_pending_parts(chunk.id);
        if (!pending)
            log("error", std::format("pending parts of chunk {}: {}", chunk.id, describe(pending.error())));
        return pending.value_or(true);
    }

    void compact_chunk(const Topology& t, const ChunkInfo& chunk, Report& report) {
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
                if (compaction_budget <= 0 || !leading())
                    return;  // resumes next pass
                const Relocation r = relocate(ref, chunk.target_policy);
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
        if (has_pending_parts(chunk) || !delete_chunk(t, chunk))
            return;
        ++report.chunks_deleted;
        if (moved > 0)
            ++report.chunks_compacted;
        log("info", std::format("{} chunk {} ({} of {} bytes garbage, {} extents moved)",
                                moved > 0 ? "compacted" : "deleted", chunk.id, chunk.garbage_bytes, chunk.size, moved));
    }

    // Copies one live Extent, stored bytes unchanged, into a Chunk of the same target policy.
    Relocation relocate(const ExtentRef& ref, const StoragePolicy& policy) {
        auto bytes = reader.read(ref.extent);
        if (!bytes) {
            log("error", std::format("compaction read from chunk {}: {}", ref.extent.chunk_id, describe(bytes.error())));
            return Relocation::failed;
        }
        auto copy = compactor.write(policy, *bytes);
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
        if (auto g = cluster.add_garbage(copy->chunk_id, copy->length); !g)
            log("warn", std::format("add garbage to chunk {}: {}", copy->chunk_id, describe(g.error())));
        return s.error().code == Errc::aborted ? Relocation::raced : Relocation::failed;
    }

    // Files first, then the record: a failure leaves the Chunk listed and the next pass retries.
    bool delete_chunk(const Topology& t, const ChunkInfo& chunk) {
        for (std::size_t i = 0; i < chunk.nodes.size(); ++i) {
            const std::string address = t.address(chunk.nodes[i]);
            if (address.empty())
                continue;  // the node is gone, and its files with it
            if (!confirm())
                return false;
            if (auto s = data_nodes.remove(address, file_of(chunk, i)); !s) {
                log("warn", std::format("delete chunk {} on {}: {}", chunk.id, chunk.nodes[i], describe(s.error())));
                return false;
            }
        }
        if (auto s = cluster.delete_chunk(chunk.id, fence()); !s && s.error().code != Errc::not_found) {
            log("error", std::format("delete chunk record {}: {}", chunk.id, describe(s.error())));
            return false;
        }
        return true;
    }

    // ----- 8. Master Key rotation (ADR 0017)

    void rotate_master_key(Report& report) {
        const std::uint32_t active = deps.keys->active_id();
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
            auto to = crypto::rewrap(*deps.keys, from, active);
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
        auto chunks = list_all(cluster, std::nullopt);
        if (!chunks)
            return;
        for (auto& [state, gauge] : metrics.chunks)
            gauge->set(static_cast<double>(std::ranges::count(*chunks, state, &ChunkInfo::state)));
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

Result<std::unique_ptr<Controller>> Controller::create(Options options, Dependencies deps) {
    if (options.id.empty())
        return fail(Errc::invalid_argument, "controller id is empty");
    if (!deps.stores.meta || !deps.stores.cluster || !deps.keys || !deps.channels)
        return fail(Errc::invalid_argument, "controller dependencies are incomplete");
    if (options.interval.count() <= 0 || options.lead_ttl.count() <= 0)
        return fail(Errc::invalid_argument, "interval and lead_ttl must be positive");
    if (options.chunk_capacity <= kChunkHeaderSize)
        return fail(Errc::invalid_argument, "chunk capacity must exceed the chunk header");
    if (options.compaction_garbage_percent < 1 || options.compaction_garbage_percent > 100)
        return fail(Errc::invalid_argument, "compaction garbage percent must be 1-100");
    if (options.rewrap_batch == 0 || options.compaction_bytes_per_second == 0)
        return fail(Errc::invalid_argument, "rewrap batch and compaction rate must be positive");
    std::unique_ptr<Controller> controller(new Controller());
    controller->impl_ = std::make_unique<Impl>(std::move(options), std::move(deps));
    controller->impl_->renewer = std::thread([impl = controller->impl_.get()] { impl->renew_loop(); });
    return controller;
}

Controller::~Controller() {
    if (!impl_)
        return;
    {
        std::lock_guard lock(impl_->stop_mu);
        impl_->stopping = true;
    }
    impl_->stop_cv.notify_all();
    if (impl_->loop.joinable())
        impl_->loop.join();
    if (impl_->renewer.joinable())
        impl_->renewer.join();
}

Result<Report> Controller::run_once(Clock::time_point now) {
    return impl_->run_once(now);
}

void Controller::start() {
    std::lock_guard lock(impl_->stop_mu);
    if (impl_->loop.joinable())
        return;
    impl_->loop = std::thread([impl = impl_.get()] { impl->run_loop(); });
}

}  // namespace rmk::controller
