#include "fdb.hpp"

namespace rmk::fdb {
namespace {

std::string describe(ChunkId id) { return "chunk " + std::to_string(id); }

struct Keys {
    const std::string& p;

    std::string space(Tag t) const { return p + static_cast<char>(t); }
    std::string node(std::string_view id) const {
        std::string k = space(kNode);
        put_str(k, id);
        return k;
    }
    std::string chunk(ChunkId id) const { return id_key(kChunk, id); }
    std::string garbage(ChunkId id) const { return id_key(kChunkGarbage, id); }
    std::string by_state(ChunkState s) const { return space(kChunkByState) + static_cast<char>(s); }
    std::string by_state(ChunkState s, ChunkId id) const {
        std::string k = by_state(s);
        put_be(k, id, 8);
        return k;
    }

private:
    std::string id_key(Tag t, ChunkId id) const {
        std::string k = space(t);
        put_be(k, id, 8);
        return k;
    }
};

std::string encode_node(const NodeInfo& n) {
    return Writer()
        .str(n.id)
        .str(n.address)
        .str(n.k8s_node)
        .str(n.zone)
        .u64(n.capacity_bytes)
        .u64(n.used_bytes)
        .time(n.last_heartbeat)
        .take();
}

std::optional<NodeInfo> decode_node(std::string_view v) {
    Reader r(v);
    NodeInfo n;
    n.id = r.str();
    n.address = r.str();
    n.k8s_node = r.str();
    n.zone = r.str();
    n.capacity_bytes = r.u64();
    n.used_bytes = r.u64();
    n.last_heartbeat = r.time();
    return r.ok() ? std::optional(std::move(n)) : std::nullopt;
}

// garbage_bytes lives in its own counter key (atomic add), not in the record.
std::string encode_chunk(const ChunkInfo& c) {
    Writer w;
    w.u64(c.id)
        .u8(static_cast<std::uint8_t>(c.state))
        .u8(c.target_policy.data_units)
        .u8(c.target_policy.parity_units)
        .u8(c.data_units)
        .u8(c.parity_units)
        .u32(static_cast<std::uint32_t>(c.nodes.size()));
    for (const auto& n : c.nodes) w.str(n);
    w.u64(c.capacity).u64(c.size).time(c.sealed_at).str(c.writer).time(c.writer_lease_expiry).u64(c.revision);
    return w.take();
}

std::optional<ChunkInfo> decode_chunk(std::string_view v, std::optional<std::string_view> garbage) {
    Reader r(v);
    ChunkInfo c;
    c.id = r.u64();
    std::uint8_t state = r.u8();
    c.target_policy.data_units = r.u8();
    c.target_policy.parity_units = r.u8();
    c.data_units = r.u8();
    c.parity_units = r.u8();
    std::uint32_t n = r.u32();
    for (std::uint32_t i = 0; i < n && i <= v.size(); ++i) c.nodes.push_back(r.str());
    c.capacity = r.u64();
    c.size = r.u64();
    c.sealed_at = r.time();
    c.writer = r.str();
    c.writer_lease_expiry = r.time();
    c.revision = r.u64();
    if (!r.ok() || state > static_cast<std::uint8_t>(ChunkState::erasure_coded)) return std::nullopt;
    c.state = static_cast<ChunkState>(state);
    c.garbage_bytes = garbage ? from_le64(*garbage) : 0;
    return c;
}

Result<std::optional<ChunkInfo>> load_chunk(Txn& tx, const Keys& k, ChunkId id, bool snapshot = false) {
    auto v = tx.get_many({k.chunk(id), k.garbage(id)}, snapshot);
    if (!v) return std::unexpected(v.error());
    if (!(*v)[0]) return std::nullopt;
    auto c = decode_chunk(*(*v)[0], (*v)[1] ? std::optional<std::string_view>(*(*v)[1]) : std::nullopt);
    if (!c) return corrupt("chunk");
    return c;
}

void store_chunk(Txn& tx, const Keys& k, const ChunkInfo& c) { tx.set(k.chunk(c.id), encode_chunk(c)); }

struct Leader {
    std::string holder;
    Clock::time_point expiry;
    std::uint64_t epoch = 0;
};

std::optional<Leader> decode_leader(std::string_view v) {
    Reader r(v);
    Leader l;
    l.holder = r.str();
    l.expiry = r.time();
    l.epoch = r.u64();
    return r.ok() ? std::optional(std::move(l)) : std::nullopt;
}

// A fenced write reads the lease record with a conflict range, so it commits only while the
// lease still names this holder and epoch.
Status check_fence(Txn& tx, const Keys& k, const std::optional<Fence>& fence) {
    if (!fence) return {};
    auto v = tx.get(k.space(kLeader));
    if (!v) return std::unexpected(v.error());
    auto leader = *v ? decode_leader(**v) : std::nullopt;
    if (*v && !leader) return corrupt("leader");
    if (leader && leader->holder == fence->holder && leader->epoch == fence->epoch) return {};
    return fail(Errc::aborted, "leadership lost: " + fence->holder);
}

}  // namespace

FdbClusterStore::FdbClusterStore(std::shared_ptr<Database> db, std::string key_prefix)
    : db_(std::move(db)), prefix_(encode_prefix(key_prefix)) {}

// --- Data Nodes

Status FdbClusterStore::heartbeat(const NodeInfo& node, Clock::time_point now) {
    if (node.id.empty()) return fail(Errc::invalid_argument, "node id is empty");
    Keys k{prefix_};
    NodeInfo stamped = node;
    stamped.last_heartbeat = now;
    return run(*db_, [&](Txn& tx) -> Status {
        tx.set(k.node(node.id), encode_node(stamped));
        return {};
    });
}

Result<std::vector<NodeInfo>> FdbClusterStore::list_nodes() {
    Keys k{prefix_};
    return run(*db_, [&](Txn& tx) -> Result<std::vector<NodeInfo>> {
        std::string space = k.space(kNode);
        auto kvs = tx.range(space, strinc(space));
        if (!kvs) return std::unexpected(kvs.error());
        std::vector<NodeInfo> out;
        for (const auto& kv : *kvs) {
            auto n = decode_node(kv.value);
            if (!n) return corrupt("node");
            out.push_back(std::move(*n));
        }
        return out;
    });
}

Status FdbClusterStore::remove_node(std::string_view id, const std::optional<Fence>& fence) {
    Keys k{prefix_};
    return run(*db_, [&](Txn& tx) -> Status {
        if (auto ok = check_fence(tx, k, fence); !ok) return ok;
        auto v = tx.get(k.node(id));
        if (!v) return std::unexpected(v.error());
        if (!*v) {
            if (tx.maybe_committed) return {};
            return fail(Errc::not_found, "no such node: " + std::string(id));
        }
        tx.clear(k.node(id));
        return {};
    });
}

// --- Chunks

// A counter: only the leading Controller allocates, so it is never contended. Ids taken by
// put_chunk without allocation are skipped; a retried allocation may leave a gap.
Result<ChunkId> FdbClusterStore::allocate_chunk_id() {
    Keys k{prefix_};
    return run(*db_, [&](Txn& tx) -> Result<ChunkId> {
        std::string counter = k.space(kChunkCounter);
        auto v = tx.get(counter);
        if (!v) return std::unexpected(v.error());
        ChunkId id = *v ? get_be(**v) : 0;
        for (;;) {
            ++id;
            auto taken = tx.get(k.chunk(id));
            if (!taken) return std::unexpected(taken.error());
            if (!*taken) break;
        }
        std::string value;
        put_be(value, id, 8);
        tx.set(counter, value);
        return id;
    });
}

Status FdbClusterStore::put_chunk(const ChunkInfo& chunk, const std::optional<Fence>& fence) {
    Keys k{prefix_};
    return run(*db_, [&](Txn& tx) -> Status {
        if (auto ok = check_fence(tx, k, fence); !ok) return ok;
        auto existing = load_chunk(tx, k, chunk.id);
        if (!existing) return std::unexpected(existing.error());
        if (*existing) {
            if (tx.maybe_committed && encode_chunk(**existing) == encode_chunk(chunk)) return {};
            return fail(Errc::already_exists, "exists: " + describe(chunk.id));
        }
        store_chunk(tx, k, chunk);
        tx.set(k.by_state(chunk.state, chunk.id), "");
        tx.set(k.garbage(chunk.id), le64(chunk.garbage_bytes));
        return {};
    });
}

Result<ChunkInfo> FdbClusterStore::get_chunk(ChunkId id) {
    Keys k{prefix_};
    return run(*db_, [&](Txn& tx) -> Result<ChunkInfo> {
        auto c = load_chunk(tx, k, id);
        if (!c) return std::unexpected(c.error());
        if (!*c) return fail(Errc::not_found, "no such " + describe(id));
        return std::move(**c);
    });
}

Result<std::vector<ChunkInfo>> FdbClusterStore::list_chunks(std::optional<ChunkState> state,
                                                            std::optional<ChunkId> after, std::size_t limit) {
    if (limit == 0) return std::vector<ChunkInfo>{};
    Keys k{prefix_};
    return run(*db_, [&](Txn& tx) -> Result<std::vector<ChunkInfo>> {
        std::string space = state ? k.by_state(*state) : k.space(kChunk);
        std::string begin = space;
        if (after) {
            put_be(begin, *after, 8);
            begin += '\0';
        }
        auto kvs = tx.range(begin, strinc(space), {.limit = limit});
        if (!kvs) return std::unexpected(kvs.error());
        std::vector<std::string> keys;
        for (const auto& kv : *kvs) {
            if (kv.key.size() != space.size() + 8) return corrupt("chunk key");
            ChunkId id = get_be(std::string_view(kv.key).substr(space.size()));
            keys.push_back(k.chunk(id));
            keys.push_back(k.garbage(id));
        }
        auto values = tx.get_many(keys);
        if (!values) return std::unexpected(values.error());
        std::vector<ChunkInfo> out;
        for (std::size_t i = 0; i < values->size(); i += 2) {
            const auto& g = (*values)[i + 1];
            auto c = (*values)[i] ? decode_chunk(*(*values)[i], g ? std::optional<std::string_view>(*g) : std::nullopt)
                                  : std::nullopt;
            if (!c) return corrupt("chunk");
            out.push_back(std::move(*c));
        }
        return out;
    });
}

// garbage_bytes in `next` is ignored: the counter belongs to add_garbage, so a stale copy read
// before an add cannot overwrite it.
Result<ChunkInfo> FdbClusterStore::update_chunk(const ChunkInfo& next, const std::optional<Fence>& fence) {
    Keys k{prefix_};
    return run(*db_, [&](Txn& tx) -> Result<ChunkInfo> {
        if (auto ok = check_fence(tx, k, fence); !ok) return std::unexpected(ok.error());
        auto loaded = load_chunk(tx, k, next.id);
        if (!loaded) return std::unexpected(loaded.error());
        if (!*loaded) return fail(Errc::not_found, "no such " + describe(next.id));
        ChunkInfo current = std::move(**loaded);
        ChunkInfo stored = next;
        stored.revision = next.revision + 1;
        stored.garbage_bytes = current.garbage_bytes;
        if (current.revision != next.revision) {
            if (tx.maybe_committed && encode_chunk(current) == encode_chunk(stored)) return current;
            return fail(Errc::aborted, "revision changed: " + describe(next.id));
        }
        store_chunk(tx, k, stored);
        if (current.state != stored.state) {
            tx.clear(k.by_state(current.state, stored.id));
            tx.set(k.by_state(stored.state, stored.id), "");
        }
        return stored;
    });
}

Status FdbClusterStore::delete_chunk(ChunkId id, const std::optional<Fence>& fence) {
    Keys k{prefix_};
    return run(*db_, [&](Txn& tx) -> Status {
        if (auto ok = check_fence(tx, k, fence); !ok) return ok;
        auto c = load_chunk(tx, k, id);
        if (!c) return std::unexpected(c.error());
        if (!*c) {
            if (tx.maybe_committed) return {};
            return fail(Errc::not_found, "no such " + describe(id));
        }
        tx.clear(k.chunk(id));
        tx.clear(k.garbage(id));
        tx.clear(k.by_state((*c)->state, id));
        return {};
    });
}

// Candidates are read without conflict ranges; only the claimed Chunk's record conflicts, so
// Gateways claiming different Chunks do not abort each other.
Result<ChunkInfo> FdbClusterStore::claim_open_chunk(const StoragePolicy& policy, std::string_view writer,
                                                    std::uint64_t min_free, Clock::time_point lease_until,
                                                    Clock::time_point now) {
    Keys k{prefix_};
    return run(*db_, [&](Txn& tx) -> Result<ChunkInfo> {
        std::string space = k.by_state(ChunkState::open);
        std::optional<ChunkInfo> chosen;
        std::optional<Error> bad;
        auto s = tx.scan(space, strinc(space), {.snapshot = true}, [&](KeyValue&& kv) {
            ChunkId id = get_be(std::string_view(kv.key).substr(space.size()));
            auto c = load_chunk(tx, k, id, true);
            if (!c || !*c) {
                bad = c ? corrupt("chunk index").error() : c.error();
                return false;
            }
            const ChunkInfo& chunk = **c;
            bool lease_free = chunk.writer.empty() || chunk.writer == writer || chunk.writer_lease_expiry <= now;
            std::uint64_t free = chunk.capacity > chunk.size ? chunk.capacity - chunk.size : 0;
            if (chunk.state != ChunkState::open || chunk.target_policy != policy || !lease_free || free < min_free)
                return true;
            chosen = std::move(**c);
            return false;
        });
        if (!s) return std::unexpected(s.error());
        if (bad) return std::unexpected(*bad);
        if (!chosen) return fail(Errc::not_found, "no open chunk available");
        tx.add_conflict(k.chunk(chosen->id), false);
        chosen->writer = std::string(writer);
        chosen->writer_lease_expiry = lease_until;
        chosen->revision += 1;
        store_chunk(tx, k, *chosen);
        return std::move(*chosen);
    });
}

Status FdbClusterStore::add_garbage(ChunkId id, std::uint64_t bytes) {
    Keys k{prefix_};
    return run(*db_, [&](Txn& tx) -> Status {
        auto v = tx.get(k.chunk(id));
        if (!v) return std::unexpected(v.error());
        if (!*v) return fail(Errc::not_found, "no such " + describe(id));
        tx.add(k.garbage(id), bytes);
        return {};
    });
}

// --- Leadership: one record {holder, expiry, epoch}.

Result<std::uint64_t> FdbClusterStore::try_lead(std::string_view holder, std::chrono::seconds ttl,
                                                Clock::time_point now) {
    Keys k{prefix_};
    return run(*db_, [&](Txn& tx) -> Result<std::uint64_t> {
        std::string key = k.space(kLeader);
        auto v = tx.get(key);
        if (!v) return std::unexpected(v.error());
        Leader leader;
        if (!*v) {
            leader = Leader{std::string(holder), now + ttl, 1};
        } else {
            auto stored = decode_leader(**v);
            if (!stored) return corrupt("leader");
            leader = std::move(*stored);
            if (leader.holder == holder && now < leader.expiry)
                leader.expiry = now + ttl;
            else if (leader.expiry <= now)
                leader = Leader{std::string(holder), now + ttl, leader.epoch + 1};
            else
                return fail(Errc::aborted, "leadership held by " + leader.holder);
        }
        tx.set(key, Writer().str(leader.holder).time(leader.expiry).u64(leader.epoch).take());
        return leader.epoch;
    });
}

}  // namespace rmk::fdb
