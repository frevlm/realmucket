#include <string>

#include "rmk/memory_stores.hpp"

namespace rmk {
namespace {

std::string describe(ChunkId id) { return "chunk " + std::to_string(id); }

bool lease_free(const ChunkInfo& chunk, std::string_view writer, Clock::time_point now) {
    return chunk.writer.empty() || chunk.writer == writer || chunk.writer_lease_expiry <= now;
}

std::uint64_t free_space(const ChunkInfo& chunk) {
    return chunk.capacity > chunk.size ? chunk.capacity - chunk.size : 0;
}

}  // namespace

// --- Data Nodes

Status MemoryClusterStore::check_fence(const std::optional<Fence>& fence) const {
    if (!fence) return {};
    if (leader_ && leader_->holder == fence->holder && leader_->epoch == fence->epoch) return {};
    return fail(Errc::aborted, "leadership lost: " + fence->holder);
}

Status MemoryClusterStore::heartbeat(const NodeInfo& node, Clock::time_point now) {
    if (node.id.empty()) return fail(Errc::invalid_argument, "node id is empty");
    std::lock_guard lock(mutex_);
    NodeInfo& slot = nodes_[node.id];
    slot = node;
    slot.last_heartbeat = now;
    return {};
}

Result<std::vector<NodeInfo>> MemoryClusterStore::list_nodes() {
    std::lock_guard lock(mutex_);
    std::vector<NodeInfo> out;
    for (const auto& [id, node] : nodes_) out.push_back(node);
    return out;
}

Status MemoryClusterStore::remove_node(std::string_view id, const std::optional<Fence>& fence) {
    std::lock_guard lock(mutex_);
    if (auto ok = check_fence(fence); !ok) return ok;
    auto it = nodes_.find(id);
    if (it == nodes_.end()) return fail(Errc::not_found, "no such node: " + std::string(id));
    nodes_.erase(it);
    return {};
}

// --- Chunks

Result<ChunkId> MemoryClusterStore::allocate_chunk_id() {
    std::lock_guard lock(mutex_);
    return ++last_chunk_id_;
}

Status MemoryClusterStore::put_chunk(const ChunkInfo& chunk, const std::optional<Fence>& fence) {
    std::lock_guard lock(mutex_);
    if (auto ok = check_fence(fence); !ok) return ok;
    auto [it, inserted] = chunks_.try_emplace(chunk.id, chunk);
    if (!inserted) return fail(Errc::already_exists, "exists: " + describe(chunk.id));
    return {};
}

Result<ChunkInfo> MemoryClusterStore::get_chunk(ChunkId id) {
    std::lock_guard lock(mutex_);
    auto it = chunks_.find(id);
    if (it == chunks_.end()) return fail(Errc::not_found, "no such " + describe(id));
    return it->second;
}

Result<std::vector<ChunkInfo>> MemoryClusterStore::list_chunks(std::optional<ChunkState> state,
                                                               std::optional<ChunkId> after,
                                                               std::size_t limit) {
    std::lock_guard lock(mutex_);
    std::vector<ChunkInfo> out;
    for (auto it = after ? chunks_.upper_bound(*after) : chunks_.begin();
         it != chunks_.end() && out.size() < limit; ++it)
        if (!state || it->second.state == *state) out.push_back(it->second);
    return out;
}

Result<ChunkInfo> MemoryClusterStore::update_chunk(const ChunkInfo& next,
                                                   const std::optional<Fence>& fence) {
    std::lock_guard lock(mutex_);
    if (auto ok = check_fence(fence); !ok) return std::unexpected(ok.error());
    auto it = chunks_.find(next.id);
    if (it == chunks_.end()) return fail(Errc::not_found, "no such " + describe(next.id));
    if (it->second.revision != next.revision)
        return fail(Errc::aborted, "revision changed: " + describe(next.id));
    std::uint64_t garbage_bytes = it->second.garbage_bytes;
    it->second = next;
    it->second.garbage_bytes = garbage_bytes;
    it->second.revision = next.revision + 1;
    return it->second;
}

Status MemoryClusterStore::delete_chunk(ChunkId id, const std::optional<Fence>& fence) {
    std::lock_guard lock(mutex_);
    if (auto ok = check_fence(fence); !ok) return ok;
    if (chunks_.erase(id) == 0) return fail(Errc::not_found, "no such " + describe(id));
    return {};
}

Result<ChunkInfo> MemoryClusterStore::claim_open_chunk(const StoragePolicy& policy,
                                                       std::string_view writer,
                                                       std::uint64_t min_free,
                                                       Clock::time_point lease_until,
                                                       Clock::time_point now) {
    std::lock_guard lock(mutex_);
    for (auto& [id, chunk] : chunks_) {
        if (chunk.state != ChunkState::open || chunk.target_policy != policy) continue;
        if (!lease_free(chunk, writer, now) || free_space(chunk) < min_free) continue;
        chunk.writer = std::string(writer);
        chunk.writer_lease_expiry = lease_until;
        chunk.revision += 1;
        return chunk;
    }
    return fail(Errc::not_found, "no open chunk available");
}

Status MemoryClusterStore::add_garbage(ChunkId id, std::uint64_t bytes) {
    std::lock_guard lock(mutex_);
    auto it = chunks_.find(id);
    if (it == chunks_.end()) return fail(Errc::not_found, "no such " + describe(id));
    it->second.garbage_bytes += bytes;
    return {};
}

// --- Leadership

Result<std::uint64_t> MemoryClusterStore::try_lead(std::string_view holder,
                                                   std::chrono::seconds ttl, Clock::time_point now) {
    std::lock_guard lock(mutex_);
    if (!leader_) leader_ = Leader{std::string(holder), now + ttl, 1};
    else if (leader_->holder == holder && now < leader_->expiry) leader_->expiry = now + ttl;
    else if (leader_->expiry <= now) leader_ = Leader{std::string(holder), now + ttl, leader_->epoch + 1};
    else return fail(Errc::aborted, "leadership held by " + leader_->holder);
    return leader_->epoch;
}

}  // namespace rmk
