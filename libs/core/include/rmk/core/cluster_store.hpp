#pragma once

#include <chrono>
#include <cstddef>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "rmk/core/error.hpp"
#include "rmk/core/types.hpp"

namespace rmk {

// Proof of Controller leadership, checked inside the same transaction as the write it guards.
// A write carrying a Fence fails with `aborted` unless the lease record still names this holder
// with this epoch — so a Controller that lost its lease can never change cluster state.
struct Fence {
    std::string holder;
    std::uint64_t epoch = 0;
};

// Data Node membership, Chunk placement and the Controller lease (ADR 0005, 0009).
class ClusterStore {
public:
    virtual ~ClusterStore() = default;

    // --- Data Nodes. heartbeat() upserts the record and stamps last_heartbeat = now.
    virtual Status heartbeat(const NodeInfo& node, Clock::time_point now) = 0;
    virtual Result<std::vector<NodeInfo>> list_nodes() = 0;
    virtual Status remove_node(std::string_view id, const std::optional<Fence>& fence = std::nullopt) = 0;

    // --- Chunks.
    virtual Result<ChunkId> allocate_chunk_id() = 0;  // never returns the same id twice
    virtual Status put_chunk(const ChunkInfo& chunk,
                             const std::optional<Fence>& fence = std::nullopt) = 0;  // create; already_exists
    virtual Result<ChunkInfo> get_chunk(ChunkId id) = 0;
    // Chunks in id order after `after`, optionally only those in `state`.
    virtual Result<std::vector<ChunkInfo>> list_chunks(std::optional<ChunkState> state,
                                                       std::optional<ChunkId> after,
                                                       std::size_t limit) = 0;
    // Compare-and-set on `revision`: succeeds only if the stored revision equals
    // next.revision, and stores next with revision + 1. aborted otherwise.
    // garbage_bytes is owned by add_garbage: update_chunk keeps the stored value and ignores
    // next.garbage_bytes, so a stale copy can never erase a concurrent add.
    virtual Result<ChunkInfo> update_chunk(const ChunkInfo& next,
                                           const std::optional<Fence>& fence = std::nullopt) = 0;
    virtual Status delete_chunk(ChunkId id, const std::optional<Fence>& fence = std::nullopt) = 0;
    // Takes the writer lease on an open Chunk with target_policy == policy whose lease is free
    // (empty writer, expired, or already ours) and that has at least `min_free` bytes left.
    // not_found if there is none.
    virtual Result<ChunkInfo> claim_open_chunk(const StoragePolicy& policy, std::string_view writer,
                                               std::uint64_t min_free, Clock::time_point lease_until,
                                               Clock::time_point now) = 0;
    virtual Status add_garbage(ChunkId id, std::uint64_t bytes) = 0;  // atomic add

    // --- Controller leadership. Acquires or renews; aborted while another holder's lease lives.
    // Returns the epoch, which grows every time leadership changes hands.
    virtual Result<std::uint64_t> try_lead(std::string_view holder, std::chrono::seconds ttl,
                                           Clock::time_point now) = 0;
};

}  // namespace rmk
