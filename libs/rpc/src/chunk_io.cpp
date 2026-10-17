#include <algorithm>
#include <array>
#include <condition_variable>
#include <cstring>
#include <map>
#include <numeric>
#include <optional>
#include <random>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>

#include "rmk/core/chunk_file.hpp"
#include "rmk/core/crc32c.hpp"
#include "rmk/ec/ec.hpp"
#include "rmk/rpc/rpc.hpp"
#include "rmk/v1/datanode.grpc.pb.h"

namespace rmk::rpc {
namespace {

namespace pb = rmk::internal::v1;

constexpr std::size_t kMessageBytes = std::size_t{1} << 20;
constexpr auto kStatTimeout = std::chrono::seconds(5);
constexpr int kMaxClaims = 16;

// Generous for a slow disk: 30 s plus one second per MiB.
std::chrono::system_clock::time_point io_deadline(std::uint64_t bytes) {
    return std::chrono::system_clock::now() + std::chrono::seconds(30) + std::chrono::seconds(bytes >> 20);
}

pb::FileRef replica_ref(ChunkId id) {
    pb::FileRef ref;
    ref.set_chunk_id(id);
    return ref;
}

pb::FileRef shard_ref(ChunkId id, std::uint32_t index) {
    pb::FileRef ref;
    ref.set_chunk_id(id);
    ref.set_shard(true);
    ref.set_shard_index(index);
    return ref;
}

std::string chunk_name(ChunkId id) {
    return "chunk " + std::to_string(id);
}

Result<pb::StatResponse> stat_file(const std::shared_ptr<grpc::Channel>& channel, const pb::FileRef& ref) {
    auto stub = pb::DataNode::NewStub(channel);
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + kStatTimeout);
    pb::StatResponse response;
    grpc::Status status = stub->Stat(&ctx, ref, &response);
    if (!status.ok())
        return std::unexpected(from_status(status));
    return response;
}

// Streams `bytes` to one file at `offset`; returns the crc32c the Data Node computed after fsync.
Result<std::uint32_t> write_file(const std::shared_ptr<grpc::Channel>& channel, const pb::FileRef& ref,
                                 std::uint64_t offset, std::span<const std::byte> bytes) {
    auto stub = pb::DataNode::NewStub(channel);
    grpc::ClientContext ctx;
    ctx.set_deadline(io_deadline(bytes.size()));
    pb::WriteResponse response;
    auto stream = stub->Write(&ctx, &response);

    pb::WriteRequest request;
    *request.mutable_header()->mutable_file() = ref;
    request.mutable_header()->set_offset(offset);
    bool sent = stream->Write(request);
    for (std::size_t pos = 0; sent && pos < bytes.size(); pos += kMessageBytes) {
        auto piece = bytes.subspan(pos, std::min(kMessageBytes, bytes.size() - pos));
        request.set_data(reinterpret_cast<const char*>(piece.data()), piece.size());
        sent = stream->Write(request);
    }
    if (sent)
        stream->WritesDone();
    grpc::Status status = stream->Finish();
    if (!status.ok())
        return std::unexpected(from_status(status));
    if (response.offset() != offset || response.length() != bytes.size())
        return fail(Errc::io_error, "data node wrote an unexpected range of " + chunk_name(ref.chunk_id()));
    return response.crc32c();
}

Result<std::vector<std::byte>> read_file(const std::shared_ptr<grpc::Channel>& channel, const pb::FileRef& ref,
                                         std::uint64_t offset, std::uint64_t length) {
    auto stub = pb::DataNode::NewStub(channel);
    grpc::ClientContext ctx;
    ctx.set_deadline(io_deadline(length));
    pb::ReadRequest request;
    *request.mutable_file() = ref;
    request.set_offset(offset);
    request.set_length(length);

    std::vector<std::byte> data;
    data.reserve(length);
    auto stream = stub->Read(&ctx, request);
    pb::ReadResponse response;
    while (stream->Read(&response)) {
        const std::string& piece = response.data();
        if (piece.size() > length - data.size()) {
            ctx.TryCancel();
            break;
        }
        const auto* p = reinterpret_cast<const std::byte*>(piece.data());
        data.insert(data.end(), p, p + piece.size());
    }
    grpc::Status status = stream->Finish();
    if (!status.ok() && status.error_code() != grpc::StatusCode::CANCELLED)
        return std::unexpected(from_status(status));
    if (data.size() != length)
        return fail(Errc::io_error, "short read from " + chunk_name(ref.chunk_id()));
    return data;
}

// NodeId -> gRPC address, refreshed from the ClusterStore when an id is unknown.
class NodeDirectory {
public:
    explicit NodeDirectory(std::shared_ptr<ClusterStore> cluster) : cluster_(std::move(cluster)) {}

    // One address per node; empty for a node the ClusterStore does not know.
    std::vector<std::string> resolve(const std::vector<NodeId>& nodes) {
        std::lock_guard lock(mu_);
        bool missing = std::ranges::any_of(nodes, [&](const NodeId& id) { return !addresses_.contains(id); });
        if (missing) {
            if (auto list = cluster_->list_nodes(); list) {
                addresses_.clear();
                for (const NodeInfo& node : *list)
                    addresses_[node.id] = node.address;
            }
        }
        std::vector<std::string> out;
        out.reserve(nodes.size());
        for (const NodeId& id : nodes) {
            auto it = addresses_.find(id);
            out.push_back(it == addresses_.end() ? std::string() : it->second);
        }
        return out;
    }

private:
    std::shared_ptr<ClusterStore> cluster_;
    std::mutex mu_;
    std::unordered_map<NodeId, std::string> addresses_;
};

// One open Chunk this writer holds the lease for.
struct Slot {
    std::mutex busy;  // held for the duration of one write
    std::mutex mu;    // guards the fields below
    ChunkInfo info;   // as last stored; info.writer is the lease holder name
    std::uint64_t offset = 0;
    std::vector<std::string> addresses;
    bool alive = true;
    std::chrono::steady_clock::time_point last_used = std::chrono::steady_clock::now();
};

struct Pool {
    std::vector<std::shared_ptr<Slot>> slots;
    std::size_t claiming = 0;
};

}  // namespace

// ================================================================================ ChunkWriter

struct ChunkWriter::Impl {
    std::shared_ptr<ClusterStore> cluster;
    std::shared_ptr<Channels> channels;
    WriterOptions options;
    NodeDirectory directory;

    std::mutex mu;
    std::condition_variable freed;
    std::map<StoragePolicy, Pool> pools;
    std::uint64_t next_lease = 0;

    std::mutex stop_mu;
    std::condition_variable stop_cv;
    bool stopping = false;
    std::thread renewer;

    Impl(std::shared_ptr<ClusterStore> c, std::shared_ptr<Channels> ch, WriterOptions o)
        : cluster(std::move(c)), channels(std::move(ch)), options(std::move(o)), directory(cluster) {
        options.writers_per_policy = std::max<std::size_t>(options.writers_per_policy, 1);
        options.attempts = std::max(options.attempts, 1);
        options.lease = std::max(options.lease, std::chrono::seconds(1));
    }

    // A Slot whose busy mutex the caller holds; released (and waiters woken) on destruction.
    struct Held {
        Impl* impl = nullptr;
        std::shared_ptr<Slot> slot;
        std::unique_lock<std::mutex> busy;

        Held(Impl* i, std::shared_ptr<Slot> s, std::unique_lock<std::mutex> b)
            : impl(i), slot(std::move(s)), busy(std::move(b)) {}
        Held(Held&&) = default;
        Held& operator=(Held&&) = delete;
        ~Held() {
            if (!busy.owns_lock())
                return;
            {
                std::lock_guard lock(impl->mu);
                busy.unlock();
            }
            impl->freed.notify_all();
        }
    };

    // Stores `mutate(info)` with compare-and-set. A revision bump by someone else is tolerated
    // while the Chunk is still open under our lease; otherwise the lease is lost. The recorded
    // size never shrinks. Caller holds slot.mu.
    template <class F>
    Status store_locked(Slot& slot, F mutate) {
        for (int round = 0; round < 2; ++round) {
            ChunkInfo next = slot.info;
            mutate(next);
            next.size = std::max(next.size, slot.info.size);
            auto stored = cluster->update_chunk(next);
            if (stored) {
                slot.info = *stored;
                return {};
            }
            if (stored.error().code != Errc::aborted)
                return std::unexpected(stored.error());
            auto fresh = cluster->get_chunk(slot.info.id);
            if (!fresh)
                return std::unexpected(fresh.error());
            if (fresh->state != ChunkState::open || fresh->writer != slot.info.writer) {
                slot.alive = false;
                return fail(Errc::aborted, "writer lease on " + chunk_name(slot.info.id) + " lost");
            }
            slot.info = *fresh;
        }
        return fail(Errc::aborted, "could not update " + chunk_name(slot.info.id));
    }

    void seal(Slot& slot) {
        std::lock_guard lock(slot.mu);
        if (!slot.alive)
            return;
        const std::uint64_t size = slot.offset;
        (void)store_locked(slot, [size](ChunkInfo& c) {
            c.state = ChunkState::sealed;
            c.sealed_at = Clock::now();
            c.size = size;
            c.writer.clear();
        });
        slot.alive = false;
    }

    void release(Slot& slot) {
        std::lock_guard lock(slot.mu);
        if (!slot.alive)
            return;
        const std::uint64_t size = slot.offset;
        (void)store_locked(slot, [size](ChunkInfo& c) {
            c.size = size;
            c.writer.clear();
        });
        slot.alive = false;
    }

    // Renews the lease of every slot in use; releases slots idle for a whole lease period so
    // other writers can claim their Chunks.
    void renew_all() {
        std::vector<std::shared_ptr<Slot>> slots;
        {
            std::lock_guard lock(mu);
            for (auto& [policy, pool] : pools)
                slots.insert(slots.end(), pool.slots.begin(), pool.slots.end());
        }
        for (auto& slot : slots) {
            std::unique_lock busy(slot->busy, std::try_to_lock);
            bool idle = false;
            {
                std::lock_guard lock(slot->mu);
                idle = busy.owns_lock() && std::chrono::steady_clock::now() - slot->last_used >= options.lease;
            }
            if (idle) {
                release(*slot);
            } else {
                std::lock_guard lock(slot->mu);
                const auto until = Clock::now() + options.lease;
                const std::uint64_t size = slot->offset;
                if (slot->alive)
                    (void)store_locked(*slot, [&](ChunkInfo& c) {
                        c.writer_lease_expiry = until;
                        c.size = size;
                    });
            }
            if (busy.owns_lock()) {
                {
                    std::lock_guard lock(mu);
                    busy.unlock();
                }
                freed.notify_all();
            }
        }
    }

    void renew_loop() {
        using std::chrono::milliseconds;
        const auto period = std::max(std::chrono::duration_cast<milliseconds>(options.lease) / 3, milliseconds(100));
        std::unique_lock lock(stop_mu);
        while (!stop_cv.wait_for(lock, period, [&] { return stopping; })) {
            lock.unlock();
            renew_all();
            lock.lock();
        }
    }

    // Claims an open Chunk and finds where to write: the largest replica size, so bytes a
    // crashed writer left behind are never overwritten. Chunks with an unreachable replica
    // or without room are sealed and skipped.
    Result<std::shared_ptr<Slot>> claim(const StoragePolicy& policy, std::uint64_t length) {
        for (int i = 0; i < kMaxClaims; ++i) {
            std::string lease_name;
            {
                std::lock_guard lock(mu);
                lease_name = options.writer_id + "/" + std::to_string(next_lease++);
            }
            const auto now = Clock::now();
            auto claimed = cluster->claim_open_chunk(policy, lease_name, length, now + options.lease, now);
            if (!claimed) {
                if (claimed.error().code == Errc::not_found)
                    return fail(Errc::unavailable, "no open chunk with room for " + std::to_string(length) + " bytes");
                return std::unexpected(claimed.error());
            }

            auto slot = std::make_shared<Slot>();
            slot->info = *claimed;
            slot->addresses = directory.resolve(claimed->nodes);
            std::uint64_t offset = std::max<std::uint64_t>(claimed->size, kChunkHeaderSize);
            bool reachable = !slot->addresses.empty();
            for (const std::string& address : slot->addresses) {
                Result<pb::StatResponse> st = fail(Errc::unavailable, "unknown node");
                if (!address.empty())
                    st = stat_file(channels->get(address), replica_ref(claimed->id));
                if (!st || !st->exists()) {
                    reachable = false;
                    continue;
                }
                offset = std::max<std::uint64_t>(offset, st->size());
            }
            slot->offset = offset;
            if (reachable && offset <= claimed->capacity && length <= claimed->capacity - offset)
                return slot;
            seal(*slot);
        }
        return fail(Errc::unavailable, "no usable open chunk");
    }

    // A slot with room for `length` bytes. Slots without room are sealed, but only when the
    // bytes would fit an empty Chunk: an oversized write must not consume the open Chunk pool.
    Result<Held> acquire(const StoragePolicy& policy, std::uint64_t length) {
        std::unique_lock lock(mu);
        for (;;) {
            Pool& pool = pools[policy];
            std::vector<std::shared_ptr<Slot>> full;
            bool any_busy = pool.claiming > 0;
            for (auto it = pool.slots.begin(); it != pool.slots.end();) {
                std::unique_lock busy((*it)->busy, std::try_to_lock);
                if (!busy.owns_lock()) {
                    any_busy = true;
                    ++it;
                    continue;
                }
                bool alive = false;
                bool fits = false;
                bool fits_empty = false;
                {
                    std::lock_guard slot_lock((*it)->mu);
                    const std::uint64_t capacity = (*it)->info.capacity;
                    alive = (*it)->alive;
                    fits = (*it)->offset <= capacity && length <= capacity - (*it)->offset;
                    fits_empty = kChunkHeaderSize <= capacity && length <= capacity - kChunkHeaderSize;
                }
                if (alive && fits)
                    return Held(this, *it, std::move(busy));
                if (alive && !fits_empty) {
                    ++it;
                    continue;
                }
                if (alive)
                    full.push_back(*it);
                it = pool.slots.erase(it);
            }
            if (!full.empty()) {
                lock.unlock();
                for (auto& slot : full)
                    seal(*slot);
                lock.lock();
                continue;
            }
            if (pool.slots.size() + pool.claiming < options.writers_per_policy) {
                ++pool.claiming;
                lock.unlock();
                auto slot = claim(policy, length);
                lock.lock();
                Pool& again = pools[policy];
                --again.claiming;
                if (!slot) {
                    freed.notify_all();
                    return std::unexpected(slot.error());
                }
                std::unique_lock busy((*slot)->busy);
                again.slots.push_back(*slot);
                return Held(this, *slot, std::move(busy));
            }
            if (!any_busy)
                return fail(Errc::unavailable, "no open chunk has room for " + std::to_string(length) + " bytes");
            freed.wait(lock);
        }
    }

    // Same bytes, same offset, every replica; then records the new size under our lease.
    Result<Extent> write_replicas(Slot& slot, std::span<const std::byte> bytes) {
        std::uint64_t offset = 0;
        ChunkId id = 0;
        std::vector<std::string> addresses;
        {
            std::lock_guard lock(slot.mu);
            if (!slot.alive)
                return fail(Errc::aborted, "writer lease on " + chunk_name(slot.info.id) + " lost");
            offset = slot.offset;
            id = slot.info.id;
            addresses = slot.addresses;
        }
        const std::uint32_t crc = crc32c(bytes);

        std::vector<Result<std::uint32_t>> results(addresses.size());
        {
            std::vector<std::jthread> threads;
            for (std::size_t i = 0; i < addresses.size(); ++i)
                threads.emplace_back([&, i] {
                    results[i] = write_file(channels->get(addresses[i]), replica_ref(id), offset, bytes);
                });
        }
        for (std::size_t i = 0; i < results.size(); ++i) {
            if (!results[i])
                return fail(results[i].error().code,
                            "replica " + addresses[i] + " of " + chunk_name(id) + ": " + results[i].error().message);
            if (*results[i] != crc)
                return fail(Errc::corruption, "replica " + addresses[i] + " of " + chunk_name(id) + " disagrees on crc32c");
        }

        std::lock_guard lock(slot.mu);
        if (!slot.alive)
            return fail(Errc::aborted, "writer lease on " + chunk_name(id) + " lost");
        const std::uint64_t end = offset + bytes.size();
        const auto until = Clock::now() + options.lease;
        if (auto s = store_locked(slot, [&](ChunkInfo& c) {
                c.size = end;
                c.writer_lease_expiry = until;
            });
            !s)
            return std::unexpected(s.error());
        slot.offset = end;
        return Extent{id, offset, bytes.size(), crc};
    }
};

ChunkWriter::ChunkWriter(std::shared_ptr<ClusterStore> cluster, std::shared_ptr<Channels> channels,
                         WriterOptions options)
    : impl_(std::make_unique<Impl>(std::move(cluster), std::move(channels), std::move(options))) {
    impl_->renewer = std::thread([impl = impl_.get()] { impl->renew_loop(); });
}

ChunkWriter::~ChunkWriter() {
    {
        std::lock_guard lock(impl_->stop_mu);
        impl_->stopping = true;
    }
    impl_->stop_cv.notify_all();
    impl_->renewer.join();

    std::vector<std::shared_ptr<Slot>> slots;
    {
        std::lock_guard lock(impl_->mu);
        for (auto& [policy, pool] : impl_->pools)
            slots.insert(slots.end(), pool.slots.begin(), pool.slots.end());
        impl_->pools.clear();
    }
    for (auto& slot : slots) {
        std::lock_guard busy(slot->busy);
        impl_->release(*slot);
    }
}

Result<Extent> ChunkWriter::write(const StoragePolicy& policy, std::span<const std::byte> bytes) {
    Error last{Errc::unavailable, "no attempt made"};
    for (int attempt = 0; attempt < impl_->options.attempts; ++attempt) {
        auto held = impl_->acquire(policy, bytes.size());
        if (!held)
            return std::unexpected(held.error());
        auto extent = impl_->write_replicas(*held->slot, bytes);
        {
            std::lock_guard lock(held->slot->mu);
            held->slot->last_used = std::chrono::steady_clock::now();
        }
        if (extent)
            return extent;
        last = extent.error();
        impl_->seal(*held->slot);
    }
    return fail(Errc::unavailable, "write failed after " + std::to_string(impl_->options.attempts) +
                                       " attempts: " + last.message);
}

// ================================================================================ ChunkReader

struct ChunkReader::Impl {
    static constexpr std::size_t kMaxCachedChunks = 1 << 16;

    std::shared_ptr<ClusterStore> cluster;
    std::shared_ptr<Channels> channels;
    NodeDirectory directory;

    std::mutex mu;
    std::unordered_map<ChunkId, ChunkInfo> infos;
    std::mt19937_64 rng{std::random_device{}()};

    Impl(std::shared_ptr<ClusterStore> c, std::shared_ptr<Channels> ch)
        : cluster(std::move(c)), channels(std::move(ch)), directory(cluster) {}

    Result<ChunkInfo> chunk_info(ChunkId id, bool refresh) {
        if (!refresh) {
            std::lock_guard lock(mu);
            if (auto it = infos.find(id); it != infos.end())
                return it->second;
        }
        auto info = cluster->get_chunk(id);
        if (!info)
            return info;
        std::lock_guard lock(mu);
        if (infos.size() >= kMaxCachedChunks)
            infos.clear();
        infos[id] = *info;
        return info;
    }

    Result<std::vector<std::byte>> read_replicated(const ChunkInfo& info, const Extent& extent) {
        auto addresses = directory.resolve(info.nodes);
        std::vector<std::size_t> order(addresses.size());
        std::iota(order.begin(), order.end(), 0);
        {
            std::lock_guard lock(mu);
            std::ranges::shuffle(order, rng);
        }
        Error last{Errc::unavailable, chunk_name(info.id) + " has no reachable replica"};
        for (std::size_t i : order) {
            if (addresses[i].empty())
                continue;
            auto data = read_file(channels->get(addresses[i]), replica_ref(info.id), extent.offset, extent.length);
            if (!data) {
                last = data.error();
                continue;
            }
            if (crc32c(*data) == extent.crc32c)
                return data;
            last = Error{Errc::corruption, "replica " + addresses[i] + " of " + chunk_name(info.id) + " fails crc32c"};
        }
        return std::unexpected(last);
    }

    // Recreates bytes [shard_offset, shard_offset + length) of shard `want` from k others.
    Result<std::vector<std::byte>> reconstruct(const ChunkInfo& info, const std::vector<std::string>& addresses,
                                               const ec::Codec& codec, std::uint8_t want,
                                               std::optional<std::uint8_t> exclude, std::uint64_t shard_offset,
                                               std::uint64_t length) {
        const std::size_t n = addresses.size();
        std::vector<std::vector<std::byte>> shards(n);
        std::array<bool, 256> present{};
        std::size_t got = 0;
        for (std::size_t i = 0; i < n && got < info.data_units; ++i) {
            if (i == want || (exclude && i == *exclude) || addresses[i].empty())
                continue;
            auto data = read_file(channels->get(addresses[i]), shard_ref(info.id, static_cast<std::uint32_t>(i)),
                                  kChunkHeaderSize + shard_offset, length);
            if (!data)
                continue;
            shards[i] = std::move(*data);
            present[i] = true;
            ++got;
        }
        if (got < info.data_units)
            return fail(Errc::unavailable, chunk_name(info.id) + ": only " + std::to_string(got) + " of " +
                                               std::to_string(info.data_units) + " needed shards readable");
        std::vector<std::span<std::byte>> spans;
        for (auto& shard : shards) {
            shard.resize(length);
            spans.emplace_back(shard);
        }
        if (auto s = codec.reconstruct(spans, std::span<const bool>(present.data(), n)); !s)
            return std::unexpected(s.error());
        return std::move(shards[want]);
    }

    Result<std::vector<std::byte>> read_ec_once(const ChunkInfo& info, const std::vector<std::string>& addresses,
                                                const ec::Codec& codec, const Extent& extent,
                                                std::optional<std::uint8_t> exclude) {
        std::vector<std::byte> out;
        out.reserve(extent.length);
        for (const ec::ShardPiece& piece : ec::map_range(info.size, info.data_units, extent.offset, extent.length)) {
            Result<std::vector<std::byte>> data = fail(Errc::unavailable, "shard excluded");
            if (piece.shard != exclude && !addresses[piece.shard].empty())
                data = read_file(channels->get(addresses[piece.shard]), shard_ref(info.id, piece.shard),
                                 kChunkHeaderSize + piece.shard_offset, piece.length);
            if (!data)
                data = reconstruct(info, addresses, codec, piece.shard, exclude, piece.shard_offset, piece.length);
            if (!data)
                return data;
            out.insert(out.end(), data->begin(), data->end());
        }
        if (crc32c(out) != extent.crc32c)
            return fail(Errc::corruption, chunk_name(info.id) + ": erasure-coded extent fails crc32c");
        return out;
    }

    Result<std::vector<std::byte>> read_ec(const ChunkInfo& info, const Extent& extent) {
        if (info.nodes.size() != std::size_t{info.data_units} + info.parity_units)
            return fail(Errc::corruption, chunk_name(info.id) + " lists the wrong number of shard nodes");
        if (extent.offset > info.size || extent.length > info.size - extent.offset)
            return fail(Errc::invalid_argument, "extent beyond the end of " + chunk_name(info.id));
        auto codec = ec::Codec::create(info.data_units, info.parity_units);
        if (!codec)
            return std::unexpected(codec.error());
        auto addresses = directory.resolve(info.nodes);

        auto data = read_ec_once(info, addresses, *codec, extent, std::nullopt);
        if (data || data.error().code != Errc::corruption)
            return data;
        // Some shard returned bad bytes, either one holding the extent or one used to rebuild
        // it: leave each shard out in turn.
        for (std::size_t bad = 0; bad < addresses.size(); ++bad)
            if (auto retry = read_ec_once(info, addresses, *codec, extent, static_cast<std::uint8_t>(bad)); retry)
                return retry;
        return data;
    }
};

ChunkReader::ChunkReader(std::shared_ptr<ClusterStore> cluster, std::shared_ptr<Channels> channels)
    : impl_(std::make_unique<Impl>(std::move(cluster), std::move(channels))) {}

ChunkReader::~ChunkReader() = default;

Result<std::vector<std::byte>> ChunkReader::read(const Extent& extent) {
    Error last{Errc::unavailable, "no attempt made"};
    for (int pass = 0; pass < 2; ++pass) {
        auto info = impl_->chunk_info(extent.chunk_id, pass > 0);
        if (!info)
            return std::unexpected(info.error());
        auto data = info->state == ChunkState::erasure_coded ? impl_->read_ec(*info, extent)
                                                             : impl_->read_replicated(*info, extent);
        if (data)
            return data;
        last = data.error();
    }
    return std::unexpected(last);
}

}  // namespace rmk::rpc
