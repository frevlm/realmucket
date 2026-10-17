#include "rmk/local/chunk_store.hpp"

#include <algorithm>
#include <condition_variable>
#include <format>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <system_error>

namespace rmk::local {
namespace fs = std::filesystem;
namespace {

constexpr std::string_view kSuffix = ".chunk";

fs::path chunk_path(const fs::path& dir, ChunkId id) {
    return dir / "chunks" / std::format("{:016}{}", id, kSuffix);
}

}  // namespace

// Writers claim a range, copy their bytes into it and then wait until a flush covers it. The flush
// is done by whichever waiting writer gets there first (the leader); it covers every range below
// the lowest one still being copied, so everyone who finished meanwhile is released by that one
// flush. A range counts as flushed only if a flush that started after its bytes were in place
// returned success.
struct ChunkStore::Slot {
    Slot(ChunkFile f, const Options* options, std::atomic<std::uint64_t>* counter)
        : file(std::move(f)), options(options), counter(counter), flushed(file.size()) {}

    ChunkFile file;
    const Options* options;
    std::atomic<std::uint64_t>* counter;

    std::mutex m;
    std::condition_variable cv;
    std::set<std::uint64_t> copying;  // starts of the ranges whose bytes are not all in place yet
    std::uint64_t flushed;            // everything below this is on disk
    bool flushing = false;
    std::optional<Error> broken;      // a flush failed: this file can no longer promise anything

    // Claims `length` bytes. The claim and the bookkeeping are one step, so a flush can never
    // decide a range is safe before its owner has had the chance to say it is still being copied.
    Result<std::uint64_t> claim(std::uint64_t length) {
        std::lock_guard lock(m);
        auto at = file.reserve(length);
        if (at)
            copying.insert(*at);
        return at;
    }

    void copied(std::uint64_t at) {
        {
            std::lock_guard lock(m);
            copying.erase(at);
        }
        cv.notify_all();
    }

    std::uint64_t safe_end() const { return copying.empty() ? file.size() : *copying.begin(); }

    Status flush_through(std::uint64_t end) {
        std::unique_lock lock(m);
        for (;;) {
            if (broken)
                return std::unexpected(*broken);
            if (flushed >= end)
                return {};
            if (flushing) {
                cv.wait(lock);
                continue;
            }
            const std::uint64_t target = safe_end();
            if (target <= flushed) {  // an earlier range is still being copied
                cv.wait(lock);
                continue;
            }
            flushing = true;
            lock.unlock();
            counter->fetch_add(1);
            Status s = options->sync ? options->sync(file, target) : file.sync();
            lock.lock();
            flushing = false;
            if (s)
                flushed = std::max(flushed, target);
            else
                broken = s.error();
            cv.notify_all();
        }
    }
};

Result<std::unique_ptr<ChunkStore>> ChunkStore::open(Options options) {
    if (options.chunk_capacity <= kChunkHeaderSize)
        return fail(Errc::invalid_argument, "chunk capacity must exceed the chunk header");
    std::error_code ec;
    fs::create_directories(options.dir / "chunks", ec);
    if (ec)
        return fail(Errc::io_error, "create " + (options.dir / "chunks").string() + ": " + ec.message());

    std::unique_ptr<ChunkStore> store(new ChunkStore(std::move(options)));
    for (const auto& item : fs::directory_iterator(store->options_.dir / "chunks", ec)) {
        const fs::path& path = item.path();
        if (path.extension() != kSuffix)
            continue;
        auto file = ChunkFile::open(path);
        if (!file)
            return fail(file.error().code, path.string() + ": " + file.error().message);
        const ChunkId id = file->header().chunk_id;
        if (path != chunk_path(store->options_.dir, id))
            return fail(Errc::corruption, path.string() + " holds chunk " + std::to_string(id));
        store->chunks_[id] = Entry{std::make_shared<Slot>(std::move(*file), &store->options_, &store->syncs_), 0, {}};
        store->next_id_ = std::max(store->next_id_, id + 1);
    }
    if (ec)
        return fail(Errc::io_error, "list chunks: " + ec.message());
    return store;
}

ChunkStore::~ChunkStore() = default;

void ChunkStore::seal_unlocked() {
    if (open_ != 0)
        chunks_.at(open_).sealed_at = Clock::now();
    open_ = 0;
}

void ChunkStore::seal_open() {
    std::unique_lock lock(mu_);
    seal_unlocked();
}

void ChunkStore::stop_appending(ChunkId id) {
    std::unique_lock lock(mu_);
    if (open_ == id)
        seal_unlocked();
}

Status ChunkStore::start_chunk() {
    seal_unlocked();
    const ChunkId id = next_id_;
    ChunkHeader header;
    header.chunk_id = id;
    header.capacity = options_.chunk_capacity;
    header.data_units = 1;
    header.parity_units = 0;
    const fs::path path = chunk_path(options_.dir, id);
    auto file = ChunkFile::create(path, header);
    if (!file)
        return std::unexpected(file.error());
    chunks_[id] = Entry{std::make_shared<Slot>(std::move(*file), &options_, &syncs_), 0, {}};
    next_id_ = id + 1;
    open_ = id;
    return {};
}

Result<Extent> ChunkStore::write(const StoragePolicy&, std::span<const std::byte> bytes) {
    if (bytes.size() > options_.chunk_capacity - kChunkHeaderSize)
        return fail(Errc::invalid_argument, std::format("{} bytes do not fit in a chunk of {} bytes", bytes.size(),
                                                        options_.chunk_capacity));
    // Claim a range of the open Chunk under the shared lock; only starting a new Chunk takes the
    // exclusive lock.
    std::shared_ptr<Slot> slot;
    std::uint64_t offset = 0;
    for (;;) {
        {
            std::shared_lock lock(mu_);
            slot = open_ == 0 ? nullptr : chunks_.at(open_).slot;
        }
        if (slot) {
            auto claimed = slot->claim(bytes.size());
            if (claimed) {
                offset = *claimed;
                break;
            }
            if (claimed.error().code != Errc::no_space)
                return std::unexpected(claimed.error());
        }
        std::unique_lock lock(mu_);
        // Someone else may have started the next Chunk while we waited for the lock.
        if (open_ == 0 || chunks_.at(open_).slot == slot) {
            if (auto s = start_chunk(); !s)
                return std::unexpected(s.error());
        }
    }

    const ChunkId chunk = slot->file.header().chunk_id;
    if (options_.after_claim)
        options_.after_claim(offset);
    auto written = slot->file.write_reserved(offset, bytes);
    slot->copied(offset);  // also on failure: the hole must not hold back everyone behind it
    if (!written) {
        (void)add_garbage(chunk, bytes.size());  // the reserved range stays a hole
        return written;
    }
    if (auto s = slot->flush_through(offset + bytes.size()); !s) {
        stop_appending(chunk);  // nothing more is promised about this file
        (void)add_garbage(chunk, bytes.size());
        return std::unexpected(s.error());
    }
    return *written;
}

Result<std::vector<std::byte>> ChunkStore::read(const Extent& extent) {
    std::shared_ptr<Slot> slot;
    {
        std::shared_lock lock(mu_);
        auto it = chunks_.find(extent.chunk_id);
        if (it == chunks_.end())
            return fail(Errc::not_found, "chunk " + std::to_string(extent.chunk_id));
        slot = it->second.slot;
    }
    return slot->file.read(extent);  // no lock held: the disk read does not block writers
}

Status ChunkStore::add_garbage(ChunkId id, std::uint64_t bytes) {
    std::unique_lock lock(mu_);
    auto it = chunks_.find(id);
    if (it == chunks_.end())
        return fail(Errc::not_found, "chunk " + std::to_string(id));
    it->second.garbage_bytes += bytes;
    return {};
}

Status ChunkStore::set_garbage(ChunkId id, std::uint64_t bytes) {
    std::unique_lock lock(mu_);
    auto it = chunks_.find(id);
    if (it == chunks_.end())
        return fail(Errc::not_found, "chunk " + std::to_string(id));
    it->second.garbage_bytes = bytes;
    return {};
}

Status ChunkStore::remove(ChunkId id) {
    std::unique_lock lock(mu_);
    auto it = chunks_.find(id);
    if (it == chunks_.end())
        return fail(Errc::not_found, "chunk " + std::to_string(id));
    if (id == open_)
        return fail(Errc::invalid_argument, "chunk " + std::to_string(id) + " still takes appends");
    const fs::path path = chunk_path(options_.dir, id);
    std::error_code ec;
    fs::remove(path, ec);
    if (ec)
        return fail(Errc::io_error, "remove " + path.string() + ": " + ec.message());
    chunks_.erase(it);
    return {};
}

std::vector<ChunkStat> ChunkStore::chunks() const {
    std::shared_lock lock(mu_);
    std::vector<ChunkStat> out;
    for (const auto& [id, entry] : chunks_)
        out.push_back({id, entry.slot->file.size(), entry.slot->file.header().capacity, entry.garbage_bytes, id == open_,
                       entry.sealed_at});
    return out;
}

}  // namespace rmk::local
