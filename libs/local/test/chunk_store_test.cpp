#include "rmk/local/chunk_store.hpp"

#include <gtest/gtest.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <functional>
#include <future>
#include <fstream>
#include <string>
#include <thread>
#include <vector>

namespace rmk::local {
namespace {

namespace fs = std::filesystem;

std::vector<std::byte> bytes_of(const std::string& s) {
    const auto* p = reinterpret_cast<const std::byte*>(s.data());
    return {p, p + s.size()};
}

class ChunkStoreTest : public ::testing::Test {
protected:
    void SetUp() override {
        std::string templ = (fs::temp_directory_path() / "rmk-local-XXXXXX").string();
        ASSERT_NE(mkdtemp(templ.data()), nullptr);
        dir = templ;
    }
    void TearDown() override {
        std::error_code ec;
        fs::remove_all(dir, ec);
    }

    std::unique_ptr<ChunkStore> open(std::uint64_t capacity = std::uint64_t{1} << 20,
                                     std::function<Status(ChunkFile&, std::uint64_t)> sync = nullptr) {
        auto store = ChunkStore::open({dir, capacity, std::move(sync)});
        EXPECT_TRUE(store.has_value()) << (store ? "" : store.error().message);
        return store ? std::move(*store) : nullptr;
    }

    fs::path dir;
};

TEST_F(ChunkStoreTest, WrittenBytesReadBack) {
    auto store = open();
    ASSERT_TRUE(store);
    std::vector<Extent> extents;
    const std::vector<std::string> payloads{"alpha", "bravo-bravo", std::string(5000, 'c')};
    for (const auto& p : payloads) {
        auto e = store->write(bytes_of(p));
        ASSERT_TRUE(e.has_value()) << e.error().message;
        EXPECT_EQ(e->length, p.size());
        extents.push_back(*e);
    }
    for (std::size_t i = 0; i < payloads.size(); ++i) {
        auto data = store->read(extents[i]);
        ASSERT_TRUE(data.has_value()) << data.error().message;
        EXPECT_EQ(*data, bytes_of(payloads[i]));
    }
    EXPECT_EQ(extents[0].chunk_id, extents[1].chunk_id);
    EXPECT_LT(extents[0].offset, extents[1].offset);
}

TEST_F(ChunkStoreTest, StartsANewChunkWhenTheOpenOneIsFull) {
    auto store = open(kChunkHeaderSize + 100);
    ASSERT_TRUE(store);
    auto first = store->write(bytes_of(std::string(60, 'a')));
    auto second = store->write(bytes_of(std::string(60, 'b')));
    ASSERT_TRUE(first && second);
    EXPECT_NE(first->chunk_id, second->chunk_id);
    EXPECT_EQ(*store->read(*first), bytes_of(std::string(60, 'a')));
    EXPECT_EQ(*store->read(*second), bytes_of(std::string(60, 'b')));

    const auto chunks = store->chunks();
    ASSERT_EQ(chunks.size(), 2u);
    EXPECT_FALSE(chunks[0].open);
    EXPECT_TRUE(chunks[1].open);
}

TEST_F(ChunkStoreTest, RefusesBytesThatCannotFitAnyChunk) {
    auto store = open(kChunkHeaderSize + 100);
    ASSERT_TRUE(store);
    auto e = store->write(bytes_of(std::string(101, 'x')));
    ASSERT_FALSE(e.has_value());
    EXPECT_EQ(e.error().code, Errc::invalid_argument);
    EXPECT_TRUE(store->write(bytes_of(std::string(100, 'x'))).has_value());
}

TEST_F(ChunkStoreTest, ReopenKeepsDataAndNeverReusesAChunkId) {
    Extent before;
    {
        auto store = open();
        ASSERT_TRUE(store);
        auto e = store->write(bytes_of("survives"));
        ASSERT_TRUE(e.has_value());
        before = *e;
    }
    auto store = open();
    ASSERT_TRUE(store);
    auto data = store->read(before);
    ASSERT_TRUE(data.has_value()) << data.error().message;
    EXPECT_EQ(*data, bytes_of("survives"));

    auto after = store->write(bytes_of("new"));
    ASSERT_TRUE(after.has_value());
    EXPECT_GT(after->chunk_id, before.chunk_id);
    for (const auto& chunk : store->chunks())
        EXPECT_EQ(chunk.open, chunk.id == after->chunk_id);
}

TEST_F(ChunkStoreTest, DamagedBytesAreNoticed) {
    Extent e;
    {
        auto store = open();
        ASSERT_TRUE(store);
        e = *store->write(bytes_of("precious"));
    }
    {
        std::fstream f(dir / "chunks" / "0000000000000001.chunk", std::ios::in | std::ios::out | std::ios::binary);
        f.seekp(static_cast<std::streamoff>(e.offset));
        f.put('X');
    }
    auto store = open();
    ASSERT_TRUE(store);
    auto data = store->read(e);
    ASSERT_FALSE(data.has_value());
    EXPECT_EQ(data.error().code, Errc::corruption);
}

TEST_F(ChunkStoreTest, UnknownChunkIsNotFound) {
    auto store = open();
    ASSERT_TRUE(store);
    auto data = store->read(Extent{99, 64, 1, 0});
    ASSERT_FALSE(data.has_value());
    EXPECT_EQ(data.error().code, Errc::not_found);
    EXPECT_EQ(store->add_garbage(99, 1).error().code, Errc::not_found);
}

TEST_F(ChunkStoreTest, GarbageIsCountedPerChunk) {
    auto store = open(kChunkHeaderSize + 100);
    ASSERT_TRUE(store);
    auto a = *store->write(bytes_of(std::string(60, 'a')));
    auto b = *store->write(bytes_of(std::string(60, 'b')));
    ASSERT_TRUE(store->add_garbage(a.chunk_id, 60).has_value());
    ASSERT_TRUE(store->add_garbage(a.chunk_id, 5).has_value());
    for (const auto& chunk : store->chunks())
        EXPECT_EQ(chunk.garbage_bytes, chunk.id == a.chunk_id ? 65u : 0u) << chunk.id << " vs " << b.chunk_id;
}

TEST_F(ChunkStoreTest, SealOpenMakesTheNextWriteStartANewChunk) {
    auto store = open();
    ASSERT_TRUE(store);
    store->seal_open();  // nothing open yet: harmless
    auto a = *store->write(bytes_of("a"));
    store->seal_open();
    auto b = *store->write(bytes_of("b"));
    EXPECT_NE(a.chunk_id, b.chunk_id);
    for (const auto& chunk : store->chunks()) {
        EXPECT_EQ(chunk.open, chunk.id == b.chunk_id);
        EXPECT_EQ(chunk.sealed_at != Clock::time_point{}, chunk.id == a.chunk_id);
    }
}

TEST_F(ChunkStoreTest, ChunksFoundAtOpenAreSealedLongAgo) {
    {
        auto store = open();
        ASSERT_TRUE(store);
        ASSERT_TRUE(store->write(bytes_of("a")).has_value());
    }
    auto store = open();
    ASSERT_TRUE(store);
    ASSERT_EQ(store->chunks().size(), 1u);
    EXPECT_FALSE(store->chunks()[0].open);
    EXPECT_EQ(store->chunks()[0].sealed_at, Clock::time_point{});
}

TEST_F(ChunkStoreTest, RemoveDeletesOnlySealedChunks) {
    auto store = open();
    ASSERT_TRUE(store);
    auto a = *store->write(bytes_of("a"));
    EXPECT_EQ(store->remove(a.chunk_id).error().code, Errc::invalid_argument);  // still open
    store->seal_open();
    ASSERT_TRUE(store->remove(a.chunk_id).has_value());
    EXPECT_FALSE(fs::exists(dir / "chunks" / "0000000000000001.chunk"));
    EXPECT_EQ(store->read(a).error().code, Errc::not_found);
    EXPECT_EQ(store->remove(a.chunk_id).error().code, Errc::not_found);
    EXPECT_TRUE(store->chunks().empty());
    auto b = *store->write(bytes_of("b"));  // ids are not reused
    EXPECT_GT(b.chunk_id, a.chunk_id);
}

TEST_F(ChunkStoreTest, SetGarbageReplacesTheCount) {
    auto store = open();
    ASSERT_TRUE(store);
    auto a = *store->write(bytes_of("a"));
    ASSERT_TRUE(store->add_garbage(a.chunk_id, 10).has_value());
    ASSERT_TRUE(store->set_garbage(a.chunk_id, 3).has_value());
    EXPECT_EQ(store->chunks()[0].garbage_bytes, 3u);
    EXPECT_EQ(store->set_garbage(99, 1).error().code, Errc::not_found);
}

TEST_F(ChunkStoreTest, ConcurrentWritersAndReaders) {
    auto store = open(kChunkHeaderSize + 4096);
    ASSERT_TRUE(store);
    constexpr int kThreads = 8, kPerThread = 60;
    std::vector<std::vector<std::pair<Extent, std::string>>> written(kThreads);
    std::atomic<int> failures{0};
    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; ++t)
        threads.emplace_back([&, t] {
            for (int i = 0; i < kPerThread; ++i) {
                std::string payload = std::to_string(t) + ":" + std::to_string(i) + std::string(50 + i, 'z');
                auto e = store->write(bytes_of(payload));
                if (!e) {
                    ++failures;
                    continue;
                }
                written[t].push_back({*e, payload});
                auto back = store->read(*e);  // read while others append
                if (!back || *back != bytes_of(payload))
                    ++failures;
            }
        });
    for (auto& th : threads)
        th.join();
    EXPECT_EQ(failures.load(), 0);
    for (const auto& per : written)
        for (const auto& [e, payload] : per) {
            auto back = store->read(e);
            ASSERT_TRUE(back.has_value());
            EXPECT_EQ(*back, bytes_of(payload));
        }
}

}  // namespace
TEST_F(ChunkStoreTest, SequentialWritesFlushOncePerWrite) {
    auto store = open();
    ASSERT_TRUE(store);
    for (int i = 0; i < 5; ++i)
        ASSERT_TRUE(store->write(bytes_of("x")).has_value());
    EXPECT_EQ(store->sync_calls(), 5u);
}

TEST_F(ChunkStoreTest, ConcurrentWritersShareFlushes) {
    // A slow flush gives the other writers time to pile up behind it.
    auto store = open(std::uint64_t{1} << 20, [](ChunkFile& file, std::uint64_t) {
        std::this_thread::sleep_for(std::chrono::milliseconds(30));
        return file.sync();
    });
    ASSERT_TRUE(store);
    constexpr int kWriters = 32;
    std::atomic<int> failures{0};
    std::vector<Extent> extents(kWriters);
    std::vector<std::thread> threads;
    for (int i = 0; i < kWriters; ++i)
        threads.emplace_back([&, i] {
            auto e = store->write(bytes_of("writer-" + std::to_string(i)));
            if (e)
                extents[i] = *e;
            else
                ++failures;
        });
    for (auto& t : threads)
        t.join();
    EXPECT_EQ(failures.load(), 0);
    EXPECT_GE(store->sync_calls(), 1u);
    EXPECT_LE(store->sync_calls(), 8u) << "32 concurrent writes should not need 32 flushes";
    for (int i = 0; i < kWriters; ++i)
        EXPECT_EQ(*store->read(extents[i]), bytes_of("writer-" + std::to_string(i)));
}

TEST_F(ChunkStoreTest, AWriteWaitsForEarlierRangesStillBeingCopied) {
    // A claims the first range and stalls before copying. B claims the next one and copies at once.
    // A flush cannot vouch for B's bytes while A's range below them is unwritten, so B must wait.
    std::promise<void> a_claimed, release_a;
    auto release = release_a.get_future().share();
    auto store = ChunkStore::open({dir, std::uint64_t{1} << 20, nullptr, [&](std::uint64_t offset) {
                                       if (offset == kChunkHeaderSize) {
                                           a_claimed.set_value();
                                           release.wait();
                                       }
                                   }});
    ASSERT_TRUE(store.has_value());
    std::atomic<bool> b_returned{false};
    Result<Extent> a_result = fail(Errc::aborted, "not run"), b_result = fail(Errc::aborted, "not run");
    std::thread a([&] { a_result = (*store)->write(bytes_of("first")); });
    a_claimed.get_future().wait();
    std::thread b([&] {
        b_result = (*store)->write(bytes_of("second"));
        b_returned = true;
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    EXPECT_FALSE(b_returned.load()) << "B returned although the range below it was not written yet";
    release_a.set_value();
    a.join();
    b.join();
    ASSERT_TRUE(a_result.has_value());
    ASSERT_TRUE(b_result.has_value());
    EXPECT_EQ(*(*store)->read(*a_result), bytes_of("first"));
    EXPECT_EQ(*(*store)->read(*b_result), bytes_of("second"));
}

TEST_F(ChunkStoreTest, AFailedFlushFailsTheWriteAndTheNextOneStartsAFreshChunk) {
    std::atomic<int> calls{0};
    auto store = open(std::uint64_t{1} << 20, [&](ChunkFile& file, std::uint64_t) -> Status {
        if (calls++ == 0)
            return fail(Errc::io_error, "disk on fire");
        return file.sync();
    });
    ASSERT_TRUE(store);
    auto failed = store->write(bytes_of("lost"));
    ASSERT_FALSE(failed.has_value());
    EXPECT_EQ(failed.error().code, Errc::io_error);

    auto next = store->write(bytes_of("fine"));
    ASSERT_TRUE(next.has_value()) << next.error().message;
    EXPECT_EQ(*store->read(*next), bytes_of("fine"));
    const auto chunks = store->chunks();
    ASSERT_EQ(chunks.size(), 2u);  // the broken file is left behind and counted as garbage
    EXPECT_FALSE(chunks[0].open);
    EXPECT_EQ(chunks[0].garbage_bytes, 4u);
    EXPECT_TRUE(chunks[1].open);
}

}  // namespace rmk::local
