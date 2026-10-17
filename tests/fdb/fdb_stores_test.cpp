#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <map>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "rmk/fdb_stores.hpp"
#include "store_contract/common.hpp"

std::shared_ptr<rmk::fdb::Database> fdb_test_database();
std::string fdb_test_prefix();

namespace {

using namespace rmk;
using contract::kT0;

class FdbStores : public ::testing::Test {
protected:
    void SetUp() override {
        db = fdb_test_database();
        if (!db) GTEST_SKIP() << "RMK_FDB_CLUSTER_FILE not set";
        prefix = fdb_test_prefix();
        meta = std::make_unique<fdb::FdbMetaStore>(db, prefix, std::chrono::hours(1));
        cluster = std::make_unique<fdb::FdbClusterStore>(db, prefix);
        config = std::make_unique<fdb::FdbConfigStore>(db, prefix);
    }

    void TearDown() override {
        if (!db) return;
        db->simulate_unknown_commit_results(0);
        (void)fdb::clear_prefix(*db, prefix);
    }

    void init(bool versioning, Quota quota = {}) {
        RMK_ASSERT_OK(meta->create_tenant("acme", quota));
        BucketConfig config;
        config.versioning = versioning;
        RMK_ASSERT_OK(meta->create_bucket(bucket, config));
    }

    Part part(std::uint32_t number, std::uint64_t size = 10, ChunkId chunk = 1) {
        Part p;
        p.number = number;
        p.size = size;
        p.checksum = number;
        p.extent = {chunk, next_offset.fetch_add(1000), size + 16, number};
        return p;
    }

    Result<Generation> put(const ObjectRef& ref, Precondition pre = {}, std::string data_key = "dk",
                           std::uint64_t size = 10) {
        auto id = meta->begin_upload(ref, {1, std::move(data_key)}, kT0);
        if (!id) return std::unexpected(id.error());
        if (auto s = meta->put_part(*id, part(1, size), kT0); !s) return std::unexpected(s.error());
        return meta->commit_upload(*id, pre, kT0);
    }

    // Arms the fault injection and checks afterwards that it fired.
    template <class F>
    void with_unknown_result(F&& op) {
        db->simulate_unknown_commit_results(1);
        op();
        EXPECT_FALSE(db->take_simulated_unknown_result()) << "injected commit_unknown_result was not used";
    }

    std::vector<std::pair<std::string, std::string>> dump() {
        auto all = fdb::dump_prefix(*db, prefix);
        EXPECT_TRUE(all.has_value());
        if (!all) return {};
        return *all;
    }

    bool contains_bytes(const std::string& needle) {
        for (const auto& [k, v] : dump())
            if (k.find(needle) != std::string::npos || v.find(needle) != std::string::npos) return true;
        return false;
    }

    std::shared_ptr<fdb::Database> db;
    std::string prefix;
    std::unique_ptr<fdb::FdbMetaStore> meta;
    std::unique_ptr<fdb::FdbClusterStore> cluster;
    std::unique_ptr<fdb::FdbConfigStore> config;
    BucketRef bucket{"acme", "data"};
    std::atomic<std::uint64_t> next_offset{64};
};

std::string random_bytes(std::size_t n) {
    static std::mt19937_64 rng{std::random_device{}()};
    std::string out(n, '\0');
    for (auto& c : out) c = static_cast<char>(rng());
    return out;
}

// --- Concurrency

TEST_F(FdbStores, ConcurrentConditionalCommitsHaveExactlyOneWinnerPerGeneration) {
    init(true);
    ObjectRef ref{bucket, "contended"};
    struct Win {
        Generation expected;
        Generation got;
    };
    std::mutex mu;
    std::vector<Win> wins;
    std::atomic<int> failures{0};
    std::vector<std::thread> threads;
    for (int t = 0; t < 8; ++t)
        threads.emplace_back([&] {
            for (int i = 0; i < 15; ++i) {
                auto current = meta->head(ref);
                Generation expected = current ? current->generation : kNoObject;
                auto got = put(ref, Precondition{expected});
                if (got) {
                    std::lock_guard lock(mu);
                    wins.push_back({expected, *got});
                } else if (got.error().code == Errc::precondition_failed) {
                    ++failures;
                } else {
                    ADD_FAILURE() << errc_name(got.error().code) << ": " << got.error().message;
                }
            }
        });
    for (auto& t : threads) t.join();

    std::sort(wins.begin(), wins.end(), [](const Win& a, const Win& b) { return a.got < b.got; });
    ASSERT_FALSE(wins.empty());
    EXPECT_GT(failures.load(), 0);
    EXPECT_EQ(wins.front().expected, kNoObject);
    for (std::size_t i = 1; i < wins.size(); ++i) {
        EXPECT_GT(wins[i].got, wins[i - 1].got);
        EXPECT_EQ(wins[i].expected, wins[i - 1].got) << "two commits won against the same generation";
    }
    auto versions = meta->list_versions(ref);
    RMK_ASSERT_OK(versions);
    EXPECT_EQ(versions->size(), wins.size());
    EXPECT_EQ(meta->head(ref)->generation, wins.back().got);
}

TEST_F(FdbStores, UsageCountersStayExactUnderConcurrency) {
    init(false);
    constexpr int kThreads = 8;
    constexpr int kPerThread = 20;
    auto key = [](int t, int i) { return "t" + std::to_string(t) + "/" + std::to_string(i); };
    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; ++t)
        threads.emplace_back([&, t] {
            for (int i = 0; i < kPerThread; ++i) RMK_EXPECT_OK(put({bucket, key(t, i)}, {}, "dk", 7));
        });
    for (auto& t : threads) t.join();
    EXPECT_EQ(meta->get_tenant("acme")->usage, (Usage{7ull * kThreads * kPerThread, kThreads * kPerThread}));

    threads.clear();
    for (int t = 0; t < kThreads; ++t)
        threads.emplace_back([&, t] {
            for (int i = 0; i < kPerThread; ++i) RMK_EXPECT_OK(meta->remove({bucket, key(t, i)}, {}, kT0));
        });
    for (auto& t : threads) t.join();
    EXPECT_EQ(meta->get_tenant("acme")->usage, (Usage{0, 0}));
}

TEST_F(FdbStores, ParallelPartsOfOneUploadAllLand) {
    init(false);
    ObjectRef ref{bucket, "big"};
    auto id = meta->begin_upload(ref, {1, "dk"}, kT0);
    RMK_ASSERT_OK(id);
    std::vector<std::thread> threads;
    for (std::uint32_t t = 0; t < 8; ++t)
        threads.emplace_back([&, t] {
            for (std::uint32_t n = t + 1; n <= 200; n += 8) RMK_EXPECT_OK(meta->put_part(*id, part(n), kT0 + std::chrono::seconds(n)));
        });
    for (auto& t : threads) t.join();
    auto gen = meta->commit_upload(*id, {}, kT0 + std::chrono::seconds(300));
    RMK_ASSERT_OK(gen);
    auto head = meta->head(ref);
    RMK_ASSERT_OK(head);
    EXPECT_EQ(head->parts.size(), 200u);
    // Stale deadline index entries left by racing put_part calls are dropped without effect.
    auto reclaimed = meta->reclaim_expired(kT0 + std::chrono::hours(48), 1000);
    RMK_ASSERT_OK(reclaimed);
    EXPECT_EQ(*reclaimed, 0u);
    for (const auto& [k, v] : dump()) EXPECT_NE(k[prefix.size() + 1], 'D') << "deadline index entry left";
}

TEST_F(FdbStores, GenerationsFollowCommitOrderAcrossObjects) {
    init(false);
    Generation last = kNoObject;
    for (int i = 0; i < 20; ++i) {
        auto g = put({bucket, "k" + std::to_string(i % 3)});
        RMK_ASSERT_OK(g);
        EXPECT_GT(*g, last);
        last = *g;
    }
}

// --- commit_unknown_result: the transaction committed but the client was told "unknown";
// the retry must notice its own commit instead of applying the operation twice or failing.

TEST_F(FdbStores, UnknownResultOnCommitUploadReturnsTheCommittedGeneration) {
    init(true);
    ObjectRef ref{bucket, "obj"};
    auto id = meta->begin_upload(ref, {1, "dk"}, kT0);
    RMK_ASSERT_OK(id);
    RMK_ASSERT_OK(meta->put_part(*id, part(1), kT0));
    Result<Generation> gen = fail(Errc::io_error, "not run");
    with_unknown_result([&] { gen = meta->commit_upload(*id, Precondition{kNoObject}, kT0); });
    RMK_ASSERT_OK(gen);
    auto versions = meta->list_versions(ref);
    RMK_ASSERT_OK(versions);
    ASSERT_EQ(versions->size(), 1u);
    EXPECT_EQ(versions->front().generation, *gen);
    EXPECT_EQ(meta->get_tenant("acme")->usage, (Usage{10, 1}));
}

TEST_F(FdbStores, UnknownResultOnPutPartMakesNoGarbage) {
    init(false);
    auto id = meta->begin_upload({bucket, "obj"}, {1, "dk"}, kT0);
    RMK_ASSERT_OK(id);
    Part p = part(1);
    with_unknown_result([&] { RMK_EXPECT_OK(meta->put_part(*id, p, kT0)); });
    EXPECT_TRUE(meta->peek_garbage(100)->empty());
    EXPECT_EQ(meta->get_upload(*id, kT0)->parts.at(1), p);
}

TEST_F(FdbStores, UnknownResultOnBeginUploadLeavesOneUpload) {
    init(false);
    Result<UploadId> id = fail(Errc::io_error, "not run");
    with_unknown_result([&] { id = meta->begin_upload({bucket, "obj"}, {1, "dk"}, kT0); });
    RMK_ASSERT_OK(id);
    RMK_ASSERT_OK(meta->abort_upload(*id));
    RMK_EXPECT_OK(meta->delete_bucket(bucket));  // no second Pending Upload holds it
}

TEST_F(FdbStores, UnknownResultOnRemoveDoesNotAddASecondMarker) {
    init(true);
    ObjectRef ref{bucket, "obj"};
    auto gen = put(ref);
    RMK_ASSERT_OK(gen);
    Result<Generation> marker = fail(Errc::io_error, "not run");
    with_unknown_result([&] { marker = meta->remove(ref, Precondition{*gen}, kT0); });
    RMK_ASSERT_OK(marker);
    auto versions = meta->list_versions(ref);
    RMK_ASSERT_OK(versions);
    ASSERT_EQ(versions->size(), 2u);
    EXPECT_TRUE(versions->front().delete_marker);
    EXPECT_EQ(versions->front().generation, *marker);
}

TEST_F(FdbStores, UnknownResultOnRemoveWithoutVersioningDeletesOnlyOneVersion) {
    init(true);
    ObjectRef ref{bucket, "obj"};
    RMK_ASSERT_OK(put(ref));
    RMK_ASSERT_OK(put(ref));
    RMK_ASSERT_OK(meta->set_bucket_config(bucket, {}));
    Result<Generation> removed = fail(Errc::io_error, "not run");
    with_unknown_result([&] { removed = meta->remove(ref, {}, kT0); });
    RMK_ASSERT_OK(removed);
    EXPECT_EQ(*removed, kNoObject);
    EXPECT_EQ(meta->list_versions(ref)->size(), 1u);
}

TEST_F(FdbStores, UnknownResultOnIdempotentOperationsReportsSuccess) {
    init(true);
    ObjectRef ref{bucket, "obj"};
    auto gen = put(ref);
    RMK_ASSERT_OK(gen);
    auto head = meta->head(ref);
    RMK_ASSERT_OK(head);
    Extent to{9, 64, head->parts[0].extent.length, 7};
    with_unknown_result([&] { RMK_EXPECT_OK(meta->relocate_part(ref, *gen, 1, head->parts[0].extent, to)); });
    Encryption rewrapped{2, "dk2"};
    with_unknown_result([&] { RMK_EXPECT_OK(meta->rewrap(ref, *gen, head->encryption, rewrapped)); });
    auto after = meta->head(ref);
    EXPECT_EQ(after->parts[0].extent, to);
    EXPECT_EQ(after->encryption, rewrapped);
    EXPECT_EQ(meta->extents_in_chunk(9, 10)->size(), 1u);
    EXPECT_TRUE(meta->extents_in_chunk(1, 10)->empty());
    with_unknown_result([&] { RMK_EXPECT_OK(meta->remove_version(ref, *gen)); });
    with_unknown_result([&] { RMK_EXPECT_OK(meta->create_tenant("other", {})); });
    with_unknown_result([&] { RMK_EXPECT_OK(meta->create_bucket({"other", "b"}, {})); });
    with_unknown_result([&] { RMK_EXPECT_OK(meta->delete_bucket({"other", "b"})); });

    ChunkInfo chunk;
    chunk.id = 77;
    chunk.capacity = 1000;
    with_unknown_result([&] { RMK_EXPECT_OK(cluster->put_chunk(chunk)); });
    ChunkInfo next = *cluster->get_chunk(77);
    next.size = 500;
    with_unknown_result([&] {
        auto stored = cluster->update_chunk(next);
        RMK_ASSERT_OK(stored);
        EXPECT_EQ(stored->revision, next.revision + 1);
    });
    with_unknown_result([&] { RMK_EXPECT_OK(cluster->delete_chunk(77)); });

    ClusterConfig cfg{"1.2.3"};
    with_unknown_result([&] { RMK_EXPECT_OK(config->set_cluster_config(cfg)); });
    EXPECT_EQ(*config->get_cluster_config(), cfg);
}

// --- Crypto-shredding (ADR 0016): a permanently deleted version leaves no Data Key bytes.

TEST_F(FdbStores, PermanentDeletionLeavesNoDataKeyBytes) {
    init(true);
    ObjectRef ref{bucket, "secret"};
    std::string kept = random_bytes(60);
    std::string shredded = random_bytes(60);
    auto old_gen = put(ref, {}, shredded);
    RMK_ASSERT_OK(old_gen);
    RMK_ASSERT_OK(put(ref, {}, kept));
    ASSERT_TRUE(contains_bytes(shredded));
    RMK_ASSERT_OK(meta->remove_version(ref, *old_gen));
    EXPECT_FALSE(contains_bytes(shredded));
    EXPECT_TRUE(contains_bytes(kept));

    RMK_ASSERT_OK(meta->set_bucket_config(bucket, {}));
    std::string replacement = random_bytes(60);
    RMK_ASSERT_OK(put(ref, {}, replacement));  // replaces `kept` without Versioning
    EXPECT_FALSE(contains_bytes(kept));
    RMK_ASSERT_OK(meta->remove(ref, {}, kT0));
    EXPECT_FALSE(contains_bytes(replacement));

    std::string aborted = random_bytes(60);
    auto id = meta->begin_upload(ref, {1, aborted}, kT0);
    RMK_ASSERT_OK(id);
    RMK_ASSERT_OK(meta->abort_upload(*id));
    EXPECT_FALSE(contains_bytes(aborted));

    std::string expired = random_bytes(60);
    id = meta->begin_upload(ref, {1, expired}, kT0);
    RMK_ASSERT_OK(id);
    RMK_ASSERT_OK(meta->reclaim_expired(kT0 + std::chrono::hours(2), 10));
    EXPECT_FALSE(contains_bytes(expired));
}

// --- Layout

TEST_F(FdbStores, PrefixesThatArePrefixesOfEachOtherStayIsolated) {
    std::string a = prefix + "x";
    std::string b = prefix + "x1";
    fdb::FdbMetaStore store_a(db, a, std::chrono::hours(1));
    fdb::FdbMetaStore store_b(db, b, std::chrono::hours(1));
    RMK_ASSERT_OK(store_a.create_tenant("acme", {}));
    RMK_ASSERT_OK(store_b.create_tenant("other", {}));
    EXPECT_EQ(*store_a.list_tenants(), std::vector<std::string>{"acme"});
    RMK_ASSERT_OK(fdb::clear_prefix(*db, a));
    EXPECT_EQ(*store_b.list_tenants(), std::vector<std::string>{"other"});
    RMK_ASSERT_OK(fdb::clear_prefix(*db, b));
}

// The largest commit: kMaxParts Parts under a 1024-byte key, replacing a version just as large
// (its Parts become garbage and its index entries are cleared in the same transaction).
TEST_F(FdbStores, LargestCommitAndOverwriteFitInOneTransaction) {
    init(false);
    ObjectRef ref{bucket, std::string(1024, 'k')};
    auto upload_max = [&](ChunkId first_chunk) -> Result<Generation> {
        auto id = meta->begin_upload(ref, {1, "dk"}, kT0);
        if (!id) return std::unexpected(id.error());
        for (std::uint32_t n = 1; n <= kMaxParts; ++n)
            if (auto s = meta->put_part(*id, part(n, 1, first_chunk + n % 4), kT0); !s) return std::unexpected(s.error());
        return meta->commit_upload(*id, {}, kT0);
    };
    RMK_ASSERT_OK(upload_max(1));
    RMK_ASSERT_OK(upload_max(11));
    auto head = meta->head(ref);
    RMK_ASSERT_OK(head);
    EXPECT_EQ(head->parts.size(), kMaxParts);
    EXPECT_EQ(head->size, kMaxParts);
    EXPECT_TRUE(meta->extents_in_chunk(1, 5000)->empty());
    auto in_chunk = meta->extents_in_chunk(12, 5000);
    RMK_ASSERT_OK(in_chunk);
    ASSERT_EQ(in_chunk->size(), kMaxParts / 4);
    EXPECT_EQ(in_chunk->front().ref, ref);
    EXPECT_EQ(meta->peek_garbage(10000)->size(), kMaxParts);
    EXPECT_EQ(meta->get_tenant("acme")->usage, (Usage{kMaxParts, 1}));
    RMK_ASSERT_OK(meta->remove(ref, {}, kT0));
    EXPECT_EQ(meta->peek_garbage(10000)->size(), 2 * kMaxParts);
    EXPECT_TRUE(meta->extents_in_chunk(12, 5000)->empty());
}

TEST_F(FdbStores, PartNumbersAboveTheLimitAreRejected) {
    init(false);
    auto id = meta->begin_upload({bucket, "obj"}, {1, "dk"}, kT0);
    RMK_ASSERT_OK(id);
    RMK_EXPECT_ERR(meta->put_part(*id, part(kMaxParts + 1), kT0), invalid_argument);
}

TEST_F(FdbStores, ListCountsDeleteMarkersAgainstTheLimit) {
    init(true);
    for (const char* key : {"a", "b", "c", "d"}) RMK_ASSERT_OK(put({bucket, key}));
    for (const char* key : {"a", "b", "c"}) RMK_ASSERT_OK(meta->remove({bucket, key}, {}, kT0));
    auto page = meta->list(bucket, "", std::nullopt, 2);
    RMK_ASSERT_OK(page);
    EXPECT_TRUE(page->objects.empty());
    EXPECT_EQ(page->next_start_after, "b");
    page = meta->list(bucket, "", "b", 2);
    RMK_ASSERT_OK(page);
    ASSERT_EQ(page->objects.size(), 1u);
    EXPECT_EQ(page->objects[0].ref.key, "d");
    EXPECT_TRUE(page->objects[0].parts.empty());  // listings read headers only
    EXPECT_EQ(page->next_start_after, std::nullopt);
}

TEST_F(FdbStores, FencedWritesNeedTheCurrentLease) {
    auto epoch = cluster->try_lead("a", std::chrono::seconds(15), kT0);
    RMK_ASSERT_OK(epoch);
    ChunkInfo chunk;
    chunk.id = 5;
    chunk.capacity = 100;
    RMK_ASSERT_OK(cluster->put_chunk(chunk, Fence{"a", *epoch}));
    RMK_ASSERT_OK(cluster->try_lead("b", std::chrono::seconds(15), kT0 + std::chrono::seconds(20)));
    ChunkInfo next = *cluster->get_chunk(5);
    next.size = 50;
    RMK_EXPECT_ERR(cluster->update_chunk(next, Fence{"a", *epoch}), aborted);
    RMK_EXPECT_ERR(cluster->delete_chunk(5, Fence{"a", *epoch}), aborted);
    RMK_EXPECT_OK(cluster->update_chunk(next, Fence{"b", *epoch + 1}));
}

TEST_F(FdbStores, KeysWithNulBytesInPrefixAndOddCharactersRoundTrip) {
    fdb::FdbMetaStore store(db, prefix + std::string("\0\xff", 2), std::chrono::hours(1));
    RMK_ASSERT_OK(store.create_tenant("acme", {}));
    RMK_ASSERT_OK(store.create_bucket(bucket, {}));
    std::vector<std::string> keys{"a", "a/b", "a\x01", "\xc3\xa9", "z\x7f"};
    for (const auto& key : keys) {
        auto id = store.begin_upload({bucket, key}, {1, "dk"}, kT0);
        RMK_ASSERT_OK(id);
        RMK_ASSERT_OK(store.commit_upload(*id, {}, kT0));
    }
    auto page = store.list(bucket, "", std::nullopt, 100);
    RMK_ASSERT_OK(page);
    std::vector<std::string> listed;
    for (const auto& m : page->objects) listed.push_back(m.ref.key);
    auto expected = keys;
    std::sort(expected.begin(), expected.end());
    EXPECT_EQ(listed, expected);
    auto prefixed = store.list(bucket, "a", std::nullopt, 100);
    RMK_ASSERT_OK(prefixed);
    EXPECT_EQ(prefixed->objects.size(), 3u);
    RMK_ASSERT_OK(fdb::clear_prefix(*db, prefix + std::string("\0\xff", 2)));
}

}  // namespace

namespace {

TEST_F(FdbStores, PendingPartIndexFollowsReplacementAndIsCleared) {
    init(false);
    auto id = meta->begin_upload({bucket, "obj"}, {1, "dk"}, kT0);
    RMK_ASSERT_OK(id);
    RMK_ASSERT_OK(meta->put_part(*id, part(1, 10, 5), kT0));
    EXPECT_TRUE(*meta->has_pending_parts(5));
    RMK_ASSERT_OK(meta->put_part(*id, part(1, 10, 6), kT0));  // replaced into another Chunk
    EXPECT_FALSE(*meta->has_pending_parts(5));
    EXPECT_TRUE(*meta->has_pending_parts(6));
    RMK_ASSERT_OK(meta->commit_upload(*id, {}, kT0));
    EXPECT_FALSE(*meta->has_pending_parts(6));
    for (const auto& [k, v] : dump()) EXPECT_NE(k[prefix.size() + 1], 'Q') << "pending part index entry left";
}

}  // namespace
