#include <gtest/gtest.h>
#include <unistd.h>

#include <atomic>
#include <filesystem>
#include <thread>

#include "rmk/rocksdb/rocks_stores.hpp"

namespace rmk::rocks {
namespace {

namespace fs = std::filesystem;

const Clock::time_point kNow = Clock::time_point{} + std::chrono::hours(24 * 365 * 40);

Part part_of(std::uint32_t number, std::uint64_t size, std::uint64_t offset) {
    Part p;
    p.number = number;
    p.size = size;
    p.checksum = 0x1000 + number;
    p.extent = {1, offset, size + 16, 0xC0DE0000u + static_cast<std::uint32_t>(offset)};
    return p;
}

class RocksTest : public ::testing::Test {
protected:
    void SetUp() override {
        std::string templ = (fs::temp_directory_path() / "rmk-rocks-XXXXXX").string();
        ASSERT_NE(mkdtemp(templ.data()), nullptr);
        dir = templ;
    }
    void TearDown() override {
        meta.reset();
        config.reset();
        db.reset();
        std::error_code ec;
        fs::remove_all(dir, ec);
    }

    void open(bool sync) {
        meta.reset();
        config.reset();
        db.reset();
        auto opened = Database::open({dir / "db", sync});
        ASSERT_TRUE(opened.has_value()) << opened.error().message;
        db = *opened;
        meta = make_meta_store(db, std::chrono::hours(1));
        config = make_config_store(db);
    }

    Result<Generation> put(const ObjectRef& ref, std::vector<std::uint64_t> sizes, std::uint64_t base_offset = 1000) {
        auto id = meta->begin_upload(ref, Encryption{1, "wrapped"}, kNow);
        if (!id) return std::unexpected(id.error());
        std::uint32_t number = 1;
        for (std::uint64_t size : sizes) {
            if (auto s = meta->put_part(*id, part_of(number, size, base_offset + 1000 * number), kNow); !s)
                return std::unexpected(s.error());
            ++number;
        }
        return meta->commit_upload(*id, {}, kNow);
    }

    fs::path dir;
    std::shared_ptr<Database> db;
    std::shared_ptr<MetaStore> meta;
    std::shared_ptr<ConfigStore> config;
};

TEST_F(RocksTest, EverythingSurvivesAReopen) {
    open(true);
    EXPECT_TRUE(db->created());
    ASSERT_TRUE(meta->create_tenant("acme", Quota{1000, 10}));
    ASSERT_TRUE(meta->create_bucket({"acme", "data"}, BucketConfig{}));
    const ObjectRef ref{{"acme", "data"}, "photo"};
    auto first = put(ref, {10, 20});
    ASSERT_TRUE(first.has_value());
    auto second = put(ref, {30});  // overwrites: the first version's Extents become garbage
    ASSERT_TRUE(second.has_value());
    auto pending = meta->begin_upload({{"acme", "data"}, "later"}, Encryption{1, "k"}, kNow);
    ASSERT_TRUE(pending.has_value());
    ASSERT_TRUE(meta->put_part(*pending, part_of(1, 5, 9000), kNow));
    ASSERT_TRUE(config->set_cluster_config(ClusterConfig{"1.2.3"}));

    open(true);
    EXPECT_FALSE(db->created());
    auto tenant = meta->get_tenant("acme");
    ASSERT_TRUE(tenant.has_value());
    EXPECT_EQ(tenant->quota.max_bytes, 1000u);
    EXPECT_EQ(tenant->usage, (Usage{30, 1}));
    auto head = meta->head(ref);
    ASSERT_TRUE(head.has_value());
    EXPECT_EQ(head->generation, *second);
    EXPECT_EQ(head->size, 30u);
    ASSERT_EQ(head->parts.size(), 1u);
    EXPECT_EQ(head->parts[0].extent, part_of(1, 30, 1000 + 1000).extent);
    auto upload = meta->get_upload(*pending, kNow);
    ASSERT_TRUE(upload.has_value());
    EXPECT_EQ(upload->parts.size(), 1u);
    auto garbage = meta->peek_garbage(100);
    ASSERT_TRUE(garbage.has_value());
    EXPECT_EQ(garbage->size(), 2u);
    EXPECT_EQ(config->get_cluster_config()->min_sdk_version, "1.2.3");
}

TEST_F(RocksTest, GenerationsKeepGrowingAcrossReopens) {
    open(false);
    ASSERT_TRUE(meta->create_tenant("acme", Quota{}));
    ASSERT_TRUE(meta->create_bucket({"acme", "data"}, BucketConfig{}));
    auto a = put({{"acme", "data"}, "a"}, {1});
    ASSERT_TRUE(a.has_value());
    open(false);
    auto b = put({{"acme", "data"}, "b"}, {1}, 5000);
    ASSERT_TRUE(b.has_value());
    EXPECT_GT(*b, *a);
}

TEST_F(RocksTest, AnEmptyDirectoryIsCreatedOnceOnly) {
    open(false);
    EXPECT_TRUE(db->created());
    open(false);
    EXPECT_FALSE(db->created());
}

TEST_F(RocksTest, ConcurrentCommitsKeepUsageConsistent) {
    open(false);
    ASSERT_TRUE(meta->create_tenant("acme", Quota{}));
    ASSERT_TRUE(meta->create_bucket({"acme", "data"}, BucketConfig{}));
    constexpr int kThreads = 8, kPerThread = 50;
    std::atomic<int> failures{0};
    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; ++t)
        threads.emplace_back([&, t] {
            for (int i = 0; i < kPerThread; ++i) {
                ObjectRef ref{{"acme", "data"}, "t" + std::to_string(t) + "/" + std::to_string(i)};
                if (!put(ref, {3}, static_cast<std::uint64_t>(t) * 1'000'000 + static_cast<std::uint64_t>(i) * 100))
                    ++failures;
            }
        });
    for (auto& th : threads)
        th.join();
    EXPECT_EQ(failures.load(), 0);
    auto tenant = meta->get_tenant("acme");
    ASSERT_TRUE(tenant.has_value());
    EXPECT_EQ(tenant->usage, (Usage{3ull * kThreads * kPerThread, static_cast<std::uint64_t>(kThreads * kPerThread)}));
}

TEST_F(RocksTest, DurableCommitsOfConcurrentCallersShareFlushes) {
    open(true);
    ASSERT_TRUE(meta->create_tenant("acme", Quota{}));
    ASSERT_TRUE(meta->create_bucket({"acme", "data"}, BucketConfig{}));
    constexpr int kThreads = 16, kPerThread = 10;
    std::atomic<int> failures{0};
    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; ++t)
        threads.emplace_back([&, t] {
            for (int i = 0; i < kPerThread; ++i) {
                ObjectRef ref{{"acme", "data"}, "t" + std::to_string(t) + "/" + std::to_string(i)};
                if (!put(ref, {3}, static_cast<std::uint64_t>(t) * 1'000'000 + static_cast<std::uint64_t>(i) * 100))
                    ++failures;
            }
        });
    for (auto& th : threads)
        th.join();
    EXPECT_EQ(failures.load(), 0);
    const auto stats = db->stats();
    EXPECT_GE(stats.transactions, static_cast<std::uint64_t>(kThreads * kPerThread * 3));
    EXPECT_LT(stats.flushes * 2, stats.transactions) << stats.flushes << " flushes for " << stats.transactions;
}

TEST_F(RocksTest, ATransactionThatFailsLeavesNothingBehind) {
    open(false);
    ASSERT_TRUE(meta->create_tenant("acme", Quota{100, 1}));
    ASSERT_TRUE(meta->create_bucket({"acme", "data"}, BucketConfig{}));
    ASSERT_TRUE(put({{"acme", "data"}, "one"}, {10}).has_value());
    // The second object exceeds the object quota: commit_upload fails after reading and before writing.
    auto id = meta->begin_upload({{"acme", "data"}, "two"}, Encryption{1, "k"}, kNow);
    ASSERT_TRUE(id.has_value());
    ASSERT_TRUE(meta->put_part(*id, part_of(1, 10, 7000), kNow));
    auto over = meta->commit_upload(*id, {}, kNow);
    ASSERT_FALSE(over.has_value());
    EXPECT_EQ(over.error().code, Errc::quota_exceeded);
    EXPECT_EQ(meta->get_tenant("acme")->usage, (Usage{10, 1}));
    EXPECT_FALSE(meta->head({{"acme", "data"}, "two"}).has_value());
    EXPECT_TRUE(meta->get_upload(*id, kNow).has_value());  // still pending, untouched
}

}  // namespace
}  // namespace rmk::rocks
