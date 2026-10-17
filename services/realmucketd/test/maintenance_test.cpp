#include "rmk/realmucketd/maintenance.hpp"

#include <gtest/gtest.h>
#include <unistd.h>

#include <filesystem>
#include <map>
#include <string>

#include "rmk/core/crc32c.hpp"
#include "rmk/stores/memory_stores.hpp"

namespace rmk::realmucketd {
namespace {

namespace fs = std::filesystem;

constexpr std::uint64_t kCapacity = kChunkHeaderSize + 1000;  // fits four 200-byte Parts (228 bytes stored each)

std::vector<std::byte> bytes_of(const std::string& s) {
    const auto* p = reinterpret_cast<const std::byte*>(s.data());
    return {p, p + s.size()};
}

crypto::Key key_of(int seed) {
    crypto::Key k{};
    k[0] = static_cast<std::byte>(seed);
    return k;
}

class MaintenanceTest : public ::testing::Test {
protected:
    void SetUp() override {
        std::string templ = (fs::temp_directory_path() / "rmk-maint-XXXXXX").string();
        ASSERT_NE(mkdtemp(templ.data()), nullptr);
        dir = templ;
        keys = std::make_shared<crypto::StaticKeyProvider>(std::map<std::uint32_t, crypto::Key>{{1, key_of(1)}}, 1);
        meta = std::make_shared<MemoryMetaStore>(std::chrono::hours{1});
        ASSERT_TRUE(meta->create_tenant("t", Quota{}));
        ASSERT_TRUE(meta->create_bucket({"t", "b"}, BucketConfig{}));
        reopen();
    }

    void TearDown() override {
        maintenance.reset();
        store.reset();
        std::error_code ec;
        fs::remove_all(dir, ec);
    }

    // Closes the store and opens it again on the same directory, as a restart would.
    void reopen() {
        maintenance.reset();
        store.reset();
        auto opened = local::ChunkStore::open({dir, kCapacity});
        ASSERT_TRUE(opened.has_value()) << opened.error().message;
        store = std::move(*opened);
        make_maintenance();
    }

    void make_maintenance(std::shared_ptr<crypto::KeyProvider> provider = nullptr) {
        MaintenanceOptions options;
        options.compaction_grace = std::chrono::seconds{0};
        options.rewrap_batch = 100;
        auto created = Maintenance::create(options, meta, store, provider ? provider : keys);
        ASSERT_TRUE(created.has_value()) << created.error().message;
        maintenance = std::move(*created);
    }

    // Stores one Part of `size` bytes as the object `key`, committing it as a new version.
    Part put(const std::string& key, std::size_t size, char fill = 'x', bool commit = true) {
        const ObjectRef ref{{"t", "b"}, key};
        auto data_key = crypto::new_data_key(*keys);
        EXPECT_TRUE(data_key.has_value());
        auto upload = meta->begin_upload(ref, data_key->encryption, Clock::now());
        EXPECT_TRUE(upload.has_value());
        const std::string plain(size, fill);
        auto sealed = crypto::seal_part(data_key->key, 1, bytes_of(plain));
        auto extent = store->write(sealed);
        EXPECT_TRUE(extent.has_value()) << (extent ? "" : extent.error().message);
        Part part{1, size, crc32c(bytes_of(plain)), *extent};
        EXPECT_TRUE(meta->put_part(*upload, part, Clock::now()));
        if (commit)
            EXPECT_TRUE(meta->commit_upload(*upload, Precondition{}, Clock::now()).has_value());
        pending = *upload;
        return part;
    }

    std::string read(const std::string& key) {
        auto version = meta->head({{"t", "b"}, key});
        EXPECT_TRUE(version.has_value());
        if (!version)
            return {};
        auto stored = store->read(version->parts.at(0).extent);
        EXPECT_TRUE(stored.has_value()) << (stored ? "" : stored.error().message);
        auto key_bytes = crypto::unwrap(*keys, version->encryption);
        EXPECT_TRUE(key_bytes.has_value());
        auto plain = crypto::open_part(*key_bytes, 1, *stored);
        EXPECT_TRUE(plain.has_value());
        return plain ? std::string(reinterpret_cast<const char*>(plain->data()), plain->size()) : std::string();
    }

    std::size_t chunk_count() const { return store->chunks().size(); }

    fs::path dir;
    std::shared_ptr<crypto::StaticKeyProvider> keys;
    std::shared_ptr<MemoryMetaStore> meta;
    std::shared_ptr<local::ChunkStore> store;
    std::unique_ptr<Maintenance> maintenance;
    UploadId pending = 0;
};

TEST_F(MaintenanceTest, OverwrittenBytesBecomeGarbageAndTheChunkIsCompacted) {
    put("keep", 200, 'k');
    for (int i = 0; i < 9; ++i)
        put("churn", 200, static_cast<char>('a' + i));
    ASSERT_GE(chunk_count(), 3u);
    const ChunkId first = store->chunks().front().id;

    auto report = maintenance->run_once(Clock::now());
    EXPECT_EQ(report.garbage_entries, 8u);  // every overwritten version
    EXPECT_GE(report.chunks_deleted, 1u);
    EXPECT_GE(report.chunks_compacted, 1u);  // "keep" lived in the first chunk
    for (const auto& chunk : store->chunks())
        EXPECT_NE(chunk.id, first);
    EXPECT_EQ(read("keep"), std::string(200, 'k'));
    EXPECT_EQ(read("churn"), std::string(200, 'i'));
}

TEST_F(MaintenanceTest, ChunksBelowTheGarbageThresholdAreLeftAlone) {
    for (int i = 0; i < 6; ++i)
        put("k" + std::to_string(i), 200);
    const auto before = chunk_count();
    auto report = maintenance->run_once(Clock::now());
    EXPECT_EQ(report.chunks_deleted, 0u);
    EXPECT_EQ(chunk_count(), before);
}

TEST_F(MaintenanceTest, ChunksYoungerThanTheGraceAreLeftAlone) {
    MaintenanceOptions options;
    options.compaction_grace = std::chrono::hours{1};
    auto created = Maintenance::create(options, meta, store, keys);
    ASSERT_TRUE(created.has_value());
    maintenance = std::move(*created);
    for (int i = 0; i < 8; ++i)
        put("churn", 200);
    auto report = maintenance->run_once(Clock::now());
    EXPECT_GT(report.garbage_entries, 0u);
    EXPECT_EQ(report.chunks_deleted, 0u);
    EXPECT_EQ(maintenance->run_once(Clock::now() + std::chrono::hours{2}).chunks_deleted >= 1u, true);
}

TEST_F(MaintenanceTest, ChunksWithPendingPartsWait) {
    put("churn", 200);
    put("churn", 200, 'y', /*commit=*/false);  // a Part of an upload nobody has committed
    const UploadId open_upload = pending;
    for (int i = 0; i < 8; ++i)
        put("churn", 200);
    ASSERT_GE(chunk_count(), 3u);
    const ChunkId first = store->chunks().front().id;

    maintenance->run_once(Clock::now());
    bool still_there = false;
    for (const auto& chunk : store->chunks())
        still_there |= chunk.id == first;
    EXPECT_TRUE(still_there);

    ASSERT_TRUE(meta->abort_upload(open_upload));
    maintenance->run_once(Clock::now());
    for (const auto& chunk : store->chunks())
        EXPECT_NE(chunk.id, first);
}

TEST_F(MaintenanceTest, ExpiredUploadsAreReclaimed) {
    put("x", 200, 'x', /*commit=*/false);
    EXPECT_EQ(maintenance->run_once(Clock::now()).uploads_reclaimed, 0u);
    EXPECT_EQ(maintenance->run_once(Clock::now() + std::chrono::hours{2}).uploads_reclaimed, 1u);
}

TEST_F(MaintenanceTest, GarbageHeavyOpenChunkIsSealedSoItCanBeCompacted) {
    for (int i = 0; i < 3; ++i)  // fills the open chunk with one live and two dead versions
        put("churn", 200);
    ASSERT_EQ(chunk_count(), 1u);
    auto report = maintenance->run_once(Clock::now());
    EXPECT_EQ(report.chunks_sealed, 1u);
    EXPECT_FALSE(store->chunks().front().open);
    put("other", 200);  // lands in a fresh chunk
    EXPECT_EQ(chunk_count(), 2u);
    maintenance->run_once(Clock::now());
    EXPECT_EQ(read("churn"), std::string(200, 'x'));
    EXPECT_EQ(read("other"), std::string(200, 'x'));
}

TEST_F(MaintenanceTest, RestartRecountsGarbageFromTheMetadata) {
    put("keep", 200, 'k');
    for (int i = 0; i < 6; ++i)
        put("churn", 200, static_cast<char>('a' + i));
    maintenance->run_once(Clock::now());  // drains the garbage list, then loses its counts on restart
    const auto before = chunk_count();
    reopen();
    for (const auto& chunk : store->chunks())
        EXPECT_EQ(chunk.garbage_bytes, 0u);
    auto report = maintenance->run_once(Clock::now());
    EXPECT_EQ(report.chunks_reconciled, before);
    EXPECT_EQ(read("keep"), std::string(200, 'k'));
    EXPECT_EQ(read("churn"), std::string(200, 'f'));
}

TEST_F(MaintenanceTest, ChunksNoMetadataPointsAtAreDeletedAfterARestart) {
    put("gone", 200);
    reopen();
    meta = std::make_shared<MemoryMetaStore>(std::chrono::hours{1});  // the index was lost
    make_maintenance();
    auto report = maintenance->run_once(Clock::now());
    EXPECT_EQ(report.chunks_reconciled, 1u);
    EXPECT_GE(report.chunks_deleted, 1u);
    EXPECT_EQ(chunk_count(), 0u);
}

TEST_F(MaintenanceTest, DataKeysAreRewrappedUnderTheActiveMasterKey) {
    put("a", 200, 'a');
    put("b", 200, 'b');
    EXPECT_EQ(meta->head({{"t", "b"}, "a"})->encryption.master_key_id, 1u);

    auto rotated = std::make_shared<crypto::StaticKeyProvider>(
        std::map<std::uint32_t, crypto::Key>{{1, key_of(1)}, {2, key_of(2)}}, 2);
    make_maintenance(rotated);
    EXPECT_EQ(maintenance->run_once(Clock::now()).versions_rewrapped, 2u);
    EXPECT_EQ(meta->head({{"t", "b"}, "a"})->encryption.master_key_id, 2u);
    EXPECT_EQ(maintenance->run_once(Clock::now()).versions_rewrapped, 0u);

    keys = rotated;  // reads now unwrap with the new key alone being active
    EXPECT_EQ(read("a"), std::string(200, 'a'));
    EXPECT_EQ(read("b"), std::string(200, 'b'));
}

TEST(MaintenanceOptionsCheck, RejectsNonsense) {
    auto meta = std::make_shared<MemoryMetaStore>(std::chrono::hours{1});
    auto keys = std::make_shared<crypto::StaticKeyProvider>(std::map<std::uint32_t, crypto::Key>{{1, key_of(1)}}, 1);
    std::string templ = (fs::temp_directory_path() / "rmk-maint-opt-XXXXXX").string();
    ASSERT_NE(mkdtemp(templ.data()), nullptr);
    auto store = local::ChunkStore::open({templ, kCapacity});
    ASSERT_TRUE(store.has_value());
    std::shared_ptr<local::ChunkStore> shared = std::move(*store);

    MaintenanceOptions bad;
    bad.compaction_garbage_percent = 0;
    EXPECT_EQ(Maintenance::create(bad, meta, shared, keys).error().code, Errc::invalid_argument);
    EXPECT_EQ(Maintenance::create({}, nullptr, shared, keys).error().code, Errc::invalid_argument);
    std::error_code ec;
    fs::remove_all(templ, ec);
}

}  // namespace
}  // namespace rmk::realmucketd
