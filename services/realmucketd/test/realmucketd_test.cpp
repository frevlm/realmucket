#include "rmk/realmucketd/realmucketd.hpp"

#include <gtest/gtest.h>
#include <unistd.h>

#include <filesystem>
#include <fstream>
#include <iterator>
#include <map>
#include <string>

#include "rmk/client.hpp"
#include "rmk/core/crc32c.hpp"

namespace rmk::realmucketd {
namespace {

namespace fs = std::filesystem;
namespace rc = rmk::client;

constexpr std::uint64_t kKiB = 1024;

EnvLookup env_of(std::map<std::string, std::string> vars) {
    auto shared = std::make_shared<std::map<std::string, std::string>>(std::move(vars));
    return [shared](const char* name) -> const char* {
        auto it = shared->find(name);
        return it == shared->end() ? nullptr : it->second.c_str();
    };
}

std::string pattern(std::size_t n, int seed) {
    std::string s(n, '\0');
    for (std::size_t i = 0; i < n; ++i)
        s[i] = static_cast<char>((i * 31 + static_cast<std::size_t>(seed) * 7) & 0xFF);
    return s;
}

template <class T>
T take(std::expected<T, rc::Error> result) {
    EXPECT_TRUE(result.has_value()) << (result ? "" : result.error().message);
    return result ? std::move(*result) : T{};
}

TEST(RealmucketdOptions, NeedDataAndKeyDirectories) {
    EXPECT_EQ(load_options(env_of({{"RMK_INSECURE", "true"}, {"RMK_MASTER_KEY_DIR", "/k"}})).error().code,
              Errc::invalid_argument);
    EXPECT_EQ(load_options(env_of({{"RMK_INSECURE", "true"}, {"RMK_DATA_DIR", "/d"}})).error().code,
              Errc::invalid_argument);
}

TEST(RealmucketdOptions, ReadTheEnvironment) {
    auto options = load_options(env_of({{"RMK_INSECURE", "true"},
                                        {"RMK_DATA_DIR", "/d"},
                                        {"RMK_MASTER_KEY_DIR", "/k"},
                                        {"RMK_LISTEN", "127.0.0.1:7777"},
                                        {"RMK_CHUNK_CAPACITY_BYTES", "999999999"},
                                        {"RMK_METRICS_PORT", "9200"}}));
    ASSERT_TRUE(options) << options.error().message;
    EXPECT_EQ(options->data_dir, "/d");
    EXPECT_EQ(options->master_key_dir, "/k");
    EXPECT_EQ(options->listen, "127.0.0.1:7777");
    EXPECT_EQ(options->chunk_capacity, 999999999u);
    EXPECT_EQ(options->metrics_port, 9200u);
}

TEST(RealmucketdOptions, AChunkMustHoldAWholePart) {
    auto options = load_options(env_of({{"RMK_INSECURE", "true"},
                                        {"RMK_DATA_DIR", "/d"},
                                        {"RMK_MASTER_KEY_DIR", "/k"},
                                        {"RMK_CHUNK_CAPACITY_BYTES", "1048576"}}));
    ASSERT_FALSE(options);
    EXPECT_EQ(options.error().code, Errc::invalid_argument);
}

// The daemon on a temp directory, driven through the Client SDK.
class RealmucketdTest : public ::testing::Test {
protected:
    void SetUp() override {
        std::string templ = (fs::temp_directory_path() / "rmk-daemon-XXXXXX").string();
        ASSERT_NE(mkdtemp(templ.data()), nullptr);
        root = templ;
        start();
    }

    void TearDown() override {
        stop();
        std::error_code ec;
        fs::remove_all(root, ec);
    }

    Options daemon_options(bool sync = false) const {
        Options options;
        options.listen = "127.0.0.1:0";
        options.data_dir = root / "data";
        options.tls.insecure = true;
        options.sync_metadata = sync;  // most tests do not care about surviving a crash
        options.chunk_capacity = 1 << 20;  // small, so a few Parts span several Chunks
        options.gateway.part_size = 64 * kKiB;
        options.gateway.max_part_bytes = 512 * kKiB;
        return options;
    }

    static std::shared_ptr<crypto::KeyProvider> test_keys() {
        crypto::Key master{};
        master[0] = std::byte{7};
        return std::make_shared<crypto::StaticKeyProvider>(std::map<std::uint32_t, crypto::Key>{{1, master}}, 1);
    }

    void start(bool sync = false) {
        auto started = Daemon::start(daemon_options(sync), test_keys());
        ASSERT_TRUE(started.has_value()) << started.error().message;
        daemon = std::move(*started);

        rc::Options co;
        co.endpoint = "127.0.0.1:" + std::to_string(daemon->port());
        co.insecure = true;
        co.multipart_threshold = 100 * kKiB;
        co.part_size = 64 * kKiB;
        co.timeout = std::chrono::seconds{30};
        auto connected = rc::Client::connect(co);
        ASSERT_TRUE(connected.has_value()) << connected.error().message;
        client = std::make_unique<rc::Client>(std::move(*connected));
    }

    void stop() {
        client.reset();
        daemon.reset();
    }

    std::size_t chunk_files() const {
        std::size_t n = 0;
        for (const auto& item : fs::directory_iterator(root / "data" / "chunks"))
            n += item.path().extension() == ".chunk";
        return n;
    }

    fs::path root;
    std::unique_ptr<Daemon> daemon;
    std::unique_ptr<rc::Client> client;
};

TEST_F(RealmucketdTest, SmallAndMultipartObjectsRoundTrip) {
    take(client->create_tenant("rec", rc::Quota{64 << 20, 1000}));
    take(client->create_bucket("rec", "models"));

    const rc::Location small_at{"rec", "models", "small"};
    auto small_info = take(client->put(small_at, "a small object"));
    EXPECT_EQ(small_info.size, 14u);
    EXPECT_EQ(take(client->get(small_at)), "a small object");

    const std::string big = pattern(5 * 64 * kKiB + 123, 1);
    const rc::Location big_at{"rec", "models", "big"};
    auto big_info = take(client->put(big_at, big));
    EXPECT_EQ(big_info.size, big.size());
    EXPECT_EQ(big_info.checksum, crc32c(std::as_bytes(std::span(big))));
    EXPECT_EQ(take(client->get(big_at)), big);
}

TEST_F(RealmucketdTest, BytesLandInSeveralChunksAndStayEncrypted) {
    take(client->create_tenant("rec"));
    take(client->create_bucket("rec", "data"));
    const std::string secret = "TOP-SECRET-" + std::string(200 * kKiB, 'q');
    for (int i = 0; i < 6; ++i)
        take(client->put({"rec", "data", "k" + std::to_string(i)}, secret));
    EXPECT_GE(chunk_files(), 2u);  // 1 MiB chunks, 1.2 MiB written
    for (int i = 0; i < 6; ++i)
        EXPECT_EQ(take(client->get({"rec", "data", "k" + std::to_string(i)})), secret);

    for (const auto& item : fs::directory_iterator(root / "data" / "chunks")) {
        std::ifstream in(item.path(), std::ios::binary);
        std::string raw((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
        EXPECT_EQ(raw.find("TOP-SECRET"), std::string::npos) << item.path();
    }
}

TEST_F(RealmucketdTest, ConditionalWritesAndQuota) {
    take(client->create_tenant("tiny", rc::Quota{1000, 0}));
    take(client->create_bucket("tiny", "data"));
    const rc::Location where{"tiny", "data", "doc"};

    auto first = take(client->put(where, "v1", rc::Precondition::if_not_exists()));
    auto again = client->put(where, "v1b", rc::Precondition::if_not_exists());
    ASSERT_FALSE(again.has_value());
    EXPECT_EQ(again.error().code, rc::ErrorCode::precondition_failed);
    take(client->put(where, "v2", rc::Precondition::if_generation(first.generation)));
    EXPECT_EQ(take(client->get(where)), "v2");

    auto over = client->put({"tiny", "data", "big"}, pattern(2000, 3));
    ASSERT_FALSE(over.has_value());
    EXPECT_EQ(over.error().code, rc::ErrorCode::quota_exceeded);
}

TEST_F(RealmucketdTest, DeleteRemovesTheObject) {
    take(client->create_tenant("rec"));
    take(client->create_bucket("rec", "data"));
    const rc::Location where{"rec", "data", "gone"};
    take(client->put(where, "bye"));
    take(client->remove(where));
    auto missing = client->get(where);
    ASSERT_FALSE(missing.has_value());
    EXPECT_EQ(missing.error().code, rc::ErrorCode::not_found);
}

}  // namespace
TEST_F(RealmucketdTest, ObjectsSurviveARestart) {
    stop();
    start(/*sync=*/true);
    take(client->create_tenant("rec", rc::Quota{64 << 20, 1000}));
    take(client->create_bucket("rec", "models"));
    const std::string big = pattern(3 * 64 * kKiB + 11, 5);
    const rc::Location small_at{"rec", "models", "small"}, big_at{"rec", "models", "big"};
    take(client->put(small_at, "kept"));
    auto stored = take(client->put(big_at, big));

    stop();
    start(/*sync=*/true);
    EXPECT_EQ(take(client->get(small_at)), "kept");
    EXPECT_EQ(take(client->get(big_at)), big);
    EXPECT_EQ(take(client->head(big_at)).generation, stored.generation);
    EXPECT_EQ(take(client->get_tenant("rec")).used_objects, 2u);
    // New writes keep working and get newer Generations.
    auto next = take(client->put({"rec", "models", "later"}, "more"));
    EXPECT_GT(next.generation, stored.generation);
}

TEST_F(RealmucketdTest, TwoDaemonsCannotShareADataDirectory) {
    auto second = Daemon::start(daemon_options(), test_keys());
    ASSERT_FALSE(second.has_value());
    EXPECT_EQ(second.error().code, Errc::unavailable);
    // The first one is unharmed.
    take(client->create_tenant("rec"));
}

TEST_F(RealmucketdTest, ChunkFilesWithoutADatabaseAreNotTouched) {
    take(client->create_tenant("rec"));
    take(client->create_bucket("rec", "data"));
    take(client->put({"rec", "data", "k"}, "value"));
    stop();
    ASSERT_GE(chunk_files(), 1u);

    fs::remove_all(root / "data" / "meta");  // the database is gone, the chunk files are not
    auto started = Daemon::start(daemon_options(), test_keys());
    ASSERT_FALSE(started.has_value());
    EXPECT_EQ(started.error().code, Errc::invalid_argument);
    EXPECT_GE(chunk_files(), 1u);
}

}  // namespace rmk::realmucketd
