#pragma once

// The whole system in one process, for tests that talk to it only through rmk::client::Client:
// realmucketd on a temporary directory.

#include <gtest/gtest.h>
#include <stdlib.h>

#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>
#include <random>
#include <span>
#include <string>

#include "rmk/client.hpp"
#include "rmk/core/crc32c.hpp"
#include "rmk/crypto/crypto.hpp"
#include "rmk/realmucketd/realmucketd.hpp"

namespace rmk::e2e {

namespace fs = std::filesystem;
namespace rc = ::rmk::client;

inline constexpr std::uint64_t kKiB = 1024;
inline constexpr std::uint64_t kMiB = 1024 * 1024;

inline std::string pattern(std::size_t size, std::uint64_t seed) {
    std::mt19937_64 rng(seed);
    std::string out(size, '\0');
    for (auto& c : out)
        c = static_cast<char>(rng() & 0xFF);
    return out;
}

inline std::uint32_t crc_of(std::string_view s) {
    return crc32c(std::as_bytes(std::span(s.data(), s.size())));
}

// Unwraps a client Result; a failure is reported and yields a default value.
template <class T>
T take(rc::Result<T> r) {
    if (!r) {
        ADD_FAILURE() << "client error " << static_cast<int>(r.error().code) << ": " << r.error().message;
        return T{};
    }
    return std::move(*r);
}

inline void take(const rc::Status& s) {
    if (!s)
        ADD_FAILURE() << "client error " << static_cast<int>(s.error().code) << ": " << s.error().message;
}

template <class R>
void expect_error(const R& r, rc::ErrorCode want) {
    if (r)
        ADD_FAILURE() << "expected error " << static_cast<int>(want) << " but the call succeeded";
    else
        EXPECT_EQ(r.error().code, want) << r.error().message;
}

class E2E : public ::testing::Test {
protected:
    void SetUp() override {
        std::string templ = (fs::temp_directory_path() / "rmk-e2e-XXXXXX").string();
        ASSERT_NE(mkdtemp(templ.data()), nullptr);
        root = templ;

        realmucketd::Options options;
        options.listen = "127.0.0.1:0";
        options.data_dir = root / "data";
        options.tls.insecure = true;
        options.sync_metadata = false;
        options.chunk_capacity = 1 * kMiB;  // small, so Chunks fill and seal quickly
        options.gateway.part_size = 64 * kKiB;
        options.gateway.max_part_bytes = 512 * kKiB;
        crypto::Key master = crypto::random_key();
        auto keys = std::make_shared<crypto::StaticKeyProvider>(std::map<std::uint32_t, crypto::Key>{{1, master}}, 1);
        auto started = realmucketd::Daemon::start(options, keys);
        ASSERT_TRUE(started.has_value()) << started.error().message;
        daemon = std::move(*started);
        endpoint = "127.0.0.1:" + std::to_string(daemon->port());
        admin = connect();
    }

    void TearDown() override {
        admin.reset();
        daemon.reset();
        std::error_code ec;
        fs::remove_all(root, ec);
    }

    // Small multipart threshold and Part size so a few hundred KiB use many Parts.
    rc::Options client_options() const {
        rc::Options o;
        o.endpoint = endpoint;
        o.insecure = true;
        o.multipart_threshold = 100 * kKiB;
        o.part_size = 64 * kKiB;
        o.parallel_parts = 4;
        o.timeout = std::chrono::seconds{30};
        return o;
    }

    std::unique_ptr<rc::Client> connect() const {
        auto c = rc::Client::connect(client_options());
        if (!c) {
            ADD_FAILURE() << "connect: " << c.error().message;
            return nullptr;
        }
        return std::make_unique<rc::Client>(std::move(*c));
    }

    // Creates a Tenant and returns a fresh client for it.
    std::unique_ptr<rc::Client> tenant_admin(const std::string& tenant, const rc::Quota& quota = {}) {
        take(admin->create_tenant(tenant, quota));
        return connect();
    }

    fs::path root;
    std::unique_ptr<realmucketd::Daemon> daemon;
    std::string endpoint;
    std::unique_ptr<rc::Client> admin;  // creates Tenants
};

}  // namespace rmk::e2e
