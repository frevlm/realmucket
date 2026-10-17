#include "rmk/core/config.hpp"

#include <gtest/gtest.h>

#include <map>
#include <string>

namespace {

rmk::EnvLookup env(std::map<std::string, std::string> vars) {
    return [vars = std::move(vars)](const char* name) -> const char* {
        auto it = vars.find(name);
        return it == vars.end() ? nullptr : it->second.c_str();
    };
}

TEST(Config, UnsetVariablesKeepDefaults) {
    auto config = rmk::load_config(env({}));
    ASSERT_TRUE(config);
    EXPECT_EQ(config->chunk_capacity_bytes, std::uint64_t{1} << 30);
    EXPECT_EQ(config->pending_upload_retention, std::chrono::days{7});
}

TEST(Config, ReadsValuesFromEnvironment) {
    auto config = rmk::load_config(env({
        {"RMK_CHUNK_CAPACITY_BYTES", "268435456"},
        {"RMK_PENDING_UPLOAD_RETENTION_SECONDS", "3600"},
    }));
    ASSERT_TRUE(config);
    EXPECT_EQ(config->chunk_capacity_bytes, 268435456u);
    EXPECT_EQ(config->pending_upload_retention, std::chrono::hours{1});
}

TEST(Config, RejectsMalformedNumber) {
    auto config = rmk::load_config(env({{"RMK_CHUNK_CAPACITY_BYTES", "1GB"}}));
    ASSERT_FALSE(config);
    EXPECT_EQ(config.error().code, rmk::Errc::invalid_argument);
}

TEST(Config, RejectsValuesBelowMinimum) {
    auto capacity = rmk::load_config(env({{"RMK_CHUNK_CAPACITY_BYTES", "64"}}));
    ASSERT_FALSE(capacity);
    EXPECT_EQ(capacity.error().code, rmk::Errc::invalid_argument);

    auto retention = rmk::load_config(env({{"RMK_PENDING_UPLOAD_RETENTION_SECONDS", "0"}}));
    ASSERT_FALSE(retention);
    EXPECT_EQ(retention.error().code, rmk::Errc::invalid_argument);
}

}  // namespace
