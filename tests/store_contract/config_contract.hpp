#pragma once

#include <string>

#include "store_contract/common.hpp"

class ConfigStoreContract : public ::testing::TestWithParam<rmk::contract::StoreFactory> {
protected:
    void SetUp() override {
        rmk::contract::StoreSet set = GetParam()(std::chrono::hours(1));
        if (!set.meta) GTEST_SKIP() << "store unavailable";
        store = set.config;
    }

    std::shared_ptr<rmk::ConfigStore> store;
};

using namespace rmk;
using namespace rmk::contract;

TEST_P(ConfigStoreContract, ClusterConfigDefaultsWhenNeverSet) {
    auto config = store->get_cluster_config();
    RMK_ASSERT_OK(config);
    EXPECT_EQ(*config, ClusterConfig{});
    EXPECT_EQ(config->min_sdk_version, "");
}

TEST_P(ConfigStoreContract, ClusterConfigRoundTripsAndOverwrites) {
    RMK_ASSERT_OK(store->set_cluster_config(ClusterConfig{"1.2.3"}));
    auto config = store->get_cluster_config();
    RMK_ASSERT_OK(config);
    EXPECT_EQ(config->min_sdk_version, "1.2.3");
    RMK_ASSERT_OK(store->set_cluster_config(ClusterConfig{"2.0.0"}));
    config = store->get_cluster_config();
    RMK_ASSERT_OK(config);
    EXPECT_EQ(config->min_sdk_version, "2.0.0");
    RMK_ASSERT_OK(store->set_cluster_config(ClusterConfig{}));
    config = store->get_cluster_config();
    RMK_ASSERT_OK(config);
    EXPECT_EQ(config->min_sdk_version, "");
}
