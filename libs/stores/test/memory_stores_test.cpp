#include "rmk/stores/memory_stores.hpp"

#include <memory>
#include <thread>
#include <vector>

#include "store_contract/contract.hpp"

namespace {

rmk::contract::StoreSet make_memory_stores(std::chrono::seconds retention) {
    return {std::make_shared<rmk::MemoryMetaStore>(retention), std::make_shared<rmk::MemoryConfigStore>()};
}

TEST(MemoryStores, FactoryReturnsIndependentStores) {
    auto a = make_memory_stores(std::chrono::hours(1));
    auto b = make_memory_stores(std::chrono::hours(1));
    ASSERT_TRUE(a.meta->create_tenant("acme", {}).has_value());
    auto listed = b.meta->list_tenants();
    ASSERT_TRUE(listed.has_value());
    EXPECT_EQ(listed->size(), 0u);
}

TEST(MemoryStores, ConcurrentCommitsKeepUsageConsistent) {
    auto stores = make_memory_stores(std::chrono::hours(1));
    auto& meta = *stores.meta;
    ASSERT_TRUE(meta.create_tenant("acme", {}).has_value());
    ASSERT_TRUE(meta.create_bucket({"acme", "data"}, {}).has_value());
    constexpr int kThreads = 8;
    constexpr int kPerThread = 50;
    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; ++t)
        threads.emplace_back([&meta, t] {
            for (int i = 0; i < kPerThread; ++i) {
                rmk::ObjectRef ref{{"acme", "data"}, "t" + std::to_string(t) + "/" + std::to_string(i)};
                auto id = meta.begin_upload(ref, {}, rmk::contract::kT0);
                rmk::Part part;
                part.number = 1;
                part.size = 3;
                part.extent = {1, static_cast<std::uint64_t>(t * 100000 + i * 100), 19, 0};
                (void)meta.put_part(*id, part, rmk::contract::kT0);
                (void)meta.commit_upload(*id, {}, rmk::contract::kT0);
            }
        });
    for (auto& thread : threads) thread.join();
    auto info = meta.get_tenant("acme");
    ASSERT_TRUE(info.has_value());
    EXPECT_EQ(info->usage, (rmk::Usage{3ull * kThreads * kPerThread, static_cast<std::uint64_t>(kThreads * kPerThread)}));
}

}  // namespace

RMK_INSTANTIATE_STORE_CONTRACT(Memory, make_memory_stores);
