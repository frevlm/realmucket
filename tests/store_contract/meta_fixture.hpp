#pragma once

#include <algorithm>
#include <string>
#include <tuple>
#include <vector>

#include "store_contract/common.hpp"

// Shared fixture of the MetaStore contract. Lives in the global namespace so that
// INSTANTIATE_TEST_SUITE_P can name it.
class MetaStoreContract : public ::testing::TestWithParam<rmk::contract::StoreFactory> {
protected:
    static constexpr std::chrono::seconds kRetention{3600};

    void SetUp() override { reset(kRetention); }

    void reset(std::chrono::seconds retention) {
        rmk::contract::StoreSet set = GetParam()(retention);
        if (!set.meta) GTEST_SKIP() << "store unavailable";
        store = set.meta;
    }

    static rmk::BucketRef bucket_ref(std::string tenant = "acme", std::string bucket = "data") {
        return {std::move(tenant), std::move(bucket)};
    }
    static rmk::ObjectRef obj(std::string key, std::string tenant = "acme", std::string bucket = "data") {
        return {bucket_ref(std::move(tenant), std::move(bucket)), std::move(key)};
    }
    static rmk::Encryption enc(std::uint32_t master_key_id = 1, std::string key = "wrapped-key") {
        return {master_key_id, std::move(key)};
    }
    static rmk::BucketConfig versioned(bool on) {
        rmk::BucketConfig config;
        config.versioning = on;
        return config;
    }

    // Tenant "acme" with bucket "data".
    void init(bool versioning = false, rmk::Quota quota = {}) {
        RMK_ASSERT_OK(store->create_tenant("acme", quota));
        RMK_ASSERT_OK(store->create_bucket(bucket_ref(), versioned(versioning)));
    }

    // A Part with an Extent that no other Part made by this fixture shares.
    rmk::Part make_part(std::uint32_t number, std::uint64_t size = 10, rmk::ChunkId chunk = 1) {
        rmk::Part part;
        part.number = number;
        part.size = size;
        part.checksum = 0x1000 + number;
        part.extent = {chunk, next_offset_, size + 16, 0xC0DE0000u + static_cast<std::uint32_t>(next_offset_)};
        next_offset_ += 1000;
        return part;
    }

    // Uploads parts of the given sizes (numbered from 1) and commits.
    rmk::Result<rmk::Generation> put_object(const rmk::ObjectRef& ref, std::vector<std::uint64_t> sizes = {10},
                                            rmk::Precondition precondition = {},
                                            rmk::Clock::time_point now = rmk::contract::kT0,
                                            rmk::ChunkId chunk = 1) {
        auto id = store->begin_upload(ref, enc(), now);
        if (!id) return std::unexpected(id.error());
        std::uint32_t number = 1;
        for (std::uint64_t size : sizes) {
            auto put = store->put_part(*id, make_part(number++, size, chunk), now);
            if (!put) return std::unexpected(put.error());
        }
        return store->commit_upload(*id, precondition, now);
    }

    static std::vector<rmk::Extent> extents_of(const rmk::ObjectMeta& meta) {
        std::vector<rmk::Extent> out;
        for (const auto& part : meta.parts) out.push_back(part.extent);
        return out;
    }

    // Every garbage Extent currently reported, sorted.
    std::vector<rmk::Extent> garbage() {
        auto entries = store->peek_garbage(100000);
        std::vector<rmk::Extent> out;
        if (!entries) return out;
        for (const auto& entry : *entries) out.push_back(entry.extent);
        return sorted(out);
    }

    static std::vector<rmk::Extent> sorted(std::vector<rmk::Extent> extents) {
        std::sort(extents.begin(), extents.end(), [](const rmk::Extent& a, const rmk::Extent& b) {
            return std::tie(a.chunk_id, a.offset) < std::tie(b.chunk_id, b.offset);
        });
        return extents;
    }

    static std::vector<rmk::Extent> concat(std::vector<rmk::Extent> a, const std::vector<rmk::Extent>& b) {
        a.insert(a.end(), b.begin(), b.end());
        return sorted(std::move(a));
    }

    rmk::Usage usage(const std::string& tenant = "acme") {
        auto info = store->get_tenant(tenant);
        return info ? info->usage : rmk::Usage{~0ull, ~0ull};
    }

    static std::vector<std::string> keys_of(const rmk::ListPage& page) {
        std::vector<std::string> out;
        for (const auto& meta : page.objects) out.push_back(meta.ref.key);
        return out;
    }

    std::shared_ptr<rmk::MetaStore> store;

private:
    std::uint64_t next_offset_ = 64;
};
