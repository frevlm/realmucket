#pragma once

#include <string>

#include "rmk/core/crc32c.hpp"
#include "store_contract/meta_fixture.hpp"

using namespace rmk;
using namespace rmk::contract;

// ---------------------------------------------------------------------------------------------
// Tenants

TEST_P(MetaStoreContract, CreateTenantStoresQuotaAndZeroUsage) {
    Quota quota{1000, 5};
    RMK_ASSERT_OK(store->create_tenant("acme", quota));
    auto info = store->get_tenant("acme");
    RMK_ASSERT_OK(info);
    EXPECT_EQ(info->name, "acme");
    EXPECT_EQ(info->quota, quota);
    EXPECT_EQ(info->usage, (Usage{0, 0}));
}

TEST_P(MetaStoreContract, CreateTenantTwiceIsAlreadyExists) {
    RMK_ASSERT_OK(store->create_tenant("acme", {}));
    RMK_EXPECT_ERR(store->create_tenant("acme", {}), already_exists);
}

TEST_P(MetaStoreContract, GetMissingTenantIsNotFound) {
    RMK_EXPECT_ERR(store->get_tenant("ghost"), not_found);
}

TEST_P(MetaStoreContract, ListTenantsIsSortedByName) {
    for (const char* name : {"zeta", "alpha", "mid"}) RMK_ASSERT_OK(store->create_tenant(name, {}));
    auto names = store->list_tenants();
    RMK_ASSERT_OK(names);
    EXPECT_EQ(*names, (std::vector<std::string>{"alpha", "mid", "zeta"}));
}

TEST_P(MetaStoreContract, ListTenantsOfEmptyStoreIsEmpty) {
    auto names = store->list_tenants();
    RMK_ASSERT_OK(names);
    EXPECT_EQ(names->size(), 0u);
}

TEST_P(MetaStoreContract, SetQuotaReplacesQuotaAndKeepsUsage) {
    init();
    RMK_ASSERT_OK(put_object(obj("a"), {10}));
    RMK_ASSERT_OK(store->set_quota("acme", {500, 7}));
    auto info = store->get_tenant("acme");
    RMK_ASSERT_OK(info);
    EXPECT_EQ(info->quota, (Quota{500, 7}));
    EXPECT_EQ(info->usage, (Usage{10, 1}));
}

TEST_P(MetaStoreContract, SetQuotaOnMissingTenantIsNotFound) {
    RMK_EXPECT_ERR(store->set_quota("ghost", {}), not_found);
}

TEST_P(MetaStoreContract, InvalidTenantNamesAreRejected) {
    const std::string too_long(64, 'a');
    for (const std::string& name : {std::string(""), std::string("-lead"), std::string(".lead"),
                                    std::string("_lead"), std::string("Upper"), std::string("a/b"),
                                    std::string("sp ace"), std::string("semi;colon"),
                                    std::string("caf\xC3\xA9"), too_long}) {
        RMK_EXPECT_ERR(store->create_tenant(name, {}), invalid_argument) << name;
    }
}

TEST_P(MetaStoreContract, ValidTenantNamesAreAccepted) {
    const std::string longest(63, 'a');
    for (const std::string& name : {std::string("a"), std::string("0"), std::string("a.b-c"),
                                    std::string("a-"), std::string("9lives"), std::string("under_score"), longest}) {
        RMK_EXPECT_OK(store->create_tenant(name, {})) << name;
    }
}

// ---------------------------------------------------------------------------------------------
// Buckets

TEST_P(MetaStoreContract, CreateBucketNeedsTenant) {
    RMK_EXPECT_ERR(store->create_bucket(bucket_ref(), {}), not_found);
}

TEST_P(MetaStoreContract, CreateBucketStoresConfig) {
    RMK_ASSERT_OK(store->create_tenant("acme", {}));
    BucketConfig config;
    config.versioning = true;
    config.storage_policy = StoragePolicy{4, 2};
    RMK_ASSERT_OK(store->create_bucket(bucket_ref(), config));
    auto info = store->get_bucket(bucket_ref());
    RMK_ASSERT_OK(info);
    EXPECT_EQ(info->ref, bucket_ref());
    EXPECT_EQ(info->config, config);
}

TEST_P(MetaStoreContract, CreateBucketTwiceIsAlreadyExists) {
    init();
    RMK_EXPECT_ERR(store->create_bucket(bucket_ref(), {}), already_exists);
}

TEST_P(MetaStoreContract, SameBucketNameInTwoTenantsIsFine) {
    RMK_ASSERT_OK(store->create_tenant("one", {}));
    RMK_ASSERT_OK(store->create_tenant("two", {}));
    RMK_EXPECT_OK(store->create_bucket(bucket_ref("one"), {}));
    RMK_EXPECT_OK(store->create_bucket(bucket_ref("two"), {}));
}

TEST_P(MetaStoreContract, GetMissingBucketIsNotFound) {
    init();
    RMK_EXPECT_ERR(store->get_bucket(bucket_ref("acme", "ghost")), not_found);
    RMK_EXPECT_ERR(store->get_bucket(bucket_ref("ghost", "data")), not_found);
}

TEST_P(MetaStoreContract, InvalidBucketNamesAreRejected) {
    RMK_ASSERT_OK(store->create_tenant("acme", {}));
    const std::string too_long(64, 'a');
    for (const std::string& name : {std::string(""), std::string("-lead"), std::string("Upper"),
                                    std::string("a/b"), std::string("a b"), too_long}) {
        RMK_EXPECT_ERR(store->create_bucket(bucket_ref("acme", name), {}), invalid_argument) << name;
    }
    RMK_EXPECT_OK(store->create_bucket(bucket_ref("acme", std::string(63, 'b')), {}));
}

TEST_P(MetaStoreContract, BucketConfigValidation) {
    RMK_ASSERT_OK(store->create_tenant("acme", {}));
    auto config = [](int data, int parity) {
        BucketConfig c;
        c.storage_policy = StoragePolicy{static_cast<std::uint8_t>(data), static_cast<std::uint8_t>(parity)};
        return c;
    };
    RMK_EXPECT_ERR(store->create_bucket(bucket_ref("acme", "zero"), config(0, 2)), invalid_argument);
    RMK_EXPECT_ERR(store->create_bucket(bucket_ref("acme", "big"), config(10, 11)), invalid_argument);
    RMK_EXPECT_ERR(store->create_bucket(bucket_ref("acme", "huge"), config(255, 255)), invalid_argument);
    RMK_EXPECT_OK(store->create_bucket(bucket_ref("acme", "max"), config(10, 10)));
    RMK_EXPECT_OK(store->create_bucket(bucket_ref("acme", "wide"), config(20, 0)));
    RMK_EXPECT_OK(store->create_bucket(bucket_ref("acme", "mirror"), config(1, 0)));
    RMK_EXPECT_ERR(store->set_bucket_config(bucket_ref("acme", "max"), config(0, 1)), invalid_argument);
    RMK_EXPECT_ERR(store->set_bucket_config(bucket_ref("acme", "max"), config(15, 6)), invalid_argument);
}

TEST_P(MetaStoreContract, SetBucketConfigReplacesConfig) {
    init();
    BucketConfig config;
    config.versioning = true;
    config.storage_policy = StoragePolicy{6, 3};
    RMK_ASSERT_OK(store->set_bucket_config(bucket_ref(), config));
    auto info = store->get_bucket(bucket_ref());
    RMK_ASSERT_OK(info);
    EXPECT_EQ(info->config, config);
}

TEST_P(MetaStoreContract, SetBucketConfigOnMissingBucketIsNotFound) {
    init();
    RMK_EXPECT_ERR(store->set_bucket_config(bucket_ref("acme", "ghost"), {}), not_found);
}

TEST_P(MetaStoreContract, ListBucketsReturnsOnlyThatTenantsBucketsInNameOrder) {
    RMK_ASSERT_OK(store->create_tenant("one", {}));
    RMK_ASSERT_OK(store->create_tenant("one2", {}));
    RMK_ASSERT_OK(store->create_tenant("two", {}));
    for (const char* name : {"zz", "aa", "mm"}) RMK_ASSERT_OK(store->create_bucket(bucket_ref("one", name), {}));
    RMK_ASSERT_OK(store->create_bucket(bucket_ref("one2", "other"), {}));
    RMK_ASSERT_OK(store->create_bucket(bucket_ref("two", "other"), {}));
    auto buckets = store->list_buckets("one");
    RMK_ASSERT_OK(buckets);
    ASSERT_EQ(buckets->size(), 3u);
    EXPECT_EQ((*buckets)[0].ref, bucket_ref("one", "aa"));
    EXPECT_EQ((*buckets)[1].ref, bucket_ref("one", "mm"));
    EXPECT_EQ((*buckets)[2].ref, bucket_ref("one", "zz"));
}

TEST_P(MetaStoreContract, ListBucketsOfTenantWithoutBucketsIsEmpty) {
    RMK_ASSERT_OK(store->create_tenant("one", {}));
    auto buckets = store->list_buckets("one");
    RMK_ASSERT_OK(buckets);
    EXPECT_EQ(buckets->size(), 0u);
}

TEST_P(MetaStoreContract, DeleteEmptyBucket) {
    init();
    RMK_ASSERT_OK(store->delete_bucket(bucket_ref()));
    RMK_EXPECT_ERR(store->get_bucket(bucket_ref()), not_found);
    RMK_EXPECT_OK(store->create_bucket(bucket_ref(), {}));
}

TEST_P(MetaStoreContract, DeleteMissingBucketIsNotFound) {
    init();
    RMK_EXPECT_ERR(store->delete_bucket(bucket_ref("acme", "ghost")), not_found);
}

TEST_P(MetaStoreContract, DeleteBucketWithObjectIsNotEmpty) {
    init();
    RMK_ASSERT_OK(put_object(obj("a")));
    RMK_EXPECT_ERR(store->delete_bucket(bucket_ref()), not_empty);
}

TEST_P(MetaStoreContract, DeleteBucketWithOnlyDeleteMarkerIsNotEmpty) {
    init(true);
    auto g = put_object(obj("a"));
    RMK_ASSERT_OK(g);
    RMK_ASSERT_OK(store->remove(obj("a"), {}, at(1)));
    RMK_ASSERT_OK(store->remove_version(obj("a"), *g));
    RMK_EXPECT_ERR(store->delete_bucket(bucket_ref()), not_empty);
}

TEST_P(MetaStoreContract, DeleteBucketWithPendingUploadIsNotEmpty) {
    init();
    auto id = store->begin_upload(obj("a"), enc(), kT0);
    RMK_ASSERT_OK(id);
    RMK_EXPECT_ERR(store->delete_bucket(bucket_ref()), not_empty);
    RMK_ASSERT_OK(store->abort_upload(*id));
    RMK_EXPECT_OK(store->delete_bucket(bucket_ref()));
}

TEST_P(MetaStoreContract, DeleteBucketWithExpiredButUnreclaimedUploadIsNotEmpty) {
    init();
    RMK_ASSERT_OK(store->begin_upload(obj("a"), enc(), kT0));
    Clock::time_point later = kT0 + kRetention + std::chrono::seconds(10);
    RMK_EXPECT_ERR(store->delete_bucket(bucket_ref()), not_empty);
    auto reclaimed = store->reclaim_expired(later, 10);
    RMK_ASSERT_OK(reclaimed);
    EXPECT_EQ(*reclaimed, 1u);
    RMK_EXPECT_OK(store->delete_bucket(bucket_ref()));
}

TEST_P(MetaStoreContract, DeleteBucketAfterEveryVersionIsRemoved) {
    init(true);
    auto g1 = put_object(obj("a"));
    auto g2 = put_object(obj("a"));
    RMK_ASSERT_OK(g1);
    RMK_ASSERT_OK(g2);
    RMK_ASSERT_OK(store->remove_version(obj("a"), *g1));
    RMK_EXPECT_ERR(store->delete_bucket(bucket_ref()), not_empty);
    RMK_ASSERT_OK(store->remove_version(obj("a"), *g2));
    RMK_EXPECT_OK(store->delete_bucket(bucket_ref()));
}

TEST_P(MetaStoreContract, DeleteBucketIgnoresOtherBuckets) {
    init();
    RMK_ASSERT_OK(store->create_bucket(bucket_ref("acme", "other"), {}));
    RMK_ASSERT_OK(put_object(obj("a", "acme", "other")));
    RMK_EXPECT_OK(store->delete_bucket(bucket_ref()));
}

// ---------------------------------------------------------------------------------------------
// Pending Uploads

TEST_P(MetaStoreContract, BeginUploadNeedsBucket) {
    RMK_ASSERT_OK(store->create_tenant("acme", {}));
    RMK_EXPECT_ERR(store->begin_upload(obj("a"), enc(), kT0), not_found);
}

TEST_P(MetaStoreContract, BeginUploadRecordsTargetEncryptionAndDeadline) {
    init();
    auto id = store->begin_upload(obj("dir/a"), enc(7, "k7"), at(100));
    RMK_ASSERT_OK(id);
    auto upload = store->get_upload(*id, at(101));
    RMK_ASSERT_OK(upload);
    EXPECT_EQ(upload->id, *id);
    EXPECT_EQ(upload->target, obj("dir/a"));
    EXPECT_EQ(upload->encryption, enc(7, "k7"));
    EXPECT_EQ(upload->deadline, at(100) + kRetention);
    EXPECT_EQ(upload->parts.size(), 0u);
}

TEST_P(MetaStoreContract, UploadIdsAreUnique) {
    init();
    auto a = store->begin_upload(obj("a"), enc(), kT0);
    auto b = store->begin_upload(obj("a"), enc(), kT0);
    auto c = store->begin_upload(obj("b"), enc(), kT0);
    RMK_ASSERT_OK(a);
    RMK_ASSERT_OK(b);
    RMK_ASSERT_OK(c);
    EXPECT_NE(*a, *b);
    EXPECT_NE(*a, *c);
    EXPECT_NE(*b, *c);
}

TEST_P(MetaStoreContract, ObjectKeyValidation) {
    init();
    const std::string longest(1024, 'k');
    const std::string too_long(1025, 'k');
    const std::string nul("a\0b", 3);
    for (const std::string& key : {std::string(""), too_long, nul, std::string("\xFF"),
                                   std::string("\xC3"), std::string("\xC0\x80"),
                                   std::string("\xED\xA0\x80"), std::string("\xF4\x90\x80\x80"),
                                   std::string("a\x80")}) {
        RMK_EXPECT_ERR(store->begin_upload(obj(key), enc(), kT0), invalid_argument) << key.size();
    }
    for (const std::string& key : {longest, std::string("a"), std::string("dir/\xC3\xA9.txt"),
                                   std::string("\xE2\x82\xAC"), std::string("\xF0\x9F\x98\x80"),
                                   std::string("with space/and%20stuff"), std::string("/leading/slash")}) {
        RMK_EXPECT_OK(store->begin_upload(obj(key), enc(), kT0)) << key.size();
    }
}

TEST_P(MetaStoreContract, GetUnknownUploadIsNotFound) {
    init();
    RMK_EXPECT_ERR(store->get_upload(424242, kT0), not_found);
}

TEST_P(MetaStoreContract, PutPartIsVisibleInUpload) {
    init();
    auto id = store->begin_upload(obj("a"), enc(), kT0);
    RMK_ASSERT_OK(id);
    Part p2 = make_part(2, 20);
    Part p1 = make_part(1, 10);
    RMK_ASSERT_OK(store->put_part(*id, p2, kT0));
    RMK_ASSERT_OK(store->put_part(*id, p1, kT0));
    auto upload = store->get_upload(*id, kT0);
    RMK_ASSERT_OK(upload);
    ASSERT_EQ(upload->parts.size(), 2u);
    EXPECT_EQ(upload->parts.at(1), p1);
    EXPECT_EQ(upload->parts.at(2), p2);
}

TEST_P(MetaStoreContract, PutPartRejectsNumberZero) {
    init();
    auto id = store->begin_upload(obj("a"), enc(), kT0);
    RMK_ASSERT_OK(id);
    RMK_EXPECT_ERR(store->put_part(*id, make_part(0), kT0), invalid_argument);
}

TEST_P(MetaStoreContract, PutPartAcceptsNumbersUpToKMaxParts) {
    init();
    auto id = store->begin_upload(obj("a"), enc(), kT0);
    RMK_ASSERT_OK(id);
    RMK_EXPECT_OK(store->put_part(*id, make_part(kMaxParts), kT0));
    RMK_EXPECT_ERR(store->put_part(*id, make_part(kMaxParts + 1), kT0), invalid_argument);
    auto upload = store->get_upload(*id, kT0);
    RMK_ASSERT_OK(upload);
    EXPECT_EQ(upload->parts.size(), 1u);
    EXPECT_EQ(upload->parts.count(kMaxParts), 1u);
}

TEST_P(MetaStoreContract, CommitOfKMaxPartsParts) {
    init();
    auto id = store->begin_upload(obj("a"), enc(), kT0);
    RMK_ASSERT_OK(id);
    for (std::uint32_t n = 1; n <= kMaxParts; ++n) RMK_ASSERT_OK(store->put_part(*id, make_part(n, 1), kT0));
    RMK_ASSERT_OK(store->commit_upload(*id, {}, kT0));
    auto meta = store->head(obj("a"));
    RMK_ASSERT_OK(meta);
    EXPECT_EQ(meta->parts.size(), static_cast<std::size_t>(kMaxParts));
    EXPECT_EQ(meta->size, static_cast<std::uint64_t>(kMaxParts));
}

TEST_P(MetaStoreContract, PutPartOnUnknownUploadIsNotFound) {
    init();
    RMK_EXPECT_ERR(store->put_part(99, make_part(1), kT0), not_found);
}

TEST_P(MetaStoreContract, ReplacingPartKeepsNewOneAndMakesOldExtentGarbage) {
    init();
    auto id = store->begin_upload(obj("a"), enc(), kT0);
    RMK_ASSERT_OK(id);
    Part first = make_part(1, 10);
    Part second = make_part(1, 30);
    RMK_ASSERT_OK(store->put_part(*id, first, kT0));
    EXPECT_EQ(garbage(), std::vector<Extent>{});
    RMK_ASSERT_OK(store->put_part(*id, second, kT0));
    auto upload = store->get_upload(*id, kT0);
    RMK_ASSERT_OK(upload);
    ASSERT_EQ(upload->parts.size(), 1u);
    EXPECT_EQ(upload->parts.at(1), second);
    EXPECT_EQ(garbage(), std::vector<Extent>{first.extent});
}

TEST_P(MetaStoreContract, ReputtingTheSameExtentDoesNotMakeItGarbage) {
    init();
    auto id = store->begin_upload(obj("a"), enc(), kT0);
    RMK_ASSERT_OK(id);
    Part part = make_part(1);
    RMK_ASSERT_OK(store->put_part(*id, part, kT0));
    RMK_ASSERT_OK(store->put_part(*id, part, kT0));
    EXPECT_EQ(garbage(), std::vector<Extent>{});
}

TEST_P(MetaStoreContract, UploadExpiresAfterRetentionWithoutParts) {
    init();
    auto id = store->begin_upload(obj("a"), enc(), at(0));
    RMK_ASSERT_OK(id);
    RMK_EXPECT_OK(store->get_upload(*id, at(3599)));
    RMK_EXPECT_ERR(store->get_upload(*id, at(3601)), expired);
    RMK_EXPECT_ERR(store->put_part(*id, make_part(1), at(3601)), expired);
    RMK_EXPECT_ERR(store->commit_upload(*id, {}, at(3601)), expired);
}

TEST_P(MetaStoreContract, PutPartExtendsTheDeadline) {
    init();
    auto id = store->begin_upload(obj("a"), enc(), at(0));
    RMK_ASSERT_OK(id);
    RMK_ASSERT_OK(store->put_part(*id, make_part(1), at(3000)));
    auto upload = store->get_upload(*id, at(3001));
    RMK_ASSERT_OK(upload);
    EXPECT_EQ(upload->deadline, at(3000) + kRetention);
    RMK_EXPECT_OK(store->get_upload(*id, at(6000)));
    RMK_EXPECT_ERR(store->get_upload(*id, at(6601)), expired);
}

TEST_P(MetaStoreContract, RetentionComesFromTheFactory) {
    reset(std::chrono::seconds(60));
    init();
    auto id = store->begin_upload(obj("a"), enc(), at(0));
    RMK_ASSERT_OK(id);
    RMK_EXPECT_OK(store->get_upload(*id, at(59)));
    RMK_EXPECT_ERR(store->get_upload(*id, at(61)), expired);
}

TEST_P(MetaStoreContract, AbortUploadRemovesItAndMakesItsPartsGarbage) {
    init();
    auto id = store->begin_upload(obj("a"), enc(), kT0);
    RMK_ASSERT_OK(id);
    Part p1 = make_part(1);
    Part p2 = make_part(2);
    RMK_ASSERT_OK(store->put_part(*id, p1, kT0));
    RMK_ASSERT_OK(store->put_part(*id, p2, kT0));
    RMK_ASSERT_OK(store->abort_upload(*id));
    RMK_EXPECT_ERR(store->get_upload(*id, kT0), not_found);
    EXPECT_EQ(garbage(), sorted({p1.extent, p2.extent}));
    RMK_EXPECT_ERR(store->abort_upload(*id), not_found);
}

TEST_P(MetaStoreContract, AbortOfExpiredUploadStillWorks) {
    init();
    auto id = store->begin_upload(obj("a"), enc(), at(0));
    RMK_ASSERT_OK(id);
    Part p1 = make_part(1);
    RMK_ASSERT_OK(store->put_part(*id, p1, at(0)));
    RMK_ASSERT_OK(store->abort_upload(*id));
    EXPECT_EQ(garbage(), std::vector<Extent>{p1.extent});
}

TEST_P(MetaStoreContract, AbortUnknownUploadIsNotFound) {
    init();
    RMK_EXPECT_ERR(store->abort_upload(31337), not_found);
}

// ---------------------------------------------------------------------------------------------
// commit_upload

TEST_P(MetaStoreContract, CommitMakesObjectVisibleWithDerivedMetadata) {
    init();
    const std::string data1 = "hello, ";
    const std::string data2 = "world";
    const std::string data3 = "!!!";
    auto crc = [](const std::string& s) { return crc32c(std::as_bytes(std::span(s.data(), s.size()))); };
    auto id = store->begin_upload(obj("greeting"), enc(3, "wk3"), at(5));
    RMK_ASSERT_OK(id);
    std::vector<Part> parts;
    std::uint32_t number = 1;
    for (const std::string* data : {&data1, &data2, &data3}) {
        Part p = make_part(number++, data->size());
        p.checksum = crc(*data);
        parts.push_back(p);
    }
    // Upload out of order; commit orders them.
    RMK_ASSERT_OK(store->put_part(*id, parts[2], at(5)));
    RMK_ASSERT_OK(store->put_part(*id, parts[0], at(5)));
    RMK_ASSERT_OK(store->put_part(*id, parts[1], at(5)));
    auto generation = store->commit_upload(*id, {}, at(10));
    RMK_ASSERT_OK(generation);
    EXPECT_NE(*generation, kNoObject);

    auto meta = store->head(obj("greeting"));
    RMK_ASSERT_OK(meta);
    EXPECT_EQ(meta->ref, obj("greeting"));
    EXPECT_EQ(meta->generation, *generation);
    EXPECT_FALSE(meta->delete_marker);
    EXPECT_EQ(meta->size, data1.size() + data2.size() + data3.size());
    EXPECT_EQ(meta->checksum, crc(data1 + data2 + data3));
    EXPECT_EQ(meta->parts, parts);
    EXPECT_EQ(meta->encryption, enc(3, "wk3"));
    EXPECT_EQ(meta->created, at(10));
}

TEST_P(MetaStoreContract, CommitOfSinglePartUsesItsChecksum) {
    init();
    auto id = store->begin_upload(obj("a"), enc(), kT0);
    RMK_ASSERT_OK(id);
    Part p = make_part(1, 77);
    p.checksum = 0xDEADBEEF;
    RMK_ASSERT_OK(store->put_part(*id, p, kT0));
    RMK_ASSERT_OK(store->commit_upload(*id, {}, kT0));
    auto meta = store->head(obj("a"));
    RMK_ASSERT_OK(meta);
    EXPECT_EQ(meta->size, 77u);
    EXPECT_EQ(meta->checksum, 0xDEADBEEFu);
}

TEST_P(MetaStoreContract, CommitWithoutPartsCreatesEmptyObject) {
    init();
    auto id = store->begin_upload(obj("empty"), enc(), kT0);
    RMK_ASSERT_OK(id);
    RMK_ASSERT_OK(store->commit_upload(*id, {}, kT0));
    auto meta = store->head(obj("empty"));
    RMK_ASSERT_OK(meta);
    EXPECT_EQ(meta->size, 0u);
    EXPECT_EQ(meta->parts.size(), 0u);
    EXPECT_FALSE(meta->delete_marker);
    EXPECT_EQ(usage(), (Usage{0, 1}));
}

TEST_P(MetaStoreContract, CommitConsumesTheUpload) {
    init();
    auto id = store->begin_upload(obj("a"), enc(), kT0);
    RMK_ASSERT_OK(id);
    RMK_ASSERT_OK(store->put_part(*id, make_part(1), kT0));
    RMK_ASSERT_OK(store->commit_upload(*id, {}, kT0));
    RMK_EXPECT_ERR(store->commit_upload(*id, {}, kT0), not_found);
    RMK_EXPECT_ERR(store->get_upload(*id, kT0), not_found);
    RMK_EXPECT_ERR(store->put_part(*id, make_part(2), kT0), not_found);
    EXPECT_EQ(garbage(), std::vector<Extent>{});
}

TEST_P(MetaStoreContract, CommitOfUnknownUploadIsNotFound) {
    init();
    RMK_EXPECT_ERR(store->commit_upload(555, {}, kT0), not_found);
}

TEST_P(MetaStoreContract, CommitRejectsGapsInPartNumbers) {
    init();
    auto id = store->begin_upload(obj("a"), enc(), kT0);
    RMK_ASSERT_OK(id);
    RMK_ASSERT_OK(store->put_part(*id, make_part(1), kT0));
    RMK_ASSERT_OK(store->put_part(*id, make_part(3), kT0));
    RMK_EXPECT_ERR(store->commit_upload(*id, {}, kT0), invalid_argument);
    // The upload survives and can be completed.
    RMK_ASSERT_OK(store->put_part(*id, make_part(2), kT0));
    RMK_EXPECT_OK(store->commit_upload(*id, {}, kT0));
    auto meta = store->head(obj("a"));
    RMK_ASSERT_OK(meta);
    EXPECT_EQ(meta->parts.size(), 3u);
}

TEST_P(MetaStoreContract, CommitRejectsPartsNotStartingAtOne) {
    init();
    auto id = store->begin_upload(obj("a"), enc(), kT0);
    RMK_ASSERT_OK(id);
    RMK_ASSERT_OK(store->put_part(*id, make_part(2), kT0));
    RMK_EXPECT_ERR(store->commit_upload(*id, {}, kT0), invalid_argument);
    RMK_EXPECT_ERR(store->head(obj("a")), not_found);
}

TEST_P(MetaStoreContract, GenerationsIncreaseForOneObject) {
    init();
    Generation previous = kNoObject;
    for (int i = 0; i < 5; ++i) {
        auto g = put_object(obj("a"));
        RMK_ASSERT_OK(g);
        EXPECT_GT(*g, previous);
        previous = *g;
    }
}

TEST_P(MetaStoreContract, GenerationsAreUniquePerObject) {
    init(true);
    auto a = put_object(obj("a"));
    auto b = put_object(obj("b"));
    auto a2 = put_object(obj("a"));
    RMK_ASSERT_OK(a);
    RMK_ASSERT_OK(b);
    RMK_ASSERT_OK(a2);
    EXPECT_NE(*a, *a2);
    EXPECT_GT(*a2, *a);
}

TEST_P(MetaStoreContract, PreconditionNoObjectMeansCreateOnly) {
    init();
    Precondition create_only{kNoObject};
    RMK_ASSERT_OK(put_object(obj("a"), {10}, create_only));
    RMK_EXPECT_ERR(put_object(obj("a"), {10}, create_only), precondition_failed);
}

TEST_P(MetaStoreContract, PreconditionOnCurrentGeneration) {
    init();
    auto g1 = put_object(obj("a"));
    RMK_ASSERT_OK(g1);
    auto g2 = put_object(obj("a"), {10}, Precondition{*g1});
    RMK_ASSERT_OK(g2);
    RMK_EXPECT_ERR(put_object(obj("a"), {10}, Precondition{*g1}), precondition_failed);
    RMK_EXPECT_OK(put_object(obj("a"), {10}, Precondition{*g2}));
}

TEST_P(MetaStoreContract, PreconditionOnAbsentObjectWithNonZeroGeneration) {
    init();
    RMK_EXPECT_ERR(put_object(obj("a"), {10}, Precondition{12345}), precondition_failed);
}

TEST_P(MetaStoreContract, FailedPreconditionChangesNothing) {
    init();
    auto g = put_object(obj("a"), {10});
    RMK_ASSERT_OK(g);
    RMK_EXPECT_ERR(put_object(obj("a"), {50}, Precondition{kNoObject}), precondition_failed);
    auto meta = store->head(obj("a"));
    RMK_ASSERT_OK(meta);
    EXPECT_EQ(meta->generation, *g);
    EXPECT_EQ(usage(), (Usage{10, 1}));
}

TEST_P(MetaStoreContract, DeleteMarkerCountsAsAbsentForPrecondition) {
    init(true);
    auto g1 = put_object(obj("a"));
    RMK_ASSERT_OK(g1);
    auto marker = store->remove(obj("a"), {}, at(1));
    RMK_ASSERT_OK(marker);
    RMK_EXPECT_ERR(put_object(obj("a"), {10}, Precondition{*g1}), precondition_failed);
    RMK_EXPECT_ERR(put_object(obj("a"), {10}, Precondition{*marker}), precondition_failed);
    RMK_EXPECT_OK(put_object(obj("a"), {10}, Precondition{kNoObject}));
}

// ---------------------------------------------------------------------------------------------
// Quota and usage

TEST_P(MetaStoreContract, CommitAddsToUsage) {
    init();
    RMK_ASSERT_OK(put_object(obj("a"), {10, 20}));
    EXPECT_EQ(usage(), (Usage{30, 1}));
    RMK_ASSERT_OK(put_object(obj("b"), {5}));
    EXPECT_EQ(usage(), (Usage{35, 2}));
}

TEST_P(MetaStoreContract, UsageIsPerTenantAcrossBuckets) {
    init();
    RMK_ASSERT_OK(store->create_bucket(bucket_ref("acme", "other"), {}));
    RMK_ASSERT_OK(store->create_tenant("rival", {}));
    RMK_ASSERT_OK(store->create_bucket(bucket_ref("rival", "data"), {}));
    RMK_ASSERT_OK(put_object(obj("a"), {10}));
    RMK_ASSERT_OK(put_object(obj("a", "acme", "other"), {7}));
    RMK_ASSERT_OK(put_object(obj("a", "rival", "data"), {100}));
    EXPECT_EQ(usage("acme"), (Usage{17, 2}));
    EXPECT_EQ(usage("rival"), (Usage{100, 1}));
}

TEST_P(MetaStoreContract, QuotaBytesExceededFailsAndChangesNothing) {
    init(false, Quota{100, 1000});
    RMK_ASSERT_OK(put_object(obj("a"), {60}));
    RMK_EXPECT_ERR(put_object(obj("b"), {41}), quota_exceeded);
    EXPECT_EQ(usage(), (Usage{60, 1}));
    RMK_EXPECT_ERR(store->head(obj("b")), not_found);
}

TEST_P(MetaStoreContract, QuotaAllowsFillingToTheLimit) {
    init(false, Quota{100, 2});
    RMK_ASSERT_OK(put_object(obj("a"), {60}));
    RMK_ASSERT_OK(put_object(obj("b"), {40}));
    EXPECT_EQ(usage(), (Usage{100, 2}));
}

TEST_P(MetaStoreContract, QuotaObjectsExceeded) {
    init(false, Quota{1000, 2});
    RMK_ASSERT_OK(put_object(obj("a"), {1}));
    RMK_ASSERT_OK(put_object(obj("b"), {1}));
    RMK_EXPECT_ERR(put_object(obj("c"), {1}), quota_exceeded);
    EXPECT_EQ(usage(), (Usage{2, 2}));
}

TEST_P(MetaStoreContract, QuotaIsCheckedAgainstTheTenantAcrossBuckets) {
    init(false, Quota{100, 1000});
    RMK_ASSERT_OK(store->create_bucket(bucket_ref("acme", "other"), {}));
    RMK_ASSERT_OK(put_object(obj("a"), {70}));
    RMK_EXPECT_ERR(put_object(obj("a", "acme", "other"), {31}), quota_exceeded);
    RMK_EXPECT_OK(put_object(obj("a", "acme", "other"), {30}));
}

TEST_P(MetaStoreContract, ReplacementWithoutVersioningSubtractsOldVersion) {
    init(false, Quota{100, 1});
    RMK_ASSERT_OK(put_object(obj("a"), {70}));
    RMK_ASSERT_OK(put_object(obj("a"), {20}));
    EXPECT_EQ(usage(), (Usage{20, 1}));
    // Replacing at the object limit and growing within the byte limit is allowed.
    RMK_ASSERT_OK(put_object(obj("a"), {100}));
    EXPECT_EQ(usage(), (Usage{100, 1}));
    RMK_EXPECT_ERR(put_object(obj("a"), {101}), quota_exceeded);
    EXPECT_EQ(usage(), (Usage{100, 1}));
}

TEST_P(MetaStoreContract, EveryVersionCountsWithVersioning) {
    init(true);
    RMK_ASSERT_OK(put_object(obj("a"), {10}));
    RMK_ASSERT_OK(put_object(obj("a"), {20}));
    RMK_ASSERT_OK(put_object(obj("a"), {30}));
    EXPECT_EQ(usage(), (Usage{60, 3}));
}

TEST_P(MetaStoreContract, VersionsCountTowardsQuota) {
    init(true, Quota{1000, 2});
    RMK_ASSERT_OK(put_object(obj("a"), {1}));
    RMK_ASSERT_OK(put_object(obj("a"), {1}));
    RMK_EXPECT_ERR(put_object(obj("a"), {1}), quota_exceeded);
}

TEST_P(MetaStoreContract, DeleteMarkersAreNotCounted) {
    init(true, Quota{1000, 1});
    RMK_ASSERT_OK(put_object(obj("a"), {10}));
    RMK_ASSERT_OK(store->remove(obj("a"), {}, at(1)));
    EXPECT_EQ(usage(), (Usage{10, 1}));
    RMK_ASSERT_OK(store->remove_version(obj("a"), store->list_versions(obj("a")).value().front().generation));
    EXPECT_EQ(usage(), (Usage{10, 1}));
}

TEST_P(MetaStoreContract, SetQuotaBelowUsageBlocksNewCommitsOnly) {
    init();
    RMK_ASSERT_OK(put_object(obj("a"), {50}));
    RMK_ASSERT_OK(store->set_quota("acme", {10, 100}));
    RMK_EXPECT_ERR(put_object(obj("b"), {1}), quota_exceeded);
    RMK_EXPECT_OK(store->head(obj("a")));
}

TEST_P(MetaStoreContract, RemoveAndRemoveVersionReleaseUsage) {
    init(true);
    auto g1 = put_object(obj("a"), {10});
    auto g2 = put_object(obj("a"), {20});
    RMK_ASSERT_OK(g1);
    RMK_ASSERT_OK(g2);
    RMK_ASSERT_OK(store->remove_version(obj("a"), *g1));
    EXPECT_EQ(usage(), (Usage{20, 1}));
    RMK_ASSERT_OK(store->remove_version(obj("a"), *g2));
    EXPECT_EQ(usage(), (Usage{0, 0}));
}

TEST_P(MetaStoreContract, RemoveWithoutVersioningReleasesUsage) {
    init();
    RMK_ASSERT_OK(put_object(obj("a"), {10}));
    RMK_ASSERT_OK(put_object(obj("b"), {5}));
    RMK_ASSERT_OK(store->remove(obj("a"), {}, at(1)));
    EXPECT_EQ(usage(), (Usage{5, 1}));
}

TEST_P(MetaStoreContract, AbortedAndPendingUploadsDoNotCountAsUsage) {
    init();
    auto id = store->begin_upload(obj("a"), enc(), kT0);
    RMK_ASSERT_OK(id);
    RMK_ASSERT_OK(store->put_part(*id, make_part(1, 99), kT0));
    EXPECT_EQ(usage(), (Usage{0, 0}));
    RMK_ASSERT_OK(store->abort_upload(*id));
    EXPECT_EQ(usage(), (Usage{0, 0}));
}
