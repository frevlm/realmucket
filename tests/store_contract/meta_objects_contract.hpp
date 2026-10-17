#pragma once

#include <string>

#include "store_contract/meta_fixture.hpp"

using namespace rmk;
using namespace rmk::contract;

// ---------------------------------------------------------------------------------------------
// Versioning off

TEST_P(MetaStoreContract, CommitWithoutVersioningReplacesTheVersion) {
    init();
    auto g1 = put_object(obj("a"), {10, 10});
    RMK_ASSERT_OK(g1);
    auto old_meta = store->head(obj("a"));
    RMK_ASSERT_OK(old_meta);
    auto g2 = put_object(obj("a"), {5});
    RMK_ASSERT_OK(g2);
    EXPECT_GT(*g2, *g1);

    auto meta = store->head(obj("a"));
    RMK_ASSERT_OK(meta);
    EXPECT_EQ(meta->generation, *g2);
    EXPECT_EQ(meta->size, 5u);
    auto versions = store->list_versions(obj("a"));
    RMK_ASSERT_OK(versions);
    ASSERT_EQ(versions->size(), 1u);
    EXPECT_EQ((*versions)[0].generation, *g2);
    RMK_EXPECT_ERR(store->head_version(obj("a"), *g1), not_found);
    EXPECT_EQ(garbage(), sorted(extents_of(*old_meta)));
}

TEST_P(MetaStoreContract, RemoveWithoutVersioningDeletesPermanently) {
    init();
    auto g = put_object(obj("a"), {10, 10});
    RMK_ASSERT_OK(g);
    auto meta = store->head(obj("a"));
    RMK_ASSERT_OK(meta);
    auto removed = store->remove(obj("a"), {}, at(1));
    RMK_ASSERT_OK(removed);
    EXPECT_EQ(*removed, kNoObject);
    RMK_EXPECT_ERR(store->head(obj("a")), not_found);
    RMK_EXPECT_ERR(store->head_version(obj("a"), *g), not_found);
    auto versions = store->list_versions(obj("a"));
    RMK_ASSERT_OK(versions);
    EXPECT_EQ(versions->size(), 0u);
    EXPECT_EQ(garbage(), sorted(extents_of(*meta)));
    RMK_EXPECT_OK(store->delete_bucket(bucket_ref()));
}

TEST_P(MetaStoreContract, RemoveAbsentObjectIsNotFound) {
    init();
    RMK_EXPECT_ERR(store->remove(obj("ghost"), {}, at(1)), not_found);
}

TEST_P(MetaStoreContract, RemoveAlreadyRemovedObjectIsNotFound) {
    init();
    RMK_ASSERT_OK(put_object(obj("a")));
    RMK_ASSERT_OK(store->remove(obj("a"), {}, at(1)));
    RMK_EXPECT_ERR(store->remove(obj("a"), {}, at(2)), not_found);
}

TEST_P(MetaStoreContract, RemoveChecksPrecondition) {
    init();
    auto g = put_object(obj("a"));
    RMK_ASSERT_OK(g);
    RMK_EXPECT_ERR(store->remove(obj("a"), Precondition{*g + 100}, at(1)), precondition_failed);
    RMK_EXPECT_ERR(store->remove(obj("a"), Precondition{kNoObject}, at(1)), precondition_failed);
    RMK_EXPECT_OK(store->head(obj("a")));
    RMK_EXPECT_OK(store->remove(obj("a"), Precondition{*g}, at(1)));
    RMK_EXPECT_ERR(store->head(obj("a")), not_found);
}

TEST_P(MetaStoreContract, RemoveOfAbsentObjectWithMismatchingPreconditionFailsThePrecondition) {
    init();
    RMK_EXPECT_ERR(store->remove(obj("ghost"), Precondition{5}, at(1)), precondition_failed);
}

TEST_P(MetaStoreContract, RemoveDoesNotTouchOtherObjects) {
    init();
    RMK_ASSERT_OK(put_object(obj("a")));
    RMK_ASSERT_OK(put_object(obj("b")));
    RMK_ASSERT_OK(store->remove(obj("a"), {}, at(1)));
    RMK_EXPECT_OK(store->head(obj("b")));
}

// ---------------------------------------------------------------------------------------------
// Versioning on

TEST_P(MetaStoreContract, CommitWithVersioningAddsAVersion) {
    init(true);
    auto g1 = put_object(obj("a"), {10});
    auto g2 = put_object(obj("a"), {20});
    RMK_ASSERT_OK(g1);
    RMK_ASSERT_OK(g2);
    auto versions = store->list_versions(obj("a"));
    RMK_ASSERT_OK(versions);
    ASSERT_EQ(versions->size(), 2u);
    EXPECT_EQ((*versions)[0].generation, *g2);
    EXPECT_EQ((*versions)[1].generation, *g1);
    auto head = store->head(obj("a"));
    RMK_ASSERT_OK(head);
    EXPECT_EQ(head->generation, *g2);
    EXPECT_EQ(head->size, 20u);
    auto old = store->head_version(obj("a"), *g1);
    RMK_ASSERT_OK(old);
    EXPECT_EQ(old->size, 10u);
    EXPECT_EQ(garbage(), std::vector<Extent>{});
}

TEST_P(MetaStoreContract, HeadVersionOfUnknownGenerationIsNotFound) {
    init(true);
    auto g = put_object(obj("a"));
    RMK_ASSERT_OK(g);
    RMK_EXPECT_ERR(store->head_version(obj("a"), *g + 1000), not_found);
    RMK_EXPECT_ERR(store->head_version(obj("other"), *g), not_found);
}

TEST_P(MetaStoreContract, RemoveWithVersioningAddsDeleteMarker) {
    init(true);
    auto g = put_object(obj("a"), {10, 10});
    RMK_ASSERT_OK(g);
    auto marker = store->remove(obj("a"), {}, at(50));
    RMK_ASSERT_OK(marker);
    EXPECT_GT(*marker, *g);

    RMK_EXPECT_ERR(store->head(obj("a")), not_found);
    auto versions = store->list_versions(obj("a"));
    RMK_ASSERT_OK(versions);
    ASSERT_EQ(versions->size(), 2u);
    const ObjectMeta& m = (*versions)[0];
    EXPECT_EQ(m.generation, *marker);
    EXPECT_TRUE(m.delete_marker);
    EXPECT_EQ(m.size, 0u);
    EXPECT_EQ(m.parts.size(), 0u);
    EXPECT_EQ(m.encryption, Encryption{});
    EXPECT_EQ(m.created, at(50));
    EXPECT_EQ(m.ref, obj("a"));
    EXPECT_EQ((*versions)[1].generation, *g);
    EXPECT_FALSE((*versions)[1].delete_marker);
    EXPECT_EQ(garbage(), std::vector<Extent>{});
}

TEST_P(MetaStoreContract, HeadVersionShowsTheDeleteMarker) {
    init(true);
    RMK_ASSERT_OK(put_object(obj("a")));
    auto marker = store->remove(obj("a"), {}, at(1));
    RMK_ASSERT_OK(marker);
    auto meta = store->head_version(obj("a"), *marker);
    RMK_ASSERT_OK(meta);
    EXPECT_TRUE(meta->delete_marker);
}

TEST_P(MetaStoreContract, CommitAfterDeleteMarkerMakesObjectVisibleAgain) {
    init(true);
    RMK_ASSERT_OK(put_object(obj("a"), {10}));
    RMK_ASSERT_OK(store->remove(obj("a"), {}, at(1)));
    auto g = put_object(obj("a"), {30});
    RMK_ASSERT_OK(g);
    auto head = store->head(obj("a"));
    RMK_ASSERT_OK(head);
    EXPECT_EQ(head->generation, *g);
    auto versions = store->list_versions(obj("a"));
    RMK_ASSERT_OK(versions);
    EXPECT_EQ(versions->size(), 3u);
}

TEST_P(MetaStoreContract, RemoveWhenNewestIsDeleteMarkerIsNotFound) {
    init(true);
    RMK_ASSERT_OK(put_object(obj("a")));
    RMK_ASSERT_OK(store->remove(obj("a"), {}, at(1)));
    RMK_EXPECT_ERR(store->remove(obj("a"), {}, at(2)), not_found);
    auto versions = store->list_versions(obj("a"));
    RMK_ASSERT_OK(versions);
    EXPECT_EQ(versions->size(), 2u);
}

TEST_P(MetaStoreContract, RemovePreconditionComparesWithNewestVersion) {
    init(true);
    auto g1 = put_object(obj("a"));
    auto g2 = put_object(obj("a"));
    RMK_ASSERT_OK(g1);
    RMK_ASSERT_OK(g2);
    RMK_EXPECT_ERR(store->remove(obj("a"), Precondition{*g1}, at(1)), precondition_failed);
    RMK_EXPECT_ERR(store->remove(obj("a"), Precondition{kNoObject}, at(1)), precondition_failed);
    auto marker = store->remove(obj("a"), Precondition{*g2}, at(1));
    RMK_ASSERT_OK(marker);
    // The marker's own generation is not a match: it counts as "no object".
    RMK_EXPECT_ERR(store->remove(obj("a"), Precondition{*marker}, at(2)), precondition_failed);
}

TEST_P(MetaStoreContract, RemoveVersionDeletesExactlyThatVersionAndItsExtents) {
    init(true);
    auto g1 = put_object(obj("a"), {10});
    auto g2 = put_object(obj("a"), {20, 20});
    auto g3 = put_object(obj("a"), {30});
    RMK_ASSERT_OK(g1);
    RMK_ASSERT_OK(g2);
    RMK_ASSERT_OK(g3);
    auto middle = store->head_version(obj("a"), *g2);
    RMK_ASSERT_OK(middle);
    RMK_ASSERT_OK(store->remove_version(obj("a"), *g2));
    auto versions = store->list_versions(obj("a"));
    RMK_ASSERT_OK(versions);
    ASSERT_EQ(versions->size(), 2u);
    EXPECT_EQ((*versions)[0].generation, *g3);
    EXPECT_EQ((*versions)[1].generation, *g1);
    EXPECT_EQ(garbage(), sorted(extents_of(*middle)));
}

TEST_P(MetaStoreContract, RemoveVersionOfNewestRevealsThePreviousOne) {
    init(true);
    auto g1 = put_object(obj("a"));
    auto g2 = put_object(obj("a"));
    RMK_ASSERT_OK(g1);
    RMK_ASSERT_OK(g2);
    RMK_ASSERT_OK(store->remove_version(obj("a"), *g2));
    auto head = store->head(obj("a"));
    RMK_ASSERT_OK(head);
    EXPECT_EQ(head->generation, *g1);
}

TEST_P(MetaStoreContract, RemoveVersionOfDeleteMarkerUndeletes) {
    init(true);
    auto g = put_object(obj("a"));
    RMK_ASSERT_OK(g);
    auto marker = store->remove(obj("a"), {}, at(1));
    RMK_ASSERT_OK(marker);
    RMK_ASSERT_OK(store->remove_version(obj("a"), *marker));
    auto head = store->head(obj("a"));
    RMK_ASSERT_OK(head);
    EXPECT_EQ(head->generation, *g);
    EXPECT_EQ(garbage(), std::vector<Extent>{});
}

TEST_P(MetaStoreContract, RemoveVersionWorksWithoutVersioningToo) {
    init();
    auto g = put_object(obj("a"), {10});
    RMK_ASSERT_OK(g);
    auto meta = store->head(obj("a"));
    RMK_ASSERT_OK(meta);
    RMK_ASSERT_OK(store->remove_version(obj("a"), *g));
    RMK_EXPECT_ERR(store->head(obj("a")), not_found);
    EXPECT_EQ(garbage(), sorted(extents_of(*meta)));
}

TEST_P(MetaStoreContract, RemoveVersionOfMissingTargetIsNotFound) {
    init(true);
    auto g = put_object(obj("a"));
    RMK_ASSERT_OK(g);
    RMK_EXPECT_ERR(store->remove_version(obj("a"), *g + 1000), not_found);
    RMK_EXPECT_ERR(store->remove_version(obj("ghost"), *g), not_found);
    RMK_ASSERT_OK(store->remove_version(obj("a"), *g));
    RMK_EXPECT_ERR(store->remove_version(obj("a"), *g), not_found);
}

TEST_P(MetaStoreContract, DisablingVersioningKeepsExistingVersionsAndReplacesOnlyTheNewest) {
    init(true);
    auto g1 = put_object(obj("a"), {10});
    auto g2 = put_object(obj("a"), {20});
    RMK_ASSERT_OK(g1);
    RMK_ASSERT_OK(g2);
    auto v2 = store->head_version(obj("a"), *g2);
    RMK_ASSERT_OK(v2);
    RMK_ASSERT_OK(store->set_bucket_config(bucket_ref(), versioned(false)));

    auto g3 = put_object(obj("a"), {30});
    RMK_ASSERT_OK(g3);
    auto versions = store->list_versions(obj("a"));
    RMK_ASSERT_OK(versions);
    ASSERT_EQ(versions->size(), 2u);
    EXPECT_EQ((*versions)[0].generation, *g3);
    EXPECT_EQ((*versions)[1].generation, *g1);
    EXPECT_EQ(garbage(), sorted(extents_of(*v2)));
    EXPECT_EQ(usage(), (Usage{40, 2}));
}

TEST_P(MetaStoreContract, EnablingVersioningLaterStartsKeepingVersions) {
    init(false);
    auto g1 = put_object(obj("a"));
    RMK_ASSERT_OK(g1);
    RMK_ASSERT_OK(store->set_bucket_config(bucket_ref(), versioned(true)));
    auto g2 = put_object(obj("a"));
    RMK_ASSERT_OK(g2);
    auto versions = store->list_versions(obj("a"));
    RMK_ASSERT_OK(versions);
    EXPECT_EQ(versions->size(), 2u);
}

TEST_P(MetaStoreContract, ListVersionsOfUnknownObjectIsEmpty) {
    init(true);
    auto versions = store->list_versions(obj("ghost"));
    RMK_ASSERT_OK(versions);
    EXPECT_EQ(versions->size(), 0u);
}

TEST_P(MetaStoreContract, HeadOfUnknownObjectIsNotFound) {
    init();
    RMK_EXPECT_ERR(store->head(obj("ghost")), not_found);
}

// ---------------------------------------------------------------------------------------------
// list

TEST_P(MetaStoreContract, ListReturnsKeysInByteOrder) {
    init();
    for (const char* key : {"b", "a/b", "a", "a0", "B", "\xC3\xA9", "a.b", "a b"})
        RMK_ASSERT_OK(put_object(obj(key)));
    auto page = store->list(bucket_ref(), "", std::nullopt, 100);
    RMK_ASSERT_OK(page);
    EXPECT_EQ(keys_of(*page), (std::vector<std::string>{"B", "a", "a b", "a.b", "a/b", "a0", "b", "\xC3\xA9"}));
    EXPECT_EQ(page->next_start_after, std::nullopt);
}

TEST_P(MetaStoreContract, ListReturnsNewestVersionMetadata) {
    init(true);
    RMK_ASSERT_OK(put_object(obj("a"), {10}));
    auto g2 = put_object(obj("a"), {25});
    RMK_ASSERT_OK(g2);
    auto page = store->list(bucket_ref(), "", std::nullopt, 10);
    RMK_ASSERT_OK(page);
    ASSERT_EQ(page->objects.size(), 1u);
    EXPECT_EQ(page->objects[0].generation, *g2);
    EXPECT_EQ(page->objects[0].size, 25u);
    EXPECT_EQ(page->objects[0].ref, obj("a"));
}

TEST_P(MetaStoreContract, ListFiltersByPrefix) {
    init();
    for (const char* key : {"logs/1", "logs/2", "logs", "log", "logz", "data/1"})
        RMK_ASSERT_OK(put_object(obj(key)));
    auto page = store->list(bucket_ref(), "logs", std::nullopt, 100);
    RMK_ASSERT_OK(page);
    EXPECT_EQ(keys_of(*page), (std::vector<std::string>{"logs", "logs/1", "logs/2"}));
    page = store->list(bucket_ref(), "logs/", std::nullopt, 100);
    RMK_ASSERT_OK(page);
    EXPECT_EQ(keys_of(*page), (std::vector<std::string>{"logs/1", "logs/2"}));
    page = store->list(bucket_ref(), "zzz", std::nullopt, 100);
    RMK_ASSERT_OK(page);
    EXPECT_EQ(keys_of(*page), std::vector<std::string>{});
}

TEST_P(MetaStoreContract, ListStartAfterIsExclusive) {
    init();
    for (const char* key : {"a", "b", "c", "e"}) RMK_ASSERT_OK(put_object(obj(key)));
    auto page = store->list(bucket_ref(), "", "b", 100);
    RMK_ASSERT_OK(page);
    EXPECT_EQ(keys_of(*page), (std::vector<std::string>{"c", "e"}));
    page = store->list(bucket_ref(), "", "d", 100);
    RMK_ASSERT_OK(page);
    EXPECT_EQ(keys_of(*page), (std::vector<std::string>{"e"}));
    page = store->list(bucket_ref(), "", "e", 100);
    RMK_ASSERT_OK(page);
    EXPECT_EQ(keys_of(*page), std::vector<std::string>{});
}

TEST_P(MetaStoreContract, ListStartAfterAndPrefixCombine) {
    init();
    for (const char* key : {"a/1", "a/2", "a/3", "b/1"}) RMK_ASSERT_OK(put_object(obj(key)));
    auto page = store->list(bucket_ref(), "a/", "a/1", 100);
    RMK_ASSERT_OK(page);
    EXPECT_EQ(keys_of(*page), (std::vector<std::string>{"a/2", "a/3"}));
    // start_after before the prefix does not widen the range.
    page = store->list(bucket_ref(), "b/", "a/9", 100);
    RMK_ASSERT_OK(page);
    EXPECT_EQ(keys_of(*page), (std::vector<std::string>{"b/1"}));
}

TEST_P(MetaStoreContract, ListLimitSetsNextStartAfterOnlyWhenMoreRemain) {
    init();
    for (const char* key : {"a", "b", "c"}) RMK_ASSERT_OK(put_object(obj(key)));
    auto page = store->list(bucket_ref(), "", std::nullopt, 2);
    RMK_ASSERT_OK(page);
    EXPECT_EQ(keys_of(*page), (std::vector<std::string>{"a", "b"}));
    EXPECT_EQ(page->next_start_after, std::optional<std::string>("b"));
    page = store->list(bucket_ref(), "", "b", 2);
    RMK_ASSERT_OK(page);
    EXPECT_EQ(keys_of(*page), (std::vector<std::string>{"c"}));
    EXPECT_EQ(page->next_start_after, std::nullopt);
    page = store->list(bucket_ref(), "", std::nullopt, 3);
    RMK_ASSERT_OK(page);
    EXPECT_EQ(keys_of(*page), (std::vector<std::string>{"a", "b", "c"}));
    EXPECT_EQ(page->next_start_after, std::nullopt);
}

TEST_P(MetaStoreContract, ListPagingVisitsEveryKeyOnce) {
    init();
    std::vector<std::string> expected;
    for (int i = 0; i < 23; ++i) {
        std::string key = "k" + std::string(i < 10 ? "0" : "") + std::to_string(i);
        expected.push_back(key);
        RMK_ASSERT_OK(put_object(obj(key)));
    }
    std::vector<std::string> seen;
    std::optional<std::string> cursor;
    int pages = 0;
    do {
        auto page = store->list(bucket_ref(), "", cursor ? std::optional<std::string_view>(*cursor) : std::nullopt, 5);
        RMK_ASSERT_OK(page);
        for (const auto& key : keys_of(*page)) seen.push_back(key);
        cursor = page->next_start_after;
        ++pages;
    } while (cursor && pages < 100);
    EXPECT_EQ(seen, expected);
    EXPECT_EQ(pages, 5);
}

TEST_P(MetaStoreContract, ListHidesDeleteMarkersAndCountsThemAgainstTheLimit) {
    init(true);
    for (const char* key : {"a", "b", "c", "d", "e"}) RMK_ASSERT_OK(put_object(obj(key)));
    RMK_ASSERT_OK(store->remove(obj("a"), {}, at(1)));
    RMK_ASSERT_OK(store->remove(obj("b"), {}, at(1)));
    RMK_ASSERT_OK(store->remove(obj("d"), {}, at(1)));

    // One page may be short (or empty) as long as it says more remains.
    std::vector<std::string> seen;
    std::optional<std::string> cursor;
    int pages = 0;
    do {
        auto page = store->list(bucket_ref(), "", cursor ? std::optional<std::string_view>(*cursor) : std::nullopt, 2);
        RMK_ASSERT_OK(page);
        EXPECT_LE(page->objects.size(), 2u);
        for (const auto& key : keys_of(*page)) seen.push_back(key);
        cursor = page->next_start_after;
        ++pages;
    } while (cursor && pages < 100);
    EXPECT_EQ(seen, (std::vector<std::string>{"c", "e"}));
    EXPECT_EQ(pages, 3);
}

TEST_P(MetaStoreContract, ListOfOnlyDeleteMarkersEventuallyEndsWithoutCursor) {
    init(true);
    for (const char* key : {"a", "b"}) {
        RMK_ASSERT_OK(put_object(obj(key)));
        RMK_ASSERT_OK(store->remove(obj(key), {}, at(1)));
    }
    std::optional<std::string> cursor;
    int pages = 0;
    do {
        auto page = store->list(bucket_ref(), "", cursor ? std::optional<std::string_view>(*cursor) : std::nullopt, 1);
        RMK_ASSERT_OK(page);
        EXPECT_EQ(page->objects.size(), 0u);
        cursor = page->next_start_after;
        ++pages;
    } while (cursor && pages < 100);
    EXPECT_EQ(pages, 2);
}

TEST_P(MetaStoreContract, ListShowsObjectAgainAfterItsDeleteMarkerIsRemoved) {
    init(true);
    RMK_ASSERT_OK(put_object(obj("a")));
    auto marker = store->remove(obj("a"), {}, at(1));
    RMK_ASSERT_OK(marker);
    RMK_ASSERT_OK(store->remove_version(obj("a"), *marker));
    auto page = store->list(bucket_ref(), "", std::nullopt, 10);
    RMK_ASSERT_OK(page);
    EXPECT_EQ(keys_of(*page), std::vector<std::string>{"a"});
}

TEST_P(MetaStoreContract, ListLimitBounds) {
    init();
    RMK_EXPECT_ERR(store->list(bucket_ref(), "", std::nullopt, 0), invalid_argument);
    RMK_EXPECT_ERR(store->list(bucket_ref(), "", std::nullopt, 1001), invalid_argument);
    RMK_EXPECT_OK(store->list(bucket_ref(), "", std::nullopt, 1));
    RMK_EXPECT_OK(store->list(bucket_ref(), "", std::nullopt, 1000));
}

TEST_P(MetaStoreContract, ListOfMissingBucketIsNotFound) {
    init();
    RMK_EXPECT_ERR(store->list(bucket_ref("acme", "ghost"), "", std::nullopt, 10), not_found);
}

TEST_P(MetaStoreContract, ListIsScopedToTheBucket) {
    init();
    RMK_ASSERT_OK(store->create_bucket(bucket_ref("acme", "data2"), {}));
    RMK_ASSERT_OK(store->create_bucket(bucket_ref("acme", "dat"), {}));
    RMK_ASSERT_OK(store->create_tenant("other", {}));
    RMK_ASSERT_OK(store->create_bucket(bucket_ref("other", "data"), {}));
    RMK_ASSERT_OK(put_object(obj("in-data")));
    RMK_ASSERT_OK(put_object(obj("in-data2", "acme", "data2")));
    RMK_ASSERT_OK(put_object(obj("in-dat", "acme", "dat")));
    RMK_ASSERT_OK(put_object(obj("in-other", "other", "data")));
    auto page = store->list(bucket_ref(), "", std::nullopt, 10);
    RMK_ASSERT_OK(page);
    EXPECT_EQ(keys_of(*page), std::vector<std::string>{"in-data"});
    page = store->list(bucket_ref("acme", "data2"), "", std::nullopt, 10);
    RMK_ASSERT_OK(page);
    EXPECT_EQ(keys_of(*page), std::vector<std::string>{"in-data2"});
}

TEST_P(MetaStoreContract, SameKeyInDifferentBucketsIsIndependent) {
    init();
    RMK_ASSERT_OK(store->create_bucket(bucket_ref("acme", "other"), {}));
    auto g1 = put_object(obj("k"), {1});
    auto g2 = put_object(obj("k", "acme", "other"), {2});
    RMK_ASSERT_OK(g1);
    RMK_ASSERT_OK(g2);
    auto m1 = store->head(obj("k"));
    auto m2 = store->head(obj("k", "acme", "other"));
    RMK_ASSERT_OK(m1);
    RMK_ASSERT_OK(m2);
    EXPECT_EQ(m1->size, 1u);
    EXPECT_EQ(m2->size, 2u);
}
