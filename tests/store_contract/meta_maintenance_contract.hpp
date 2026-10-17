#pragma once

#include <set>
#include <string>

#include "store_contract/meta_fixture.hpp"

using namespace rmk;
using namespace rmk::contract;

// ---------------------------------------------------------------------------------------------
// Garbage

TEST_P(MetaStoreContract, PlainCommitProducesNoGarbage) {
    init();
    RMK_ASSERT_OK(put_object(obj("a"), {10, 10}));
    RMK_ASSERT_OK(put_object(obj("b"), {10}));
    EXPECT_EQ(garbage(), std::vector<Extent>{});
}

TEST_P(MetaStoreContract, GarbageSourcesAccumulate) {
    init();
    // Replaced Part.
    auto id = store->begin_upload(obj("up"), enc(), kT0);
    RMK_ASSERT_OK(id);
    Part first = make_part(1);
    RMK_ASSERT_OK(store->put_part(*id, first, kT0));
    RMK_ASSERT_OK(store->put_part(*id, make_part(1), kT0));
    // Aborted upload.
    auto aborted = store->begin_upload(obj("ab"), enc(), kT0);
    RMK_ASSERT_OK(aborted);
    Part aborted_part = make_part(1);
    RMK_ASSERT_OK(store->put_part(*aborted, aborted_part, kT0));
    RMK_ASSERT_OK(store->abort_upload(*aborted));
    // Replaced version.
    RMK_ASSERT_OK(put_object(obj("rep"), {10}));
    auto replaced = store->head(obj("rep"));
    RMK_ASSERT_OK(replaced);
    RMK_ASSERT_OK(put_object(obj("rep"), {10}));
    // Removed object.
    RMK_ASSERT_OK(put_object(obj("rm"), {10}));
    auto removed = store->head(obj("rm"));
    RMK_ASSERT_OK(removed);
    RMK_ASSERT_OK(store->remove(obj("rm"), {}, at(1)));

    std::vector<Extent> expected{first.extent, aborted_part.extent};
    expected = concat(expected, extents_of(*replaced));
    expected = concat(expected, extents_of(*removed));
    EXPECT_EQ(garbage(), expected);
}

TEST_P(MetaStoreContract, RelocatePartDoesNotProduceGarbage) {
    init();
    RMK_ASSERT_OK(put_object(obj("a"), {10}));
    auto meta = store->head(obj("a"));
    RMK_ASSERT_OK(meta);
    Extent moved{9, 64, 26, 0xABCD};
    RMK_ASSERT_OK(store->relocate_part(obj("a"), meta->generation, 1, meta->parts[0].extent, moved));
    EXPECT_EQ(garbage(), std::vector<Extent>{});
}

TEST_P(MetaStoreContract, PeekGarbageHonoursLimitAndIsRepeatable) {
    init();
    for (int i = 0; i < 4; ++i) {
        auto id = store->begin_upload(obj("a" + std::to_string(i)), enc(), kT0);
        RMK_ASSERT_OK(id);
        RMK_ASSERT_OK(store->put_part(*id, make_part(1), kT0));
        RMK_ASSERT_OK(store->abort_upload(*id));
    }
    auto two = store->peek_garbage(2);
    RMK_ASSERT_OK(two);
    EXPECT_EQ(two->size(), 2u);
    auto again = store->peek_garbage(2);
    RMK_ASSERT_OK(again);
    ASSERT_EQ(again->size(), 2u);
    EXPECT_EQ((*two)[0].id, (*again)[0].id);
    EXPECT_EQ((*two)[1].id, (*again)[1].id);
    auto all = store->peek_garbage(100);
    RMK_ASSERT_OK(all);
    EXPECT_EQ(all->size(), 4u);
}

TEST_P(MetaStoreContract, GarbageEntryIdsAreUniqueAndNonZero) {
    init();
    for (int i = 0; i < 5; ++i) {
        auto id = store->begin_upload(obj("a"), enc(), kT0);
        RMK_ASSERT_OK(id);
        RMK_ASSERT_OK(store->put_part(*id, make_part(1), kT0));
        RMK_ASSERT_OK(store->put_part(*id, make_part(2), kT0));
        RMK_ASSERT_OK(store->abort_upload(*id));
    }
    auto all = store->peek_garbage(100);
    RMK_ASSERT_OK(all);
    std::set<std::uint64_t> ids;
    for (const auto& entry : *all) {
        EXPECT_NE(entry.id, 0u);
        ids.insert(entry.id);
    }
    EXPECT_EQ(ids.size(), 10u);
}

TEST_P(MetaStoreContract, AckGarbageRemovesOnlyTheAcknowledgedEntries) {
    init();
    std::vector<Part> parts;
    auto id = store->begin_upload(obj("a"), enc(), kT0);
    RMK_ASSERT_OK(id);
    for (std::uint32_t n = 1; n <= 3; ++n) {
        parts.push_back(make_part(n));
        RMK_ASSERT_OK(store->put_part(*id, parts.back(), kT0));
    }
    RMK_ASSERT_OK(store->abort_upload(*id));
    auto all = store->peek_garbage(100);
    RMK_ASSERT_OK(all);
    ASSERT_EQ(all->size(), 3u);
    RMK_ASSERT_OK(store->ack_garbage({(*all)[0].id, (*all)[2].id}));
    auto rest = store->peek_garbage(100);
    RMK_ASSERT_OK(rest);
    ASSERT_EQ(rest->size(), 1u);
    EXPECT_EQ((*rest)[0].id, (*all)[1].id);
    EXPECT_EQ((*rest)[0].extent, (*all)[1].extent);
    RMK_ASSERT_OK(store->ack_garbage({(*all)[1].id}));
    EXPECT_EQ(garbage(), std::vector<Extent>{});
}

TEST_P(MetaStoreContract, AckGarbageIgnoresUnknownIds) {
    init();
    auto id = store->begin_upload(obj("a"), enc(), kT0);
    RMK_ASSERT_OK(id);
    Part part = make_part(1);
    RMK_ASSERT_OK(store->put_part(*id, part, kT0));
    RMK_ASSERT_OK(store->abort_upload(*id));
    RMK_ASSERT_OK(store->ack_garbage({987654321}));
    RMK_ASSERT_OK(store->ack_garbage({}));
    EXPECT_EQ(garbage(), std::vector<Extent>{part.extent});
}

TEST_P(MetaStoreContract, PeekGarbageOnEmptyStoreIsEmpty) {
    auto entries = store->peek_garbage(10);
    RMK_ASSERT_OK(entries);
    EXPECT_EQ(entries->size(), 0u);
}

// ---------------------------------------------------------------------------------------------
// reclaim_expired

TEST_P(MetaStoreContract, ReclaimExpiredDropsOnlyExpiredUploadsAndMakesTheirPartsGarbage) {
    init();
    auto old_id = store->begin_upload(obj("old"), enc(), at(0));
    auto fresh_id = store->begin_upload(obj("fresh"), enc(), at(5000));
    RMK_ASSERT_OK(old_id);
    RMK_ASSERT_OK(fresh_id);
    Part old_part = make_part(1);
    Part fresh_part = make_part(1);
    RMK_ASSERT_OK(store->put_part(*old_id, old_part, at(0)));
    RMK_ASSERT_OK(store->put_part(*fresh_id, fresh_part, at(5000)));

    auto reclaimed = store->reclaim_expired(at(3601), 100);
    RMK_ASSERT_OK(reclaimed);
    EXPECT_EQ(*reclaimed, 1u);
    RMK_EXPECT_ERR(store->get_upload(*old_id, at(3601)), not_found);
    RMK_EXPECT_OK(store->get_upload(*fresh_id, at(3601)));
    EXPECT_EQ(garbage(), std::vector<Extent>{old_part.extent});
}

TEST_P(MetaStoreContract, ReclaimExpiredWithNothingExpiredReturnsZero) {
    init();
    RMK_ASSERT_OK(store->begin_upload(obj("a"), enc(), at(0)));
    auto reclaimed = store->reclaim_expired(at(10), 100);
    RMK_ASSERT_OK(reclaimed);
    EXPECT_EQ(*reclaimed, 0u);
}

TEST_P(MetaStoreContract, ReclaimExpiredHonoursLimit) {
    init();
    for (int i = 0; i < 3; ++i) RMK_ASSERT_OK(store->begin_upload(obj("a"), enc(), at(0)));
    auto first = store->reclaim_expired(at(4000), 2);
    RMK_ASSERT_OK(first);
    EXPECT_EQ(*first, 2u);
    auto second = store->reclaim_expired(at(4000), 2);
    RMK_ASSERT_OK(second);
    EXPECT_EQ(*second, 1u);
    auto third = store->reclaim_expired(at(4000), 2);
    RMK_ASSERT_OK(third);
    EXPECT_EQ(*third, 0u);
}

TEST_P(MetaStoreContract, ReclaimExpiredRespectsPutPartRefresh) {
    init();
    auto id = store->begin_upload(obj("a"), enc(), at(0));
    RMK_ASSERT_OK(id);
    RMK_ASSERT_OK(store->put_part(*id, make_part(1), at(3000)));
    auto reclaimed = store->reclaim_expired(at(4000), 100);
    RMK_ASSERT_OK(reclaimed);
    EXPECT_EQ(*reclaimed, 0u);
    RMK_EXPECT_OK(store->get_upload(*id, at(4000)));
}

TEST_P(MetaStoreContract, ReclaimExpiredNeverTouchesCommittedObjects) {
    init();
    RMK_ASSERT_OK(put_object(obj("a"), {10}, {}, at(0)));
    auto reclaimed = store->reclaim_expired(at(100000), 100);
    RMK_ASSERT_OK(reclaimed);
    EXPECT_EQ(*reclaimed, 0u);
    RMK_EXPECT_OK(store->head(obj("a")));
    EXPECT_EQ(garbage(), std::vector<Extent>{});
}

// ---------------------------------------------------------------------------------------------
// extents_in_chunk and relocate_part

TEST_P(MetaStoreContract, ExtentsInChunkCoversEveryVersionOfEveryObject) {
    init(true);
    auto g1 = put_object(obj("a"), {10, 10}, {}, kT0, 5);
    auto g2 = put_object(obj("a"), {10}, {}, kT0, 5);
    auto g3 = put_object(obj("b"), {10}, {}, kT0, 5);
    RMK_ASSERT_OK(g1);
    RMK_ASSERT_OK(g2);
    RMK_ASSERT_OK(g3);
    RMK_ASSERT_OK(put_object(obj("c"), {10}, {}, kT0, 6));
    RMK_ASSERT_OK(store->remove(obj("a"), {}, at(1)));

    auto refs = store->extents_in_chunk(5, 100);
    RMK_ASSERT_OK(refs);
    ASSERT_EQ(refs->size(), 4u);
    std::set<std::tuple<std::string, Generation, std::uint32_t>> seen;
    for (const auto& r : *refs) {
        EXPECT_EQ(r.extent.chunk_id, 5u);
        seen.emplace(r.ref.key, r.generation, r.part_number);
    }
    std::set<std::tuple<std::string, Generation, std::uint32_t>> expected{
        {"a", *g1, 1}, {"a", *g1, 2}, {"a", *g2, 1}, {"b", *g3, 1}};
    EXPECT_EQ(seen, expected);
}

TEST_P(MetaStoreContract, ExtentsInChunkReportsRefGenerationPartAndExtent) {
    init();
    auto g = put_object(obj("dir/a"), {10, 20}, {}, kT0, 7);
    RMK_ASSERT_OK(g);
    auto meta = store->head(obj("dir/a"));
    RMK_ASSERT_OK(meta);
    auto refs = store->extents_in_chunk(7, 100);
    RMK_ASSERT_OK(refs);
    ASSERT_EQ(refs->size(), 2u);
    for (const auto& r : *refs) {
        EXPECT_EQ(r.ref, obj("dir/a"));
        EXPECT_EQ(r.generation, *g);
        ASSERT_GE(r.part_number, 1u);
        ASSERT_LE(r.part_number, 2u);
        EXPECT_EQ(r.extent, meta->parts[r.part_number - 1].extent);
    }
}

TEST_P(MetaStoreContract, ExtentsInChunkExcludesPendingUploads) {
    init();
    auto id = store->begin_upload(obj("a"), enc(), kT0);
    RMK_ASSERT_OK(id);
    RMK_ASSERT_OK(store->put_part(*id, make_part(1, 10, 3), kT0));
    auto refs = store->extents_in_chunk(3, 100);
    RMK_ASSERT_OK(refs);
    EXPECT_EQ(refs->size(), 0u);
    RMK_ASSERT_OK(store->commit_upload(*id, {}, kT0));
    refs = store->extents_in_chunk(3, 100);
    RMK_ASSERT_OK(refs);
    EXPECT_EQ(refs->size(), 1u);
}

TEST_P(MetaStoreContract, ExtentsInChunkHonoursLimit) {
    init();
    RMK_ASSERT_OK(put_object(obj("a"), {1, 1, 1, 1, 1}, {}, kT0, 4));
    auto refs = store->extents_in_chunk(4, 3);
    RMK_ASSERT_OK(refs);
    EXPECT_EQ(refs->size(), 3u);
    refs = store->extents_in_chunk(4, 100);
    RMK_ASSERT_OK(refs);
    EXPECT_EQ(refs->size(), 5u);
}

TEST_P(MetaStoreContract, ExtentsInChunkShrinksWhenVersionsAreRemoved) {
    init(true);
    auto g1 = put_object(obj("a"), {10}, {}, kT0, 4);
    auto g2 = put_object(obj("a"), {10}, {}, kT0, 4);
    RMK_ASSERT_OK(g1);
    RMK_ASSERT_OK(g2);
    RMK_ASSERT_OK(store->remove_version(obj("a"), *g1));
    auto refs = store->extents_in_chunk(4, 100);
    RMK_ASSERT_OK(refs);
    ASSERT_EQ(refs->size(), 1u);
    EXPECT_EQ((*refs)[0].generation, *g2);
}

TEST_P(MetaStoreContract, HasPendingPartsIsTrueAfterPutPart) {
    init();
    auto id = store->begin_upload(obj("a"), enc(), kT0);
    RMK_ASSERT_OK(id);
    auto before = store->has_pending_parts(4);
    RMK_ASSERT_OK(before);
    EXPECT_EQ(*before, false);
    RMK_ASSERT_OK(store->put_part(*id, make_part(1, 10, 4), kT0));
    auto after = store->has_pending_parts(4);
    RMK_ASSERT_OK(after);
    EXPECT_EQ(*after, true);
    auto other = store->has_pending_parts(5);
    RMK_ASSERT_OK(other);
    EXPECT_EQ(*other, false);
}

TEST_P(MetaStoreContract, HasPendingPartsIsFalseAfterCommit) {
    init();
    auto id = store->begin_upload(obj("a"), enc(), kT0);
    RMK_ASSERT_OK(id);
    RMK_ASSERT_OK(store->put_part(*id, make_part(1, 10, 4), kT0));
    RMK_ASSERT_OK(store->commit_upload(*id, {}, kT0));
    auto pending = store->has_pending_parts(4);
    RMK_ASSERT_OK(pending);
    EXPECT_EQ(*pending, false);
}

TEST_P(MetaStoreContract, HasPendingPartsIsFalseAfterAbort) {
    init();
    auto id = store->begin_upload(obj("a"), enc(), kT0);
    RMK_ASSERT_OK(id);
    RMK_ASSERT_OK(store->put_part(*id, make_part(1, 10, 4), kT0));
    RMK_ASSERT_OK(store->abort_upload(*id));
    auto pending = store->has_pending_parts(4);
    RMK_ASSERT_OK(pending);
    EXPECT_EQ(*pending, false);
}

TEST_P(MetaStoreContract, HasPendingPartsStaysTrueUntilExpiredUploadIsReclaimed) {
    init();
    auto id = store->begin_upload(obj("a"), enc(), at(0));
    RMK_ASSERT_OK(id);
    RMK_ASSERT_OK(store->put_part(*id, make_part(1, 10, 4), at(0)));
    auto before = store->has_pending_parts(4);
    RMK_ASSERT_OK(before);
    EXPECT_EQ(*before, true);
    RMK_ASSERT_OK(store->reclaim_expired(at(4000), 10));
    auto after = store->has_pending_parts(4);
    RMK_ASSERT_OK(after);
    EXPECT_EQ(*after, false);
}

TEST_P(MetaStoreContract, HasPendingPartsFollowsAReplacedPartToItsNewChunk) {
    init();
    auto id = store->begin_upload(obj("a"), enc(), kT0);
    RMK_ASSERT_OK(id);
    RMK_ASSERT_OK(store->put_part(*id, make_part(1, 10, 4), kT0));
    RMK_ASSERT_OK(store->put_part(*id, make_part(1, 10, 5), kT0));
    auto old_chunk = store->has_pending_parts(4);
    auto new_chunk = store->has_pending_parts(5);
    RMK_ASSERT_OK(old_chunk);
    RMK_ASSERT_OK(new_chunk);
    EXPECT_EQ(*old_chunk, false);
    EXPECT_EQ(*new_chunk, true);
}

TEST_P(MetaStoreContract, RelocatePartMovesTheExtent) {
    init();
    auto g = put_object(obj("a"), {10, 10}, {}, kT0, 4);
    RMK_ASSERT_OK(g);
    auto meta = store->head(obj("a"));
    RMK_ASSERT_OK(meta);
    Extent to{8, 128, 26, 0x1234};
    RMK_ASSERT_OK(store->relocate_part(obj("a"), *g, 2, meta->parts[1].extent, to));

    auto after = store->head(obj("a"));
    RMK_ASSERT_OK(after);
    EXPECT_EQ(after->parts[0], meta->parts[0]);
    EXPECT_EQ(after->parts[1].extent, to);
    EXPECT_EQ(after->parts[1].number, 2u);
    EXPECT_EQ(after->parts[1].size, meta->parts[1].size);
    EXPECT_EQ(after->parts[1].checksum, meta->parts[1].checksum);
    EXPECT_EQ(after->generation, *g);

    auto old_chunk = store->extents_in_chunk(4, 100);
    auto new_chunk = store->extents_in_chunk(8, 100);
    RMK_ASSERT_OK(old_chunk);
    RMK_ASSERT_OK(new_chunk);
    EXPECT_EQ(old_chunk->size(), 1u);
    ASSERT_EQ(new_chunk->size(), 1u);
    EXPECT_EQ((*new_chunk)[0].part_number, 2u);
}

TEST_P(MetaStoreContract, RelocatePartIsAbortedWhenTheExtentChanged) {
    init();
    auto g = put_object(obj("a"), {10}, {}, kT0, 4);
    RMK_ASSERT_OK(g);
    auto meta = store->head(obj("a"));
    RMK_ASSERT_OK(meta);
    Extent wrong = meta->parts[0].extent;
    wrong.offset += 1;
    RMK_EXPECT_ERR(store->relocate_part(obj("a"), *g, 1, wrong, Extent{8, 0, 1, 1}), aborted);
    auto after = store->head(obj("a"));
    RMK_ASSERT_OK(after);
    EXPECT_EQ(after->parts[0], meta->parts[0]);
}

TEST_P(MetaStoreContract, RelocateTwiceWithTheSameSourceIsAborted) {
    init();
    auto g = put_object(obj("a"), {10}, {}, kT0, 4);
    RMK_ASSERT_OK(g);
    auto meta = store->head(obj("a"));
    RMK_ASSERT_OK(meta);
    Extent first{8, 0, 26, 1};
    Extent second{9, 0, 26, 2};
    RMK_ASSERT_OK(store->relocate_part(obj("a"), *g, 1, meta->parts[0].extent, first));
    RMK_EXPECT_ERR(store->relocate_part(obj("a"), *g, 1, meta->parts[0].extent, second), aborted);
    auto after = store->head(obj("a"));
    RMK_ASSERT_OK(after);
    EXPECT_EQ(after->parts[0].extent, first);
}

TEST_P(MetaStoreContract, RelocatePartOfVanishedTargetIsAborted) {
    init(true);
    auto g = put_object(obj("a"), {10}, {}, kT0, 4);
    RMK_ASSERT_OK(g);
    auto meta = store->head(obj("a"));
    RMK_ASSERT_OK(meta);
    Extent from = meta->parts[0].extent;
    Extent to{8, 0, 26, 1};
    RMK_EXPECT_ERR(store->relocate_part(obj("a"), *g, 2, from, to), aborted);
    RMK_EXPECT_ERR(store->relocate_part(obj("a"), *g + 1000, 1, from, to), aborted);
    RMK_EXPECT_ERR(store->relocate_part(obj("ghost"), *g, 1, from, to), aborted);
    RMK_ASSERT_OK(store->remove_version(obj("a"), *g));
    RMK_EXPECT_ERR(store->relocate_part(obj("a"), *g, 1, from, to), aborted);
}

TEST_P(MetaStoreContract, RelocatePartWorksOnNoncurrentVersions) {
    init(true);
    auto g1 = put_object(obj("a"), {10}, {}, kT0, 4);
    auto g2 = put_object(obj("a"), {10}, {}, kT0, 4);
    RMK_ASSERT_OK(g1);
    RMK_ASSERT_OK(g2);
    auto old = store->head_version(obj("a"), *g1);
    RMK_ASSERT_OK(old);
    Extent to{8, 0, 26, 1};
    RMK_ASSERT_OK(store->relocate_part(obj("a"), *g1, 1, old->parts[0].extent, to));
    auto after = store->head_version(obj("a"), *g1);
    RMK_ASSERT_OK(after);
    EXPECT_EQ(after->parts[0].extent, to);
}

// ---------------------------------------------------------------------------------------------
// scan_versions

namespace meta_contract_detail {

inline std::vector<std::tuple<std::string, std::string, std::string, Generation>> scan_all(
    MetaStore& store, std::size_t page_size, int* pages = nullptr) {
    std::vector<std::tuple<std::string, std::string, std::string, Generation>> out;
    std::optional<std::string> cursor;
    int count = 0;
    do {
        auto page = store.scan_versions(cursor ? std::optional<std::string_view>(*cursor) : std::nullopt, page_size);
        if (!page) {
            ADD_FAILURE() << errc_name(page.error().code) << ": " << page.error().message;
            break;
        }
        for (const auto& v : page->versions)
            out.emplace_back(v.ref.bucket.tenant, v.ref.bucket.bucket, v.ref.key, v.generation);
        cursor = page->next_cursor;
        ++count;
    } while (cursor && count < 1000);
    if (pages) *pages = count;
    return out;
}

}  // namespace meta_contract_detail

TEST_P(MetaStoreContract, ScanVersionsWalksTenantBucketKeyGeneration) {
    RMK_ASSERT_OK(store->create_tenant("beta", {}));
    RMK_ASSERT_OK(store->create_tenant("alpha", {}));
    RMK_ASSERT_OK(store->create_bucket(bucket_ref("alpha", "b2"), versioned(true)));
    RMK_ASSERT_OK(store->create_bucket(bucket_ref("alpha", "b1"), versioned(true)));
    RMK_ASSERT_OK(store->create_bucket(bucket_ref("beta", "b1"), versioned(true)));

    std::vector<std::tuple<std::string, std::string, std::string, Generation>> expected;
    struct Target { const char* tenant; const char* bucket; const char* key; };
    // Written in scrambled order; versions of one object are committed oldest first.
    std::vector<Target> writes = {
        {"beta", "b1", "k"},   {"alpha", "b2", "z"}, {"alpha", "b1", "b"}, {"alpha", "b1", "a"},
        {"alpha", "b1", "b"},  {"alpha", "b1", "a/x"}, {"alpha", "b2", "a"}, {"alpha", "b1", "b"},
    };
    std::map<std::tuple<std::string, std::string, std::string>, std::vector<Generation>> by_object;
    for (const auto& w : writes) {
        auto g = put_object(obj(w.key, w.tenant, w.bucket));
        RMK_ASSERT_OK(g);
        by_object[{w.tenant, w.bucket, w.key}].push_back(*g);
    }
    for (const auto& [object, generations] : by_object)
        for (Generation g : generations)
            expected.emplace_back(std::get<0>(object), std::get<1>(object), std::get<2>(object), g);

    for (std::size_t page_size : {1, 2, 3, 100}) {
        auto seen = meta_contract_detail::scan_all(*store, page_size);
        EXPECT_EQ(seen, expected) << "page size " << page_size;
    }
}

TEST_P(MetaStoreContract, ScanVersionsPagesAndOnlyTheLastHasNoCursor) {
    init(true);
    for (const char* key : {"a", "b", "c", "d", "e"}) RMK_ASSERT_OK(put_object(obj(key)));
    auto page = store->scan_versions(std::nullopt, 2);
    RMK_ASSERT_OK(page);
    EXPECT_EQ(page->versions.size(), 2u);
    EXPECT_TRUE(page->next_cursor.has_value());
    std::string cursor = *page->next_cursor;
    page = store->scan_versions(std::string_view(cursor), 2);
    RMK_ASSERT_OK(page);
    EXPECT_EQ(page->versions.size(), 2u);
    EXPECT_TRUE(page->next_cursor.has_value());
    cursor = *page->next_cursor;
    page = store->scan_versions(std::string_view(cursor), 2);
    RMK_ASSERT_OK(page);
    ASSERT_EQ(page->versions.size(), 1u);
    EXPECT_EQ(page->versions[0].ref.key, "e");
    EXPECT_FALSE(page->next_cursor.has_value());
}

TEST_P(MetaStoreContract, ScanVersionsExactFitHasNoCursor) {
    init(true);
    RMK_ASSERT_OK(put_object(obj("a")));
    RMK_ASSERT_OK(put_object(obj("b")));
    auto page = store->scan_versions(std::nullopt, 2);
    RMK_ASSERT_OK(page);
    EXPECT_EQ(page->versions.size(), 2u);
    EXPECT_FALSE(page->next_cursor.has_value());
}

TEST_P(MetaStoreContract, ScanVersionsOfEmptyStoreIsEmpty) {
    auto page = store->scan_versions(std::nullopt, 10);
    RMK_ASSERT_OK(page);
    EXPECT_EQ(page->versions.size(), 0u);
    EXPECT_FALSE(page->next_cursor.has_value());
}

TEST_P(MetaStoreContract, ScanVersionsCarriesFullMetadataAndIncludesDeleteMarkers) {
    init(true);
    auto g = put_object(obj("a"), {10});
    RMK_ASSERT_OK(g);
    auto marker = store->remove(obj("a"), {}, at(1));
    RMK_ASSERT_OK(marker);
    auto head = store->head_version(obj("a"), *g);
    RMK_ASSERT_OK(head);
    auto page = store->scan_versions(std::nullopt, 10);
    RMK_ASSERT_OK(page);
    ASSERT_EQ(page->versions.size(), 2u);
    EXPECT_EQ(page->versions[0].generation, *g);
    EXPECT_EQ(page->versions[0].encryption, head->encryption);
    EXPECT_EQ(page->versions[0].parts, head->parts);
    EXPECT_EQ(page->versions[1].generation, *marker);
    EXPECT_TRUE(page->versions[1].delete_marker);
}

TEST_P(MetaStoreContract, ScanVersionsCursorSurvivesConcurrentWrites) {
    init(true);
    for (const char* key : {"b", "d", "f"}) RMK_ASSERT_OK(put_object(obj(key)));
    auto page = store->scan_versions(std::nullopt, 2);
    RMK_ASSERT_OK(page);
    ASSERT_TRUE(page->next_cursor.has_value());
    EXPECT_EQ(page->versions[1].ref.key, "d");
    std::string cursor = *page->next_cursor;
    // Writes before and after the cursor position, plus a removal of the last item seen.
    RMK_ASSERT_OK(put_object(obj("a")));
    RMK_ASSERT_OK(put_object(obj("e")));
    RMK_ASSERT_OK(store->remove_version(obj("d"), page->versions[1].generation));
    page = store->scan_versions(std::string_view(cursor), 10);
    RMK_ASSERT_OK(page);
    std::vector<std::string> keys;
    for (const auto& v : page->versions) keys.push_back(v.ref.key);
    EXPECT_EQ(keys, (std::vector<std::string>{"e", "f"}));
    EXPECT_FALSE(page->next_cursor.has_value());
}

// ---------------------------------------------------------------------------------------------
// rewrap

TEST_P(MetaStoreContract, RewrapReplacesTheEncryption) {
    init();
    RMK_ASSERT_OK(put_object(obj("a"), {10}));
    auto meta = store->head(obj("a"));
    RMK_ASSERT_OK(meta);
    Encryption to = enc(2, "rewrapped");
    RMK_ASSERT_OK(store->rewrap(obj("a"), meta->generation, meta->encryption, to));
    auto after = store->head(obj("a"));
    RMK_ASSERT_OK(after);
    EXPECT_EQ(after->encryption, to);
    EXPECT_EQ(after->parts, meta->parts);
    EXPECT_EQ(after->size, meta->size);
    EXPECT_EQ(after->generation, meta->generation);
}

TEST_P(MetaStoreContract, RewrapIsAbortedWhenFromDoesNotMatch) {
    init();
    RMK_ASSERT_OK(put_object(obj("a"), {10}));
    auto meta = store->head(obj("a"));
    RMK_ASSERT_OK(meta);
    RMK_EXPECT_ERR(store->rewrap(obj("a"), meta->generation, enc(1, "other"), enc(2, "x")), aborted);
    RMK_EXPECT_ERR(store->rewrap(obj("a"), meta->generation, enc(9, "wrapped-key"), enc(2, "x")), aborted);
    auto after = store->head(obj("a"));
    RMK_ASSERT_OK(after);
    EXPECT_EQ(after->encryption, meta->encryption);
}

TEST_P(MetaStoreContract, RewrapTwiceWithTheSameFromIsAborted) {
    init();
    RMK_ASSERT_OK(put_object(obj("a"), {10}));
    auto meta = store->head(obj("a"));
    RMK_ASSERT_OK(meta);
    RMK_ASSERT_OK(store->rewrap(obj("a"), meta->generation, meta->encryption, enc(2, "x")));
    RMK_EXPECT_ERR(store->rewrap(obj("a"), meta->generation, meta->encryption, enc(3, "y")), aborted);
}

TEST_P(MetaStoreContract, RewrapOfVanishedVersionIsAborted) {
    init();
    auto g = put_object(obj("a"), {10});
    RMK_ASSERT_OK(g);
    RMK_EXPECT_ERR(store->rewrap(obj("a"), *g + 1000, enc(), enc(2, "x")), aborted);
    RMK_EXPECT_ERR(store->rewrap(obj("ghost"), *g, enc(), enc(2, "x")), aborted);
}

TEST_P(MetaStoreContract, RewrapWorksOnNoncurrentVersionsAndLeavesOthersAlone) {
    init(true);
    auto g1 = put_object(obj("a"));
    auto g2 = put_object(obj("a"));
    RMK_ASSERT_OK(g1);
    RMK_ASSERT_OK(g2);
    RMK_ASSERT_OK(store->rewrap(obj("a"), *g1, enc(), enc(2, "x")));
    auto v1 = store->head_version(obj("a"), *g1);
    auto v2 = store->head_version(obj("a"), *g2);
    RMK_ASSERT_OK(v1);
    RMK_ASSERT_OK(v2);
    EXPECT_EQ(v1->encryption, enc(2, "x"));
    EXPECT_EQ(v2->encryption, enc());
}
