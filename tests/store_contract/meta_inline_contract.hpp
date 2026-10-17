#pragma once

#include <cstring>

#include "rmk/core/crc32c.hpp"
#include "store_contract/meta_fixture.hpp"

// Inline Parts: bytes that live in the metadata and go with the records that point at them.

namespace inline_contract {

inline std::vector<std::byte> bytes_of(const std::string& s) {
    std::vector<std::byte> out(s.size());
    std::memcpy(out.data(), s.data(), s.size());
    return out;
}

inline rmk::Part part_for(std::uint32_t number, const std::vector<std::byte>& bytes) {
    rmk::Part part;
    part.number = number;
    part.size = bytes.size();
    part.checksum = 0x2000 + number;
    part.extent.crc32c = rmk::crc32c(bytes);
    return part;
}

}  // namespace inline_contract

TEST_P(MetaStoreContract, InlinePartRoundTrips) {
    using namespace inline_contract;
    init();
    auto id = store->begin_upload(obj("k"), enc(), rmk::contract::kT0);
    RMK_ASSERT_OK(id);
    const auto data = bytes_of("sealed part bytes");
    auto put = store->put_inline_part(*id, part_for(1, data), data, rmk::contract::kT0);
    RMK_ASSERT_OK(put);
    EXPECT_EQ(put->extent.chunk_id, rmk::kInlineChunk);
    EXPECT_EQ(put->extent.length, data.size());
    EXPECT_EQ(put->extent.crc32c, rmk::crc32c(data));
    RMK_ASSERT_OK(store->commit_upload(*id, {}, rmk::contract::kT0));

    auto head = store->head(obj("k"));
    RMK_ASSERT_OK(head);
    ASSERT_EQ(head->parts.size(), 1u);
    EXPECT_EQ(head->parts[0].extent, put->extent);
    auto back = store->read_inline(head->parts[0].extent);
    RMK_ASSERT_OK(back);
    EXPECT_EQ(*back, data);
    EXPECT_TRUE(garbage().empty());
}

TEST_P(MetaStoreContract, InlinePartsGetDistinctBlobs) {
    using namespace inline_contract;
    init();
    auto id = store->begin_upload(obj("k"), enc(), rmk::contract::kT0);
    RMK_ASSERT_OK(id);
    const auto a = bytes_of("aaaa");
    const auto b = bytes_of("bbbb");
    auto pa = store->put_inline_part(*id, part_for(1, a), a, rmk::contract::kT0);
    auto pb = store->put_inline_part(*id, part_for(2, b), b, rmk::contract::kT0);
    RMK_ASSERT_OK(pa);
    RMK_ASSERT_OK(pb);
    EXPECT_NE(pa->extent.offset, pb->extent.offset);
    EXPECT_EQ(*store->read_inline(pa->extent), a);
    EXPECT_EQ(*store->read_inline(pb->extent), b);
}

TEST_P(MetaStoreContract, ReplacingAnInlinePartFreesTheOldBytes) {
    using namespace inline_contract;
    init();
    auto id = store->begin_upload(obj("k"), enc(), rmk::contract::kT0);
    RMK_ASSERT_OK(id);
    const auto first = bytes_of("first");
    const auto second = bytes_of("second!");
    auto one = store->put_inline_part(*id, part_for(1, first), first, rmk::contract::kT0);
    auto two = store->put_inline_part(*id, part_for(1, second), second, rmk::contract::kT0);
    RMK_ASSERT_OK(one);
    RMK_ASSERT_OK(two);
    RMK_EXPECT_ERR(store->read_inline(one->extent), not_found);
    EXPECT_EQ(*store->read_inline(two->extent), second);
    EXPECT_TRUE(garbage().empty());
}

TEST_P(MetaStoreContract, AbortingAnUploadFreesItsInlineBytes) {
    using namespace inline_contract;
    init();
    auto id = store->begin_upload(obj("k"), enc(), rmk::contract::kT0);
    RMK_ASSERT_OK(id);
    const auto data = bytes_of("gone soon");
    auto put = store->put_inline_part(*id, part_for(1, data), data, rmk::contract::kT0);
    RMK_ASSERT_OK(put);
    RMK_ASSERT_OK(store->abort_upload(*id));
    RMK_EXPECT_ERR(store->read_inline(put->extent), not_found);
    EXPECT_TRUE(garbage().empty());
}

TEST_P(MetaStoreContract, ReclaimingAnExpiredUploadFreesItsInlineBytes) {
    using namespace inline_contract;
    reset(std::chrono::seconds{10});
    init();
    auto id = store->begin_upload(obj("k"), enc(), rmk::contract::kT0);
    RMK_ASSERT_OK(id);
    const auto data = bytes_of("abandoned");
    auto put = store->put_inline_part(*id, part_for(1, data), data, rmk::contract::kT0);
    RMK_ASSERT_OK(put);
    auto reclaimed = store->reclaim_expired(rmk::contract::at(60), 10);
    RMK_ASSERT_OK(reclaimed);
    EXPECT_EQ(*reclaimed, 1u);
    RMK_EXPECT_ERR(store->read_inline(put->extent), not_found);
    EXPECT_TRUE(garbage().empty());
}

TEST_P(MetaStoreContract, DeletingAVersionFreesItsInlineBytes) {
    using namespace inline_contract;
    init();
    auto id = store->begin_upload(obj("k"), enc(), rmk::contract::kT0);
    RMK_ASSERT_OK(id);
    const auto data = bytes_of("live bytes");
    auto put = store->put_inline_part(*id, part_for(1, data), data, rmk::contract::kT0);
    RMK_ASSERT_OK(put);
    RMK_ASSERT_OK(store->commit_upload(*id, {}, rmk::contract::kT0));
    RMK_ASSERT_OK(store->remove(obj("k"), {}, rmk::contract::at(1)));
    RMK_EXPECT_ERR(store->read_inline(put->extent), not_found);
    EXPECT_TRUE(garbage().empty());
}

TEST_P(MetaStoreContract, OverwritingWithoutVersioningFreesTheOldInlineBytes) {
    using namespace inline_contract;
    init(false);
    rmk::Extent old_extent;
    for (const char* text : {"version one", "version two"}) {
        auto id = store->begin_upload(obj("k"), enc(), rmk::contract::kT0);
        RMK_ASSERT_OK(id);
        const auto data = bytes_of(text);
        auto put = store->put_inline_part(*id, part_for(1, data), data, rmk::contract::kT0);
        RMK_ASSERT_OK(put);
        RMK_ASSERT_OK(store->commit_upload(*id, {}, rmk::contract::kT0));
        if (std::string(text) == "version one") {
            old_extent = put->extent;
        } else {
            RMK_EXPECT_ERR(store->read_inline(old_extent), not_found);
            EXPECT_EQ(*store->read_inline(put->extent), data);
        }
    }
    EXPECT_TRUE(garbage().empty());
}

TEST_P(MetaStoreContract, RemovingAVersionFreesItsInlineBytesWhenVersioned) {
    using namespace inline_contract;
    init(true);
    auto id = store->begin_upload(obj("k"), enc(), rmk::contract::kT0);
    RMK_ASSERT_OK(id);
    const auto data = bytes_of("kept until removed");
    auto put = store->put_inline_part(*id, part_for(1, data), data, rmk::contract::kT0);
    RMK_ASSERT_OK(put);
    auto generation = store->commit_upload(*id, {}, rmk::contract::kT0);
    RMK_ASSERT_OK(generation);
    RMK_ASSERT_OK(store->remove(obj("k"), {}, rmk::contract::at(1)));  // a delete marker: bytes stay
    EXPECT_EQ(*store->read_inline(put->extent), data);
    RMK_ASSERT_OK(store->remove_version(obj("k"), *generation));
    RMK_EXPECT_ERR(store->read_inline(put->extent), not_found);
}

TEST_P(MetaStoreContract, InlineReadChecksItsExtent) {
    using namespace inline_contract;
    init();
    auto id = store->begin_upload(obj("k"), enc(), rmk::contract::kT0);
    RMK_ASSERT_OK(id);
    const auto data = bytes_of("checked");
    auto put = store->put_inline_part(*id, part_for(1, data), data, rmk::contract::kT0);
    RMK_ASSERT_OK(put);
    rmk::Extent wrong_crc = put->extent;
    wrong_crc.crc32c ^= 1;
    RMK_EXPECT_ERR(store->read_inline(wrong_crc), corruption);
    rmk::Extent wrong_length = put->extent;
    wrong_length.length += 1;
    RMK_EXPECT_ERR(store->read_inline(wrong_length), not_found);
    rmk::Extent in_a_chunk = put->extent;
    in_a_chunk.chunk_id = 1;
    RMK_EXPECT_ERR(store->read_inline(in_a_chunk), invalid_argument);
}

TEST_P(MetaStoreContract, InlineAndChunkPartsMixInOneObject) {
    using namespace inline_contract;
    init();
    auto id = store->begin_upload(obj("k"), enc(), rmk::contract::kT0);
    RMK_ASSERT_OK(id);
    const auto data = bytes_of("small head");
    auto small = store->put_inline_part(*id, part_for(1, data), data, rmk::contract::kT0);
    RMK_ASSERT_OK(small);
    const rmk::Part big = make_part(2, 4096, 7);
    RMK_ASSERT_OK(store->put_part(*id, big, rmk::contract::kT0));
    RMK_ASSERT_OK(store->commit_upload(*id, {}, rmk::contract::kT0));

    auto in_chunk = store->extents_in_chunk(7, 10);
    RMK_ASSERT_OK(in_chunk);
    ASSERT_EQ(in_chunk->size(), 1u);
    EXPECT_EQ((*in_chunk)[0].extent, big.extent);
    EXPECT_EQ((*store->has_pending_parts(7)), false);

    RMK_ASSERT_OK(store->remove(obj("k"), {}, rmk::contract::at(1)));
    EXPECT_EQ(garbage(), std::vector<rmk::Extent>{big.extent});  // only the Chunk part is garbage
    RMK_EXPECT_ERR(store->read_inline(small->extent), not_found);
}

TEST_P(MetaStoreContract, InlinePartOnAnExpiredUploadStoresNothing) {
    using namespace inline_contract;
    reset(std::chrono::seconds{10});
    init();
    auto id = store->begin_upload(obj("k"), enc(), rmk::contract::kT0);
    RMK_ASSERT_OK(id);
    const auto data = bytes_of("too late");
    RMK_EXPECT_ERR(store->put_inline_part(*id, part_for(1, data), data, rmk::contract::at(60)), expired);
}
