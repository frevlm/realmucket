#include "rmk/ec/ec.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstddef>
#include <memory>
#include <random>
#include <vector>

namespace rmk::ec {
namespace {

using Bytes = std::vector<std::byte>;

Bytes random_bytes(std::mt19937_64& rng, std::size_t n) {
    Bytes out(n);
    for (auto& b : out) b = static_cast<std::byte>(rng() & 0xFF);
    return out;
}

std::vector<Bytes> encode_all(const Codec& codec, std::mt19937_64& rng, std::size_t len) {
    const std::size_t k = codec.data_units();
    const std::size_t m = codec.parity_units();
    std::vector<Bytes> shards;
    for (std::size_t i = 0; i < k; ++i) shards.push_back(random_bytes(rng, len));
    for (std::size_t i = 0; i < m; ++i) shards.emplace_back(len);

    std::vector<std::span<const std::byte>> data;
    std::vector<std::span<std::byte>> parity;
    for (std::size_t i = 0; i < k; ++i) data.emplace_back(shards[i]);
    for (std::size_t i = 0; i < m; ++i) parity.emplace_back(shards[k + i]);
    codec.encode(data, parity);
    return shards;
}

// Erases the shards whose bit is set in `mask`, reconstructs, and compares with the original.
void check_erasure(const Codec& codec, const std::vector<Bytes>& original, unsigned mask) {
    std::vector<Bytes> work = original;
    std::vector<std::span<std::byte>> spans;
    std::unique_ptr<bool[]> present(new bool[work.size()]);
    for (std::size_t i = 0; i < work.size(); ++i) {
        present[i] = !(mask >> i & 1);
        if (!present[i]) std::fill(work[i].begin(), work[i].end(), std::byte{0xA5});
        spans.emplace_back(work[i]);
    }
    auto st = codec.reconstruct(spans, std::span<const bool>(present.get(), work.size()));
    ASSERT_TRUE(st.has_value()) << st.error().message;
    for (std::size_t i = 0; i < work.size(); ++i)
        EXPECT_EQ(work[i], original[i]) << "mask " << mask << " shard " << i;
}

class EcRoundTrip : public ::testing::TestWithParam<std::pair<int, int>> {};

TEST_P(EcRoundTrip, RecoversEveryErasurePattern) {
    const auto [k, m] = GetParam();
    auto codec = Codec::create(static_cast<std::uint8_t>(k), static_cast<std::uint8_t>(m));
    ASSERT_TRUE(codec.has_value());
    std::mt19937_64 rng(1234 + k * 100 + m);
    const auto original = encode_all(*codec, rng, 257);
    const unsigned n = static_cast<unsigned>(k + m);

    std::size_t checked = 0;
    if (n <= 10) {
        for (unsigned mask = 0; mask < (1u << n); ++mask) {
            if (static_cast<int>(__builtin_popcount(mask)) > m) continue;
            check_erasure(*codec, original, mask);
            ++checked;
        }
    } else {
        for (int round = 0; round < 300; ++round) {
            unsigned mask = 0;
            const int erase = static_cast<int>(rng() % (m + 1));
            while (static_cast<int>(__builtin_popcount(mask)) < erase) mask |= 1u << (rng() % n);
            check_erasure(*codec, original, mask);
            ++checked;
        }
        // The first m shards and the last m shards.
        check_erasure(*codec, original, (1u << m) - 1);
        check_erasure(*codec, original, ((1u << m) - 1) << (k + m - m));
        checked += 2;
    }
    EXPECT_GT(checked, 0u);
}

INSTANTIATE_TEST_SUITE_P(Configs, EcRoundTrip,
                         ::testing::Values(std::pair{1, 2}, std::pair{4, 2}, std::pair{6, 3},
                                           std::pair{10, 4}));

TEST(Ec, ZeroLengthShards) {
    auto codec = Codec::create(4, 2);
    ASSERT_TRUE(codec.has_value());
    std::mt19937_64 rng(7);
    const auto original = encode_all(*codec, rng, 0);
    check_erasure(*codec, original, 0b000011);
    check_erasure(*codec, original, 0b110000);
}

TEST(Ec, ZeroParityIsAllowed) {
    auto codec = Codec::create(3, 0);
    ASSERT_TRUE(codec.has_value());
    EXPECT_EQ(codec->parity_units(), 0);
}

TEST(Ec, CreateValidatesArguments) {
    EXPECT_EQ(Codec::create(0, 2).error().code, Errc::invalid_argument);
    EXPECT_EQ(Codec::create(200, 56).error().code, Errc::invalid_argument);
    EXPECT_TRUE(Codec::create(200, 55).has_value());
}

TEST(Ec, ReconstructValidatesArguments) {
    auto codec = Codec::create(4, 2);
    ASSERT_TRUE(codec.has_value());
    std::vector<Bytes> bufs(6, Bytes(8));
    std::vector<std::span<std::byte>> spans(bufs.begin(), bufs.end());
    const bool all[6] = {true, true, true, true, true, true};
    const bool three[6] = {true, true, true, false, false, false};

    EXPECT_EQ(codec->reconstruct(spans, std::span<const bool>(three, 6)).error().code,
              Errc::invalid_argument);
    EXPECT_EQ(codec->reconstruct(std::span(spans).first(5), std::span<const bool>(all, 5)).error().code,
              Errc::invalid_argument);
    EXPECT_EQ(codec->reconstruct(spans, std::span<const bool>(all, 5)).error().code,
              Errc::invalid_argument);

    bufs[2].resize(9);
    spans.assign(bufs.begin(), bufs.end());
    EXPECT_EQ(codec->reconstruct(spans, std::span<const bool>(all, 6)).error().code,
              Errc::invalid_argument);
}

TEST(Layout, ShardLength) {
    EXPECT_EQ(shard_length(0, 4), 0u);
    EXPECT_EQ(shard_length(1, 4), 1u);
    EXPECT_EQ(shard_length(8, 4), 2u);
    EXPECT_EQ(shard_length(9, 4), 3u);
    EXPECT_EQ(shard_length(100, 1), 100u);
}

TEST(Layout, MapRangeWithinOneShard) {
    const auto p = map_range(100, 4, 3, 10);
    ASSERT_EQ(p.size(), 1u);
    EXPECT_EQ(p[0].shard, 0);
    EXPECT_EQ(p[0].shard_offset, 3u);
    EXPECT_EQ(p[0].length, 10u);
    EXPECT_EQ(p[0].logical_offset, 3u);
}

TEST(Layout, MapRangeAcrossBoundaries) {
    // 100 bytes, k=4: shard length 25.
    const auto p = map_range(100, 4, 20, 60);
    ASSERT_EQ(p.size(), 4u);
    const ShardPiece expect[] = {{0, 20, 5, 20}, {1, 0, 25, 25}, {2, 0, 25, 50}, {3, 0, 5, 75}};
    for (std::size_t i = 0; i < 4; ++i) {
        EXPECT_EQ(p[i].shard, expect[i].shard) << i;
        EXPECT_EQ(p[i].shard_offset, expect[i].shard_offset) << i;
        EXPECT_EQ(p[i].length, expect[i].length) << i;
        EXPECT_EQ(p[i].logical_offset, expect[i].logical_offset) << i;
    }
}

TEST(Layout, MapRangeLastPaddedShard) {
    // 10 bytes, k=4: shard length 3, shards hold 3,3,3,1 real bytes.
    const auto p = map_range(10, 4, 0, 10);
    ASSERT_EQ(p.size(), 4u);
    EXPECT_EQ(p[3].shard, 3);
    EXPECT_EQ(p[3].shard_offset, 0u);
    EXPECT_EQ(p[3].length, 1u);
    EXPECT_EQ(p[3].logical_offset, 9u);
}

TEST(Layout, MapRangeSingleBytes) {
    for (std::uint64_t off = 0; off < 10; ++off) {
        const auto p = map_range(10, 4, off, 1);
        ASSERT_EQ(p.size(), 1u) << off;
        EXPECT_EQ(p[0].shard, off / 3);
        EXPECT_EQ(p[0].shard_offset, off % 3);
        EXPECT_EQ(p[0].length, 1u);
        EXPECT_EQ(p[0].logical_offset, off);
    }
}

TEST(Layout, MapRangeEmpty) {
    EXPECT_EQ(map_range(100, 4, 50, 0).size(), 0u);
    EXPECT_EQ(map_range(0, 4, 0, 0).size(), 0u);
}

TEST(Layout, MapRangeCoversExactlyTheRange) {
    std::mt19937_64 rng(99);
    for (int round = 0; round < 200; ++round) {
        const std::uint8_t k = static_cast<std::uint8_t>(1 + rng() % 10);
        const std::uint64_t size = 1 + rng() % 5000;
        const std::uint64_t off = rng() % size;
        const std::uint64_t len = rng() % (size - off + 1);
        std::uint64_t pos = off;
        for (const auto& piece : map_range(size, k, off, len)) {
            EXPECT_EQ(piece.logical_offset, pos);
            EXPECT_EQ(piece.shard, pos / shard_length(size, k));
            EXPECT_EQ(piece.shard_offset, pos % shard_length(size, k));
            pos += piece.length;
        }
        EXPECT_EQ(pos, off + len);
    }
}

}  // namespace
}  // namespace rmk::ec
