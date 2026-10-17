#include "rmk/core/crc32c.hpp"

#include <gtest/gtest.h>

#include <string>
#include <string_view>
#include <vector>

namespace {

std::span<const std::byte> bytes(std::string_view s) {
    return std::as_bytes(std::span(s.data(), s.size()));
}

TEST(Crc32c, MatchesStandardCheckValue) {
    EXPECT_EQ(rmk::crc32c(bytes("123456789")), 0xE3069283u);
}

TEST(Crc32c, ExtendingEqualsWholeInput) {
    std::uint32_t partial = rmk::crc32c(bytes("1234"));
    EXPECT_EQ(rmk::crc32c(bytes("56789"), partial), rmk::crc32c(bytes("123456789")));
}

TEST(Crc32c, CombineMatchesConcatenation) {
    std::vector<std::byte> data(3 * 1024 * 1024 + 17);
    std::uint32_t state = 12345;
    for (auto& b : data) {
        state = state * 1664525u + 1013904223u;
        b = static_cast<std::byte>(state >> 24);
    }
    std::span<const std::byte> all(data);
    const std::size_t splits[] = {0, 1, 2, 7, 8, 1000, 4096, 1024 * 1024 + 5, data.size() - 1, data.size()};
    for (std::size_t split : splits) {
        auto a = all.first(split);
        auto b = all.subspan(split);
        EXPECT_EQ(rmk::crc32c_combine(rmk::crc32c(a), rmk::crc32c(b), b.size()), rmk::crc32c(all))
            << "split=" << split;
    }
}

TEST(Crc32c, CombineWithEmptyB) {
    std::uint32_t a = rmk::crc32c(bytes("123456789"));
    EXPECT_EQ(rmk::crc32c_combine(a, rmk::crc32c({}), 0), a);
}

TEST(Crc32c, CombineLargeB) {
    std::vector<std::byte> b((1u << 20) + 123, std::byte{0xA5});
    std::vector<std::byte> whole;
    auto a = bytes("prefix");
    whole.insert(whole.end(), a.begin(), a.end());
    whole.insert(whole.end(), b.begin(), b.end());
    EXPECT_EQ(rmk::crc32c_combine(rmk::crc32c(a), rmk::crc32c(b), b.size()), rmk::crc32c(whole));
}

}  // namespace
