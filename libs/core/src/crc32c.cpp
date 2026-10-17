#include "rmk/core/crc32c.hpp"

#include <array>

namespace rmk {
namespace {

constexpr std::array<std::uint32_t, 256> make_table() {
    std::array<std::uint32_t, 256> table{};
    for (std::uint32_t i = 0; i < 256; ++i) {
        std::uint32_t c = i;
        for (int bit = 0; bit < 8; ++bit)
            c = (c & 1) ? (c >> 1) ^ 0x82F63B78u : c >> 1;
        table[i] = c;
    }
    return table;
}

constexpr auto kTable = make_table();

using Matrix = std::array<std::uint32_t, 32>;

std::uint32_t times(const Matrix& mat, std::uint32_t vec) {
    std::uint32_t sum = 0;
    for (std::size_t i = 0; vec != 0; vec >>= 1, ++i)
        if (vec & 1) sum ^= mat[i];
    return sum;
}

Matrix square(const Matrix& mat) {
    Matrix out{};
    for (std::size_t i = 0; i < 32; ++i)
        out[i] = times(mat, mat[i]);
    return out;
}

}  // namespace

std::uint32_t crc32c(std::span<const std::byte> data, std::uint32_t crc) {
    std::uint32_t c = ~crc;
    for (std::byte b : data)
        c = kTable[(c ^ std::to_integer<std::uint32_t>(b)) & 0xFF] ^ (c >> 8);
    return ~c;
}

std::uint32_t crc32c_combine(std::uint32_t crc_a, std::uint32_t crc_b, std::uint64_t length_b) {
    if (length_b == 0) return crc_a;

    // odd = operator for appending one zero bit; squared up to the zero-byte operators.
    Matrix odd{};
    odd[0] = 0x82F63B78u;
    for (std::size_t i = 1; i < 32; ++i)
        odd[i] = std::uint32_t{1} << (i - 1);
    Matrix even = square(odd);  // two zero bits
    odd = square(even);         // four zero bits

    do {
        even = square(odd);  // first pass: one zero byte
        if (length_b & 1) crc_a = times(even, crc_a);
        length_b >>= 1;
        if (length_b == 0) break;
        odd = square(even);
        if (length_b & 1) crc_a = times(odd, crc_a);
        length_b >>= 1;
    } while (length_b != 0);

    return crc_a ^ crc_b;
}

}  // namespace rmk
