#include "rmk/core/crc32c.hpp"

#include <array>
#include <cstring>

#if defined(__aarch64__) && (defined(__clang__) || defined(__GNUC__))
#define RMK_CRC32C_ARM 1
#include <arm_acle.h>
#if defined(__linux__)
#include <sys/auxv.h>
#endif
#elif defined(__x86_64__) && (defined(__clang__) || defined(__GNUC__))
#define RMK_CRC32C_X86 1
#include <nmmintrin.h>
#endif

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

std::uint32_t crc32c_portable(std::span<const std::byte> data, std::uint32_t crc) {
    std::uint32_t c = ~crc;
    for (std::byte b : data)
        c = kTable[(c ^ std::to_integer<std::uint32_t>(b)) & 0xFF] ^ (c >> 8);
    return ~c;
}

namespace {

using Impl = std::uint32_t (*)(std::span<const std::byte>, std::uint32_t);

#if defined(RMK_CRC32C_ARM)

#if defined(__clang__)
#define RMK_CRC_TARGET __attribute__((target("crc")))
#else
#define RMK_CRC_TARGET __attribute__((target("+crc")))
#endif

RMK_CRC_TARGET std::uint32_t crc32c_arm(std::span<const std::byte> data, std::uint32_t crc) {
    std::uint32_t c = ~crc;
    const std::byte* p = data.data();
    std::size_t n = data.size();
    for (; n >= 8; n -= 8, p += 8) {
        std::uint64_t word;
        std::memcpy(&word, p, 8);
        c = __crc32cd(c, word);
    }
    for (; n > 0; --n, ++p)
        c = __crc32cb(c, std::to_integer<std::uint8_t>(*p));
    return ~c;
}

bool arm_has_crc() {
#if defined(__APPLE__)
    return true;  // every Apple silicon core has it
#elif defined(__linux__) && defined(HWCAP_CRC32)
    return (getauxval(AT_HWCAP) & HWCAP_CRC32) != 0;
#else
    return false;
#endif
}

Impl pick() { return arm_has_crc() ? crc32c_arm : nullptr; }

#elif defined(RMK_CRC32C_X86)

__attribute__((target("sse4.2"))) std::uint32_t crc32c_x86(std::span<const std::byte> data, std::uint32_t crc) {
    std::uint64_t c = static_cast<std::uint32_t>(~crc);
    const std::byte* p = data.data();
    std::size_t n = data.size();
    for (; n >= 8; n -= 8, p += 8) {
        std::uint64_t word;
        std::memcpy(&word, p, 8);
        c = _mm_crc32_u64(c, word);
    }
    auto c32 = static_cast<std::uint32_t>(c);
    for (; n > 0; --n, ++p)
        c32 = _mm_crc32_u8(c32, std::to_integer<std::uint8_t>(*p));
    return ~c32;
}

Impl pick() { return __builtin_cpu_supports("sse4.2") ? crc32c_x86 : nullptr; }

#else

Impl pick() { return nullptr; }

#endif

Impl chosen() {
    static const Impl impl = pick();
    return impl;
}

}  // namespace

bool crc32c_accelerated() {
    return chosen() != nullptr;
}

std::uint32_t crc32c(std::span<const std::byte> data, std::uint32_t crc) {
    const Impl impl = chosen();
    return impl ? impl(data, crc) : crc32c_portable(data, crc);
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
