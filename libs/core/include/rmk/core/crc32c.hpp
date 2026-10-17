#pragma once

#include <cstddef>
#include <cstdint>
#include <span>

namespace rmk {

// CRC-32C (Castagnoli). Pass a previous result as `crc` to extend it over more data.
std::uint32_t crc32c(std::span<const std::byte> data, std::uint32_t crc = 0);

// crc32c(A || B) from crc32c(A), crc32c(B) and the length of B, without the data.
// Lets an Object's checksum be derived from the checksums of its Parts.
std::uint32_t crc32c_combine(std::uint32_t crc_a, std::uint32_t crc_b, std::uint64_t length_b);

}  // namespace rmk
