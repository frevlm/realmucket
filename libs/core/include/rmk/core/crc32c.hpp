#pragma once

#include <cstddef>
#include <cstdint>
#include <span>

namespace rmk {

// CRC-32C (Castagnoli). Pass a previous result as `crc` to extend it over more data.
// Uses the CPU's CRC32C instructions where there are any (ARMv8 CRC32, x86 SSE4.2), otherwise the
// table-driven code; both give the same answer.
std::uint32_t crc32c(std::span<const std::byte> data, std::uint32_t crc = 0);

// The table-driven implementation, available everywhere. For tests and as the reference.
std::uint32_t crc32c_portable(std::span<const std::byte> data, std::uint32_t crc = 0);

// Whether crc32c() runs on CPU instructions on this machine.
bool crc32c_accelerated();

// crc32c(A || B) from crc32c(A), crc32c(B) and the length of B, without the data.
// Lets an Object's checksum be derived from the checksums of its Parts.
std::uint32_t crc32c_combine(std::uint32_t crc_a, std::uint32_t crc_b, std::uint64_t length_b);

}  // namespace rmk
