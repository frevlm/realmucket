#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

#include "rmk/core/error.hpp"

namespace rmk::ec {

// Systematic Reed-Solomon over GF(2^8) with a Cauchy generator matrix:
// any `data_units` of the `data_units + parity_units` shards recover all of them.
class Codec {
public:
    // invalid_argument unless 1 <= data_units, 0 <= parity_units, data_units + parity_units <= 255.
    static Result<Codec> create(std::uint8_t data_units, std::uint8_t parity_units);

    std::uint8_t data_units() const { return k_; }
    std::uint8_t parity_units() const { return m_; }

    // All shards have the same length. data.size() == k, parity.size() == m.
    void encode(std::span<const std::span<const std::byte>> data,
                std::span<const std::span<std::byte>> parity) const;

    // shards.size() == k + m, all the same length; present[i] says whether shards[i] holds data.
    // Fills every missing shard. invalid_argument if fewer than k are present.
    Status reconstruct(std::span<const std::span<std::byte>> shards,
                       std::span<const bool> present) const;

private:
    Codec(std::uint8_t k, std::uint8_t m, std::vector<std::uint8_t> matrix);
    std::uint8_t k_;
    std::uint8_t m_;
    std::vector<std::uint8_t> matrix_;  // (k + m) x k, top k rows identity
};

// Layout of an erasure-coded Chunk. The logical Chunk (the replica file, header included,
// `logical_size` bytes) is cut into k contiguous shards of shard_length() bytes each, the last
// one zero-padded. In a shard file, shard byte p sits at file offset kChunkHeaderSize + p.
std::uint64_t shard_length(std::uint64_t logical_size, std::uint8_t data_units);

struct ShardPiece {
    std::uint8_t shard = 0;
    std::uint64_t shard_offset = 0;    // within the shard's data
    std::uint64_t length = 0;
    std::uint64_t logical_offset = 0;  // where the piece starts in the logical Chunk
};

// Pieces covering logical [offset, offset + length), in order.
std::vector<ShardPiece> map_range(std::uint64_t logical_size, std::uint8_t data_units,
                                  std::uint64_t offset, std::uint64_t length);

}  // namespace rmk::ec
