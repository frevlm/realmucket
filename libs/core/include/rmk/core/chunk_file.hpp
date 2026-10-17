#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <span>
#include <vector>

#include "rmk/core/error.hpp"
#include "rmk/core/types.hpp"

namespace rmk {

inline constexpr std::size_t kChunkHeaderSize = 64;
inline constexpr std::uint32_t kChunkFormatVersion = 1;

// Self-describing header (ADR 0006). Redundancy is always data_units + parity_units:
// three replicas are simply 1 + 2, so replication and erasure coding share one description.
// A replica file holds the whole Chunk; an erasure-coded shard file holds shard `shard_index`.
struct ChunkHeader {
    std::uint32_t format_version = kChunkFormatVersion;
    ChunkId chunk_id = 0;
    std::uint64_t capacity = 0;  // total file size limit, header included
    std::uint8_t data_units = 1;
    std::uint8_t parity_units = 2;
    std::uint8_t shard_index = 0;
    bool operator==(const ChunkHeader&) const = default;
};

// One Chunk (or shard) on local disk, written front to back and read back by range.
// Writes do not reach the disk by themselves; call sync() before the data is relied upon.
class ChunkFile {
public:
    static Result<ChunkFile> create(const std::filesystem::path& path, const ChunkHeader& header);
    static Result<ChunkFile> open(const std::filesystem::path& path);

    ChunkFile(ChunkFile&& other) noexcept;
    ChunkFile& operator=(ChunkFile&& other) noexcept;
    ChunkFile(const ChunkFile&) = delete;
    ChunkFile& operator=(const ChunkFile&) = delete;
    ~ChunkFile();

    Result<Extent> append(std::span<const std::byte> data);
    // Writes at `offset`, which may not lie before the current end; any gap stays a hole.
    // Lets every replica place a write at the offset the single writer chose.
    Result<Extent> write_at(std::uint64_t offset, std::span<const std::byte> data);
    // Verifies the Extent's crc32c.
    Result<std::vector<std::byte>> read(const Extent& extent) const;
    // Raw bytes, no checksum (shard ranges, whole-chunk copies).
    Result<std::vector<std::byte>> read_range(std::uint64_t offset, std::uint64_t length) const;
    Status sync();

    const ChunkHeader& header() const { return header_; }
    std::uint64_t size() const { return size_; }

private:
    ChunkFile(int fd, ChunkHeader header, std::uint64_t size);

    int fd_ = -1;
    ChunkHeader header_;
    std::uint64_t size_ = 0;
};

}  // namespace rmk
