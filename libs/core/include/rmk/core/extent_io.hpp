#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

#include "rmk/core/error.hpp"
#include "rmk/core/types.hpp"

namespace rmk {

// What the Gateway needs from byte storage, and nothing else: put bytes somewhere durable and
// get an Extent back, read an Extent, and report bytes that no Object will reference.

class ExtentWriter {
public:
    virtual ~ExtentWriter() = default;
    // Durable (fsynced) when it returns.
    virtual Result<Extent> write(std::span<const std::byte> bytes) = 0;
};

class ExtentReader {
public:
    virtual ~ExtentReader() = default;
    // Verifies Extent.crc32c.
    virtual Result<std::vector<std::byte>> read(const Extent& extent) = 0;
};

class GarbageSink {
public:
    virtual ~GarbageSink() = default;
    virtual Status add_garbage(ChunkId id, std::uint64_t bytes) = 0;  // atomic add
};

}  // namespace rmk
