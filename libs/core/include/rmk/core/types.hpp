#pragma once

#include <chrono>
#include <compare>
#include <cstdint>
#include <limits>
#include <map>
#include <optional>
#include <string>
#include <vector>

namespace rmk {

using Clock = std::chrono::system_clock;

// Assigned when an Object version is committed (ADR 0008). Unique and strictly increasing
// per Object; non-decreasing in commit order across the whole cluster. 0 means "no object".
using Generation = std::uint64_t;
inline constexpr Generation kNoObject = 0;

using ChunkId = std::uint64_t;
using UploadId = std::uint64_t;
using NodeId = std::string;

// ---------------------------------------------------------------------------------------------
// Names

struct BucketRef {
    std::string tenant;
    std::string bucket;
    auto operator<=>(const BucketRef&) const = default;
};

struct ObjectRef {
    BucketRef bucket;
    std::string key;
    auto operator<=>(const ObjectRef&) const = default;
};

// ---------------------------------------------------------------------------------------------
// Tenants and buckets

struct Quota {
    std::uint64_t max_bytes = std::numeric_limits<std::uint64_t>::max();
    std::uint64_t max_objects = std::numeric_limits<std::uint64_t>::max();
    bool operator==(const Quota&) const = default;
};

// Soft counters (ADR 0010): plaintext bytes and object versions, delete markers excluded.
struct Usage {
    std::uint64_t bytes = 0;
    std::uint64_t objects = 0;
    bool operator==(const Usage&) const = default;
};

struct TenantInfo {
    std::string name;
    Quota quota;
    Usage usage;
};

struct BucketConfig {
    bool versioning = false;
    bool operator==(const BucketConfig&) const = default;
};

struct BucketInfo {
    BucketRef ref;
    BucketConfig config;
};

// ---------------------------------------------------------------------------------------------
// Objects

// A contiguous byte range inside one Chunk. Offsets are chunk-file offsets (header included).
struct Extent {
    ChunkId chunk_id = 0;
    std::uint64_t offset = 0;
    std::uint64_t length = 0;
    std::uint32_t crc32c = 0;  // of the stored (encrypted) bytes
    bool operator==(const Extent&) const = default;
};

// One uploaded piece of an Object (ADR 0013). Stored as one Extent holding the
// AES-256-GCM ciphertext of the plaintext followed by its 16-byte tag (ADR 0016).
struct Part {
    std::uint32_t number = 0;    // 1-based
    std::uint64_t size = 0;      // plaintext bytes
    std::uint32_t checksum = 0;  // crc32c of plaintext
    Extent extent;
    bool operator==(const Part&) const = default;
};

struct Encryption {
    std::uint32_t master_key_id = 0;
    std::string wrapped_data_key;  // Data Key sealed by that Master Key
    bool operator==(const Encryption&) const = default;
};

struct ObjectMeta {
    ObjectRef ref;
    Generation generation = kNoObject;
    bool delete_marker = false;
    std::uint64_t size = 0;      // plaintext bytes, sum of parts
    std::uint32_t checksum = 0;  // crc32c of the whole plaintext
    std::vector<Part> parts;     // ascending part number, contiguous from 1
    Encryption encryption;
    Clock::time_point created;
};

// Conditional write. if_generation_match == kNoObject means "only if it does not exist".
struct Precondition {
    std::optional<Generation> if_generation_match;
};

struct PendingUpload {
    UploadId id = 0;
    ObjectRef target;
    std::map<std::uint32_t, Part> parts;
    Encryption encryption;
    Clock::time_point deadline;
};

struct ListPage {
    std::vector<ObjectMeta> objects;
    std::optional<std::string> next_start_after;
};

// A Part of some committed Object version that lives in a given Chunk.
struct ExtentRef {
    ObjectRef ref;
    Generation generation = kNoObject;
    std::uint32_t part_number = 0;
    Extent extent;
};

struct GarbageEntry {
    std::uint64_t id = 0;
    Extent extent;
};

struct VersionScanPage {
    std::vector<ObjectMeta> versions;
    std::optional<std::string> next_cursor;  // opaque; absent at the end
};

// ---------------------------------------------------------------------------------------------
// Cluster configuration (ADR 0015)

struct ClusterConfig {
    std::string min_sdk_version;  // empty: no minimum (ADR 0015)
    bool operator==(const ClusterConfig&) const = default;
};

}  // namespace rmk
