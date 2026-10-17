#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "rmk/core/error.hpp"
#include "rmk/core/types.hpp"

namespace rmk {

inline constexpr std::uint32_t kMaxParts = 4096;

// Tenants, Buckets and the Object index (ADR 0002, 0007, 0008, 0010, 0013, 0016).
// The only door to that part of the Metadata Store; nothing backend-specific leaks through.
// Every method is one atomic transaction. All implementations must pass tests/store_contract.
class MetaStore {
public:
    virtual ~MetaStore() = default;

    // --- Tenants. Usage counters are maintained by commits and deletions.
    virtual Status create_tenant(std::string_view name, const Quota& quota) = 0;
    virtual Result<TenantInfo> get_tenant(std::string_view name) = 0;
    virtual Result<std::vector<std::string>> list_tenants() = 0;
    virtual Status set_quota(std::string_view name, const Quota& quota) = 0;

    // --- Buckets. The Tenant must exist. Deleting needs no Object versions and no Pending Uploads.
    virtual Status create_bucket(const BucketRef& bucket, const BucketConfig& config) = 0;
    virtual Status delete_bucket(const BucketRef& bucket) = 0;
    virtual Result<BucketInfo> get_bucket(const BucketRef& bucket) = 0;
    virtual Result<std::vector<BucketInfo>> list_buckets(std::string_view tenant) = 0;
    virtual Status set_bucket_config(const BucketRef& bucket, const BucketConfig& config) = 0;

    // --- Writes (ADR 0007, 0013). A Pending Upload expires when no Part was added for the
    // retention period. Re-putting a Part number replaces it; the old Extent becomes garbage.
    // Part numbers run from 1 to kMaxParts (invalid_argument otherwise), which keeps a commit
    // inside one metadata transaction.
    virtual Result<UploadId> begin_upload(const ObjectRef& target, const Encryption& encryption,
                                          Clock::time_point now) = 0;
    virtual Status put_part(UploadId id, const Part& part, Clock::time_point now) = 0;
    virtual Result<PendingUpload> get_upload(UploadId id, Clock::time_point now) = 0;
    virtual Status abort_upload(UploadId id) = 0;
    // Parts must be numbered 1..N without gaps. Checks the Precondition against the latest
    // version (a delete marker counts as absent) and the Tenant Quota (soft, ADR 0010).
    // With Versioning off the replaced version's Extents become garbage.
    virtual Result<Generation> commit_upload(UploadId id, const Precondition& precondition,
                                             Clock::time_point now) = 0;

    // --- Reads. head/list hide delete markers and report them as not_found.
    // list() entries carry no Parts and no Encryption: use head() for those.
    virtual Result<ObjectMeta> head(const ObjectRef& ref) = 0;
    virtual Result<ObjectMeta> head_version(const ObjectRef& ref, Generation generation) = 0;
    virtual Result<std::vector<ObjectMeta>> list_versions(const ObjectRef& ref) = 0;  // newest first
    virtual Result<ListPage> list(const BucketRef& bucket, std::string_view prefix,
                                  std::optional<std::string_view> start_after,
                                  std::size_t limit) = 0;

    // --- Deletes (ADR 0016). With Versioning on, remove() adds a delete marker and returns its
    // Generation; with it off, the version is gone (Data Key with it) and kNoObject is returned.
    // remove_version() always deletes that one version permanently.
    virtual Result<Generation> remove(const ObjectRef& ref, const Precondition& precondition,
                                      Clock::time_point now) = 0;
    virtual Status remove_version(const ObjectRef& ref, Generation generation) = 0;

    // --- Maintenance, driven by the Controller.
    virtual Result<std::size_t> reclaim_expired(Clock::time_point now, std::size_t limit) = 0;
    // Garbage is handed out in two steps so a crash in between can only double-count.
    virtual Result<std::vector<GarbageEntry>> peek_garbage(std::size_t limit) = 0;
    virtual Status ack_garbage(const std::vector<std::uint64_t>& ids) = 0;
    // Live Parts of committed versions stored in a Chunk (for Compaction).
    virtual Result<std::vector<ExtentRef>> extents_in_chunk(ChunkId chunk, std::size_t limit) = 0;
    // Whether any Pending Upload has a Part stored in the Chunk. Compaction must leave such a
    // Chunk alone until those uploads are committed, aborted or reclaimed.
    virtual Result<bool> has_pending_parts(ChunkId chunk) = 0;
    // Moves one Part to a new Extent if it still points at `from`; aborted otherwise.
    // The old Extent is not reported as garbage: its whole Chunk is released by Compaction.
    virtual Status relocate_part(const ObjectRef& ref, Generation generation,
                                 std::uint32_t part_number, const Extent& from,
                                 const Extent& to) = 0;
    // Walks every version of every Object, in a stable order (for Master Key rotation).
    virtual Result<VersionScanPage> scan_versions(std::optional<std::string_view> cursor,
                                                  std::size_t limit) = 0;
    // Replaces the wrapped Data Key if it still equals `from`; aborted otherwise.
    virtual Status rewrap(const ObjectRef& ref, Generation generation, const Encryption& from,
                          const Encryption& to) = 0;
};

}  // namespace rmk
