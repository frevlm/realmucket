#pragma once

#include <chrono>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "rmk/core/extent_io.hpp"
#include "rmk/core/config_store.hpp"
#include "rmk/core/meta_store.hpp"

namespace rmk {

// In-memory stores for tests and rmk-dev. Each is thread-safe behind a single mutex.

class MemoryMetaStore final : public MetaStore {
public:
    explicit MemoryMetaStore(std::chrono::seconds pending_upload_retention);

    Status create_tenant(std::string_view name, const Quota& quota) override;
    Result<TenantInfo> get_tenant(std::string_view name) override;
    Result<std::vector<std::string>> list_tenants() override;
    Status set_quota(std::string_view name, const Quota& quota) override;

    Status create_bucket(const BucketRef& bucket, const BucketConfig& config) override;
    Status delete_bucket(const BucketRef& bucket) override;
    Result<BucketInfo> get_bucket(const BucketRef& bucket) override;
    Result<std::vector<BucketInfo>> list_buckets(std::string_view tenant) override;
    Status set_bucket_config(const BucketRef& bucket, const BucketConfig& config) override;

    Result<UploadId> begin_upload(const ObjectRef& target, const Encryption& encryption,
                                  Clock::time_point now) override;
    Status put_part(UploadId id, const Part& part, Clock::time_point now) override;
    Result<PendingUpload> get_upload(UploadId id, Clock::time_point now) override;
    Status abort_upload(UploadId id) override;
    Result<Generation> commit_upload(UploadId id, const Precondition& precondition,
                                     Clock::time_point now) override;

    Result<ObjectMeta> head(const ObjectRef& ref) override;
    Result<ObjectMeta> head_version(const ObjectRef& ref, Generation generation) override;
    Result<std::vector<ObjectMeta>> list_versions(const ObjectRef& ref) override;
    Result<ListPage> list(const BucketRef& bucket, std::string_view prefix,
                          std::optional<std::string_view> start_after, std::size_t limit) override;

    Result<Generation> remove(const ObjectRef& ref, const Precondition& precondition,
                              Clock::time_point now) override;
    Status remove_version(const ObjectRef& ref, Generation generation) override;

    Result<std::size_t> reclaim_expired(Clock::time_point now, std::size_t limit) override;
    Result<std::vector<GarbageEntry>> peek_garbage(std::size_t limit) override;
    Status ack_garbage(const std::vector<std::uint64_t>& ids) override;
    Result<std::vector<ExtentRef>> extents_in_chunk(ChunkId chunk, std::size_t limit) override;
    Result<bool> has_pending_parts(ChunkId chunk) override;
    Status relocate_part(const ObjectRef& ref, Generation generation, std::uint32_t part_number,
                         const Extent& from, const Extent& to) override;
    Result<VersionScanPage> scan_versions(std::optional<std::string_view> cursor,
                                          std::size_t limit) override;
    Status rewrap(const ObjectRef& ref, Generation generation, const Encryption& from,
                  const Encryption& to) override;

private:
    using Versions = std::map<Generation, ObjectMeta>;

    // Callers hold mutex_.
    void add_garbage_locked(const Extent& extent);
    void release_version_locked(const ObjectMeta& version);

    std::mutex mutex_;
    std::chrono::seconds retention_;
    std::map<std::string, TenantInfo, std::less<>> tenants_;
    std::map<BucketRef, BucketInfo> buckets_;
    std::map<ObjectRef, Versions> objects_;
    std::map<UploadId, PendingUpload> uploads_;
    std::map<std::uint64_t, GarbageEntry> garbage_;
    Generation last_generation_ = 0;
    UploadId last_upload_ = 0;
    std::uint64_t last_garbage_ = 0;
};

class MemoryConfigStore final : public ConfigStore {
public:
    Result<ClusterConfig> get_cluster_config() override;
    Status set_cluster_config(const ClusterConfig& config) override;

private:
    std::mutex mutex_;
    ClusterConfig config_;
};

}  // namespace rmk
