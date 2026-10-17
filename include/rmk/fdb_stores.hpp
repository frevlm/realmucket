#pragma once

#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "rmk/cluster_store.hpp"
#include "rmk/config_store.hpp"
#include "rmk/meta_store.hpp"

struct FDB_database;

namespace rmk::fdb {

// One FDBDatabase handle. The FDB network thread is a process-wide singleton, started by the
// first open_database() and stopped at process exit. Key layout: docs/fdb-layout.md.
class Database {
public:
    ~Database();
    Database(const Database&) = delete;
    Database& operator=(const Database&) = delete;

    FDB_database* handle() const { return db_; }

    // Fault injection for tests: the next `count` successful commits are reported to the retry
    // loop as commit_unknown_result, so the retry runs against an already applied transaction.
    void simulate_unknown_commit_results(int count) { unknown_results_ = count; }
    bool take_simulated_unknown_result();

private:
    friend Result<std::shared_ptr<Database>> open_database(const std::string& cluster_file);
    explicit Database(FDB_database* db) : db_(db) {}

    FDB_database* db_;
    std::atomic<int> unknown_results_{0};
};

Result<std::shared_ptr<Database>> open_database(const std::string& cluster_file);

// Every key-value pair under `key_prefix`, in key order (tests, debugging).
Result<std::vector<std::pair<std::string, std::string>>> dump_prefix(Database& db,
                                                                     std::string_view key_prefix);
// Removes everything under `key_prefix` (one range clear).
Status clear_prefix(Database& db, std::string_view key_prefix);

class FdbMetaStore final : public MetaStore {
public:
    FdbMetaStore(std::shared_ptr<Database> db, std::string key_prefix,
                 std::chrono::seconds pending_upload_retention);

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
    std::shared_ptr<Database> db_;
    std::string prefix_;  // encoded key_prefix
    std::chrono::seconds retention_;
};

class FdbConfigStore final : public ConfigStore {
public:
    FdbConfigStore(std::shared_ptr<Database> db, std::string key_prefix);

    Result<ClusterConfig> get_cluster_config() override;
    Status set_cluster_config(const ClusterConfig& config) override;

private:
    std::shared_ptr<Database> db_;
    std::string prefix_;
};

class FdbClusterStore final : public ClusterStore {
public:
    FdbClusterStore(std::shared_ptr<Database> db, std::string key_prefix);

    Status heartbeat(const NodeInfo& node, Clock::time_point now) override;
    Result<std::vector<NodeInfo>> list_nodes() override;
    Status remove_node(std::string_view id, const std::optional<Fence>& fence = std::nullopt) override;

    Result<ChunkId> allocate_chunk_id() override;
    Status put_chunk(const ChunkInfo& chunk, const std::optional<Fence>& fence = std::nullopt) override;
    Result<ChunkInfo> get_chunk(ChunkId id) override;
    Result<std::vector<ChunkInfo>> list_chunks(std::optional<ChunkState> state,
                                               std::optional<ChunkId> after,
                                               std::size_t limit) override;
    Result<ChunkInfo> update_chunk(const ChunkInfo& next,
                                   const std::optional<Fence>& fence = std::nullopt) override;
    Status delete_chunk(ChunkId id, const std::optional<Fence>& fence = std::nullopt) override;
    Result<ChunkInfo> claim_open_chunk(const StoragePolicy& policy, std::string_view writer,
                                       std::uint64_t min_free, Clock::time_point lease_until,
                                       Clock::time_point now) override;
    Status add_garbage(ChunkId id, std::uint64_t bytes) override;

    Result<std::uint64_t> try_lead(std::string_view holder, std::chrono::seconds ttl,
                                   Clock::time_point now) override;

private:
    std::shared_ptr<Database> db_;
    std::string prefix_;
};

}  // namespace rmk::fdb
