#pragma once

// realmucket Client SDK (ADR 0003). The only header an application includes.
// Talks to the Gateway over gRPC; contains no storage logic.

#include <chrono>
#include <cstdint>
#include <expected>
#include <filesystem>
#include <functional>
#include <iosfwd>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace rmk::client {

inline constexpr const char* kVersion = "1.0.0";

enum class ErrorCode {
    not_found,
    already_exists,
    precondition_failed,
    not_empty,
    invalid_argument,
    expired,
    no_space,
    quota_exceeded,
    permission_denied,
    unauthenticated,
    unavailable,
    aborted,
    corruption,
    io_error,
    unknown,
};

struct Error {
    ErrorCode code = ErrorCode::unknown;
    std::string message;
};

template <class T>
using Result = std::expected<T, Error>;
using Status = std::expected<void, Error>;

struct Options {
    std::string endpoint;  // host:port of the Gateway
    std::filesystem::path ca_file;  // trust root for the Gateway's TLS certificate; empty = system
    bool insecure = false;          // plaintext; development only
    std::uint64_t multipart_threshold = std::uint64_t{64} << 20;
    std::uint64_t part_size = std::uint64_t{64} << 20;
    unsigned parallel_parts = 4;
    std::chrono::milliseconds timeout{std::chrono::minutes{5}};
};

struct Location {
    std::string tenant;  // required
    std::string bucket;
    std::string key;
};

// Write only if the current Generation equals this; 0 means "only if it does not exist".
struct Precondition {
    std::optional<std::uint64_t> if_generation_match;
    static Precondition if_not_exists() { return {0}; }
    static Precondition if_generation(std::uint64_t g) { return {g}; }
};

struct ObjectInfo {
    Location location;
    std::uint64_t generation = 0;
    std::uint64_t size = 0;
    std::uint32_t checksum = 0;  // crc32c
    bool delete_marker = false;
    std::chrono::system_clock::time_point created;
};

struct GetOptions {
    std::uint64_t generation = 0;  // 0 = latest
    std::uint64_t offset = 0;
    std::uint64_t length = 0;  // 0 = to the end
};

struct ListOptions {
    std::string prefix;
    std::string start_after;
    std::uint32_t limit = 0;  // 0 = server default
};

struct ListResult {
    std::vector<ObjectInfo> objects;
    std::string next_start_after;  // empty at the end
};

struct BucketConfig {
    bool versioning = false;
};

struct BucketInfo {
    std::string tenant;
    std::string bucket;
    BucketConfig config;
};

struct Quota {
    std::uint64_t max_bytes = 0;  // 0 = unlimited
    std::uint64_t max_objects = 0;
};

struct TenantInfo {
    std::string name;
    Quota quota;
    std::uint64_t used_bytes = 0;
    std::uint64_t used_objects = 0;
};

// Called after each Part of a multipart upload is stored, so a caller can persist
// upload_id and resume later with put_file(..., resume_upload_id).
using ProgressCallback = std::function<void(std::uint64_t upload_id, std::uint32_t part_number,
                                            std::uint64_t bytes_done)>;

class Client {
public:
    static Result<Client> connect(Options options);

    Client(Client&&) noexcept;
    Client& operator=(Client&&) noexcept;
    ~Client();

    // ----- Objects. Content larger than multipart_threshold is uploaded in Parts
    // automatically, parallel_parts at a time.
    Result<ObjectInfo> put(const Location& where, std::istream& content, Precondition precondition = {});
    Result<ObjectInfo> put(const Location& where, std::string_view content, Precondition precondition = {});
    Result<ObjectInfo> put_file(const Location& where, const std::filesystem::path& file,
                                Precondition precondition = {},
                                std::optional<std::uint64_t> resume_upload_id = std::nullopt,
                                ProgressCallback progress = {});
    Result<ObjectInfo> get(const Location& where, std::ostream& out, const GetOptions& options = {});
    Result<std::string> get(const Location& where, const GetOptions& options = {});
    Result<ObjectInfo> head(const Location& where, std::uint64_t generation = 0);
    // Returns the delete marker's Generation when Versioning kept the old content, else 0.
    Result<std::uint64_t> remove(const Location& where, Precondition precondition = {});
    Status remove_version(const Location& where, std::uint64_t generation);
    Result<ListResult> list(const std::string& tenant, const std::string& bucket,
                            const ListOptions& options = {});
    Result<std::vector<ObjectInfo>> list_versions(const Location& where);
    Status abort_upload(std::uint64_t upload_id);

    // ----- Buckets
    Result<BucketInfo> create_bucket(const std::string& tenant, const std::string& bucket,
                                     const BucketConfig& config = {});
    Status delete_bucket(const std::string& tenant, const std::string& bucket);
    Result<BucketInfo> get_bucket(const std::string& tenant, const std::string& bucket);
    Result<std::vector<BucketInfo>> list_buckets(const std::string& tenant);
    Result<BucketInfo> set_bucket_config(const std::string& tenant, const std::string& bucket,
                                         const BucketConfig& config);

    // ----- Administration
    Result<TenantInfo> create_tenant(const std::string& name, const Quota& quota = {});
    Result<TenantInfo> get_tenant(const std::string& name);
    Result<TenantInfo> set_quota(const std::string& name, const Quota& quota);
    Status set_min_sdk_version(const std::string& version);

private:
    Client() = default;
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace rmk::client
