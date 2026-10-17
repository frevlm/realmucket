#include "rmk/gateway/gateway.hpp"

#include <algorithm>
#include <condition_variable>
#include <cstdio>
#include <ctime>
#include <limits>
#include <mutex>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "rmk/core/crc32c.hpp"
#include "rmk/http/http.hpp"
#include "rmk/core/names.hpp"
#include "rmk/core/version.hpp"
#include "rmk/v1/storage.grpc.pb.h"

namespace rmk::gateway {
namespace {

namespace pb = rmk::v1;

constexpr std::size_t kMessageBytes = std::size_t{1} << 20;
constexpr int kWriteAttempts = 3;
constexpr std::string_view kNoSdkVersion = "0.0.0";

void log_line(const char* level, const std::string& message) {
    std::time_t now = Clock::to_time_t(Clock::now());
    std::tm utc{};
    gmtime_r(&now, &utc);
    char stamp[32];
    std::strftime(stamp, sizeof stamp, "%Y-%m-%dT%H:%M:%SZ", &utc);
    std::fprintf(stderr, "%s %s gateway %s\n", stamp, level, message.c_str());
}

const char* code_name(grpc::StatusCode code) {
    switch (code) {
        case grpc::StatusCode::OK: return "OK";
        case grpc::StatusCode::CANCELLED: return "CANCELLED";
        case grpc::StatusCode::UNKNOWN: return "UNKNOWN";
        case grpc::StatusCode::INVALID_ARGUMENT: return "INVALID_ARGUMENT";
        case grpc::StatusCode::DEADLINE_EXCEEDED: return "DEADLINE_EXCEEDED";
        case grpc::StatusCode::NOT_FOUND: return "NOT_FOUND";
        case grpc::StatusCode::ALREADY_EXISTS: return "ALREADY_EXISTS";
        case grpc::StatusCode::PERMISSION_DENIED: return "PERMISSION_DENIED";
        case grpc::StatusCode::RESOURCE_EXHAUSTED: return "RESOURCE_EXHAUSTED";
        case grpc::StatusCode::FAILED_PRECONDITION: return "FAILED_PRECONDITION";
        case grpc::StatusCode::ABORTED: return "ABORTED";
        case grpc::StatusCode::OUT_OF_RANGE: return "OUT_OF_RANGE";
        case grpc::StatusCode::UNIMPLEMENTED: return "UNIMPLEMENTED";
        case grpc::StatusCode::INTERNAL: return "INTERNAL";
        case grpc::StatusCode::UNAVAILABLE: return "UNAVAILABLE";
        case grpc::StatusCode::DATA_LOSS: return "DATA_LOSS";
        case grpc::StatusCode::UNAUTHENTICATED: return "UNAUTHENTICATED";
        default: return "UNKNOWN";
    }
}

// Failures that point at the cluster rather than at the request.
bool server_fault(Errc code) {
    return code == Errc::unavailable || code == Errc::corruption || code == Errc::io_error;
}

std::span<const std::byte> as_bytes(const std::string& s) {
    return std::as_bytes(std::span(s.data(), s.size()));
}

std::string metadata(const grpc::ServerContext& ctx, std::string_view name) {
    const auto& md = ctx.client_metadata();
    auto it = md.find(grpc::string_ref(name.data(), name.size()));
    if (it == md.end()) return {};
    return std::string(it->second.data(), it->second.size());
}

// ----- Conversions between the store types and the wire.

Quota to_quota(const pb::Quota& q) {
    constexpr auto kUnlimited = std::numeric_limits<std::uint64_t>::max();
    return Quota{q.max_bytes() == 0 ? kUnlimited : q.max_bytes(),
                 q.max_objects() == 0 ? kUnlimited : q.max_objects()};
}

void fill(pb::Quota* out, const Quota& q) {
    constexpr auto kUnlimited = std::numeric_limits<std::uint64_t>::max();
    out->set_max_bytes(q.max_bytes == kUnlimited ? 0 : q.max_bytes);
    out->set_max_objects(q.max_objects == kUnlimited ? 0 : q.max_objects);
}

void fill(pb::TenantInfo* out, const TenantInfo& t) {
    out->set_name(t.name);
    fill(out->mutable_quota(), t.quota);
    out->set_used_bytes(t.usage.bytes);
    out->set_used_objects(t.usage.objects);
}

Result<BucketConfig> to_config(const pb::BucketConfig& c) {
    BucketConfig config;
    config.versioning = c.versioning();
    return config;
}

void fill(pb::BucketInfo* out, const BucketInfo& b) {
    out->set_tenant(b.ref.tenant);
    out->set_bucket(b.ref.bucket);
    out->mutable_config()->set_versioning(b.config.versioning);
}

void fill(pb::ObjectInfo* out, const ObjectMeta& m) {
    out->set_tenant(m.ref.bucket.tenant);
    out->set_bucket(m.ref.bucket.bucket);
    out->set_key(m.ref.key);
    out->set_generation(m.generation);
    out->set_size(m.size);
    out->set_checksum(m.checksum);
    out->set_delete_marker(m.delete_marker);
    out->set_created_unix_ms(
        std::chrono::duration_cast<std::chrono::milliseconds>(m.created.time_since_epoch()).count());
}

void fill(pb::PartInfo* out, const Part& p) {
    out->set_part_number(p.number);
    out->set_size(p.size);
    out->set_checksum(p.checksum);
}

Precondition to_precondition(const pb::Precondition& p) {
    Precondition out;
    if (p.has_if_generation_match()) out.if_generation_match = p.if_generation_match();
    return out;
}

// Content bytes held in memory across all requests (§6 safety rule 5).
class ByteBudget {
public:
    explicit ByteBudget(std::uint64_t total) : total_(total), free_(total) {}

    // Bytes held by one request, returned when it ends.
    class Reservation {
    public:
        explicit Reservation(ByteBudget& budget) : budget_(budget) {}
        ~Reservation() { reset(); }
        Reservation(const Reservation&) = delete;
        Reservation& operator=(const Reservation&) = delete;

        // Holds n more bytes (never more than the whole budget); unavailable after `wait`.
        Status grow(std::uint64_t n, std::chrono::milliseconds wait) {
            n = std::min(n, budget_.total_ - held_);
            std::unique_lock lock(budget_.mu_);
            if (!budget_.cv_.wait_for(lock, wait, [&] { return budget_.free_ >= n; }))
                return fail(Errc::unavailable, "the gateway is buffering too much data; retry later");
            budget_.free_ -= n;
            held_ += n;
            return {};
        }

        void reset() {
            {
                std::lock_guard lock(budget_.mu_);
                budget_.free_ += held_;
            }
            held_ = 0;
            budget_.cv_.notify_all();
        }

    private:
        ByteBudget& budget_;
        std::uint64_t held_ = 0;
    };

private:
    const std::uint64_t total_;
    std::uint64_t free_;
    std::mutex mu_;
    std::condition_variable cv_;
};

const Extent* extent_of(const ObjectMeta& version, std::uint32_t number) {
    auto it = std::ranges::find(version.parts, number, &Part::number);
    return it == version.parts.end() ? nullptr : &it->extent;
}

}  // namespace

struct Gateway::Impl final : pb::Storage::Service {
    Options options;
    Dependencies deps;
    ByteBudget budget;
    metrics::Counter& bytes_in;
    metrics::Counter& bytes_out;

    std::mutex config_mu;
    std::optional<std::pair<ClusterConfig, Clock::time_point>> cached_config;

    Impl(Options o, Dependencies d)
        : options(std::move(o)),
          deps(std::move(d)),
          budget(options.max_buffered_bytes),
          bytes_in(metrics::global().counter("rmk_gateway_bytes_in_total",
                                             "Object content bytes received by the Gateway")),
          bytes_out(metrics::global().counter("rmk_gateway_bytes_out_total",
                                              "Object content bytes sent by the Gateway")) {}

    MetaStore& meta() { return *deps.stores.meta; }
    ConfigStore& config_store() { return *deps.stores.config; }

    // ----- Request pipeline: check the SDK version, run, count.

    template <class Body>
    grpc::Status run(grpc::ServerContext* ctx, const char* method, Body&& body) {
        std::string sdk = metadata(*ctx, "x-rmk-sdk-version");
        if (sdk.empty()) sdk = kNoSdkVersion;
        Status result = [&]() -> Status {
            if (auto ok = check_sdk(sdk); !ok) return ok;
            return body();
        }();

        grpc::Status status = result ? grpc::Status::OK : rpc::to_status(result.error());
        if (!result && server_fault(result.error().code))
            log_line("error", std::string(method) + ": " + errc_name(result.error().code) + ": " +
                                  result.error().message);
        // Callers choose label values; an unparsable version collapses to one series.
        std::string sdk_label = compare_versions(sdk, sdk) ? sdk : "invalid";
        metrics::global()
            .counter("rmk_gateway_requests_total", "Requests served by the Gateway",
                     {{"method", method}, {"code", code_name(status.error_code())}, {"sdk_version", sdk_label}})
            .inc();
        return status;
    }

    Result<ClusterConfig> cluster_config() {
        auto now = Clock::now();
        std::lock_guard lock(config_mu);
        if (cached_config && now - cached_config->second < options.cluster_config_cache)
            return cached_config->first;
        auto config = config_store().get_cluster_config();
        if (!config) return config;
        cached_config = {*config, now};
        return config;
    }

    Status check_sdk(const std::string& sdk) {
        auto config = cluster_config();
        if (!config) return std::unexpected(config.error());
        const std::string& min = config->min_sdk_version;
        if (min.empty()) return {};
        auto order = compare_versions(sdk, min);
        if (!order)
            return fail(Errc::invalid_argument, "malformed x-rmk-sdk-version '" + sdk + "'; upgrade the Client SDK to " +
                                                    min + " or later");
        if (*order < 0)
            return fail(Errc::precondition_failed, "Client SDK " + sdk + " is older than the minimum supported version " +
                                                       min + "; upgrade the Client SDK to " + min + " or later");
        return {};
    }

    // ----- Names: every request names its tenant.

    Result<std::string> tenant_of(const std::string& tenant) {
        if (tenant.empty()) return fail(Errc::invalid_argument, "tenant is required");
        if (auto valid = validate_tenant_name(tenant); !valid) return std::unexpected(valid.error());
        return tenant;
    }

    Result<BucketRef> bucket_ref(const std::string& tenant, const std::string& bucket) {
        auto t = tenant_of(tenant);
        if (!t) return std::unexpected(t.error());
        if (auto valid = validate_bucket_name(bucket); !valid) return std::unexpected(valid.error());
        return BucketRef{*t, bucket};
    }

    Result<ObjectRef> object_ref(const pb::ObjectLocation& location) {
        auto b = bucket_ref(location.tenant(), location.bucket());
        if (!b) return std::unexpected(b.error());
        if (auto valid = validate_object_key(location.key()); !valid) return std::unexpected(valid.error());
        return ObjectRef{*b, location.key()};
    }

    // The Bucket an object call targets; not_found when it does not exist.
    Result<BucketInfo> existing_bucket(const BucketRef& ref) { return meta().get_bucket(ref); }

    // ----- Data path (docs/IMPLEMENTATION.md §6).

    // Bytes written to a Chunk that no Object will reference (§6 safety rule 9).
    void discard(const Extent& extent) {
        if (auto ok = deps.garbage->add_garbage(extent.chunk_id, extent.length); !ok)
            log_line("error", "cannot report " + std::to_string(extent.length) + " garbage bytes in chunk " +
                                  std::to_string(extent.chunk_id) + ": " + ok.error().message);
    }

    // Encrypts one Part, places it on a Chunk and registers it with the Pending Upload. An Extent
    // is registered only within half the compaction grace of its acknowledgement, so its Chunk
    // cannot have been compacted in between (§6 safety rule 2); a late one is written again.
    Result<Part> store_part(UploadId upload, const crypto::Key& key,
                            std::uint32_t number, std::span<const std::byte> plaintext) {
        const auto sealed = crypto::seal_part(key, number, plaintext);
        const auto deadline = std::chrono::duration_cast<std::chrono::milliseconds>(options.compaction_grace) / 2;
        Part part{number, plaintext.size(), crc32c(plaintext), {}};
        for (int attempt = 0; attempt < kWriteAttempts; ++attempt) {
            auto extent = deps.writer->write(sealed);
            if (!extent) return std::unexpected(extent.error());
            const auto acknowledged = std::chrono::steady_clock::now();
            part.extent = *extent;
            if (std::chrono::steady_clock::now() - acknowledged >= deadline) {
                discard(*extent);
                continue;
            }
            if (auto ok = meta().put_part(upload, part, Clock::now()); !ok) {
                discard(*extent);
                return std::unexpected(ok.error());
            }
            return part;
        }
        return fail(Errc::unavailable, "part " + std::to_string(number) +
                                           " could not be registered within the compaction grace");
    }

    std::uint64_t put_part_size() const { return std::min(options.part_size, options.max_part_bytes); }

    // Reads a client stream of content into Parts of put_part_size().
    Status store_stream(grpc::ServerContext* ctx, grpc::ServerReader<pb::PutObjectRequest>* reader,
                        UploadId upload, const crypto::Key& key) {
        const std::uint64_t part_size = put_part_size();
        ByteBudget::Reservation held(budget);
        std::vector<std::byte> buffer;
        std::uint32_t number = 0;
        auto flush = [&]() -> Status {
            if (number == kMaxParts)
                return fail(Errc::invalid_argument, "an object holds at most " + std::to_string(kMaxParts) +
                                                        " parts of " + std::to_string(part_size) + " bytes");
            auto part = store_part(upload, key, ++number, buffer);
            if (!part) return std::unexpected(part.error());
            buffer.clear();
            held.reset();
            return {};
        };
        pb::PutObjectRequest msg;
        while (reader->Read(&msg)) {
            if (!msg.has_data()) return fail(Errc::invalid_argument, "only data may follow the header");
            bytes_in.inc(static_cast<double>(msg.data().size()));
            auto data = as_bytes(msg.data());
            while (!data.empty()) {
                auto take = std::min<std::size_t>(data.size(), part_size - buffer.size());
                if (auto ok = held.grow(take, options.buffer_wait); !ok) return ok;
                buffer.insert(buffer.end(), data.begin(), data.begin() + static_cast<std::ptrdiff_t>(take));
                data = data.subspan(take);
                if (buffer.size() == part_size)
                    if (auto ok = flush(); !ok) return ok;
            }
        }
        if (ctx->IsCancelled()) return fail(Errc::aborted, "the client cancelled the upload");
        if (!buffer.empty()) return flush();
        return {};
    }

    struct UploadAccess {
        PendingUpload upload;
        BucketInfo bucket;
    };

    // A multipart call acts on a Pending Upload and the Bucket it targets.
    Result<UploadAccess> load_upload(UploadId id) {
        auto upload = meta().get_upload(id, Clock::now());
        if (!upload) return std::unexpected(upload.error());
        auto bucket = existing_bucket(upload->target.bucket);
        if (!bucket) return std::unexpected(bucket.error());
        return UploadAccess{std::move(*upload), std::move(*bucket)};
    }

    Status committed(const ObjectRef& ref, Generation generation, pb::PutObjectResponse* resp) {
        auto version = meta().head_version(ref, generation);
        if (!version) return std::unexpected(version.error());
        fill(resp->mutable_object(), *version);
        return {};
    }

    Status send(grpc::ServerWriter<pb::GetObjectResponse>* writer, std::span<const std::byte> bytes) {
        pb::GetObjectResponse msg;
        while (!bytes.empty()) {
            auto n = std::min(bytes.size(), kMessageBytes);
            msg.set_data(bytes.data(), n);
            if (!writer->Write(msg)) return fail(Errc::aborted, "the client went away");
            bytes_out.inc(static_cast<double>(n));
            bytes = bytes.subspan(n);
        }
        return {};
    }

    // ----- Cluster and tenant administration.

    grpc::Status CreateTenant(grpc::ServerContext* ctx, const pb::CreateTenantRequest* req,
                              pb::TenantInfo* resp) override {
        return run(ctx, "CreateTenant", [&]() -> Status {
            if (auto valid = validate_tenant_name(req->name()); !valid) return valid;
            if (auto ok = meta().create_tenant(req->name(), to_quota(req->quota())); !ok) return ok;
            log_line("info", "tenant " + req->name() + " created");
            auto tenant = meta().get_tenant(req->name());
            if (!tenant) return std::unexpected(tenant.error());
            fill(resp, *tenant);
            return {};
        });
    }

    grpc::Status GetTenant(grpc::ServerContext* ctx, const pb::GetTenantRequest* req,
                           pb::TenantInfo* resp) override {
        return run(ctx, "GetTenant", [&]() -> Status {
            auto name = tenant_of(req->name());
            if (!name) return std::unexpected(name.error());
            auto tenant = meta().get_tenant(*name);
            if (!tenant) return std::unexpected(tenant.error());
            fill(resp, *tenant);
            return {};
        });
    }

    grpc::Status SetQuota(grpc::ServerContext* ctx, const pb::SetQuotaRequest* req, pb::TenantInfo* resp) override {
        return run(ctx, "SetQuota", [&]() -> Status {
            if (auto valid = validate_tenant_name(req->name()); !valid) return valid;
            if (auto ok = meta().set_quota(req->name(), to_quota(req->quota())); !ok) return ok;
            log_line("info", "quota of tenant " + req->name() + " set");
            auto tenant = meta().get_tenant(req->name());
            if (!tenant) return std::unexpected(tenant.error());
            fill(resp, *tenant);
            return {};
        });
    }

    grpc::Status SetClusterConfig(grpc::ServerContext* ctx, const pb::ClusterConfig* req,
                                  pb::ClusterConfig* resp) override {
        return run(ctx, "SetClusterConfig", [&]() -> Status {
            const std::string& min = req->min_sdk_version();
            if (!min.empty() && !compare_versions(min, min))
                return fail(Errc::invalid_argument, "min_sdk_version must be MAJOR.MINOR.PATCH");
            if (auto ok = config_store().set_cluster_config(ClusterConfig{min}); !ok) return ok;
            {
                std::lock_guard lock(config_mu);
                cached_config.reset();
            }
            log_line("info", "minimum SDK version set to '" + min + "'");
            resp->set_min_sdk_version(min);
            return {};
        });
    }

    // ----- Buckets.

    grpc::Status CreateBucket(grpc::ServerContext* ctx, const pb::CreateBucketRequest* req,
                              pb::BucketInfo* resp) override {
        return run(ctx, "CreateBucket", [&]() -> Status {
            auto ref = bucket_ref(req->tenant(), req->bucket());
            if (!ref) return std::unexpected(ref.error());
            auto config = to_config(req->config());
            if (!config) return std::unexpected(config.error());
            if (auto ok = meta().create_bucket(*ref, *config); !ok) return ok;
            auto bucket = meta().get_bucket(*ref);
            if (!bucket) return std::unexpected(bucket.error());
            fill(resp, *bucket);
            return {};
        });
    }

    // Validates names and loads the Bucket.
    Result<BucketInfo> bucket_call(const std::string& tenant, const std::string& name) {
        auto ref = bucket_ref(tenant, name);
        if (!ref) return std::unexpected(ref.error());
        return existing_bucket(*ref);
    }

    grpc::Status DeleteBucket(grpc::ServerContext* ctx, const pb::BucketRequest* req, pb::Empty*) override {
        return run(ctx, "DeleteBucket", [&]() -> Status {
            auto bucket = bucket_call(req->tenant(), req->bucket());
            if (!bucket) return std::unexpected(bucket.error());
            return meta().delete_bucket(bucket->ref);
        });
    }

    grpc::Status GetBucket(grpc::ServerContext* ctx, const pb::BucketRequest* req, pb::BucketInfo* resp) override {
        return run(ctx, "GetBucket", [&]() -> Status {
            auto bucket = bucket_call(req->tenant(), req->bucket());
            if (!bucket) return std::unexpected(bucket.error());
            fill(resp, *bucket);
            return {};
        });
    }

    grpc::Status ListBuckets(grpc::ServerContext* ctx, const pb::ListBucketsRequest* req,
                             pb::ListBucketsResponse* resp) override {
        return run(ctx, "ListBuckets", [&]() -> Status {
            auto tenant = tenant_of(req->tenant());
            if (!tenant) return std::unexpected(tenant.error());
            auto buckets = meta().list_buckets(*tenant);
            if (!buckets) return std::unexpected(buckets.error());
            for (const auto& bucket : *buckets) fill(resp->add_buckets(), bucket);
            return {};
        });
    }

    grpc::Status SetBucketConfig(grpc::ServerContext* ctx, const pb::SetBucketConfigRequest* req,
                                 pb::BucketInfo* resp) override {
        return run(ctx, "SetBucketConfig", [&]() -> Status {
            auto bucket = bucket_call(req->tenant(), req->bucket());
            if (!bucket) return std::unexpected(bucket.error());
            auto config = to_config(req->config());
            if (!config) return std::unexpected(config.error());
            if (auto ok = meta().set_bucket_config(bucket->ref, *config); !ok) return ok;
            bucket->config = *config;
            fill(resp, *bucket);
            return {};
        });
    }

    // ----- Writes (ADR 0007, 0013, 0016).

    grpc::Status PutObject(grpc::ServerContext* ctx, grpc::ServerReader<pb::PutObjectRequest>* reader,
                           pb::PutObjectResponse* resp) override {
        return run(ctx, "PutObject", [&]() -> Status {
            pb::PutObjectRequest first;
            if (!reader->Read(&first) || !first.has_header())
                return fail(Errc::invalid_argument, "PutObject must start with a header");
            auto ref = object_ref(first.header().location());
            if (!ref) return std::unexpected(ref.error());
            auto bucket = existing_bucket(ref->bucket);
            if (!bucket) return std::unexpected(bucket.error());
            auto data_key = crypto::new_data_key(*deps.keys);
            if (!data_key) return std::unexpected(data_key.error());
            auto upload = meta().begin_upload(*ref, data_key->encryption, Clock::now());
            if (!upload) return std::unexpected(upload.error());

            Result<Generation> generation = [&]() -> Result<Generation> {
                if (auto ok = store_stream(ctx, reader, *upload, data_key->key); !ok)
                    return std::unexpected(ok.error());
                return meta().commit_upload(*upload, to_precondition(first.header().precondition()), Clock::now());
            }();
            if (!generation) {
                (void)meta().abort_upload(*upload);  // its Extents become garbage now, not at expiry
                return std::unexpected(generation.error());
            }
            return committed(*ref, *generation, resp);
        });
    }

    grpc::Status InitiateUpload(grpc::ServerContext* ctx, const pb::InitiateUploadRequest* req,
                                pb::InitiateUploadResponse* resp) override {
        return run(ctx, "InitiateUpload", [&]() -> Status {
            auto ref = object_ref(req->location());
            if (!ref) return std::unexpected(ref.error());
            if (auto bucket = existing_bucket(ref->bucket); !bucket)
                return std::unexpected(bucket.error());
            auto data_key = crypto::new_data_key(*deps.keys);
            if (!data_key) return std::unexpected(data_key.error());
            auto upload = meta().begin_upload(*ref, data_key->encryption, Clock::now());
            if (!upload) return std::unexpected(upload.error());
            resp->set_upload_id(*upload);
            return {};
        });
    }

    grpc::Status UploadPart(grpc::ServerContext* ctx, grpc::ServerReader<pb::UploadPartRequest>* reader,
                            pb::UploadPartResponse* resp) override {
        return run(ctx, "UploadPart", [&]() -> Status {
            pb::UploadPartRequest msg;
            if (!reader->Read(&msg) || !msg.has_header())
                return fail(Errc::invalid_argument, "UploadPart must start with a header");
            const auto header = msg.header();
            if (header.part_number() == 0 || header.part_number() > kMaxParts)
                return fail(Errc::invalid_argument, "part numbers run from 1 to " + std::to_string(kMaxParts));
            auto access = load_upload(header.upload_id());
            if (!access) return std::unexpected(access.error());
            auto key = crypto::unwrap(*deps.keys, access->upload.encryption);
            if (!key) return std::unexpected(key.error());

            ByteBudget::Reservation held(budget);
            std::vector<std::byte> buffer;
            while (reader->Read(&msg)) {
                if (!msg.has_data()) return fail(Errc::invalid_argument, "only data may follow the header");
                if (buffer.size() + msg.data().size() > options.max_part_bytes)
                    return fail(Errc::invalid_argument,
                                "a part holds at most " + std::to_string(options.max_part_bytes) + " bytes");
                if (auto ok = held.grow(msg.data().size(), options.buffer_wait); !ok) return ok;
                bytes_in.inc(static_cast<double>(msg.data().size()));
                auto data = as_bytes(msg.data());
                buffer.insert(buffer.end(), data.begin(), data.end());
            }
            if (ctx->IsCancelled()) return fail(Errc::aborted, "the client cancelled the upload");
            auto part = store_part(header.upload_id(), *key,
                                   header.part_number(), buffer);
            if (!part) return std::unexpected(part.error());
            fill(resp->mutable_part(), *part);
            return {};
        });
    }

    grpc::Status ListParts(grpc::ServerContext* ctx, const pb::UploadRequest* req,
                           pb::ListPartsResponse* resp) override {
        return run(ctx, "ListParts", [&]() -> Status {
            auto access = load_upload(req->upload_id());
            if (!access) return std::unexpected(access.error());
            for (const auto& [number, part] : access->upload.parts) fill(resp->add_parts(), part);
            return {};
        });
    }

    grpc::Status CompleteUpload(grpc::ServerContext* ctx, const pb::CompleteUploadRequest* req,
                                pb::PutObjectResponse* resp) override {
        return run(ctx, "CompleteUpload", [&]() -> Status {
            auto access = load_upload(req->upload_id());
            if (!access) return std::unexpected(access.error());
            auto generation =
                meta().commit_upload(req->upload_id(), to_precondition(req->precondition()), Clock::now());
            if (!generation) return std::unexpected(generation.error());
            return committed(access->upload.target, *generation, resp);
        });
    }

    grpc::Status AbortUpload(grpc::ServerContext* ctx, const pb::UploadRequest* req, pb::Empty*) override {
        return run(ctx, "AbortUpload", [&]() -> Status {
            auto access = load_upload(req->upload_id());
            if (!access) return std::unexpected(access.error());
            return meta().abort_upload(req->upload_id());
        });
    }

    // ----- Reads.

    Result<ObjectMeta> version_of(const ObjectRef& ref, Generation generation) {
        return generation == kNoObject ? meta().head(ref) : meta().head_version(ref, generation);
    }

    // The stored bytes of one Part. Compaction may have moved it since `version` was read
    // (§6 safety rule 10): on not_found the version is re-read once into `moved` and its
    // current Extents are used from then on.
    Result<std::vector<std::byte>> read_part(const ObjectMeta& version, const Part& part,
                                             std::optional<ObjectMeta>& moved) {
        const Extent* extent = moved ? extent_of(*moved, part.number) : &part.extent;
        if (!extent) return fail(Errc::not_found, "part " + std::to_string(part.number) + " is gone");
        auto stored = deps.reader->read(*extent);
        if (stored || stored.error().code != Errc::not_found || moved) return stored;
        auto again = meta().head_version(version.ref, version.generation);
        if (!again) return std::unexpected(again.error());
        moved = std::move(*again);
        return read_part(version, part, moved);
    }

    grpc::Status GetObject(grpc::ServerContext* ctx, const pb::GetObjectRequest* req,
                           grpc::ServerWriter<pb::GetObjectResponse>* writer) override {
        return run(ctx, "GetObject", [&]() -> Status {
            auto ref = object_ref(req->location());
            if (!ref) return std::unexpected(ref.error());
            if (auto bucket = existing_bucket(ref->bucket); !bucket)
                return std::unexpected(bucket.error());
            auto version = version_of(*ref, req->generation());
            if (!version) return std::unexpected(version.error());
            if (version->delete_marker) return fail(Errc::not_found, "that version is a delete marker");
            if (req->offset() > version->size) return fail(Errc::invalid_argument, "offset lies beyond the end");
            const std::uint64_t begin = req->offset();
            const std::uint64_t end = req->length() == 0 || req->length() > version->size - begin
                                          ? version->size
                                          : begin + req->length();

            pb::GetObjectResponse info;
            fill(info.mutable_info(), *version);
            if (!writer->Write(info)) return fail(Errc::aborted, "the client went away");
            if (begin == end) return {};

            auto key = crypto::unwrap(*deps.keys, version->encryption);
            if (!key) return std::unexpected(key.error());
            std::optional<ObjectMeta> moved;
            std::uint64_t part_begin = 0;
            for (const Part& part : version->parts) {
                const std::uint64_t part_end = part_begin + part.size;
                if (part_end > begin && part_begin < end) {
                    ByteBudget::Reservation held(budget);  // stored and decrypted copies
                    if (auto ok = held.grow(part.extent.length + part.size, options.buffer_wait); !ok) return ok;
                    auto stored = read_part(*version, part, moved);
                    if (!stored) return std::unexpected(stored.error());
                    auto plain = crypto::open_part(*key, part.number, *stored);
                    if (!plain) return std::unexpected(plain.error());
                    if (plain->size() != part.size)
                        return fail(Errc::corruption, "part " + std::to_string(part.number) + " has the wrong size");
                    auto from = std::max(begin, part_begin) - part_begin;
                    auto to = std::min(end, part_end) - part_begin;
                    if (auto ok = send(writer, std::span<const std::byte>(*plain).subspan(from, to - from)); !ok)
                        return ok;
                }
                part_begin = part_end;
                if (part_begin >= end) break;
            }
            return {};
        });
    }

    grpc::Status HeadObject(grpc::ServerContext* ctx, const pb::HeadObjectRequest* req,
                            pb::ObjectInfo* resp) override {
        return run(ctx, "HeadObject", [&]() -> Status {
            auto ref = object_ref(req->location());
            if (!ref) return std::unexpected(ref.error());
            if (auto bucket = existing_bucket(ref->bucket); !bucket)
                return std::unexpected(bucket.error());
            auto version = version_of(*ref, req->generation());
            if (!version) return std::unexpected(version.error());
            fill(resp, *version);
            return {};
        });
    }

    grpc::Status DeleteObject(grpc::ServerContext* ctx, const pb::DeleteObjectRequest* req,
                              pb::DeleteObjectResponse* resp) override {
        return run(ctx, "DeleteObject", [&]() -> Status {
            auto ref = object_ref(req->location());
            if (!ref) return std::unexpected(ref.error());
            if (auto bucket = existing_bucket(ref->bucket); !bucket)
                return std::unexpected(bucket.error());
            if (req->generation() != kNoObject) return meta().remove_version(*ref, req->generation());
            auto marker = meta().remove(*ref, to_precondition(req->precondition()), Clock::now());
            if (!marker) return std::unexpected(marker.error());
            resp->set_delete_marker_generation(*marker);
            return {};
        });
    }

    grpc::Status ListObjects(grpc::ServerContext* ctx, const pb::ListObjectsRequest* req,
                             pb::ListObjectsResponse* resp) override {
        return run(ctx, "ListObjects", [&]() -> Status {
            auto ref = bucket_ref(req->tenant(), req->bucket());
            if (!ref) return std::unexpected(ref.error());
            if (auto bucket = existing_bucket(*ref); !bucket)
                return std::unexpected(bucket.error());
            std::size_t limit = req->limit() == 0 ? options.list_limit_max
                                                  : std::min<std::size_t>(req->limit(), options.list_limit_max);
            std::optional<std::string_view> start_after;
            if (!req->start_after().empty()) start_after = req->start_after();
            auto page = meta().list(*ref, req->prefix(), start_after, limit);
            if (!page) return std::unexpected(page.error());
            for (const auto& object : page->objects) fill(resp->add_objects(), object);
            if (page->next_start_after) resp->set_next_start_after(*page->next_start_after);
            return {};
        });
    }

    grpc::Status ListVersions(grpc::ServerContext* ctx, const pb::ListVersionsRequest* req,
                              pb::ListVersionsResponse* resp) override {
        return run(ctx, "ListVersions", [&]() -> Status {
            auto ref = object_ref(req->location());
            if (!ref) return std::unexpected(ref.error());
            if (auto bucket = existing_bucket(ref->bucket); !bucket)
                return std::unexpected(bucket.error());
            auto versions = meta().list_versions(*ref);
            if (!versions) return std::unexpected(versions.error());
            for (const auto& version : *versions) fill(resp->add_versions(), version);
            return {};
        });
    }
};

Result<std::unique_ptr<Gateway>> Gateway::create(Options options, Dependencies deps) {
    if (!deps.stores.meta || !deps.stores.config || !deps.keys || !deps.writer ||
        !deps.reader || !deps.garbage)
        return fail(Errc::invalid_argument, "gateway: missing dependency");
    if (options.max_part_bytes == 0 || options.max_buffered_bytes < options.max_part_bytes)
        return fail(Errc::invalid_argument, "gateway: max_buffered_bytes must hold at least one max_part_bytes part");
    if (options.part_size == 0 || options.list_limit_max == 0)
        return fail(Errc::invalid_argument, "gateway: part_size and list_limit_max must be positive");
    std::unique_ptr<Gateway> gateway(new Gateway());
    gateway->impl_ = std::make_unique<Impl>(std::move(options), std::move(deps));
    return gateway;
}

Gateway::~Gateway() = default;

grpc::Service* Gateway::service() { return impl_.get(); }

}  // namespace rmk::gateway
