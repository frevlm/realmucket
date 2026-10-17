#include "rmk/client.hpp"

#include <grpcpp/grpcpp.h>

#include <algorithm>
#include <cctype>
#include <cstddef>
#include <deque>
#include <fstream>
#include <future>
#include <map>
#include <span>
#include <sstream>
#include <string_view>

#include "rmk/crc32c.hpp"
#include "rmk/v1/storage.grpc.pb.h"

namespace rmk::client {

namespace pb = rmk::v1;

namespace {

constexpr std::size_t kChunk = std::size_t{1} << 20;

constexpr std::pair<const char*, ErrorCode> kErrorNames[] = {
    {"not_found", ErrorCode::not_found},
    {"already_exists", ErrorCode::already_exists},
    {"precondition_failed", ErrorCode::precondition_failed},
    {"not_empty", ErrorCode::not_empty},
    {"invalid_argument", ErrorCode::invalid_argument},
    {"expired", ErrorCode::expired},
    {"no_space", ErrorCode::no_space},
    {"quota_exceeded", ErrorCode::quota_exceeded},
    {"permission_denied", ErrorCode::permission_denied},
    {"unauthenticated", ErrorCode::unauthenticated},
    {"unavailable", ErrorCode::unavailable},
    {"aborted", ErrorCode::aborted},
    {"corruption", ErrorCode::corruption},
    {"io_error", ErrorCode::io_error},
};

Error to_error(const grpc::Status& status) {
    const std::string& msg = status.error_message();
    if (auto pos = msg.find(": "); pos != std::string::npos) {
        std::string_view name(msg.data(), pos);
        for (const auto& [n, code] : kErrorNames) {
            if (name == n) {
                return {code, msg.substr(pos + 2)};
            }
        }
    }
    if (status.error_code() == grpc::StatusCode::UNAVAILABLE) {
        return {ErrorCode::unavailable, msg};
    }
    return {ErrorCode::unknown, msg};
}

std::unexpected<Error> fail(ErrorCode code, std::string message) {
    return std::unexpected(Error{code, std::move(message)});
}

Status from_grpc(const grpc::Status& status) {
    if (status.ok()) {
        return {};
    }
    return std::unexpected(to_error(status));
}

std::chrono::system_clock::time_point to_time(std::int64_t ms) {
    return std::chrono::time_point_cast<std::chrono::system_clock::duration>(
        std::chrono::system_clock::time_point{std::chrono::milliseconds{ms}});
}

void fill(pb::ObjectLocation* out, const Location& in) {
    out->set_tenant(in.tenant);
    out->set_bucket(in.bucket);
    out->set_key(in.key);
}

void fill(pb::Precondition* out, const Precondition& in) {
    if (in.if_generation_match) {
        out->set_if_generation_match(*in.if_generation_match);
    }
}

void fill(pb::BucketConfig* out, const BucketConfig& in) {
    out->set_versioning(in.versioning);
    out->mutable_storage_policy()->set_data_units(in.storage_policy.data_units);
    out->mutable_storage_policy()->set_parity_units(in.storage_policy.parity_units);
}

void fill(pb::Quota* out, const Quota& in) {
    out->set_max_bytes(in.max_bytes);
    out->set_max_objects(in.max_objects);
}

ObjectInfo convert(const pb::ObjectInfo& in) {
    ObjectInfo out;
    out.location = {in.tenant(), in.bucket(), in.key()};
    out.generation = in.generation();
    out.size = in.size();
    out.checksum = in.checksum();
    out.delete_marker = in.delete_marker();
    out.created = to_time(in.created_unix_ms());
    return out;
}

BucketInfo convert(const pb::BucketInfo& in) {
    BucketInfo out;
    out.tenant = in.tenant();
    out.bucket = in.bucket();
    out.config.versioning = in.config().versioning();
    out.config.storage_policy = {in.config().storage_policy().data_units(),
                                 in.config().storage_policy().parity_units()};
    return out;
}

TenantInfo convert(const pb::TenantInfo& in) {
    return {in.name(), {in.quota().max_bytes(), in.quota().max_objects()}, in.used_bytes(),
            in.used_objects()};
}

using Reader = std::function<std::size_t(char*, std::size_t)>;

struct Slice {
    std::uint32_t number = 0;
    std::string data;
    std::uint64_t size = 0;
    bool stored = false;
};

using SliceSource = std::function<Result<std::optional<Slice>>()>;

}  // namespace

struct Client::Impl {
    Options options;
    std::unique_ptr<pb::Storage::Stub> stub;

    Status prepare(grpc::ClientContext& ctx) const {
        ctx.AddMetadata("x-rmk-sdk-version", kVersion);
        ctx.set_deadline(std::chrono::system_clock::now() + options.timeout);
        return {};
    }

    template <class Resp, class Req>
    Result<Resp> unary(grpc::Status (pb::Storage::Stub::*method)(grpc::ClientContext*, const Req&, Resp*),
                       const Req& req) const {
        grpc::ClientContext ctx;
        if (auto st = prepare(ctx); !st) {
            return std::unexpected(st.error());
        }
        Resp resp;
        auto status = (stub.get()->*method)(&ctx, req, &resp);
        if (auto st = from_grpc(status); !st) {
            return std::unexpected(st.error());
        }
        return resp;
    }

    template <class Req>
    Status unary_void(grpc::Status (pb::Storage::Stub::*method)(grpc::ClientContext*, const Req&, pb::Empty*),
                      const Req& req) const {
        auto r = unary<pb::Empty>(method, req);
        if (!r) {
            return std::unexpected(r.error());
        }
        return {};
    }

    Result<ObjectInfo> put_single(const Location& where, std::string_view data,
                                  const Precondition& pre) const {
        grpc::ClientContext ctx;
        if (auto st = prepare(ctx); !st) {
            return std::unexpected(st.error());
        }
        pb::PutObjectResponse resp;
        auto writer = stub->PutObject(&ctx, &resp);
        pb::PutObjectRequest req;
        auto* header = req.mutable_header();
        fill(header->mutable_location(), where);
        fill(header->mutable_precondition(), pre);
        bool ok = writer->Write(req);
        for (std::size_t pos = 0; ok && pos < data.size(); pos += kChunk) {
            req.set_data(std::string(data.substr(pos, kChunk)));
            ok = writer->Write(req);
        }
        writer->WritesDone();
        if (auto st = from_grpc(writer->Finish()); !st) {
            return std::unexpected(st.error());
        }
        return convert(resp.object());
    }

    Status upload_part(std::uint64_t upload_id, std::uint32_t number, const std::string& data) const {
        grpc::ClientContext ctx;
        if (auto st = prepare(ctx); !st) {
            return st;
        }
        pb::UploadPartResponse resp;
        auto writer = stub->UploadPart(&ctx, &resp);
        pb::UploadPartRequest req;
        req.mutable_header()->set_upload_id(upload_id);
        req.mutable_header()->set_part_number(number);
        bool ok = writer->Write(req);
        for (std::size_t pos = 0; ok && pos < data.size(); pos += kChunk) {
            req.set_data(data.substr(pos, kChunk));
            ok = writer->Write(req);
        }
        writer->WritesDone();
        return from_grpc(writer->Finish());
    }

    static Error with_upload(Error e, std::uint64_t upload_id) {
        e.message += " (upload id " + std::to_string(upload_id) +
                     " left for resumption: put_file with resume_upload_id)";
        return e;
    }

    Result<ObjectInfo> upload_parts(std::uint64_t upload_id, const Precondition& pre, const SliceSource& next,
                                    const ProgressCallback& progress) const {
        const std::size_t window = std::max(1u, options.parallel_parts);
        struct Flight {
            std::uint32_t number;
            std::uint64_t size;
            std::future<Status> result;
        };
        std::deque<Flight> flights;
        std::optional<Error> failure;
        std::uint64_t bytes_done = 0;

        auto reap = [&] {
            auto& f = flights.front();
            auto st = f.result.get();
            if (!st) {
                if (!failure) {
                    failure = st.error();
                }
            } else {
                bytes_done += f.size;
                if (progress) {
                    progress(upload_id, f.number, bytes_done);
                }
            }
            flights.pop_front();
        };

        while (!failure) {
            auto slice = next();
            if (!slice) {
                failure = slice.error();
                break;
            }
            if (!*slice) {
                break;
            }
            Slice s = std::move(**slice);
            if (s.stored) {
                bytes_done += s.size;
                continue;
            }
            if (flights.size() >= window) {
                reap();
                if (failure) {
                    break;
                }
            }
            auto number = s.number;
            auto size = s.size;
            flights.push_back({number, size,
                               std::async(std::launch::async, [this, upload_id, number, data = std::move(s.data)] {
                                   return upload_part(upload_id, number, data);
                               })});
        }
        while (!flights.empty()) {
            reap();
        }
        if (failure) {
            return std::unexpected(with_upload(*failure, upload_id));
        }

        pb::CompleteUploadRequest req;
        req.set_upload_id(upload_id);
        fill(req.mutable_precondition(), pre);
        auto resp = unary<pb::PutObjectResponse>(&pb::Storage::Stub::CompleteUpload, req);
        if (!resp) {
            return std::unexpected(with_upload(resp.error(), upload_id));
        }
        return convert(resp->object());
    }

    Result<std::uint64_t> initiate(const Location& where) const {
        pb::InitiateUploadRequest req;
        fill(req.mutable_location(), where);
        auto resp = unary<pb::InitiateUploadResponse>(&pb::Storage::Stub::InitiateUpload, req);
        if (!resp) {
            return std::unexpected(resp.error());
        }
        return resp->upload_id();
    }

    Result<ObjectInfo> put_reader(const Location& where, const Reader& read, const Precondition& pre) const {
        const std::uint64_t threshold = options.multipart_threshold;
        const std::uint64_t part_size = options.part_size;
        if (part_size == 0) {
            return fail(ErrorCode::invalid_argument, "part_size must be positive");
        }
        std::string head;
        bool eof = false;
        while (!eof && head.size() <= threshold) {
            auto want = static_cast<std::size_t>(std::min<std::uint64_t>(kChunk, threshold + 1 - head.size()));
            auto old = head.size();
            head.resize(old + want);
            auto got = read(head.data() + old, want);
            head.resize(old + got);
            eof = got == 0;
        }
        if (head.size() <= threshold) {
            return put_single(where, head, pre);
        }

        auto id = initiate(where);
        if (!id) {
            return std::unexpected(id.error());
        }
        std::size_t pos = 0;
        std::uint32_t number = 0;
        SliceSource source = [&]() -> Result<std::optional<Slice>> {
            Slice s;
            s.data.reserve(part_size);
            while (s.data.size() < part_size) {
                auto want = static_cast<std::size_t>(part_size - s.data.size());
                if (pos < head.size()) {
                    auto n = std::min(want, head.size() - pos);
                    s.data.append(head, pos, n);
                    pos += n;
                    if (pos == head.size()) {
                        head = std::string();
                        pos = 0;
                    }
                    continue;
                }
                if (eof) {
                    break;
                }
                auto old = s.data.size();
                auto n = std::min(want, kChunk);
                s.data.resize(old + n);
                auto got = read(s.data.data() + old, n);
                s.data.resize(old + got);
                if (got == 0) {
                    eof = true;
                }
            }
            if (s.data.empty()) {
                return std::nullopt;
            }
            s.number = ++number;
            s.size = s.data.size();
            return std::optional<Slice>(std::move(s));
        };
        return upload_parts(*id, pre, source, {});
    }

    Result<ObjectInfo> put_file_impl(const Location& where, const std::filesystem::path& file,
                                     const Precondition& pre, std::optional<std::uint64_t> resume,
                                     const ProgressCallback& progress) const {
        std::error_code ec;
        auto file_size = std::filesystem::file_size(file, ec);
        if (ec) {
            return fail(ErrorCode::io_error, "cannot stat " + file.string() + ": " + ec.message());
        }
        std::ifstream in(file, std::ios::binary);
        if (!in) {
            return fail(ErrorCode::io_error, "cannot open " + file.string());
        }
        const std::uint64_t part_size = options.part_size;
        if (part_size == 0) {
            return fail(ErrorCode::invalid_argument, "part_size must be positive");
        }
        if (!resume && file_size <= options.multipart_threshold) {
            std::string data(file_size, '\0');
            in.read(data.data(), static_cast<std::streamsize>(file_size));
            if (static_cast<std::uint64_t>(in.gcount()) != file_size) {
                return fail(ErrorCode::io_error, "short read from " + file.string());
            }
            return put_single(where, data, pre);
        }

        std::uint64_t id = 0;
        std::map<std::uint32_t, std::uint64_t> stored;
        if (resume) {
            id = *resume;
            pb::UploadRequest req;
            req.set_upload_id(id);
            auto parts = unary<pb::ListPartsResponse>(&pb::Storage::Stub::ListParts, req);
            if (!parts) {
                return std::unexpected(parts.error());
            }
            for (const auto& p : parts->parts()) {
                stored[p.part_number()] = p.size();
            }
        } else {
            auto init = initiate(where);
            if (!init) {
                return std::unexpected(init.error());
            }
            id = *init;
        }

        const auto total_parts = static_cast<std::uint32_t>(std::max<std::uint64_t>(1, (file_size + part_size - 1) / part_size));
        std::uint32_t number = 0;
        SliceSource source = [&]() -> Result<std::optional<Slice>> {
            if (number >= total_parts) {
                return std::nullopt;
            }
            Slice s;
            s.number = ++number;
            std::uint64_t offset = std::uint64_t{s.number - 1} * part_size;
            s.size = std::min(part_size, file_size - offset);
            if (auto it = stored.find(s.number); it != stored.end() && it->second == s.size) {
                s.stored = true;
                return std::optional<Slice>(std::move(s));
            }
            s.data.resize(s.size);
            in.clear();
            in.seekg(static_cast<std::streamoff>(offset));
            in.read(s.data.data(), static_cast<std::streamsize>(s.size));
            if (static_cast<std::uint64_t>(in.gcount()) != s.size) {
                return fail(ErrorCode::io_error, "short read from " + file.string());
            }
            return std::optional<Slice>(std::move(s));
        };
        return upload_parts(id, pre, source, progress);
    }

    Result<ObjectInfo> get_impl(const Location& where, std::ostream& out, const GetOptions& opts) const {
        grpc::ClientContext ctx;
        if (auto st = prepare(ctx); !st) {
            return std::unexpected(st.error());
        }
        pb::GetObjectRequest req;
        fill(req.mutable_location(), where);
        req.set_generation(opts.generation);
        req.set_offset(opts.offset);
        req.set_length(opts.length);
        auto reader = stub->GetObject(&ctx, req);

        const bool whole = opts.generation == 0 && opts.offset == 0 && opts.length == 0;
        std::optional<ObjectInfo> info;
        std::uint32_t crc = 0;
        std::uint64_t received = 0;
        std::optional<Error> local;
        pb::GetObjectResponse msg;
        while (reader->Read(&msg)) {
            if (msg.has_info()) {
                info = convert(msg.info());
            } else if (msg.has_data()) {
                const auto& d = msg.data();
                crc = rmk::crc32c(std::as_bytes(std::span(d.data(), d.size())), crc);
                received += d.size();
                if (!out.write(d.data(), static_cast<std::streamsize>(d.size()))) {
                    local = Error{ErrorCode::io_error, "write to output failed"};
                    ctx.TryCancel();
                    break;
                }
            }
        }
        auto status = reader->Finish();
        if (local) {
            return std::unexpected(*local);
        }
        if (auto st = from_grpc(status); !st) {
            return std::unexpected(st.error());
        }
        if (!info) {
            return fail(ErrorCode::unknown, "GetObject returned no object info");
        }
        if (whole && (received != info->size || crc != info->checksum)) {
            return fail(ErrorCode::corruption, "crc32c or size mismatch reading " + where.bucket + "/" + where.key);
        }
        return *info;
    }
};

Result<Client> Client::connect(Options options) {
    if (options.endpoint.empty()) {
        return fail(ErrorCode::invalid_argument, "endpoint is required");
    }
    std::shared_ptr<grpc::ChannelCredentials> creds;
    if (options.insecure) {
        creds = grpc::InsecureChannelCredentials();
    } else {
        grpc::SslCredentialsOptions ssl;
        if (!options.ca_file.empty()) {
            std::ifstream in(options.ca_file, std::ios::binary);
            if (!in) {
                return fail(ErrorCode::io_error, "cannot read ca_file " + options.ca_file.string());
            }
            std::ostringstream ss;
            ss << in.rdbuf();
            ssl.pem_root_certs = ss.str();
        }
        creds = grpc::SslCredentials(ssl);
    }
    Client client;
    client.impl_ = std::make_unique<Impl>();
    client.impl_->stub = pb::Storage::NewStub(grpc::CreateChannel(options.endpoint, creds));
    client.impl_->options = std::move(options);
    return client;
}

Client::Client(Client&&) noexcept = default;
Client& Client::operator=(Client&&) noexcept = default;
Client::~Client() = default;

Result<ObjectInfo> Client::put(const Location& where, std::istream& content, Precondition precondition) {
    Reader read = [&content](char* dst, std::size_t n) {
        content.read(dst, static_cast<std::streamsize>(n));
        return static_cast<std::size_t>(content.gcount());
    };
    return impl_->put_reader(where, read, precondition);
}

Result<ObjectInfo> Client::put(const Location& where, std::string_view content, Precondition precondition) {
    std::size_t pos = 0;
    Reader read = [&](char* dst, std::size_t n) {
        n = std::min(n, content.size() - pos);
        std::copy_n(content.data() + pos, n, dst);
        pos += n;
        return n;
    };
    return impl_->put_reader(where, read, precondition);
}

Result<ObjectInfo> Client::put_file(const Location& where, const std::filesystem::path& file,
                                    Precondition precondition, std::optional<std::uint64_t> resume_upload_id,
                                    ProgressCallback progress) {
    return impl_->put_file_impl(where, file, precondition, resume_upload_id, progress);
}

Result<ObjectInfo> Client::get(const Location& where, std::ostream& out, const GetOptions& options) {
    return impl_->get_impl(where, out, options);
}

Result<std::string> Client::get(const Location& where, const GetOptions& options) {
    std::ostringstream out;
    auto info = impl_->get_impl(where, out, options);
    if (!info) {
        return std::unexpected(info.error());
    }
    return out.str();
}

Result<ObjectInfo> Client::head(const Location& where, std::uint64_t generation) {
    pb::HeadObjectRequest req;
    fill(req.mutable_location(), where);
    req.set_generation(generation);
    auto r = impl_->unary<pb::ObjectInfo>(&pb::Storage::Stub::HeadObject, req);
    if (!r) {
        return std::unexpected(r.error());
    }
    return convert(*r);
}

Result<std::uint64_t> Client::remove(const Location& where, Precondition precondition) {
    pb::DeleteObjectRequest req;
    fill(req.mutable_location(), where);
    fill(req.mutable_precondition(), precondition);
    auto r = impl_->unary<pb::DeleteObjectResponse>(&pb::Storage::Stub::DeleteObject, req);
    if (!r) {
        return std::unexpected(r.error());
    }
    return r->delete_marker_generation();
}

Status Client::remove_version(const Location& where, std::uint64_t generation) {
    pb::DeleteObjectRequest req;
    fill(req.mutable_location(), where);
    req.set_generation(generation);
    auto r = impl_->unary<pb::DeleteObjectResponse>(&pb::Storage::Stub::DeleteObject, req);
    if (!r) {
        return std::unexpected(r.error());
    }
    return {};
}

Result<ListResult> Client::list(const std::string& tenant, const std::string& bucket,
                                const ListOptions& options) {
    pb::ListObjectsRequest req;
    req.set_tenant(tenant);
    req.set_bucket(bucket);
    req.set_prefix(options.prefix);
    req.set_start_after(options.start_after);
    req.set_limit(options.limit);
    auto r = impl_->unary<pb::ListObjectsResponse>(&pb::Storage::Stub::ListObjects, req);
    if (!r) {
        return std::unexpected(r.error());
    }
    ListResult out;
    for (const auto& o : r->objects()) {
        out.objects.push_back(convert(o));
    }
    out.next_start_after = r->next_start_after();
    return out;
}

Result<std::vector<ObjectInfo>> Client::list_versions(const Location& where) {
    pb::ListVersionsRequest req;
    fill(req.mutable_location(), where);
    auto r = impl_->unary<pb::ListVersionsResponse>(&pb::Storage::Stub::ListVersions, req);
    if (!r) {
        return std::unexpected(r.error());
    }
    std::vector<ObjectInfo> out;
    for (const auto& o : r->versions()) {
        out.push_back(convert(o));
    }
    return out;
}

Status Client::abort_upload(std::uint64_t upload_id) {
    pb::UploadRequest req;
    req.set_upload_id(upload_id);
    return impl_->unary_void(&pb::Storage::Stub::AbortUpload, req);
}

Result<BucketInfo> Client::create_bucket(const std::string& tenant, const std::string& bucket,
                                         const BucketConfig& config) {
    pb::CreateBucketRequest req;
    req.set_tenant(tenant);
    req.set_bucket(bucket);
    fill(req.mutable_config(), config);
    auto r = impl_->unary<pb::BucketInfo>(&pb::Storage::Stub::CreateBucket, req);
    if (!r) {
        return std::unexpected(r.error());
    }
    return convert(*r);
}

Status Client::delete_bucket(const std::string& tenant, const std::string& bucket) {
    pb::BucketRequest req;
    req.set_tenant(tenant);
    req.set_bucket(bucket);
    return impl_->unary_void(&pb::Storage::Stub::DeleteBucket, req);
}

Result<BucketInfo> Client::get_bucket(const std::string& tenant, const std::string& bucket) {
    pb::BucketRequest req;
    req.set_tenant(tenant);
    req.set_bucket(bucket);
    auto r = impl_->unary<pb::BucketInfo>(&pb::Storage::Stub::GetBucket, req);
    if (!r) {
        return std::unexpected(r.error());
    }
    return convert(*r);
}

Result<std::vector<BucketInfo>> Client::list_buckets(const std::string& tenant) {
    pb::ListBucketsRequest req;
    req.set_tenant(tenant);
    auto r = impl_->unary<pb::ListBucketsResponse>(&pb::Storage::Stub::ListBuckets, req);
    if (!r) {
        return std::unexpected(r.error());
    }
    std::vector<BucketInfo> out;
    for (const auto& b : r->buckets()) {
        out.push_back(convert(b));
    }
    return out;
}

Result<BucketInfo> Client::set_bucket_config(const std::string& tenant, const std::string& bucket,
                                             const BucketConfig& config) {
    pb::SetBucketConfigRequest req;
    req.set_tenant(tenant);
    req.set_bucket(bucket);
    fill(req.mutable_config(), config);
    auto r = impl_->unary<pb::BucketInfo>(&pb::Storage::Stub::SetBucketConfig, req);
    if (!r) {
        return std::unexpected(r.error());
    }
    return convert(*r);
}

Result<TenantInfo> Client::create_tenant(const std::string& name, const Quota& quota) {
    pb::CreateTenantRequest req;
    req.set_name(name);
    fill(req.mutable_quota(), quota);
    auto r = impl_->unary<pb::TenantInfo>(&pb::Storage::Stub::CreateTenant, req);
    if (!r) {
        return std::unexpected(r.error());
    }
    return convert(*r);
}

Result<TenantInfo> Client::get_tenant(const std::string& name) {
    pb::GetTenantRequest req;
    req.set_name(name);
    auto r = impl_->unary<pb::TenantInfo>(&pb::Storage::Stub::GetTenant, req);
    if (!r) {
        return std::unexpected(r.error());
    }
    return convert(*r);
}

Result<TenantInfo> Client::set_quota(const std::string& name, const Quota& quota) {
    pb::SetQuotaRequest req;
    req.set_name(name);
    fill(req.mutable_quota(), quota);
    auto r = impl_->unary<pb::TenantInfo>(&pb::Storage::Stub::SetQuota, req);
    if (!r) {
        return std::unexpected(r.error());
    }
    return convert(*r);
}

Status Client::set_min_sdk_version(const std::string& version) {
    pb::ClusterConfig req;
    req.set_min_sdk_version(version);
    auto r = impl_->unary<pb::ClusterConfig>(&pb::Storage::Stub::SetClusterConfig, req);
    if (!r) {
        return std::unexpected(r.error());
    }
    return {};
}

}  // namespace rmk::client
