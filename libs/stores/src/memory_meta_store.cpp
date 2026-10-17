#include <algorithm>
#include <charconv>
#include <utility>

#include "rmk/core/crc32c.hpp"
#include "rmk/stores/memory_stores.hpp"
#include "rmk/core/names.hpp"

namespace rmk {
namespace {

using VersionMap = std::map<Generation, ObjectMeta>;

std::string describe(const BucketRef& b) { return b.tenant + "/" + b.bucket; }
std::string describe(const ObjectRef& r) { return describe(r.bucket) + "/" + r.key; }

bool expired(const PendingUpload& upload, Clock::time_point now) { return upload.deadline <= now; }

// The Generation a Precondition is compared against: the newest version unless it is a
// delete marker.
Generation visible_generation(const VersionMap& versions) {
    if (versions.empty()) return kNoObject;
    const ObjectMeta& newest = versions.rbegin()->second;
    return newest.delete_marker ? kNoObject : newest.generation;
}

bool exceeds(std::uint64_t used, std::uint64_t add, std::uint64_t max) {
    return add > max || used > max - add;
}

// Cursor = tenant NUL bucket NUL key NUL generation(decimal); none of the names hold a NUL.
std::string encode_cursor(const ObjectRef& ref, Generation generation) {
    std::string out = ref.bucket.tenant;
    out += '\0';
    out += ref.bucket.bucket;
    out += '\0';
    out += ref.key;
    out += '\0';
    out += std::to_string(generation);
    return out;
}

std::optional<std::pair<ObjectRef, Generation>> decode_cursor(std::string_view cursor) {
    std::string_view fields[4];
    for (int i = 0; i < 3; ++i) {
        auto nul = cursor.find('\0');
        if (nul == std::string_view::npos) return std::nullopt;
        fields[i] = cursor.substr(0, nul);
        cursor.remove_prefix(nul + 1);
    }
    fields[3] = cursor;
    Generation generation = 0;
    auto [end, ec] = std::from_chars(fields[3].data(), fields[3].data() + fields[3].size(), generation);
    if (ec != std::errc{} || end != fields[3].data() + fields[3].size()) return std::nullopt;
    ObjectRef ref{{std::string(fields[0]), std::string(fields[1])}, std::string(fields[2])};
    return std::pair{std::move(ref), generation};
}

}  // namespace

MemoryMetaStore::MemoryMetaStore(std::chrono::seconds pending_upload_retention)
    : retention_(pending_upload_retention) {}

void MemoryMetaStore::add_garbage_locked(const Extent& extent) {
    std::uint64_t id = ++last_garbage_;
    garbage_.emplace(id, GarbageEntry{id, extent});
}

// Extents become garbage and the Tenant's usage drops; the caller erases the version.
void MemoryMetaStore::release_version_locked(const ObjectMeta& version) {
    for (const Part& part : version.parts) add_garbage_locked(part.extent);
    if (version.delete_marker) return;
    auto tenant = tenants_.find(version.ref.bucket.tenant);
    if (tenant == tenants_.end()) return;
    tenant->second.usage.bytes -= version.size;
    tenant->second.usage.objects -= 1;
}

// --- Tenants

Status MemoryMetaStore::create_tenant(std::string_view name, const Quota& quota) {
    if (auto valid = validate_tenant_name(name); !valid) return valid;
    std::lock_guard lock(mutex_);
    auto [it, inserted] = tenants_.try_emplace(std::string(name), TenantInfo{std::string(name), quota, {}});
    if (!inserted) return fail(Errc::already_exists, "tenant exists: " + std::string(name));
    return {};
}

Result<TenantInfo> MemoryMetaStore::get_tenant(std::string_view name) {
    std::lock_guard lock(mutex_);
    auto it = tenants_.find(name);
    if (it == tenants_.end()) return fail(Errc::not_found, "no such tenant: " + std::string(name));
    return it->second;
}

Result<std::vector<std::string>> MemoryMetaStore::list_tenants() {
    std::lock_guard lock(mutex_);
    std::vector<std::string> names;
    for (const auto& [name, info] : tenants_) names.push_back(name);
    return names;
}

Status MemoryMetaStore::set_quota(std::string_view name, const Quota& quota) {
    std::lock_guard lock(mutex_);
    auto it = tenants_.find(name);
    if (it == tenants_.end()) return fail(Errc::not_found, "no such tenant: " + std::string(name));
    it->second.quota = quota;
    return {};
}

// --- Buckets

Status MemoryMetaStore::create_bucket(const BucketRef& bucket, const BucketConfig& config) {
    if (auto valid = validate_tenant_name(bucket.tenant); !valid) return valid;
    if (auto valid = validate_bucket_name(bucket.bucket); !valid) return valid;
    std::lock_guard lock(mutex_);
    if (!tenants_.contains(bucket.tenant))
        return fail(Errc::not_found, "no such tenant: " + bucket.tenant);
    auto [it, inserted] = buckets_.try_emplace(bucket, BucketInfo{bucket, config});
    if (!inserted) return fail(Errc::already_exists, "bucket exists: " + describe(bucket));
    return {};
}

Status MemoryMetaStore::delete_bucket(const BucketRef& bucket) {
    std::lock_guard lock(mutex_);
    auto it = buckets_.find(bucket);
    if (it == buckets_.end()) return fail(Errc::not_found, "no such bucket: " + describe(bucket));
    auto first = objects_.lower_bound(ObjectRef{bucket, {}});
    if (first != objects_.end() && first->first.bucket == bucket)
        return fail(Errc::not_empty, "bucket holds objects: " + describe(bucket));
    for (const auto& [id, upload] : uploads_)
        if (upload.target.bucket == bucket)
            return fail(Errc::not_empty, "bucket has pending uploads: " + describe(bucket));
    buckets_.erase(it);
    return {};
}

Result<BucketInfo> MemoryMetaStore::get_bucket(const BucketRef& bucket) {
    std::lock_guard lock(mutex_);
    auto it = buckets_.find(bucket);
    if (it == buckets_.end()) return fail(Errc::not_found, "no such bucket: " + describe(bucket));
    return it->second;
}

Result<std::vector<BucketInfo>> MemoryMetaStore::list_buckets(std::string_view tenant) {
    std::lock_guard lock(mutex_);
    std::vector<BucketInfo> out;
    for (auto it = buckets_.lower_bound(BucketRef{std::string(tenant), {}});
         it != buckets_.end() && it->first.tenant == tenant; ++it)
        out.push_back(it->second);
    return out;
}

Status MemoryMetaStore::set_bucket_config(const BucketRef& bucket, const BucketConfig& config) {
    std::lock_guard lock(mutex_);
    auto it = buckets_.find(bucket);
    if (it == buckets_.end()) return fail(Errc::not_found, "no such bucket: " + describe(bucket));
    it->second.config = config;
    return {};
}

// --- Writes

Result<UploadId> MemoryMetaStore::begin_upload(const ObjectRef& target, const Encryption& encryption,
                                               Clock::time_point now) {
    if (auto valid = validate_object_key(target.key); !valid) return std::unexpected(valid.error());
    std::lock_guard lock(mutex_);
    if (!buckets_.contains(target.bucket))
        return fail(Errc::not_found, "no such bucket: " + describe(target.bucket));
    UploadId id = ++last_upload_;
    PendingUpload upload;
    upload.id = id;
    upload.target = target;
    upload.encryption = encryption;
    upload.deadline = now + retention_;
    uploads_.emplace(id, std::move(upload));
    return id;
}

Status MemoryMetaStore::put_part(UploadId id, const Part& part, Clock::time_point now) {
    std::lock_guard lock(mutex_);
    auto it = uploads_.find(id);
    if (it == uploads_.end()) return fail(Errc::not_found, "no such upload");
    if (expired(it->second, now)) return fail(Errc::expired, "upload expired");
    if (part.number == 0 || part.number > kMaxParts)
        return fail(Errc::invalid_argument, "part numbers run from 1 to kMaxParts");
    auto [slot, inserted] = it->second.parts.try_emplace(part.number, part);
    if (!inserted) {
        if (slot->second.extent != part.extent) add_garbage_locked(slot->second.extent);
        slot->second = part;
    }
    it->second.deadline = now + retention_;
    return {};
}

Result<PendingUpload> MemoryMetaStore::get_upload(UploadId id, Clock::time_point now) {
    std::lock_guard lock(mutex_);
    auto it = uploads_.find(id);
    if (it == uploads_.end()) return fail(Errc::not_found, "no such upload");
    if (expired(it->second, now)) return fail(Errc::expired, "upload expired");
    return it->second;
}

Status MemoryMetaStore::abort_upload(UploadId id) {
    std::lock_guard lock(mutex_);
    auto it = uploads_.find(id);
    if (it == uploads_.end()) return fail(Errc::not_found, "no such upload");
    for (const auto& [number, part] : it->second.parts) add_garbage_locked(part.extent);
    uploads_.erase(it);
    return {};
}

Result<Generation> MemoryMetaStore::commit_upload(UploadId id, const Precondition& precondition,
                                                  Clock::time_point now) {
    std::lock_guard lock(mutex_);
    auto up = uploads_.find(id);
    if (up == uploads_.end()) return fail(Errc::not_found, "no such upload");
    const PendingUpload& upload = up->second;
    if (expired(upload, now)) return fail(Errc::expired, "upload expired");

    ObjectMeta meta;
    meta.ref = upload.target;
    meta.encryption = upload.encryption;
    meta.created = now;
    std::uint32_t expected = 1;
    for (const auto& [number, part] : upload.parts) {
        if (number != expected++)
            return fail(Errc::invalid_argument, "parts must be numbered 1..N without gaps");
        meta.checksum = meta.parts.empty() ? part.checksum
                                           : crc32c_combine(meta.checksum, part.checksum, part.size);
        meta.size += part.size;
        meta.parts.push_back(part);
    }

    auto bucket = buckets_.find(meta.ref.bucket);
    auto tenant = tenants_.find(meta.ref.bucket.tenant);
    if (bucket == buckets_.end() || tenant == tenants_.end())
        return fail(Errc::not_found, "no such bucket: " + describe(meta.ref.bucket));

    auto object = objects_.find(meta.ref);
    Generation current = object == objects_.end() ? kNoObject : visible_generation(object->second);
    if (precondition.if_generation_match && *precondition.if_generation_match != current)
        return fail(Errc::precondition_failed, "generation mismatch: " + describe(meta.ref));

    // Without Versioning the newest version is replaced, so its share of usage is free again.
    const ObjectMeta* replaced = nullptr;
    if (!bucket->second.config.versioning && object != objects_.end() && !object->second.empty())
        replaced = &object->second.rbegin()->second;
    Usage usage = tenant->second.usage;
    if (replaced && !replaced->delete_marker) {
        usage.bytes -= replaced->size;
        usage.objects -= 1;
    }
    const Quota& quota = tenant->second.quota;
    if (exceeds(usage.bytes, meta.size, quota.max_bytes) || exceeds(usage.objects, 1, quota.max_objects))
        return fail(Errc::quota_exceeded, "tenant quota exceeded: " + meta.ref.bucket.tenant);

    if (replaced) {
        release_version_locked(*replaced);
        object->second.erase(std::prev(object->second.end()));
    }
    meta.generation = ++last_generation_;
    tenant->second.usage.bytes += meta.size;
    tenant->second.usage.objects += 1;
    objects_[meta.ref].emplace(meta.generation, meta);
    uploads_.erase(up);
    return meta.generation;
}

// --- Reads

Result<ObjectMeta> MemoryMetaStore::head(const ObjectRef& ref) {
    std::lock_guard lock(mutex_);
    auto it = objects_.find(ref);
    if (it == objects_.end() || visible_generation(it->second) == kNoObject)
        return fail(Errc::not_found, "no such object: " + describe(ref));
    return it->second.rbegin()->second;
}

Result<ObjectMeta> MemoryMetaStore::head_version(const ObjectRef& ref, Generation generation) {
    std::lock_guard lock(mutex_);
    auto it = objects_.find(ref);
    if (it != objects_.end()) {
        auto version = it->second.find(generation);
        if (version != it->second.end()) return version->second;
    }
    return fail(Errc::not_found, "no such object version: " + describe(ref));
}

Result<std::vector<ObjectMeta>> MemoryMetaStore::list_versions(const ObjectRef& ref) {
    std::lock_guard lock(mutex_);
    std::vector<ObjectMeta> out;
    auto it = objects_.find(ref);
    if (it != objects_.end())
        for (auto v = it->second.rbegin(); v != it->second.rend(); ++v) out.push_back(v->second);
    return out;
}

Result<ListPage> MemoryMetaStore::list(const BucketRef& bucket, std::string_view prefix,
                                       std::optional<std::string_view> start_after,
                                       std::size_t limit) {
    if (auto valid = validate_list_limit(limit); !valid) return std::unexpected(valid.error());
    std::lock_guard lock(mutex_);
    if (!buckets_.contains(bucket))
        return fail(Errc::not_found, "no such bucket: " + describe(bucket));

    auto it = start_after && *start_after >= prefix
                  ? objects_.upper_bound(ObjectRef{bucket, std::string(*start_after)})
                  : objects_.lower_bound(ObjectRef{bucket, std::string(prefix)});
    // Every key examined counts against the limit, delete markers included, so a page may hold
    // fewer objects than `limit` (even none) while next_start_after says more remain.
    ListPage page;
    std::size_t examined = 0;
    const std::string* last_key = nullptr;
    for (; it != objects_.end() && it->first.bucket == bucket && it->first.key.starts_with(prefix); ++it) {
        if (examined == limit) {
            page.next_start_after = *last_key;
            break;
        }
        ++examined;
        last_key = &it->first.key;
        if (visible_generation(it->second) == kNoObject) continue;
        ObjectMeta entry = it->second.rbegin()->second;
        entry.parts.clear();
        entry.encryption = {};
        page.objects.push_back(std::move(entry));
    }
    return page;
}

// --- Deletes

Result<Generation> MemoryMetaStore::remove(const ObjectRef& ref, const Precondition& precondition,
                                           Clock::time_point now) {
    std::lock_guard lock(mutex_);
    auto object = objects_.find(ref);
    Generation current = object == objects_.end() ? kNoObject : visible_generation(object->second);
    if (precondition.if_generation_match && *precondition.if_generation_match != current)
        return fail(Errc::precondition_failed, "generation mismatch: " + describe(ref));
    if (current == kNoObject) return fail(Errc::not_found, "no such object: " + describe(ref));
    auto bucket = buckets_.find(ref.bucket);
    if (bucket == buckets_.end())
        return fail(Errc::not_found, "no such bucket: " + describe(ref.bucket));

    if (bucket->second.config.versioning) {
        ObjectMeta marker;
        marker.ref = ref;
        marker.generation = ++last_generation_;
        marker.delete_marker = true;
        marker.created = now;
        object->second.emplace(marker.generation, marker);
        return marker.generation;
    }
    release_version_locked(object->second.rbegin()->second);
    object->second.erase(std::prev(object->second.end()));
    if (object->second.empty()) objects_.erase(object);
    return kNoObject;
}

Status MemoryMetaStore::remove_version(const ObjectRef& ref, Generation generation) {
    std::lock_guard lock(mutex_);
    auto object = objects_.find(ref);
    if (object == objects_.end()) return fail(Errc::not_found, "no such object: " + describe(ref));
    auto version = object->second.find(generation);
    if (version == object->second.end())
        return fail(Errc::not_found, "no such object version: " + describe(ref));
    release_version_locked(version->second);
    object->second.erase(version);
    if (object->second.empty()) objects_.erase(object);
    return {};
}

// --- Maintenance

Result<std::size_t> MemoryMetaStore::reclaim_expired(Clock::time_point now, std::size_t limit) {
    std::lock_guard lock(mutex_);
    std::size_t reclaimed = 0;
    for (auto it = uploads_.begin(); it != uploads_.end() && reclaimed < limit;) {
        if (!expired(it->second, now)) {
            ++it;
            continue;
        }
        for (const auto& [number, part] : it->second.parts) add_garbage_locked(part.extent);
        it = uploads_.erase(it);
        ++reclaimed;
    }
    return reclaimed;
}

Result<std::vector<GarbageEntry>> MemoryMetaStore::peek_garbage(std::size_t limit) {
    std::lock_guard lock(mutex_);
    std::vector<GarbageEntry> out;
    for (auto it = garbage_.begin(); it != garbage_.end() && out.size() < limit; ++it)
        out.push_back(it->second);
    return out;
}

Status MemoryMetaStore::ack_garbage(const std::vector<std::uint64_t>& ids) {
    std::lock_guard lock(mutex_);
    for (std::uint64_t id : ids) garbage_.erase(id);
    return {};
}

Result<std::vector<ExtentRef>> MemoryMetaStore::extents_in_chunk(ChunkId chunk, std::size_t limit) {
    std::lock_guard lock(mutex_);
    std::vector<ExtentRef> out;
    for (const auto& [ref, versions] : objects_)
        for (const auto& [generation, meta] : versions)
            for (const Part& part : meta.parts) {
                if (part.extent.chunk_id != chunk) continue;
                if (out.size() == limit) return out;
                out.push_back(ExtentRef{ref, generation, part.number, part.extent});
            }
    return out;
}

Result<bool> MemoryMetaStore::has_pending_parts(ChunkId chunk) {
    std::lock_guard lock(mutex_);
    for (const auto& [id, upload] : uploads_)
        for (const auto& [number, part] : upload.parts)
            if (part.extent.chunk_id == chunk) return true;
    return false;
}

Status MemoryMetaStore::relocate_part(const ObjectRef& ref, Generation generation,
                                      std::uint32_t part_number, const Extent& from,
                                      const Extent& to) {
    std::lock_guard lock(mutex_);
    auto object = objects_.find(ref);
    if (object != objects_.end()) {
        auto version = object->second.find(generation);
        if (version != object->second.end())
            for (Part& part : version->second.parts)
                if (part.number == part_number && part.extent == from) {
                    part.extent = to;
                    return {};
                }
    }
    return fail(Errc::aborted, "part no longer at the expected extent");
}

Result<VersionScanPage> MemoryMetaStore::scan_versions(std::optional<std::string_view> cursor,
                                                       std::size_t limit) {
    if (limit == 0) return fail(Errc::invalid_argument, "scan limit must be positive");
    std::optional<std::pair<ObjectRef, Generation>> resume;
    if (cursor) {
        resume = decode_cursor(*cursor);
        if (!resume) return fail(Errc::invalid_argument, "malformed cursor");
    }
    std::lock_guard lock(mutex_);
    VersionScanPage page;
    auto object = resume ? objects_.lower_bound(resume->first) : objects_.begin();
    for (; object != objects_.end(); ++object) {
        auto version = object->second.begin();
        if (resume && object->first == resume->first) version = object->second.upper_bound(resume->second);
        for (; version != object->second.end(); ++version) {
            if (page.versions.size() == limit) {
                const ObjectMeta& last = page.versions.back();
                page.next_cursor = encode_cursor(last.ref, last.generation);
                return page;
            }
            page.versions.push_back(version->second);
        }
    }
    return page;
}

Status MemoryMetaStore::rewrap(const ObjectRef& ref, Generation generation, const Encryption& from,
                               const Encryption& to) {
    std::lock_guard lock(mutex_);
    auto object = objects_.find(ref);
    if (object != objects_.end()) {
        auto version = object->second.find(generation);
        if (version != object->second.end() && version->second.encryption == from) {
            version->second.encryption = to;
            return {};
        }
    }
    return fail(Errc::aborted, "encryption changed concurrently");
}

}  // namespace rmk
