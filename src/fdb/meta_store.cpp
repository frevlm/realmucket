#include <random>
#include <set>

#include "fdb.hpp"
#include "rmk/crc32c.hpp"
#include "rmk/names.hpp"

namespace rmk::fdb {
namespace {

// Receipts only need to outlive the retry loop of the call that wrote them.
constexpr std::chrono::hours kReceiptRetention{1};
constexpr std::size_t kReclaimPartBudget = 4 * kMaxParts;

std::uint64_t random_u64() {
    thread_local std::mt19937_64 rng{std::random_device{}()};
    std::uint64_t v = 0;
    while (v == 0) v = rng();
    return v;
}

std::string be32(std::uint32_t v) {
    std::string out;
    put_be(out, v, 4);
    return out;
}

std::string be64(std::uint64_t v) {
    std::string out;
    put_be(out, v, 8);
    return out;
}

std::string describe(const BucketRef& b) { return b.tenant + "/" + b.bucket; }
std::string describe(const ObjectRef& r) { return describe(r.bucket) + "/" + r.key; }

// Every key of the MetaStore (docs/fdb-layout.md).
struct Keys {
    const std::string& p;

    std::string space(Tag t) const { return p + static_cast<char>(t); }
    std::string tenant(std::string_view t) const { return with(kTenant, t); }
    std::string usage(std::string_view t, char which) const { return with(kUsage, t) + which; }
    std::string bucket(const BucketRef& b) const { return in_bucket(kBucket, b); }
    std::string in_bucket(Tag tag, const BucketRef& b) const {
        std::string k = with(tag, b.tenant);
        put_str(k, b.bucket);
        return k;
    }
    std::string object(Tag tag, const ObjectRef& r) const {
        std::string k = in_bucket(tag, r.bucket);
        put_str(k, r.key);
        return k;
    }
    std::string upload(UploadId id) const { return space(kUpload) + be64(id); }
    std::string upload_header(UploadId id) const { return upload(id) + '\0'; }
    std::string upload_deadline(UploadId id) const { return upload(id) + '\1'; }
    std::string upload_part(UploadId id, std::uint32_t n) const { return upload(id) + '\2' + be32(n); }
    std::string pending_in_chunk(ChunkId chunk) const { return space(kPendingInChunk) + be64(chunk); }
    std::string pending_in_chunk(UploadId id, const Part& part) const {
        return pending_in_chunk(part.extent.chunk_id) + be64(id) + be32(part.number);
    }
    std::string deadline(Clock::time_point t, UploadId id) const { return until(kUploadDeadline, t) + be64(id); }
    std::string until(Tag tag, Clock::time_point t) const {
        std::string k = space(tag);
        put_time(k, t);
        return k;
    }
    std::string upload_by_bucket(const BucketRef& b, UploadId id) const { return in_bucket(kUploadByBucket, b) + be64(id); }
    std::string receipt(Clock::time_point now, std::uint64_t token) const { return until(kReceipt, now) + be64(token); }
    std::string garbage(std::uint64_t id) const { return space(kGarbage) + be64(id); }
    std::string chunk(ChunkId id) const { return space(kChunkExtent) + be64(id); }

private:
    std::string with(Tag tag, std::string_view s) const {
        std::string k = space(tag);
        put_str(k, s);
        return k;
    }
};

// ---- Records

std::string encode_extent(const Extent& e) {
    return Writer().u64(e.chunk_id).u64(e.offset).u64(e.length).u32(e.crc32c).take();
}

Extent read_extent(Reader& r) {
    Extent e;
    e.chunk_id = r.u64();
    e.offset = r.u64();
    e.length = r.u64();
    e.crc32c = r.u32();
    return e;
}

std::optional<Extent> decode_extent(std::string_view v) {
    Reader r(v);
    Extent e = read_extent(r);
    return r.ok() ? std::optional(e) : std::nullopt;
}

std::string encode_part(const Part& part) {
    Writer w;
    w.u64(part.size).u32(part.checksum);
    return w.take() + encode_extent(part.extent);
}

std::optional<Part> decode_part(std::uint32_t number, std::string_view v) {
    Reader r(v);
    Part part;
    part.number = number;
    part.size = r.u64();
    part.checksum = r.u32();
    part.extent = read_extent(r);
    return r.ok() ? std::optional(part) : std::nullopt;
}

std::string encode_header(const ObjectMeta& m) {
    return Writer()
        .u8(m.delete_marker)
        .u64(m.size)
        .u32(m.checksum)
        .time(m.created)
        .u32(m.encryption.master_key_id)
        .str(m.encryption.wrapped_data_key)
        .take();
}

bool decode_header(std::string_view v, ObjectMeta& m) {
    Reader r(v);
    m.delete_marker = r.u8() != 0;
    m.size = r.u64();
    m.checksum = r.u32();
    m.created = r.time();
    m.encryption.master_key_id = r.u32();
    m.encryption.wrapped_data_key = r.str();
    return r.ok();
}

std::string encode_bucket(const BucketInfo& b) {
    return Writer()
        .u8(b.config.versioning)
        .u8(b.config.storage_policy.data_units)
        .u8(b.config.storage_policy.parity_units)
        .take();
}

std::optional<BucketInfo> decode_bucket(const BucketRef& ref, std::string_view v) {
    Reader r(v);
    BucketInfo b;
    b.ref = ref;
    b.config.versioning = r.u8() != 0;
    b.config.storage_policy.data_units = r.u8();
    b.config.storage_policy.parity_units = r.u8();
    return r.ok() ? std::optional(b) : std::nullopt;
}

std::string encode_quota(const Quota& q) { return Writer().u64(q.max_bytes).u64(q.max_objects).take(); }

std::optional<Quota> decode_quota(std::string_view v) {
    Reader r(v);
    Quota q;
    q.max_bytes = r.u64();
    q.max_objects = r.u64();
    return r.ok() ? std::optional(q) : std::nullopt;
}

// ---- Versions. One transaction writes at most one version, so its versionstamp names it:
//   V object stamp -> header;  Y stamp part -> Part;  W stamp -> ObjectRef;
//   X chunk stamp part -> Extent.
// The Object key appears once per version, whatever the number of Parts.

struct Version {
    ObjectMeta meta;
    std::string stamp;
};

std::string stamp_key(const Keys& k, Tag tag, std::string_view stamp) { return k.space(tag) + std::string(stamp); }

std::string part_key(const Keys& k, std::string_view stamp, std::uint32_t number) {
    return stamp_key(k, kVersionPart, stamp) + be32(number);
}

std::string chunk_extent_key(const Keys& k, ChunkId chunk, std::string_view stamp, std::uint32_t number) {
    return k.chunk(chunk) + std::string(stamp) + be32(number);
}

std::string encode_ref(const ObjectRef& r) { return Writer().str(r.bucket.tenant).str(r.bucket.bucket).str(r.key).take(); }

std::optional<ObjectRef> decode_ref(std::string_view v) {
    Reader r(v);
    ObjectRef ref;
    ref.bucket.tenant = r.str();
    ref.bucket.bucket = r.str();
    ref.key = r.str();
    return r.ok() ? std::optional(std::move(ref)) : std::nullopt;
}

// A header key/value of the V space, without Parts.
Result<Version> decode_version(const KeyValue& kv, std::size_t space_size) {
    KeyReader r(std::string_view(kv.key).substr(space_size));
    Version v;
    if (!r.str(v.meta.ref.bucket.tenant) || !r.str(v.meta.ref.bucket.bucket) || !r.str(v.meta.ref.key) ||
        !r.bytes(v.stamp, kStampSize) || !r.rest().empty())
        return corrupt("version key");
    v.meta.generation = stamp_generation(v.stamp);
    if (!decode_header(kv.value, v.meta)) return corrupt("version");
    return v;
}

Result<std::vector<Version>> decode_versions(const std::vector<KeyValue>& kvs, std::size_t space_size) {
    std::vector<Version> out;
    for (const auto& kv : kvs) {
        auto v = decode_version(kv, space_size);
        if (!v) return std::unexpected(v.error());
        out.push_back(std::move(*v));
    }
    return out;
}

// Fills in the Parts of every version, reading all of them concurrently.
Status load_parts(Txn& tx, const Keys& k, std::vector<Version>& versions, bool snapshot = false) {
    std::vector<std::pair<std::string, std::string>> bounds;
    for (const auto& v : versions) {
        std::string base = stamp_key(k, kVersionPart, v.stamp);
        bounds.emplace_back(base, strinc(base));
    }
    auto loaded = tx.ranges(bounds, snapshot);
    if (!loaded) return std::unexpected(loaded.error());
    for (std::size_t i = 0; i < versions.size(); ++i)
        for (const auto& kv : (*loaded)[i]) {
            if (kv.key.size() != bounds[i].first.size() + 4) return corrupt("part key");
            auto part = decode_part(static_cast<std::uint32_t>(get_be(std::string_view(kv.key).substr(bounds[i].first.size()))), kv.value);
            if (!part) return corrupt("part");
            versions[i].meta.parts.push_back(*part);
        }
    return {};
}

// The newest version of an Object: its stamp and whether it is a delete marker.
struct Head {
    std::string stamp;
    bool marker = false;
};

std::string encode_head(std::string_view stamp, bool marker) { return std::string(stamp) + static_cast<char>(marker); }

Result<std::optional<Head>> load_head(Txn& tx, const Keys& k, const ObjectRef& ref) {
    auto v = tx.get(k.object(kHead, ref));
    if (!v) return std::unexpected(v.error());
    if (!*v) return std::nullopt;
    if ((*v)->size() != kStampSize + 1) return corrupt("head");
    return Head{(*v)->substr(0, kStampSize), (*v)->back() != 0};
}

Generation visible(const std::optional<Head>& head) {
    return head && !head->marker ? stamp_generation(head->stamp) : kNoObject;
}

// The version whose key starts with object(kVersion) + `start` (a whole stamp, or the 8-byte
// Generation), with its Parts.
Result<std::optional<Version>> load_version(Txn& tx, const Keys& k, const ObjectRef& ref, std::string_view start) {
    std::string begin = k.object(kVersion, ref) + std::string(start);
    auto kvs = tx.range(begin, strinc(begin), {.limit = 1});
    if (!kvs) return std::unexpected(kvs.error());
    if (kvs->empty()) return std::nullopt;
    auto versions = decode_versions(*kvs, k.p.size() + 1);
    if (!versions) return std::unexpected(versions.error());
    if (auto s = load_parts(tx, k, *versions); !s) return std::unexpected(s.error());
    return std::move(versions->front());
}

Result<std::optional<Version>> find_version(Txn& tx, const Keys& k, const ObjectRef& ref, Generation g) {
    return load_version(tx, k, ref, be64(g));
}

Result<Version> newest_version(Txn& tx, const Keys& k, const ObjectRef& ref, const Head& head) {
    auto v = load_version(tx, k, ref, head.stamp);
    if (!v) return std::unexpected(v.error());
    if (!*v) return corrupt("head without version");
    return std::move(**v);
}

void usage_add(Txn& tx, const Keys& k, std::string_view tenant, std::uint64_t bytes, std::uint64_t objects) {
    tx.add(k.usage(tenant, 'b'), bytes);
    tx.add(k.usage(tenant, 'o'), objects);
}

Result<Usage> load_usage(Txn& tx, const Keys& k, std::string_view tenant) {
    auto v = tx.get_many({k.usage(tenant, 'b'), k.usage(tenant, 'o')}, true);
    if (!v) return std::unexpected(v.error());
    return Usage{(*v)[0] ? from_le64(*(*v)[0]) : 0, (*v)[1] ? from_le64(*(*v)[1]) : 0};
}

Result<std::optional<BucketInfo>> load_bucket(Txn& tx, const Keys& k, const BucketRef& ref) {
    auto v = tx.get(k.bucket(ref));
    if (!v) return std::unexpected(v.error());
    if (!*v) return std::nullopt;
    auto b = decode_bucket(ref, **v);
    if (!b) return corrupt("bucket");
    return b;
}

Result<std::optional<Quota>> load_quota(Txn& tx, const Keys& k, std::string_view tenant) {
    auto v = tx.get(k.tenant(tenant));
    if (!v) return std::unexpected(v.error());
    if (!*v) return std::nullopt;
    auto q = decode_quota(**v);
    if (!q) return corrupt("tenant");
    return q;
}

// Garbage ids: seconds since the epoch in the high 40 bits, random low 24 bits, so the queue
// is roughly FIFO without a shared counter; an id already taken is drawn again.
std::uint64_t garbage_id() {
    auto seconds = std::chrono::duration_cast<std::chrono::seconds>(Clock::now().time_since_epoch()).count();
    return (static_cast<std::uint64_t>(seconds) << 24) | (random_u64() & 0xFFFFFF);
}

Status add_garbage(Txn& tx, const Keys& k, const std::vector<Extent>& extents) {
    std::set<std::uint64_t> drawn;
    std::vector<Extent> pending = extents;
    while (!pending.empty()) {
        std::vector<std::string> keys;
        for (std::size_t i = 0; i < pending.size(); ++i) {
            std::uint64_t id = garbage_id();
            while (!drawn.insert(id).second) id = garbage_id();
            keys.push_back(k.garbage(id));
        }
        auto taken = tx.get_many(keys);
        if (!taken) return std::unexpected(taken.error());
        std::vector<Extent> again;
        for (std::size_t i = 0; i < pending.size(); ++i) {
            if ((*taken)[i]) {
                again.push_back(pending[i]);
                continue;
            }
            tx.set(keys[i], encode_extent(pending[i]));
        }
        pending = std::move(again);
    }
    return {};
}

std::vector<Extent> extents_of(const std::vector<Part>& parts) {
    std::vector<Extent> out;
    for (const auto& p : parts) out.push_back(p.extent);
    return out;
}

// Writes a new version and makes it the head, all keyed by this commit's versionstamp.
void write_version(Txn& tx, const Keys& k, const ObjectMeta& meta) {
    const std::string stamp(kStampSize, '\0');
    std::string header = k.object(kVersion, meta.ref);
    tx.set_stamped_key(header + stamp, header.size(), encode_header(meta));
    const std::size_t at = k.space(kVersionRef).size();
    tx.set_stamped_key(stamp_key(k, kVersionRef, stamp), at, encode_ref(meta.ref));
    for (const Part& part : meta.parts) {
        tx.set_stamped_key(part_key(k, stamp, part.number), at, encode_part(part));
        tx.set_stamped_key(chunk_extent_key(k, part.extent.chunk_id, stamp, part.number), k.chunk(0).size(),
                           encode_extent(part.extent));
    }
    std::string head = k.object(kHead, meta.ref);
    tx.set_stamped_value(head, encode_head(stamp, meta.delete_marker), 0);
    tx.add_conflict(head, true);
}

// Permanent deletion: every key of the version (the Data Key with it) and its chunk index
// entries go; its Extents become garbage; usage drops.
Status destroy_version(Txn& tx, const Keys& k, const Version& v) {
    tx.clear(k.object(kVersion, v.meta.ref) + v.stamp);
    tx.clear(stamp_key(k, kVersionRef, v.stamp));
    std::string parts = stamp_key(k, kVersionPart, v.stamp);
    tx.clear_range(parts, strinc(parts));
    for (const Part& part : v.meta.parts) tx.clear(chunk_extent_key(k, part.extent.chunk_id, v.stamp, part.number));
    if (auto s = add_garbage(tx, k, extents_of(v.meta.parts)); !s) return s;
    if (!v.meta.delete_marker) usage_add(tx, k, v.meta.ref.bucket.tenant, ~v.meta.size + 1, ~0ull);
    return {};
}

// Points the head at the newest remaining version, or removes it.
Status refresh_head(Txn& tx, const Keys& k, const ObjectRef& ref) {
    std::string obj = k.object(kVersion, ref);
    std::string head = k.object(kHead, ref);
    auto last = tx.range(obj, strinc(obj), {.limit = 1, .reverse = true});
    if (!last) return std::unexpected(last.error());
    if (last->empty()) {
        tx.clear(head);
        return {};
    }
    const KeyValue& newest = last->front();
    if (newest.key.size() != obj.size() + kStampSize || newest.value.empty()) return corrupt("version");
    tx.set(head, encode_head(newest.key.substr(obj.size()), newest.value[0] != 0));
    return {};
}

// ---- Pending Uploads: header, deadline and Parts under upload(id).

Result<std::optional<PendingUpload>> parse_upload(const Keys& k, UploadId id, const std::vector<KeyValue>& kvs) {
    std::string base = k.upload(id);
    if (kvs.empty() || kvs.front().key != k.upload_header(id)) return std::nullopt;
    PendingUpload up;
    up.id = id;
    bool has_deadline = false;
    for (const auto& kv : kvs) {
        std::string_view suffix = std::string_view(kv.key).substr(base.size());
        Reader r(kv.value);
        if (suffix == std::string_view("\0", 1)) {
            up.target.bucket.tenant = r.str();
            up.target.bucket.bucket = r.str();
            up.target.key = r.str();
            up.encryption.master_key_id = r.u32();
            up.encryption.wrapped_data_key = r.str();
            if (!r.ok()) return corrupt("upload");
        } else if (suffix == "\1") {
            up.deadline = r.time();
            if (!r.ok()) return corrupt("upload deadline");
            has_deadline = true;
        } else if (suffix.size() == 5 && suffix[0] == '\2') {
            auto number = static_cast<std::uint32_t>(get_be(suffix.substr(1)));
            auto part = decode_part(number, kv.value);
            if (!part) return corrupt("upload part");
            up.parts.emplace(number, *part);
        } else {
            return corrupt("upload key");
        }
    }
    if (!has_deadline) return corrupt("upload without deadline");
    return up;
}

Result<std::optional<PendingUpload>> load_upload(Txn& tx, const Keys& k, UploadId id) {
    std::string base = k.upload(id);
    auto kvs = tx.range(base, strinc(base));
    if (!kvs) return std::unexpected(kvs.error());
    return parse_upload(k, id, *kvs);
}

void clear_upload(Txn& tx, const Keys& k, const PendingUpload& up) {
    std::string base = k.upload(up.id);
    tx.clear_range(base, strinc(base));
    tx.clear(k.deadline(up.deadline, up.id));
    tx.clear(k.upload_by_bucket(up.target.bucket, up.id));
    for (const auto& [number, part] : up.parts) tx.clear(k.pending_in_chunk(up.id, part));
}

// ---- Receipts: let a retried commit find out that its earlier attempt committed.
// Value = versionstamp + 1 if the call's result is that stamp's Generation, 0 for kNoObject.

void write_receipt(Txn& tx, const std::string& key, bool returns_stamp) {
    tx.set_stamped_value(key, std::string(kStampSize, '\0') + static_cast<char>(returns_stamp), 0);
}

Result<std::optional<Generation>> read_receipt(Txn& tx, const std::string& key) {
    auto v = tx.get(key);
    if (!v) return std::unexpected(v.error());
    if (!*v) return std::nullopt;
    if ((*v)->size() != kStampSize + 1) return corrupt("receipt");
    return (*v)->back() ? stamp_generation(**v) : kNoObject;
}

bool exceeds(std::uint64_t used, std::uint64_t add, std::uint64_t max) { return add > max || used > max - add; }

}  // namespace

FdbMetaStore::FdbMetaStore(std::shared_ptr<Database> db, std::string key_prefix,
                           std::chrono::seconds pending_upload_retention)
    : db_(std::move(db)), prefix_(encode_prefix(key_prefix)), retention_(pending_upload_retention) {}

// --- Tenants

Status FdbMetaStore::create_tenant(std::string_view name, const Quota& quota) {
    if (auto valid = validate_tenant_name(name); !valid) return valid;
    Keys k{prefix_};
    return run(*db_, [&](Txn& tx) -> Status {
        auto existing = load_quota(tx, k, name);
        if (!existing) return std::unexpected(existing.error());
        if (*existing) {
            if (tx.maybe_committed && **existing == quota) return {};
            return fail(Errc::already_exists, "tenant exists: " + std::string(name));
        }
        tx.set(k.tenant(name), encode_quota(quota));
        return {};
    });
}

Result<TenantInfo> FdbMetaStore::get_tenant(std::string_view name) {
    Keys k{prefix_};
    return run(*db_, [&](Txn& tx) -> Result<TenantInfo> {
        auto quota = load_quota(tx, k, name);
        if (!quota) return std::unexpected(quota.error());
        if (!*quota) return fail(Errc::not_found, "no such tenant: " + std::string(name));
        auto usage = load_usage(tx, k, name);
        if (!usage) return std::unexpected(usage.error());
        return TenantInfo{std::string(name), **quota, *usage};
    });
}

Result<std::vector<std::string>> FdbMetaStore::list_tenants() {
    Keys k{prefix_};
    return run(*db_, [&](Txn& tx) -> Result<std::vector<std::string>> {
        std::string space = k.space(kTenant);
        auto kvs = tx.range(space, strinc(space));
        if (!kvs) return std::unexpected(kvs.error());
        std::vector<std::string> names;
        for (const auto& kv : *kvs) {
            KeyReader r(std::string_view(kv.key).substr(space.size()));
            if (!r.str(names.emplace_back())) return corrupt("tenant key");
        }
        return names;
    });
}

Status FdbMetaStore::set_quota(std::string_view name, const Quota& quota) {
    Keys k{prefix_};
    return run(*db_, [&](Txn& tx) -> Status {
        auto existing = load_quota(tx, k, name);
        if (!existing) return std::unexpected(existing.error());
        if (!*existing) return fail(Errc::not_found, "no such tenant: " + std::string(name));
        tx.set(k.tenant(name), encode_quota(quota));
        return {};
    });
}

// --- Buckets

Status FdbMetaStore::create_bucket(const BucketRef& bucket, const BucketConfig& config) {
    if (auto valid = validate_tenant_name(bucket.tenant); !valid) return valid;
    if (auto valid = validate_bucket_name(bucket.bucket); !valid) return valid;
    if (auto valid = validate_bucket_config(config); !valid) return valid;
    Keys k{prefix_};
    return run(*db_, [&](Txn& tx) -> Status {
        auto tenant = load_quota(tx, k, bucket.tenant);
        if (!tenant) return std::unexpected(tenant.error());
        if (!*tenant) return fail(Errc::not_found, "no such tenant: " + bucket.tenant);
        auto existing = load_bucket(tx, k, bucket);
        if (!existing) return std::unexpected(existing.error());
        if (*existing) {
            if (tx.maybe_committed && (*existing)->config == config)
                return {};
            return fail(Errc::already_exists, "bucket exists: " + describe(bucket));
        }
        tx.set(k.bucket(bucket), encode_bucket(BucketInfo{bucket, config}));
        return {};
    });
}

Status FdbMetaStore::delete_bucket(const BucketRef& bucket) {
    Keys k{prefix_};
    return run(*db_, [&](Txn& tx) -> Status {
        auto existing = load_bucket(tx, k, bucket);
        if (!existing) return std::unexpected(existing.error());
        if (!*existing) {
            if (tx.maybe_committed) return {};
            return fail(Errc::not_found, "no such bucket: " + describe(bucket));
        }
        std::string objects = k.in_bucket(kHead, bucket);
        auto any_object = tx.range(objects, strinc(objects), {.limit = 1});
        if (!any_object) return std::unexpected(any_object.error());
        if (!any_object->empty()) return fail(Errc::not_empty, "bucket holds objects: " + describe(bucket));
        std::string uploads = k.in_bucket(kUploadByBucket, bucket);
        auto any_upload = tx.range(uploads, strinc(uploads), {.limit = 1});
        if (!any_upload) return std::unexpected(any_upload.error());
        if (!any_upload->empty())
            return fail(Errc::not_empty, "bucket has pending uploads: " + describe(bucket));
        tx.clear(k.bucket(bucket));
        return {};
    });
}

Result<BucketInfo> FdbMetaStore::get_bucket(const BucketRef& bucket) {
    Keys k{prefix_};
    return run(*db_, [&](Txn& tx) -> Result<BucketInfo> {
        auto existing = load_bucket(tx, k, bucket);
        if (!existing) return std::unexpected(existing.error());
        if (!*existing) return fail(Errc::not_found, "no such bucket: " + describe(bucket));
        return std::move(**existing);
    });
}

Result<std::vector<BucketInfo>> FdbMetaStore::list_buckets(std::string_view tenant) {
    Keys k{prefix_};
    return run(*db_, [&](Txn& tx) -> Result<std::vector<BucketInfo>> {
        std::string base = k.space(kBucket);
        put_str(base, tenant);
        auto kvs = tx.range(base, strinc(base));
        if (!kvs) return std::unexpected(kvs.error());
        std::vector<BucketInfo> out;
        for (const auto& kv : *kvs) {
            BucketRef ref{std::string(tenant), {}};
            KeyReader r(std::string_view(kv.key).substr(base.size()));
            if (!r.str(ref.bucket)) return corrupt("bucket key");
            auto b = decode_bucket(ref, kv.value);
            if (!b) return corrupt("bucket");
            out.push_back(std::move(*b));
        }
        return out;
    });
}

Status FdbMetaStore::set_bucket_config(const BucketRef& bucket, const BucketConfig& config) {
    if (auto valid = validate_bucket_config(config); !valid) return valid;
    Keys k{prefix_};
    return run(*db_, [&](Txn& tx) -> Status {
        auto existing = load_bucket(tx, k, bucket);
        if (!existing) return std::unexpected(existing.error());
        if (!*existing) return fail(Errc::not_found, "no such bucket: " + describe(bucket));
        (*existing)->config = config;
        tx.set(k.bucket(bucket), encode_bucket(**existing));
        return {};
    });
}

// --- Writes

Result<UploadId> FdbMetaStore::begin_upload(const ObjectRef& target, const Encryption& encryption,
                                            Clock::time_point now) {
    if (auto valid = validate_object_key(target.key); !valid) return std::unexpected(valid.error());
    Keys k{prefix_};
    const UploadId id = random_u64();  // drawn once, so a retry finds its own earlier commit
    return run(*db_, [&](Txn& tx) -> Result<UploadId> {
        auto bucket = load_bucket(tx, k, target.bucket);
        if (!bucket) return std::unexpected(bucket.error());
        if (!*bucket) return fail(Errc::not_found, "no such bucket: " + describe(target.bucket));
        auto existing = tx.get(k.upload_header(id));
        if (!existing) return std::unexpected(existing.error());
        if (*existing) {
            if (tx.maybe_committed) return id;
            return fail(Errc::aborted, "upload id collision");
        }
        Clock::time_point deadline = now + retention_;
        tx.set(k.upload_header(id), Writer()
                                        .str(target.bucket.tenant)
                                        .str(target.bucket.bucket)
                                        .str(target.key)
                                        .u32(encryption.master_key_id)
                                        .str(encryption.wrapped_data_key)
                                        .take());
        tx.set(k.upload_deadline(id), Writer().time(deadline).take());
        tx.set(k.deadline(deadline, id), "");
        tx.set(k.upload_by_bucket(target.bucket, id), "");
        return id;
    });
}

// Parallel put_part calls on one upload do not conflict: the deadline is read without a
// conflict range, so racing calls may each leave a deadline index entry; reclaim_expired
// drops the stale ones.
Status FdbMetaStore::put_part(UploadId id, const Part& part, Clock::time_point now) {
    Keys k{prefix_};
    return run(*db_, [&](Txn& tx) -> Status {
        auto header = tx.get(k.upload_header(id));
        if (!header) return std::unexpected(header.error());
        if (!*header) return fail(Errc::not_found, "no such upload");
        auto deadline_value = tx.get(k.upload_deadline(id), true);
        if (!deadline_value) return std::unexpected(deadline_value.error());
        if (!*deadline_value) return corrupt("upload without deadline");
        Reader r(**deadline_value);
        Clock::time_point deadline = r.time();
        if (!r.ok()) return corrupt("upload deadline");
        if (deadline <= now) return fail(Errc::expired, "upload expired");
        if (part.number == 0 || part.number > kMaxParts)
            return fail(Errc::invalid_argument, "part numbers run from 1 to kMaxParts");

        std::string key = k.upload_part(id, part.number);
        auto existing = tx.get(key);
        if (!existing) return std::unexpected(existing.error());
        if (*existing) {
            auto old = decode_part(part.number, **existing);
            if (!old) return corrupt("upload part");
            // A retry after commit_unknown_result finds its own Extent here: not garbage.
            if (old->extent != part.extent)
                if (auto s = add_garbage(tx, k, {old->extent}); !s) return s;
            tx.clear(k.pending_in_chunk(id, *old));
        }
        tx.set(key, encode_part(part));
        tx.set(k.pending_in_chunk(id, part), "");
        Clock::time_point next = now + retention_;
        if (next != deadline) {
            tx.clear(k.deadline(deadline, id));
            tx.set(k.deadline(next, id), "");
            tx.set(k.upload_deadline(id), Writer().time(next).take());
        }
        return {};
    });
}

Result<PendingUpload> FdbMetaStore::get_upload(UploadId id, Clock::time_point now) {
    Keys k{prefix_};
    return run(*db_, [&](Txn& tx) -> Result<PendingUpload> {
        auto up = load_upload(tx, k, id);
        if (!up) return std::unexpected(up.error());
        if (!*up) return fail(Errc::not_found, "no such upload");
        if ((*up)->deadline <= now) return fail(Errc::expired, "upload expired");
        return std::move(**up);
    });
}

Status FdbMetaStore::abort_upload(UploadId id) {
    Keys k{prefix_};
    return run(*db_, [&](Txn& tx) -> Status {
        auto up = load_upload(tx, k, id);
        if (!up) return std::unexpected(up.error());
        if (!*up) {
            if (tx.maybe_committed) return {};
            return fail(Errc::not_found, "no such upload");
        }
        std::vector<Extent> extents;
        for (const auto& [number, part] : (*up)->parts) extents.push_back(part.extent);
        if (auto s = add_garbage(tx, k, extents); !s) return s;
        clear_upload(tx, k, **up);
        return {};
    });
}

// Not idempotent by nature (the upload disappears), so the commit leaves a receipt keyed by
// (now, a token drawn once per call); a retry after commit_unknown_result reads it first.
Result<Generation> FdbMetaStore::commit_upload(UploadId id, const Precondition& precondition,
                                               Clock::time_point now) {
    Keys k{prefix_};
    const std::string receipt = k.receipt(now, random_u64());
    Stamp stamp{};
    auto r = run(*db_, [&](Txn& tx) -> Result<std::optional<Generation>> {
        if (tx.maybe_committed) {
            auto prior = read_receipt(tx, receipt);
            if (!prior) return std::unexpected(prior.error());
            if (*prior) return *prior;
        }
        auto loaded = load_upload(tx, k, id);
        if (!loaded) return std::unexpected(loaded.error());
        if (!*loaded) return fail(Errc::not_found, "no such upload");
        const PendingUpload& up = **loaded;
        if (up.deadline <= now) return fail(Errc::expired, "upload expired");

        ObjectMeta meta;
        meta.ref = up.target;
        meta.encryption = up.encryption;
        meta.created = now;
        std::uint32_t expected = 1;
        for (const auto& [number, part] : up.parts) {
            if (number != expected++)
                return fail(Errc::invalid_argument, "parts must be numbered 1..N without gaps");
            meta.checksum = meta.parts.empty() ? part.checksum : crc32c_combine(meta.checksum, part.checksum, part.size);
            meta.size += part.size;
            meta.parts.push_back(part);
        }

        const std::string& tenant = meta.ref.bucket.tenant;
        auto bucket = load_bucket(tx, k, meta.ref.bucket);
        if (!bucket) return std::unexpected(bucket.error());
        auto quota = load_quota(tx, k, tenant);
        if (!quota) return std::unexpected(quota.error());
        if (!*bucket || !*quota) return fail(Errc::not_found, "no such bucket: " + describe(meta.ref.bucket));

        auto head = load_head(tx, k, meta.ref);
        if (!head) return std::unexpected(head.error());
        if (precondition.if_generation_match && *precondition.if_generation_match != visible(*head))
            return fail(Errc::precondition_failed, "generation mismatch: " + describe(meta.ref));

        std::optional<Version> replaced;
        if (!(*bucket)->config.versioning && *head) {
            auto v = newest_version(tx, k, meta.ref, **head);
            if (!v) return std::unexpected(v.error());
            replaced = std::move(*v);
        }
        // Soft quota (ADR 0010): usage is read without a conflict range.
        auto usage = load_usage(tx, k, tenant);
        if (!usage) return std::unexpected(usage.error());
        if (replaced && !replaced->meta.delete_marker) {
            usage->bytes -= replaced->meta.size;
            usage->objects -= 1;
        }
        if (exceeds(usage->bytes, meta.size, (*quota)->max_bytes) || exceeds(usage->objects, 1, (*quota)->max_objects))
            return fail(Errc::quota_exceeded, "tenant quota exceeded: " + tenant);

        if (replaced)
            if (auto s = destroy_version(tx, k, *replaced); !s) return std::unexpected(s.error());
        write_version(tx, k, meta);
        usage_add(tx, k, tenant, meta.size, 1);
        clear_upload(tx, k, up);
        write_receipt(tx, receipt, true);
        return std::nullopt;
    }, &stamp);
    if (!r) return std::unexpected(r.error());
    if (*r) return **r;
    return stamp_generation(std::string_view(reinterpret_cast<const char*>(stamp.data()), stamp.size()));
}

// --- Reads

Result<ObjectMeta> FdbMetaStore::head(const ObjectRef& ref) {
    Keys k{prefix_};
    return run(*db_, [&](Txn& tx) -> Result<ObjectMeta> {
        auto head = load_head(tx, k, ref);
        if (!head) return std::unexpected(head.error());
        if (visible(*head) == kNoObject) return fail(Errc::not_found, "no such object: " + describe(ref));
        auto v = newest_version(tx, k, ref, **head);
        if (!v) return std::unexpected(v.error());
        return std::move(v->meta);
    });
}

Result<ObjectMeta> FdbMetaStore::head_version(const ObjectRef& ref, Generation generation) {
    Keys k{prefix_};
    return run(*db_, [&](Txn& tx) -> Result<ObjectMeta> {
        auto v = find_version(tx, k, ref, generation);
        if (!v) return std::unexpected(v.error());
        if (!*v) return fail(Errc::not_found, "no such object version: " + describe(ref));
        return std::move((*v)->meta);
    });
}

Result<std::vector<ObjectMeta>> FdbMetaStore::list_versions(const ObjectRef& ref) {
    Keys k{prefix_};
    return run(*db_, [&](Txn& tx) -> Result<std::vector<ObjectMeta>> {
        std::string obj = k.object(kVersion, ref);
        auto kvs = tx.range(obj, strinc(obj));
        if (!kvs) return std::unexpected(kvs.error());
        auto versions = decode_versions(*kvs, k.p.size() + 1);
        if (!versions) return std::unexpected(versions.error());
        if (auto s = load_parts(tx, k, *versions); !s) return std::unexpected(s.error());
        std::vector<ObjectMeta> out;
        for (auto v = versions->rbegin(); v != versions->rend(); ++v) out.push_back(std::move(v->meta));
        return out;
    });
}

Result<ListPage> FdbMetaStore::list(const BucketRef& bucket, std::string_view prefix,
                                    std::optional<std::string_view> start_after, std::size_t limit) {
    if (auto valid = validate_list_limit(limit); !valid) return std::unexpected(valid.error());
    Keys k{prefix_};
    return run(*db_, [&](Txn& tx) -> Result<ListPage> {
        auto b = load_bucket(tx, k, bucket);
        if (!b) return std::unexpected(b.error());
        if (!*b) return fail(Errc::not_found, "no such bucket: " + describe(bucket));

        const std::string base = k.in_bucket(kHead, bucket);
        std::string begin = base;
        put_str(begin, prefix);
        begin.pop_back();  // no terminator: every key that starts with `prefix`
        const std::string end = strinc(begin);
        if (start_after && *start_after >= prefix) {
            begin = base;
            put_str(begin, *start_after);
            begin += '\0';  // just after start_after
        }

        // Every examined key counts against the limit, delete markers included, so a page
        // may hold fewer objects than `limit` while next_start_after says more remain. Only
        // version headers are read: a listing carries no Parts.
        std::vector<std::string> headers;
        std::string last;
        std::size_t examined = 0;
        bool more = false;
        std::optional<Error> bad;
        auto s = tx.scan(begin, end, {}, [&](KeyValue&& kv) {
            if (examined == limit) {
                more = true;
                return false;
            }
            std::string key;
            KeyReader r(std::string_view(kv.key).substr(base.size()));
            if (kv.value.size() != kStampSize + 1 || !r.str(key)) {
                bad = corrupt("head").error();
                return false;
            }
            ++examined;
            if (kv.value.back() == 0)
                headers.push_back(k.object(kVersion, ObjectRef{bucket, key}) + kv.value.substr(0, kStampSize));
            last = std::move(key);
            return true;
        });
        if (!s) return std::unexpected(s.error());
        if (bad) return std::unexpected(*bad);

        auto values = tx.get_many(headers);
        if (!values) return std::unexpected(values.error());
        ListPage page;
        for (std::size_t i = 0; i < headers.size(); ++i) {
            if (!(*values)[i]) return corrupt("head without version");
            auto v = decode_version(KeyValue{headers[i], *(*values)[i]}, k.p.size() + 1);
            if (!v) return std::unexpected(v.error());
            page.objects.push_back(std::move(v->meta));
        }
        if (more) page.next_start_after = last;
        return page;
    });
}

// --- Deletes

Result<Generation> FdbMetaStore::remove(const ObjectRef& ref, const Precondition& precondition,
                                        Clock::time_point now) {
    Keys k{prefix_};
    const std::string receipt = k.receipt(now, random_u64());
    Stamp stamp{};
    auto r = run(*db_, [&](Txn& tx) -> Result<std::optional<Generation>> {
        if (tx.maybe_committed) {
            auto prior = read_receipt(tx, receipt);
            if (!prior) return std::unexpected(prior.error());
            if (*prior) return *prior;
        }
        auto head = load_head(tx, k, ref);
        if (!head) return std::unexpected(head.error());
        Generation current = visible(*head);
        if (precondition.if_generation_match && *precondition.if_generation_match != current)
            return fail(Errc::precondition_failed, "generation mismatch: " + describe(ref));
        if (current == kNoObject) return fail(Errc::not_found, "no such object: " + describe(ref));
        auto bucket = load_bucket(tx, k, ref.bucket);
        if (!bucket) return std::unexpected(bucket.error());
        if (!*bucket) return fail(Errc::not_found, "no such bucket: " + describe(ref.bucket));

        if ((*bucket)->config.versioning) {
            ObjectMeta marker;
            marker.ref = ref;
            marker.delete_marker = true;
            marker.created = now;
            write_version(tx, k, marker);
            write_receipt(tx, receipt, true);
            return std::nullopt;
        }
        auto v = newest_version(tx, k, ref, **head);
        if (!v) return std::unexpected(v.error());
        if (auto s = destroy_version(tx, k, *v); !s) return std::unexpected(s.error());
        if (auto s = refresh_head(tx, k, ref); !s) return std::unexpected(s.error());
        write_receipt(tx, receipt, false);
        return Generation{kNoObject};
    }, &stamp);
    if (!r) return std::unexpected(r.error());
    if (*r) return **r;
    return stamp_generation(std::string_view(reinterpret_cast<const char*>(stamp.data()), stamp.size()));
}

Status FdbMetaStore::remove_version(const ObjectRef& ref, Generation generation) {
    Keys k{prefix_};
    return run(*db_, [&](Txn& tx) -> Status {
        auto v = find_version(tx, k, ref, generation);
        if (!v) return std::unexpected(v.error());
        if (!*v) {
            if (tx.maybe_committed) return {};
            return fail(Errc::not_found, "no such object version: " + describe(ref));
        }
        auto head = load_head(tx, k, ref);
        if (!head) return std::unexpected(head.error());
        if (auto s = destroy_version(tx, k, **v); !s) return s;
        if (*head && (*head)->stamp == (*v)->stamp) return refresh_head(tx, k, ref);
        return {};
    });
}

// --- Maintenance

// Bounded per transaction: at most `limit` due entries and about kReclaimPartBudget Parts
// (always at least one upload; kMaxParts keeps one small enough). Receipts are cleared in a
// transaction of their own, so a large backlog never holds them up.
Result<std::size_t> FdbMetaStore::reclaim_expired(Clock::time_point now, std::size_t limit) {
    if (limit == 0) return 0;
    Keys k{prefix_};
    auto receipts = run(*db_, [&](Txn& tx) -> Status {
        tx.clear_range(k.space(kReceipt), k.until(kReceipt, now - kReceiptRetention));
        return {};
    });
    if (!receipts) return std::unexpected(receipts.error());
    return run(*db_, [&](Txn& tx) -> Result<std::size_t> {
        std::string space = k.space(kUploadDeadline);
        auto due = tx.range(space, strinc(k.until(kUploadDeadline, now)), {.limit = limit});
        if (!due) return std::unexpected(due.error());
        std::vector<UploadId> ids;
        std::vector<std::pair<std::string, std::string>> bounds;
        for (const auto& entry : *due) {
            if (entry.key.size() != space.size() + 16) return corrupt("deadline key");
            ids.push_back(get_be(std::string_view(entry.key).substr(space.size() + 8)));
            std::string base = k.upload(ids.back());
            bounds.emplace_back(base, strinc(base));
        }
        auto loaded = tx.ranges(bounds);
        if (!loaded) return std::unexpected(loaded.error());

        std::size_t reclaimed = 0;
        std::size_t parts = 0;
        std::vector<Extent> extents;
        for (std::size_t i = 0; i < ids.size(); ++i) {
            auto up = parse_upload(k, ids[i], (*loaded)[i]);
            if (!up) return std::unexpected(up.error());
            if (*up && (*up)->deadline <= now) {
                if (reclaimed > 0 && parts + (*up)->parts.size() > kReclaimPartBudget) break;
                parts += (*up)->parts.size();
                for (const auto& [number, part] : (*up)->parts) extents.push_back(part.extent);
                clear_upload(tx, k, **up);
                ++reclaimed;
            }
            tx.clear((*due)[i].key);  // reclaimed, or stale: the upload is gone or was extended
        }
        if (auto s = add_garbage(tx, k, extents); !s) return std::unexpected(s.error());
        return reclaimed;
    });
}

Result<std::vector<GarbageEntry>> FdbMetaStore::peek_garbage(std::size_t limit) {
    if (limit == 0) return std::vector<GarbageEntry>{};
    Keys k{prefix_};
    return run(*db_, [&](Txn& tx) -> Result<std::vector<GarbageEntry>> {
        std::string space = k.space(kGarbage);
        auto kvs = tx.range(space, strinc(space), {.limit = limit});
        if (!kvs) return std::unexpected(kvs.error());
        std::vector<GarbageEntry> out;
        for (const auto& kv : *kvs) {
            auto extent = decode_extent(kv.value);
            if (!extent || kv.key.size() != space.size() + 8) return corrupt("garbage");
            out.push_back(GarbageEntry{get_be(std::string_view(kv.key).substr(space.size())), *extent});
        }
        return out;
    });
}

Status FdbMetaStore::ack_garbage(const std::vector<std::uint64_t>& ids) {
    Keys k{prefix_};
    return run(*db_, [&](Txn& tx) -> Status {
        for (std::uint64_t id : ids) tx.clear(k.garbage(id));
        return {};
    });
}

Result<std::vector<ExtentRef>> FdbMetaStore::extents_in_chunk(ChunkId chunk, std::size_t limit) {
    if (limit == 0) return std::vector<ExtentRef>{};
    Keys k{prefix_};
    return run(*db_, [&](Txn& tx) -> Result<std::vector<ExtentRef>> {
        std::string base = k.chunk(chunk);
        auto kvs = tx.range(base, strinc(base), {.limit = limit});
        if (!kvs) return std::unexpected(kvs.error());
        std::vector<ExtentRef> out;
        std::vector<std::string> refs;
        for (const auto& kv : *kvs) {
            auto extent = decode_extent(kv.value);
            if (kv.key.size() != base.size() + kStampSize + 4 || !extent) return corrupt("chunk extent");
            std::string_view rest = std::string_view(kv.key).substr(base.size());
            ExtentRef e;
            e.generation = stamp_generation(rest);
            e.part_number = static_cast<std::uint32_t>(get_be(rest.substr(kStampSize)));
            e.extent = *extent;
            out.push_back(std::move(e));
            refs.push_back(stamp_key(k, kVersionRef, rest.substr(0, kStampSize)));
        }
        auto values = tx.get_many(refs);
        if (!values) return std::unexpected(values.error());
        for (std::size_t i = 0; i < out.size(); ++i) {
            auto ref = (*values)[i] ? decode_ref(*(*values)[i]) : std::nullopt;
            if (!ref) return corrupt("version ref");
            out[i].ref = std::move(*ref);
        }
        return out;
    });
}

Result<bool> FdbMetaStore::has_pending_parts(ChunkId chunk) {
    Keys k{prefix_};
    return run(*db_, [&](Txn& tx) -> Result<bool> {
        std::string base = k.pending_in_chunk(chunk);
        auto any = tx.range(base, strinc(base), {.limit = 1});
        if (!any) return std::unexpected(any.error());
        return !any->empty();
    });
}

Status FdbMetaStore::relocate_part(const ObjectRef& ref, Generation generation, std::uint32_t part_number,
                                   const Extent& from, const Extent& to) {
    Keys k{prefix_};
    return run(*db_, [&](Txn& tx) -> Status {
        std::string first = k.object(kVersion, ref) + be64(generation);
        auto header = tx.range(first, strinc(first), {.limit = 1});
        if (!header) return std::unexpected(header.error());
        if (header->empty()) return fail(Errc::aborted, "part no longer at the expected extent");
        std::string stamp = header->front().key.substr(first.size() - 8, kStampSize);
        std::string key = part_key(k, stamp, part_number);
        auto value = tx.get(key);
        if (!value) return std::unexpected(value.error());
        if (!*value) return fail(Errc::aborted, "part no longer at the expected extent");
        auto part = decode_part(part_number, **value);
        if (!part) return corrupt("part");
        if (tx.maybe_committed && part->extent == to) return {};
        if (part->extent != from) return fail(Errc::aborted, "part no longer at the expected extent");
        part->extent = to;
        tx.set(key, encode_part(*part));
        tx.clear(chunk_extent_key(k, from.chunk_id, stamp, part_number));
        tx.set(chunk_extent_key(k, to.chunk_id, stamp, part_number), encode_extent(to));
        return {};
    });
}

// Cursor = the last returned version's header key, relative to the version space.
Result<VersionScanPage> FdbMetaStore::scan_versions(std::optional<std::string_view> cursor, std::size_t limit) {
    if (limit == 0) return fail(Errc::invalid_argument, "scan limit must be positive");
    Keys k{prefix_};
    const std::string space = k.space(kVersion);
    std::string begin = space;
    if (cursor) {
        std::string resume = space + std::string(*cursor);
        if (!decode_version(KeyValue{resume, encode_header({})}, space.size()))
            return fail(Errc::invalid_argument, "malformed cursor");
        begin = resume + '\0';
    }
    return run(*db_, [&](Txn& tx) -> Result<VersionScanPage> {
        auto kvs = tx.range(begin, strinc(space), {.limit = limit + 1, .snapshot = true});
        if (!kvs) return std::unexpected(kvs.error());
        bool more = kvs->size() > limit;
        if (more) kvs->pop_back();
        auto versions = decode_versions(*kvs, space.size());
        if (!versions) return std::unexpected(versions.error());
        if (auto s = load_parts(tx, k, *versions, true); !s) return std::unexpected(s.error());
        VersionScanPage page;
        if (more) page.next_cursor = kvs->back().key.substr(space.size());
        for (auto& v : *versions) page.versions.push_back(std::move(v.meta));
        return page;
    });
}

Status FdbMetaStore::rewrap(const ObjectRef& ref, Generation generation, const Encryption& from,
                            const Encryption& to) {
    Keys k{prefix_};
    return run(*db_, [&](Txn& tx) -> Status {
        std::string first = k.object(kVersion, ref) + be64(generation);
        auto header = tx.range(first, strinc(first), {.limit = 1});
        if (!header) return std::unexpected(header.error());
        if (header->empty()) return fail(Errc::aborted, "encryption changed concurrently");
        ObjectMeta meta;
        if (!decode_header(header->front().value, meta)) return corrupt("version");
        if (tx.maybe_committed && meta.encryption == to) return {};
        if (meta.encryption != from) return fail(Errc::aborted, "encryption changed concurrently");
        meta.encryption = to;
        tx.set(header->front().key, encode_header(meta));
        return {};
    });
}

}  // namespace rmk::fdb
