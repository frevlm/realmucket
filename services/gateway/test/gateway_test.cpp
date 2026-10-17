// The Gateway end to end inside one process: in-memory stores, three Data Nodes on loopback,
// a hand-made open-Chunk pool (no Controller here) and a raw rmk.v1.Storage stub as client.

#include "rmk/gateway/gateway.hpp"

#include <gtest/gtest.h>
#include <stdlib.h>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <functional>
#include <fstream>
#include <iterator>
#include <memory>
#include <optional>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "rmk/core/chunk_file.hpp"
#include "rmk/core/crc32c.hpp"
#include "rmk/datanode/datanode.hpp"
#include "rmk/http/http.hpp"
#include "rmk/stores/memory_stores.hpp"
#include "rmk/v1/datanode.grpc.pb.h"
#include "rmk/v1/storage.grpc.pb.h"

namespace {

namespace fs = std::filesystem;
namespace pb = rmk::v1;
using grpc::StatusCode;

constexpr std::uint64_t kPartSize = 64 << 10;
constexpr std::uint64_t kChunkCapacity = 8 << 20;
constexpr int kNodes = 3;
constexpr int kChunks = 6;

std::string random_text(std::size_t n, unsigned seed) {
    std::mt19937 rng(seed);
    std::string out(n, '\0');
    for (auto& c : out) c = static_cast<char>(rng() & 0xFF);
    return out;
}

std::uint32_t crc_of(const std::string& s) {
    return rmk::crc32c(std::as_bytes(std::span(s.data(), s.size())));
}

pb::ObjectLocation loc(const std::string& bucket, const std::string& key, const std::string& tenant = "alpha") {
    pb::ObjectLocation l;
    l.set_tenant(tenant);
    l.set_bucket(bucket);
    l.set_key(key);
    return l;
}

// ----- A MetaStore that forwards to another one, with fault and timing hooks.

class HookedMetaStore final : public rmk::MetaStore {
public:
    explicit HookedMetaStore(std::shared_ptr<rmk::MetaStore> inner) : inner_(std::move(inner)) {}
    bool fail_put_part = false;
    std::function<void(const rmk::ObjectMeta&)> after_head;  // runs once

    rmk::Status create_tenant(std::string_view n, const rmk::Quota& q) override { return inner_->create_tenant(n, q); }
    rmk::Result<rmk::TenantInfo> get_tenant(std::string_view n) override { return inner_->get_tenant(n); }
    rmk::Result<std::vector<std::string>> list_tenants() override { return inner_->list_tenants(); }
    rmk::Status set_quota(std::string_view n, const rmk::Quota& q) override { return inner_->set_quota(n, q); }
    rmk::Status create_bucket(const rmk::BucketRef& b, const rmk::BucketConfig& c) override {
        return inner_->create_bucket(b, c);
    }
    rmk::Status delete_bucket(const rmk::BucketRef& b) override { return inner_->delete_bucket(b); }
    rmk::Result<rmk::BucketInfo> get_bucket(const rmk::BucketRef& b) override { return inner_->get_bucket(b); }
    rmk::Result<std::vector<rmk::BucketInfo>> list_buckets(std::string_view t) override {
        return inner_->list_buckets(t);
    }
    rmk::Status set_bucket_config(const rmk::BucketRef& b, const rmk::BucketConfig& c) override {
        return inner_->set_bucket_config(b, c);
    }
    rmk::Result<rmk::UploadId> begin_upload(const rmk::ObjectRef& r, const rmk::Encryption& e,
                                            rmk::Clock::time_point now) override {
        return inner_->begin_upload(r, e, now);
    }
    rmk::Status put_part(rmk::UploadId id, const rmk::Part& part, rmk::Clock::time_point now) override {
        if (fail_put_part) return rmk::fail(rmk::Errc::io_error, "injected put_part failure");
        return inner_->put_part(id, part, now);
    }
    rmk::Result<rmk::PendingUpload> get_upload(rmk::UploadId id, rmk::Clock::time_point now) override {
        return inner_->get_upload(id, now);
    }
    rmk::Status abort_upload(rmk::UploadId id) override { return inner_->abort_upload(id); }
    rmk::Result<rmk::Generation> commit_upload(rmk::UploadId id, const rmk::Precondition& p,
                                               rmk::Clock::time_point now) override {
        return inner_->commit_upload(id, p, now);
    }
    rmk::Result<rmk::ObjectMeta> head(const rmk::ObjectRef& r) override {
        auto out = inner_->head(r);
        if (out && after_head) std::exchange(after_head, nullptr)(*out);
        return out;
    }
    rmk::Result<rmk::ObjectMeta> head_version(const rmk::ObjectRef& r, rmk::Generation g) override {
        return inner_->head_version(r, g);
    }
    rmk::Result<std::vector<rmk::ObjectMeta>> list_versions(const rmk::ObjectRef& r) override {
        return inner_->list_versions(r);
    }
    rmk::Result<rmk::ListPage> list(const rmk::BucketRef& b, std::string_view prefix,
                                    std::optional<std::string_view> start_after, std::size_t limit) override {
        return inner_->list(b, prefix, start_after, limit);
    }
    rmk::Result<rmk::Generation> remove(const rmk::ObjectRef& r, const rmk::Precondition& p,
                                        rmk::Clock::time_point now) override {
        return inner_->remove(r, p, now);
    }
    rmk::Status remove_version(const rmk::ObjectRef& r, rmk::Generation g) override {
        return inner_->remove_version(r, g);
    }
    rmk::Result<std::size_t> reclaim_expired(rmk::Clock::time_point now, std::size_t limit) override {
        return inner_->reclaim_expired(now, limit);
    }
    rmk::Result<std::vector<rmk::GarbageEntry>> peek_garbage(std::size_t limit) override {
        return inner_->peek_garbage(limit);
    }
    rmk::Status ack_garbage(const std::vector<std::uint64_t>& ids) override { return inner_->ack_garbage(ids); }
    rmk::Result<std::vector<rmk::ExtentRef>> extents_in_chunk(rmk::ChunkId c, std::size_t limit) override {
        return inner_->extents_in_chunk(c, limit);
    }
    rmk::Result<bool> has_pending_parts(rmk::ChunkId c) override { return inner_->has_pending_parts(c); }
    rmk::Status relocate_part(const rmk::ObjectRef& r, rmk::Generation g, std::uint32_t n, const rmk::Extent& from,
                              const rmk::Extent& to) override {
        return inner_->relocate_part(r, g, n, from, to);
    }
    rmk::Result<rmk::VersionScanPage> scan_versions(std::optional<std::string_view> cursor,
                                                    std::size_t limit) override {
        return inner_->scan_versions(cursor, limit);
    }
    rmk::Status rewrap(const rmk::ObjectRef& r, rmk::Generation g, const rmk::Encryption& from,
                       const rmk::Encryption& to) override {
        return inner_->rewrap(r, g, from, to);
    }

private:
    std::shared_ptr<rmk::MetaStore> inner_;
};

// ----- One Data Node serving a temporary directory on 127.0.0.1.

struct DataNode {
    std::unique_ptr<rmk::datanode::Node> node;
    std::unique_ptr<grpc::Server> server;
    std::string id;
    std::string address;
    fs::path dir;
};

class GatewayTest : public ::testing::Test {
protected:
    fs::path root;
    std::shared_ptr<rmk::MemoryMetaStore> meta = std::make_shared<rmk::MemoryMetaStore>(std::chrono::hours{1});
    std::shared_ptr<rmk::MemoryConfigStore> config = std::make_shared<rmk::MemoryConfigStore>();
    std::shared_ptr<rmk::MemoryClusterStore> cluster = std::make_shared<rmk::MemoryClusterStore>();
    std::shared_ptr<rmk::rpc::Channels> channels;
    std::vector<DataNode> nodes;
    rmk::gateway::Options options;
    std::shared_ptr<rmk::MetaStore> gateway_meta;  // what the Gateway sees; null: `meta` itself
    std::unique_ptr<rmk::gateway::Gateway> gateway;
    std::unique_ptr<grpc::Server> server;
    std::unique_ptr<pb::Storage::Stub> stub;

    void SetUp() override {
        std::string pattern = (fs::temp_directory_path() / "rmk-gateway-XXXXXX").string();
        ASSERT_NE(mkdtemp(pattern.data()), nullptr);
        root = pattern;
        rmk::rpc::TlsConfig tls;
        tls.insecure = true;
        channels = std::make_shared<rmk::rpc::Channels>(tls);

        for (int i = 0; i < kNodes; ++i) start_node(i);
        for (int i = 0; i < kChunks; ++i) add_open_chunk();

        options.part_size = kPartSize;
        options.cluster_config_cache = std::chrono::seconds{0};
        start_gateway();

        ASSERT_TRUE(create_tenant("alpha").ok());
        ASSERT_TRUE(create_tenant("beta").ok());
        ASSERT_TRUE(create_bucket("data").ok());
    }

    void start_gateway() {
        rmk::gateway::Dependencies deps;
        deps.stores = {gateway_meta ? gateway_meta : meta, config, cluster};
        rmk::crypto::Key master{};
        master[0] = std::byte{42};
        deps.keys = std::make_shared<rmk::crypto::StaticKeyProvider>(std::map<std::uint32_t, rmk::crypto::Key>{{1, master}}, 1);
        rmk::rpc::WriterOptions writer_options;
        writer_options.writer_id = "gateway-test";
        writer_options.writers_per_policy = 2;
        deps.writer = std::make_shared<rmk::rpc::ChunkWriter>(cluster, channels, writer_options);
        deps.reader = std::make_shared<rmk::rpc::ChunkReader>(cluster, channels);
        deps.garbage = cluster;
        auto created = rmk::gateway::Gateway::create(options, std::move(deps));
        ASSERT_TRUE(created.has_value()) << created.error().message;
        gateway = std::move(*created);

        grpc::ServerBuilder builder;
        int port = 0;
        builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
        builder.RegisterService(gateway->service());
        builder.SetMaxReceiveMessageSize(32 << 20);
        server = builder.BuildAndStart();
        ASSERT_NE(port, 0);
        stub = pb::Storage::NewStub(
            grpc::CreateChannel("127.0.0.1:" + std::to_string(port), grpc::InsecureChannelCredentials()));
    }

    // Applies changed `options` / `gateway_meta`; the stores keep their contents.
    void restart_gateway() {
        server->Shutdown(std::chrono::system_clock::now());
        server.reset();
        gateway.reset();
        start_gateway();
    }

    std::uint64_t garbage_bytes() {
        std::uint64_t total = 0;
        auto chunks = cluster->list_chunks(std::nullopt, std::nullopt, 1000);
        EXPECT_TRUE(chunks.has_value());
        for (const auto& chunk : *chunks) total += chunk.garbage_bytes;
        return total;
    }

    void delete_chunk(rmk::ChunkId id) {
        for (const auto& n : nodes) {
            auto dn = rmk::internal::v1::DataNode::NewStub(channels->get(n.address));
            rmk::internal::v1::FileRef file;
            file.set_chunk_id(id);
            rmk::internal::v1::Empty resp;
            grpc::ClientContext ctx;
            ASSERT_TRUE(dn->DeleteChunk(&ctx, file, &resp).ok());
        }
        ASSERT_TRUE(cluster->delete_chunk(id).has_value());
    }

    void TearDown() override {
        if (server) server->Shutdown(std::chrono::system_clock::now());
        server.reset();
        gateway.reset();
        for (auto& n : nodes) {
            n.server->Shutdown(std::chrono::system_clock::now());
            n.server.reset();
            n.node.reset();
        }
        std::error_code ec;
        fs::remove_all(root, ec);
    }

    void start_node(int i) {
        DataNode n;
        n.id = "dn-" + std::to_string(i);
        n.dir = root / n.id;
        fs::create_directories(n.dir);
        rmk::datanode::Options options;
        options.identity.id = n.id;
        options.identity.host = "host-" + std::to_string(i);
        options.identity.address = "unbound";  // replaced below, once the port is known
        options.identity.capacity_bytes = std::uint64_t{1} << 30;
        options.data_dir = n.dir;
        auto node = rmk::datanode::Node::create(options, cluster, channels);
        ASSERT_TRUE(node.has_value()) << node.error().message;
        n.node = std::move(*node);
        grpc::ServerBuilder builder;
        int port = 0;
        builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
        builder.RegisterService(n.node->service());
        n.server = builder.BuildAndStart();
        ASSERT_NE(port, 0);
        n.address = "127.0.0.1:" + std::to_string(port);
        // The port is only known now, so the membership record is written here.
        rmk::NodeInfo info = options.identity;
        info.address = n.address;
        ASSERT_TRUE(cluster->heartbeat(info, rmk::Clock::now()).has_value());
        nodes.push_back(std::move(n));
    }

    // What the Controller would do: a Chunk replicated on every node, registered as open.
    void add_open_chunk() {
        auto id = cluster->allocate_chunk_id();
        ASSERT_TRUE(id.has_value());
        rmk::ChunkInfo chunk;
        chunk.id = *id;
        chunk.capacity = kChunkCapacity;
        chunk.size = rmk::kChunkHeaderSize;
        for (const auto& n : nodes) {
            auto dn = rmk::internal::v1::DataNode::NewStub(channels->get(n.address));
            rmk::internal::v1::CreateChunkRequest req;
            req.mutable_file()->set_chunk_id(*id);
            req.set_capacity(kChunkCapacity);
            req.set_data_units(1);
            req.set_parity_units(2);
            rmk::internal::v1::Empty resp;
            grpc::ClientContext ctx;
            auto status = dn->CreateChunk(&ctx, req, &resp);
            ASSERT_TRUE(status.ok()) << status.error_message();
            chunk.nodes.push_back(n.id);
        }
        ASSERT_TRUE(cluster->put_chunk(chunk).has_value());
    }

    // ----- Client helpers.

    static std::unique_ptr<grpc::ClientContext> ctx(const std::string& sdk = "1.0.0") {
        auto c = std::make_unique<grpc::ClientContext>();
        if (!sdk.empty()) c->AddMetadata("x-rmk-sdk-version", sdk);
        return c;
    }

    grpc::Status create_tenant(const std::string& name, std::uint64_t max_bytes = 0, std::uint64_t max_objects = 0) {
        pb::CreateTenantRequest req;
        req.set_name(name);
        req.mutable_quota()->set_max_bytes(max_bytes);
        req.mutable_quota()->set_max_objects(max_objects);
        pb::TenantInfo resp;
        return stub->CreateTenant(ctx().get(), req, &resp);
    }

    grpc::Status create_bucket(const std::string& bucket, bool versioning = false,
                               const std::string& tenant = "alpha") {
        pb::CreateBucketRequest req;
        req.set_tenant(tenant);
        req.set_bucket(bucket);
        req.mutable_config()->set_versioning(versioning);
        pb::BucketInfo resp;
        return stub->CreateBucket(ctx().get(), req, &resp);
    }

    grpc::Status put(const pb::ObjectLocation& where, const std::string& data,
                     std::optional<std::uint64_t> if_match = std::nullopt, pb::ObjectInfo* info = nullptr) {
        auto c = ctx();
        pb::PutObjectResponse resp;
        auto writer = stub->PutObject(c.get(), &resp);
        pb::PutObjectRequest msg;
        *msg.mutable_header()->mutable_location() = where;
        if (if_match) msg.mutable_header()->mutable_precondition()->set_if_generation_match(*if_match);
        writer->Write(msg);
        // Uneven message sizes, so Part boundaries never line up with messages.
        for (std::size_t at = 0; at < data.size(); at += 50'000) {
            pb::PutObjectRequest chunk;
            chunk.set_data(data.substr(at, 50'000));
            if (!writer->Write(chunk)) break;
        }
        writer->WritesDone();
        auto status = writer->Finish();
        if (info) *info = resp.object();
        return status;
    }

    grpc::Status get(const pb::ObjectLocation& where, std::string* data,
                     std::uint64_t offset = 0, std::uint64_t length = 0, std::uint64_t generation = 0,
                     pb::ObjectInfo* info = nullptr) {
        auto c = ctx();
        pb::GetObjectRequest req;
        *req.mutable_location() = where;
        req.set_offset(offset);
        req.set_length(length);
        req.set_generation(generation);
        auto reader = stub->GetObject(c.get(), req);
        pb::GetObjectResponse msg;
        data->clear();
        bool first = true;
        while (reader->Read(&msg)) {
            if (first) {
                EXPECT_TRUE(msg.has_info());
                if (info) *info = msg.info();
                first = false;
                continue;
            }
            EXPECT_LE(msg.data().size(), std::size_t{1} << 20);
            data->append(msg.data());
        }
        return reader->Finish();
    }

    grpc::Status remove(const pb::ObjectLocation& where, std::uint64_t generation = 0,
                        pb::DeleteObjectResponse* out = nullptr, std::optional<std::uint64_t> if_match = std::nullopt) {
        pb::DeleteObjectRequest req;
        *req.mutable_location() = where;
        req.set_generation(generation);
        if (if_match) req.mutable_precondition()->set_if_generation_match(*if_match);
        pb::DeleteObjectResponse resp;
        auto status = stub->DeleteObject(ctx().get(), req, &resp);
        if (out) *out = resp;
        return status;
    }

    grpc::Status upload_part(std::uint64_t upload, std::uint32_t number,
                             const std::string& data, pb::PartInfo* part = nullptr) {
        auto c = ctx();
        pb::UploadPartResponse resp;
        auto writer = stub->UploadPart(c.get(), &resp);
        pb::UploadPartRequest msg;
        msg.mutable_header()->set_upload_id(upload);
        msg.mutable_header()->set_part_number(number);
        writer->Write(msg);
        pb::UploadPartRequest body;
        body.set_data(data);
        writer->Write(body);
        writer->WritesDone();
        auto status = writer->Finish();
        if (part) *part = resp.part();
        return status;
    }

    std::string chunk_files_content() {
        std::string all;
        for (const auto& n : nodes)
            for (const auto& entry : fs::directory_iterator(n.dir)) {
                std::ifstream in(entry.path(), std::ios::binary);
                all.append(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
            }
        return all;
    }
};

// ----- Administration

TEST_F(GatewayTest, TenantsAndBuckets) {
    pb::TenantInfo tenant;
    pb::GetTenantRequest get;
    get.set_name("alpha");
    ASSERT_TRUE(stub->GetTenant(ctx().get(), get, &tenant).ok());
    EXPECT_EQ(tenant.name(), "alpha");
    EXPECT_EQ(tenant.quota().max_bytes(), 0u);  // unlimited

    EXPECT_TRUE(create_bucket("logs", false, "beta").ok());
    EXPECT_EQ(create_bucket("logs", false, "beta").error_code(), StatusCode::ALREADY_EXISTS);
    EXPECT_EQ(create_bucket("logs", false, "gamma").error_code(), StatusCode::NOT_FOUND);
    EXPECT_EQ(create_tenant("Bad Name").error_code(), StatusCode::INVALID_ARGUMENT);

    pb::ListBucketsRequest list;
    list.set_tenant("beta");
    pb::ListBucketsResponse buckets;
    ASSERT_TRUE(stub->ListBuckets(ctx().get(), list, &buckets).ok());
    ASSERT_EQ(buckets.buckets_size(), 1);
    EXPECT_EQ(buckets.buckets(0).bucket(), "logs");
}

TEST_F(GatewayTest, EveryRequestNamesItsTenant) {
    auto created = create_bucket("nameless", false, "");
    EXPECT_EQ(created.error_code(), StatusCode::INVALID_ARGUMENT);
    EXPECT_NE(created.error_message().find("tenant is required"), std::string::npos) << created.error_message();

    pb::GetTenantRequest req;
    pb::TenantInfo tenant;
    EXPECT_EQ(stub->GetTenant(ctx().get(), req, &tenant).error_code(), StatusCode::INVALID_ARGUMENT);
    EXPECT_EQ(put(loc("data", "k", ""), "x").error_code(), StatusCode::INVALID_ARGUMENT);
}

TEST_F(GatewayTest, TenantsAreSeparateNamespaces) {
    ASSERT_TRUE(create_bucket("data", false, "beta").ok());
    ASSERT_TRUE(put(loc("data", "k", "alpha"), "alpha bytes").ok());
    ASSERT_TRUE(put(loc("data", "k", "beta"), "beta bytes").ok());

    std::string data;
    ASSERT_TRUE(get(loc("data", "k", "alpha"), &data).ok());
    EXPECT_EQ(data, "alpha bytes");
    ASSERT_TRUE(get(loc("data", "k", "beta"), &data).ok());
    EXPECT_EQ(data, "beta bytes");
    EXPECT_EQ(get(loc("data", "k", "gamma"), &data).error_code(), StatusCode::NOT_FOUND);
}

TEST_F(GatewayTest, MinimumSdkVersion) {
    pb::ClusterConfig cfg;
    cfg.set_min_sdk_version("2.0.0");
    pb::ClusterConfig out;
    ASSERT_TRUE(stub->SetClusterConfig(ctx("2.0.0").get(), cfg, &out).ok());

    pb::GetTenantRequest req;
    req.set_name("alpha");
    pb::TenantInfo tenant;
    auto old = stub->GetTenant(ctx("1.9.3").get(), req, &tenant);
    EXPECT_EQ(old.error_code(), StatusCode::FAILED_PRECONDITION);
    EXPECT_NE(old.error_message().find("1.9.3"), std::string::npos) << old.error_message();
    EXPECT_NE(old.error_message().find("2.0.0"), std::string::npos) << old.error_message();
    EXPECT_NE(old.error_message().find("upgrade"), std::string::npos) << old.error_message();

    auto missing = stub->GetTenant(ctx("").get(), req, &tenant);
    EXPECT_EQ(missing.error_code(), StatusCode::FAILED_PRECONDITION);
    EXPECT_NE(missing.error_message().find("0.0.0"), std::string::npos) << missing.error_message();

    EXPECT_TRUE(stub->GetTenant(ctx("2.1.0").get(), req, &tenant).ok());

    cfg.set_min_sdk_version("two");
    EXPECT_EQ(stub->SetClusterConfig(ctx("2.0.0").get(), cfg, &out).error_code(), StatusCode::INVALID_ARGUMENT);
}

// ----- Data path

TEST_F(GatewayTest, PutGetRoundTrip) {
    pb::ObjectInfo info;
    ASSERT_TRUE(put(loc("data", "small"), "hello", std::nullopt, &info).ok());
    EXPECT_EQ(info.size(), 5u);
    EXPECT_EQ(info.checksum(), crc_of("hello"));
    EXPECT_EQ(info.tenant(), "alpha");
    EXPECT_GT(info.generation(), 0u);

    std::string data;
    ASSERT_TRUE(get(loc("data", "small"), &data).ok());
    EXPECT_EQ(data, "hello");

    ASSERT_TRUE(put(loc("data", "empty"), "").ok());
    ASSERT_TRUE(get(loc("data", "empty"), &data, 0, 0, 0, &info).ok());
    EXPECT_EQ(data, "");
    EXPECT_EQ(info.size(), 0u);

    const std::string big = random_text(5 * kPartSize + 1234, 7);  // six Parts
    ASSERT_TRUE(put(loc("data", "big"), big, std::nullopt, &info).ok());
    EXPECT_EQ(info.size(), big.size());
    EXPECT_EQ(info.checksum(), crc_of(big));
    auto head = meta->head({{"alpha", "data"}, "big"});
    ASSERT_TRUE(head.has_value());
    EXPECT_EQ(head->parts.size(), 6u);

    ASSERT_TRUE(get(loc("data", "big"), &data).ok());
    EXPECT_TRUE(data == big);

    struct Range {
        std::uint64_t offset, length;
    };
    for (Range r : {Range{kPartSize - 10, 20}, Range{100'000, 150'000}, Range{3, 0}, Range{2 * kPartSize, kPartSize},
                    Range{big.size() - 1, 100}, Range{big.size(), 0}}) {
        ASSERT_TRUE(get(loc("data", "big"), &data, r.offset, r.length).ok()) << r.offset;
        EXPECT_TRUE(data == big.substr(r.offset, r.length == 0 ? std::string::npos : r.length)) << r.offset;
    }
    EXPECT_EQ(get(loc("data", "big"), &data, big.size() + 1).error_code(), StatusCode::INVALID_ARGUMENT);
}

TEST_F(GatewayTest, StoredBytesAreCiphertext) {
    std::string plaintext;
    for (int i = 0; i < 2000; ++i) plaintext += "PLAINTEXT-MARKER-" + std::to_string(i) + ";";
    auto before = chunk_files_content().size();
    ASSERT_TRUE(put(loc("data", "secret"), plaintext).ok());
    std::string stored = chunk_files_content();
    EXPECT_GE(stored.size(), before + 3 * plaintext.size());  // three replicas
    EXPECT_EQ(stored.find("PLAINTEXT-MARKER-"), std::string::npos);
    std::string data;
    ASSERT_TRUE(get(loc("data", "secret"), &data).ok());
    EXPECT_TRUE(data == plaintext);
}

TEST_F(GatewayTest, ConditionalWrites) {
    pb::ObjectInfo first;
    ASSERT_TRUE(put(loc("data", "c"), "one", 0, &first).ok());
    EXPECT_EQ(put(loc("data", "c"), "two", 0).error_code(), StatusCode::FAILED_PRECONDITION);
    EXPECT_EQ(put(loc("data", "c"), "two", first.generation() + 1000).error_code(),
              StatusCode::FAILED_PRECONDITION);
    pb::ObjectInfo second;
    ASSERT_TRUE(put(loc("data", "c"), "two", first.generation(), &second).ok());
    EXPECT_GT(second.generation(), first.generation());

    std::string data;
    ASSERT_TRUE(get(loc("data", "c"), &data).ok());
    EXPECT_EQ(data, "two");

    EXPECT_EQ(remove(loc("data", "c"), 0, nullptr, first.generation()).error_code(),
              StatusCode::FAILED_PRECONDITION);
    EXPECT_TRUE(remove(loc("data", "c"), 0, nullptr, second.generation()).ok());
    // The rejected write left no Pending Upload behind.
    EXPECT_EQ(meta->delete_bucket({"alpha", "data"}).has_value(), true);
}

TEST_F(GatewayTest, QuotaExceeded) {
    ASSERT_TRUE(create_tenant("gamma", 0, 2).ok());
    ASSERT_TRUE(create_bucket("b", false, "gamma").ok());
    ASSERT_TRUE(put(loc("b", "1", "gamma"), "x").ok());
    ASSERT_TRUE(put(loc("b", "2", "gamma"), "y").ok());
    auto status = put(loc("b", "3", "gamma"), "z");
    EXPECT_EQ(status.error_code(), StatusCode::RESOURCE_EXHAUSTED);
    EXPECT_TRUE(put(loc("b", "1", "gamma"), "replaced").ok());  // replacing frees its own share

    pb::SetQuotaRequest quota;
    quota.set_name("gamma");
    quota.mutable_quota()->set_max_bytes(15);
    pb::TenantInfo tenant;
    ASSERT_TRUE(stub->SetQuota(ctx().get(), quota, &tenant).ok());
    EXPECT_EQ(tenant.quota().max_objects(), 0u);
    EXPECT_EQ(tenant.used_objects(), 2u);
    EXPECT_EQ(put(loc("b", "3", "gamma"), "more than fifteen bytes").error_code(), StatusCode::RESOURCE_EXHAUSTED);
    EXPECT_TRUE(put(loc("b", "3", "gamma"), "tiny").ok());
}

TEST_F(GatewayTest, DeleteWithoutVersioningIsPermanent) {
    pb::ObjectInfo info;
    ASSERT_TRUE(put(loc("data", "gone"), random_text(3 * kPartSize, 1), std::nullopt, &info).ok());
    pb::DeleteObjectResponse deleted;
    ASSERT_TRUE(remove(loc("data", "gone"), 0, &deleted).ok());
    EXPECT_EQ(deleted.delete_marker_generation(), 0u);

    std::string data;
    EXPECT_EQ(get(loc("data", "gone"), &data).error_code(), StatusCode::NOT_FOUND);
    EXPECT_EQ(get(loc("data", "gone"), &data, 0, 0, info.generation()).error_code(),
              StatusCode::NOT_FOUND);
    pb::HeadObjectRequest head;
    *head.mutable_location() = loc("data", "gone");
    pb::ObjectInfo out;
    EXPECT_EQ(stub->HeadObject(ctx().get(), head, &out).error_code(), StatusCode::NOT_FOUND);
    pb::ListVersionsRequest versions;
    *versions.mutable_location() = loc("data", "gone");
    pb::ListVersionsResponse listed;
    ASSERT_TRUE(stub->ListVersions(ctx().get(), versions, &listed).ok());
    EXPECT_EQ(listed.versions_size(), 0);
    auto garbage = meta->peek_garbage(100);
    ASSERT_TRUE(garbage.has_value());
    EXPECT_EQ(garbage->size(), 3u);  // its Extents await Compaction; the Data Key is gone
    EXPECT_EQ(remove(loc("data", "gone")).error_code(), StatusCode::NOT_FOUND);
}

TEST_F(GatewayTest, DeleteWithVersioningKeepsHistory) {
    ASSERT_TRUE(create_bucket("versioned", true).ok());
    pb::ObjectInfo v1, v2;
    ASSERT_TRUE(put(loc("versioned", "k"), "first", std::nullopt, &v1).ok());
    ASSERT_TRUE(put(loc("versioned", "k"), "second", std::nullopt, &v2).ok());
    pb::DeleteObjectResponse deleted;
    ASSERT_TRUE(remove(loc("versioned", "k"), 0, &deleted).ok());
    EXPECT_GT(deleted.delete_marker_generation(), v2.generation());

    std::string data;
    EXPECT_EQ(get(loc("versioned", "k"), &data).error_code(), StatusCode::NOT_FOUND);
    ASSERT_TRUE(get(loc("versioned", "k"), &data, 0, 0, v1.generation()).ok());
    EXPECT_EQ(data, "first");
    EXPECT_EQ(get(loc("versioned", "k"), &data, 0, 0, deleted.delete_marker_generation()).error_code(),
              StatusCode::NOT_FOUND);

    pb::ListVersionsRequest req;
    *req.mutable_location() = loc("versioned", "k");
    pb::ListVersionsResponse versions;
    ASSERT_TRUE(stub->ListVersions(ctx().get(), req, &versions).ok());
    ASSERT_EQ(versions.versions_size(), 3);
    EXPECT_TRUE(versions.versions(0).delete_marker());
    EXPECT_EQ(versions.versions(2).generation(), v1.generation());

    ASSERT_TRUE(remove(loc("versioned", "k"), v1.generation()).ok());
    EXPECT_EQ(get(loc("versioned", "k"), &data, 0, 0, v1.generation()).error_code(),
              StatusCode::NOT_FOUND);
    ASSERT_TRUE(remove(loc("versioned", "k"), deleted.delete_marker_generation()).ok());
    ASSERT_TRUE(get(loc("versioned", "k"), &data).ok());
    EXPECT_EQ(data, "second");
}

TEST_F(GatewayTest, MultipartUpload) {
    pb::InitiateUploadRequest init;
    *init.mutable_location() = loc("data", "multi");
    pb::InitiateUploadResponse started;
    ASSERT_TRUE(stub->InitiateUpload(ctx().get(), init, &started).ok());
    const auto id = started.upload_id();

    const std::string p1 = random_text(100'000, 1), p2 = random_text(70'000, 2);
    pb::PartInfo part;
    ASSERT_TRUE(upload_part(id, 2, p2, &part).ok());
    EXPECT_EQ(part.size(), p2.size());
    EXPECT_EQ(part.checksum(), crc_of(p2));
    ASSERT_TRUE(upload_part(id, 1, "to be replaced").ok());
    ASSERT_TRUE(upload_part(id, 1, p1).ok());
    EXPECT_EQ(upload_part(id, 0, "x").error_code(), StatusCode::INVALID_ARGUMENT);

    pb::UploadRequest upload;
    upload.set_upload_id(id);
    pb::ListPartsResponse parts;
    ASSERT_TRUE(stub->ListParts(ctx().get(), upload, &parts).ok());
    ASSERT_EQ(parts.parts_size(), 2);
    EXPECT_EQ(parts.parts(0).part_number(), 1u);
    EXPECT_EQ(parts.parts(0).size(), p1.size());

    pb::CompleteUploadRequest complete;
    complete.set_upload_id(id);
    complete.mutable_precondition()->set_if_generation_match(0);
    pb::PutObjectResponse done;
    ASSERT_TRUE(stub->CompleteUpload(ctx().get(), complete, &done).ok());
    EXPECT_EQ(done.object().size(), p1.size() + p2.size());
    EXPECT_EQ(done.object().checksum(), crc_of(p1 + p2));

    std::string data;
    ASSERT_TRUE(get(loc("data", "multi"), &data).ok());
    EXPECT_TRUE(data == p1 + p2);
    ASSERT_TRUE(get(loc("data", "multi"), &data, 99'990, 20).ok());
    EXPECT_TRUE(data == (p1 + p2).substr(99'990, 20));

    // A gap in the part numbers, then an abort.
    ASSERT_TRUE(stub->InitiateUpload(ctx().get(), init, &started).ok());
    ASSERT_TRUE(upload_part(started.upload_id(), 2, "two").ok());
    complete.set_upload_id(started.upload_id());
    complete.clear_precondition();
    EXPECT_EQ(stub->CompleteUpload(ctx().get(), complete, &done).error_code(),
              StatusCode::INVALID_ARGUMENT);
    upload.set_upload_id(started.upload_id());
    pb::Empty empty;
    ASSERT_TRUE(stub->AbortUpload(ctx().get(), upload, &empty).ok());
    EXPECT_EQ(stub->ListParts(ctx().get(), upload, &parts).error_code(), StatusCode::NOT_FOUND);
    ASSERT_TRUE(get(loc("data", "multi"), &data).ok());
    EXPECT_TRUE(data == p1 + p2);
}

TEST_F(GatewayTest, ListObjectsPages) {
    for (const char* key : {"a/1", "a/2", "a/3", "b/1"}) ASSERT_TRUE(put(loc("data", key), key).ok());
    pb::ListObjectsRequest req;
    req.set_tenant("alpha");
    req.set_bucket("data");
    req.set_prefix("a/");
    req.set_limit(2);
    pb::ListObjectsResponse page;
    ASSERT_TRUE(stub->ListObjects(ctx().get(), req, &page).ok());
    ASSERT_EQ(page.objects_size(), 2);
    EXPECT_EQ(page.objects(0).key(), "a/1");
    EXPECT_EQ(page.next_start_after(), "a/2");
    req.set_start_after(page.next_start_after());
    ASSERT_TRUE(stub->ListObjects(ctx().get(), req, &page).ok());
    ASSERT_EQ(page.objects_size(), 1);
    EXPECT_EQ(page.objects(0).key(), "a/3");
    EXPECT_EQ(page.next_start_after(), "");

    req.Clear();
    req.set_tenant("alpha");
    req.set_bucket("data");
    req.set_limit(100'000);  // clamped to the maximum
    ASSERT_TRUE(stub->ListObjects(ctx().get(), req, &page).ok());
    EXPECT_EQ(page.objects_size(), 4);
}

TEST_F(GatewayTest, CountsRequestsAndBytes) {
    ASSERT_TRUE(put(loc("data", "m"), "metrics").ok());
    std::string data;
    ASSERT_TRUE(get(loc("data", "m"), &data).ok());
    std::string text = rmk::metrics::global().render();
    EXPECT_NE(text.find("rmk_gateway_requests_total"), std::string::npos);
    EXPECT_NE(text.find("method=\"PutObject\""), std::string::npos);
    EXPECT_NE(text.find("sdk_version=\"1.0.0\""), std::string::npos);
    EXPECT_NE(text.find("rmk_gateway_bytes_in_total"), std::string::npos);
    EXPECT_NE(text.find("rmk_gateway_bytes_out_total"), std::string::npos);
}

// ----- Safety rules (docs/IMPLEMENTATION.md §6)

TEST_F(GatewayTest, LateRegistrationWritesThePartAgain) {
    options.compaction_grace = std::chrono::seconds{0};  // every acknowledgement is already too old
    restart_gateway();
    auto status = put(loc("data", "late"), "late bytes");
    EXPECT_EQ(status.error_code(), StatusCode::UNAVAILABLE);
    EXPECT_NE(status.error_message().find("compaction grace"), std::string::npos) << status.error_message();
    EXPECT_EQ(garbage_bytes(), 3 * (10 + rmk::crypto::kPartOverhead));  // three attempts, none registered
}

TEST_F(GatewayTest, UnregisteredBytesBecomeGarbage) {
    auto hooked = std::make_shared<HookedMetaStore>(meta);
    hooked->fail_put_part = true;
    gateway_meta = hooked;
    restart_gateway();
    EXPECT_EQ(put(loc("data", "x"), "twelve bytes").error_code(), StatusCode::INTERNAL);
    EXPECT_EQ(garbage_bytes(), 12 + rmk::crypto::kPartOverhead);
    EXPECT_TRUE(meta->delete_bucket({"alpha", "data"}).has_value());  // the upload was aborted
}

TEST_F(GatewayTest, PartSizeAndPartCountAreBounded) {
    options.part_size = 4 * kPartSize;
    options.max_part_bytes = kPartSize;
    restart_gateway();
    const std::string content = random_text(3 * kPartSize + 5, 3);
    ASSERT_TRUE(put(loc("data", "bounded"), content).ok());
    auto head = meta->head({{"alpha", "data"}, "bounded"});
    ASSERT_TRUE(head.has_value());
    EXPECT_EQ(head->parts.size(), 4u);  // PutObject cuts at max_part_bytes

    pb::InitiateUploadRequest init;
    *init.mutable_location() = loc("data", "multi");
    pb::InitiateUploadResponse started;
    ASSERT_TRUE(stub->InitiateUpload(ctx().get(), init, &started).ok());
    EXPECT_TRUE(upload_part(started.upload_id(), 1, std::string(kPartSize, 'a')).ok());
    EXPECT_EQ(upload_part(started.upload_id(), 2, std::string(kPartSize + 1, 'b')).error_code(),
              StatusCode::INVALID_ARGUMENT);
    EXPECT_TRUE(upload_part(started.upload_id(), rmk::kMaxParts, "last").ok());
    EXPECT_EQ(upload_part(started.upload_id(), rmk::kMaxParts + 1, "beyond").error_code(),
              StatusCode::INVALID_ARGUMENT);
}

TEST_F(GatewayTest, BufferedBytesAreBounded) {
    options.max_part_bytes = kPartSize;
    options.max_buffered_bytes = 100 << 10;
    options.buffer_wait = std::chrono::milliseconds{200};
    restart_gateway();
    pb::InitiateUploadRequest init;
    *init.mutable_location() = loc("data", "held");
    pb::InitiateUploadResponse started;
    ASSERT_TRUE(stub->InitiateUpload(ctx().get(), init, &started).ok());

    // A part that stays half-sent holds 60 KiB of the 100 KiB budget.
    auto c = ctx();
    pb::UploadPartResponse resp;
    auto writer = stub->UploadPart(c.get(), &resp);
    pb::UploadPartRequest msg;
    msg.mutable_header()->set_upload_id(started.upload_id());
    msg.mutable_header()->set_part_number(1);
    ASSERT_TRUE(writer->Write(msg));
    pb::UploadPartRequest body;
    body.set_data(std::string(60 << 10, 'h'));
    ASSERT_TRUE(writer->Write(body));
    std::this_thread::sleep_for(std::chrono::milliseconds{300});

    EXPECT_EQ(put(loc("data", "squeezed"), std::string(50 << 10, 's')).error_code(),
              StatusCode::UNAVAILABLE);
    writer->WritesDone();
    ASSERT_TRUE(writer->Finish().ok());
    EXPECT_TRUE(put(loc("data", "squeezed"), std::string(50 << 10, 's')).ok());
}

TEST_F(GatewayTest, ReadsFollowRelocatedParts) {
    // Between GetObject's metadata read and its first Extent read, Compaction moves every Part
    // to another Chunk and deletes the old one.
    auto hooked = std::make_shared<HookedMetaStore>(meta);
    hooked->after_head = [&](const rmk::ObjectMeta& version) {
        rmk::rpc::WriterOptions mover_options;
        mover_options.writer_id = "compactor";
        rmk::rpc::ChunkWriter mover(cluster, channels, mover_options);
        rmk::rpc::ChunkReader reader(cluster, channels);
        std::set<rmk::ChunkId> old_chunks;
        for (const auto& part : version.parts) {
            auto stored = reader.read(part.extent);
            ASSERT_TRUE(stored.has_value());
            auto moved = mover.write(rmk::kReplicated3, *stored);
            ASSERT_TRUE(moved.has_value()) << moved.error().message;
            ASSERT_NE(moved->chunk_id, part.extent.chunk_id);
            ASSERT_TRUE(meta->relocate_part(version.ref, version.generation, part.number, part.extent, *moved));
            old_chunks.insert(part.extent.chunk_id);
        }
        for (auto id : old_chunks) delete_chunk(id);
    };
    gateway_meta = hooked;
    restart_gateway();
    // Written after the restart, so the Gateway's writer still holds the lease on the old Chunk.
    const std::string content = random_text(kPartSize + 777, 5);  // two Parts
    ASSERT_TRUE(put(loc("data", "moving"), content).ok());

    std::string data;
    ASSERT_TRUE(get(loc("data", "moving"), &data).ok());
    EXPECT_TRUE(data == content);
    EXPECT_EQ(hooked->after_head, nullptr);
}

}  // namespace
