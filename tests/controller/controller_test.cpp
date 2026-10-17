#include <gtest/gtest.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <filesystem>
#include <functional>
#include <map>
#include <memory>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include <grpcpp/grpcpp.h>

#include "rmk/chunk_file.hpp"
#include "rmk/controller.hpp"
#include "rmk/crc32c.hpp"
#include "rmk/crypto.hpp"
#include "rmk/datanode.hpp"
#include "rmk/http.hpp"
#include "rmk/memory_stores.hpp"
#include "rmk/rpc.hpp"
#include "rmk/v1/datanode.grpc.pb.h"

namespace rmk::controller {
namespace {

using namespace std::chrono_literals;
namespace pb = rmk::internal::v1;

EnvLookup env_of(std::map<std::string, std::string> vars) {
    auto shared = std::make_shared<std::map<std::string, std::string>>(std::move(vars));
    return [shared](const char* name) -> const char* {
        auto it = shared->find(name);
        return it == shared->end() ? nullptr : it->second.c_str();
    };
}

std::vector<std::byte> random_bytes(std::size_t n, unsigned seed) {
    std::mt19937 rng(seed);
    std::vector<std::byte> out(n);
    for (auto& b : out)
        b = static_cast<std::byte>(rng() & 0xFF);
    return out;
}

crypto::Key key_of(unsigned char fill) {
    crypto::Key key;
    key.fill(static_cast<std::byte>(fill));
    return key;
}

const StoragePolicy kEc42{4, 2};

rpc::TlsConfig insecure() {
    rpc::TlsConfig tls;
    tls.insecure = true;
    return tls;
}

TEST(Options, DefaultsAndOverrides) {
    auto defaults = load_options(env_of({}));
    ASSERT_TRUE(defaults);
    EXPECT_FALSE(defaults->id.empty());
    EXPECT_EQ(defaults->interval, 5s);
    EXPECT_EQ(defaults->repair_delay, 900s);
    EXPECT_EQ(defaults->compaction_garbage_percent, 50u);
    EXPECT_EQ(defaults->rewrap_batch, 1000u);

    auto set = load_options(env_of({{"RMK_CONTROLLER_ID", "c-7"},
                                    {"RMK_CONTROLLER_INTERVAL_SECONDS", "2"},
                                    {"RMK_REPAIR_DELAY_SECONDS", "60"},
                                    {"RMK_CHUNK_CAPACITY_BYTES", "1048576"},
                                    {"RMK_OPEN_CHUNKS_PER_POLICY", "8"},
                                    {"RMK_COMPACTION_GARBAGE_PERCENT", "75"},
                                    {"RMK_COMPACTION_BYTES_PER_SECOND", "1000"},
                                    {"RMK_REWRAP_BATCH", "10"},
                                    {"RMK_WRITER_LEASE_SECONDS", "45"}}));
    ASSERT_TRUE(set);
    EXPECT_EQ(set->id, "c-7");
    EXPECT_EQ(set->interval, 2s);
    EXPECT_EQ(set->repair_delay, 60s);
    EXPECT_EQ(set->chunk_capacity, 1048576u);
    EXPECT_EQ(set->open_chunks_per_policy, 8u);
    EXPECT_EQ(set->compaction_garbage_percent, 75u);
    EXPECT_EQ(set->compaction_bytes_per_second, 1000u);
    EXPECT_EQ(set->rewrap_batch, 10u);
    EXPECT_EQ(set->writer_lease, 45s);

    EXPECT_EQ(load_options(env_of({{"RMK_COMPACTION_GARBAGE_PERCENT", "0"}})).error().code, Errc::invalid_argument);
    EXPECT_EQ(load_options(env_of({{"RMK_COMPACTION_GARBAGE_PERCENT", "101"}})).error().code, Errc::invalid_argument);
    EXPECT_EQ(load_options(env_of({{"RMK_CONTROLLER_ID", ""}})).error().code, Errc::invalid_argument);
    EXPECT_EQ(load_options(env_of({{"RMK_REWRAP_BATCH", "x"}})).error().code, Errc::invalid_argument);
}

// One in-process Data Node. The test publishes its heartbeats on the fake clock.
// A ClusterStore in front of the real one that can cut off the lease renewals of the Controller
// using it, the way a paused or partitioned process misses them, and that records its fenced writes.
class StallableStore final : public ClusterStore {
public:
    explicit StallableStore(std::shared_ptr<ClusterStore> inner) : inner_(std::move(inner)) {}

    std::function<void()> on_list_chunks;  // runs once, on the calling thread
    std::atomic<int> fenced_ok{0};
    std::atomic<int> fenced_rejected{0};

    // While stalled, try_lead is unavailable on every thread except the one that stalled it.
    void stall() {
        std::lock_guard lock(mu_);
        stalled_by_ = std::this_thread::get_id();
        stalled_ = true;
    }
    void resume() {
        std::lock_guard lock(mu_);
        stalled_ = false;
    }

    Result<std::uint64_t> try_lead(std::string_view holder, std::chrono::seconds ttl, Clock::time_point now) override {
        {
            std::lock_guard lock(mu_);
            if (stalled_ && std::this_thread::get_id() != stalled_by_)
                return fail(Errc::unavailable, "partitioned");
        }
        return inner_->try_lead(holder, ttl, now);
    }
    Result<std::vector<ChunkInfo>> list_chunks(std::optional<ChunkState> state, std::optional<ChunkId> after,
                                               std::size_t limit) override {
        if (auto hook = std::exchange(on_list_chunks, nullptr))
            hook();
        return inner_->list_chunks(state, after, limit);
    }
    Status heartbeat(const NodeInfo& node, Clock::time_point now) override { return inner_->heartbeat(node, now); }
    Result<std::vector<NodeInfo>> list_nodes() override { return inner_->list_nodes(); }
    Status remove_node(std::string_view id, const std::optional<Fence>& fence) override {
        return count(fence, inner_->remove_node(id, fence));
    }
    Result<ChunkId> allocate_chunk_id() override { return inner_->allocate_chunk_id(); }
    Status put_chunk(const ChunkInfo& chunk, const std::optional<Fence>& fence) override {
        return count(fence, inner_->put_chunk(chunk, fence));
    }
    Result<ChunkInfo> get_chunk(ChunkId id) override { return inner_->get_chunk(id); }
    Result<ChunkInfo> update_chunk(const ChunkInfo& next, const std::optional<Fence>& fence) override {
        return count(fence, inner_->update_chunk(next, fence));
    }
    Status delete_chunk(ChunkId id, const std::optional<Fence>& fence) override {
        return count(fence, inner_->delete_chunk(id, fence));
    }
    Result<ChunkInfo> claim_open_chunk(const StoragePolicy& policy, std::string_view writer, std::uint64_t min_free,
                                       Clock::time_point lease_until, Clock::time_point now) override {
        return inner_->claim_open_chunk(policy, writer, min_free, lease_until, now);
    }
    Status add_garbage(ChunkId id, std::uint64_t bytes) override { return inner_->add_garbage(id, bytes); }

private:
    template <class R>
    R count(const std::optional<Fence>& fence, R result) {
        if (fence)
            ++(result ? fenced_ok : fenced_rejected);
        return result;
    }

    std::shared_ptr<ClusterStore> inner_;
    std::mutex mu_;
    bool stalled_ = false;
    std::thread::id stalled_by_;
};

struct TestNode {
    NodeInfo info;
    std::unique_ptr<datanode::Node> node;
    std::unique_ptr<grpc::Server> server;
    bool heartbeating = true;
};

class ControllerTest : public ::testing::Test {
protected:
    std::shared_ptr<MemoryMetaStore> meta = std::make_shared<MemoryMetaStore>(std::chrono::hours(1));
    std::shared_ptr<MemoryConfigStore> config = std::make_shared<MemoryConfigStore>();
    std::shared_ptr<MemoryClusterStore> cluster = std::make_shared<MemoryClusterStore>();
    std::shared_ptr<rpc::Channels> channels = std::make_shared<rpc::Channels>(insecure());
    // Writers wrap Data Keys with Master Key 1.
    std::shared_ptr<crypto::StaticKeyProvider> keys = std::make_shared<crypto::StaticKeyProvider>(
        std::map<std::uint32_t, crypto::Key>{{1, key_of(1)}, {2, key_of(2)}}, 1);
    std::filesystem::path root;
    std::vector<TestNode> nodes;
    Clock::time_point now = Clock::now();
    const BucketRef bucket{"t1", "b1"};
    const BucketRef ec_bucket{"t1", "ec"};

    void SetUp() override {
        root = std::filesystem::temp_directory_path() /
               ("rmk-controller-" + std::to_string(::getpid()) + "-" +
                ::testing::UnitTest::GetInstance()->current_test_info()->name());
        std::filesystem::remove_all(root);
        ASSERT_TRUE(meta->create_tenant("t1", Quota{}));
        ASSERT_TRUE(meta->create_bucket(bucket, BucketConfig{}));
    }

    void TearDown() override {
        nodes.clear();
        std::filesystem::remove_all(root);
    }

    void add_nodes(const std::vector<std::pair<std::string, std::string>>& zones_and_k8s) {
        for (const auto& [zone, k8s] : zones_and_k8s) {
            TestNode n;
            n.info.id = "node-" + std::to_string(nodes.size());
            n.info.zone = zone;
            n.info.k8s_node = k8s;
            n.info.capacity_bytes = std::uint64_t{1} << 30;
            datanode::Options options;
            options.identity = n.info;
            options.identity.address = "unused";
            options.data_dir = root / n.info.id;
            auto node = datanode::Node::create(options, cluster, channels);
            ASSERT_TRUE(node) << node.error().message;
            n.node = std::move(*node);
            grpc::ServerBuilder builder;
            int port = 0;
            builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
            builder.RegisterService(n.node->service());
            n.server = builder.BuildAndStart();
            ASSERT_TRUE(n.server && port != 0);
            n.info.address = "127.0.0.1:" + std::to_string(port);
            ASSERT_TRUE(cluster->heartbeat(n.info, now));
            nodes.push_back(std::move(n));
        }
    }

    void add_six_nodes_in_three_zones() {
        add_nodes({{"a", "k0"}, {"a", "k1"}, {"b", "k2"}, {"b", "k3"}, {"c", "k4"}, {"c", "k5"}});
    }

    // Advances the fake clock; nodes still heartbeating report at the new time.
    void tick(std::chrono::seconds by) {
        now += by;
        for (const TestNode& n : nodes) {
            if (!n.heartbeating)
                continue;
            ASSERT_TRUE(cluster->heartbeat(n.info, now));
        }
    }

    TestNode& node(const NodeId& id) {
        return *std::ranges::find_if(nodes, [&](const TestNode& n) { return n.info.id == id; });
    }

    void stop(const NodeId& id) {
        TestNode& n = node(id);
        n.server->Shutdown(std::chrono::system_clock::now() + 1s);
        n.server.reset();
        n.heartbeating = false;
    }

    Options options(std::string id = "c1") {
        Options o;
        o.id = std::move(id);
        o.repair_delay = 120s;
        o.chunk_capacity = std::uint64_t{1} << 20;
        o.open_chunks_per_policy = 2;
        o.compaction_grace = 0s;
        return o;
    }

    std::unique_ptr<Controller> make(Options o, std::shared_ptr<crypto::KeyProvider> provider = nullptr,
                                     std::shared_ptr<ClusterStore> store = nullptr) {
        Dependencies deps{rpc::Stores{meta, config, store ? store : cluster}, provider ? provider : keys, channels};
        auto c = Controller::create(std::move(o), std::move(deps));
        EXPECT_TRUE(c);
        return std::move(*c);
    }

    Report pass(Controller& c) {
        auto r = c.run_once(now);
        EXPECT_TRUE(r);
        return r.value_or(Report{});
    }

    std::unique_ptr<rpc::ChunkWriter> writer() {
        return std::make_unique<rpc::ChunkWriter>(cluster, channels, rpc::WriterOptions{"test-writer", 30s, 1, 3});
    }

    // Writes an Object the way the Gateway does: encrypted Parts through the ChunkWriter.
    ObjectRef put(rpc::ChunkWriter& w, const BucketRef& b, const std::string& key, const std::vector<std::byte>& plain,
                  std::size_t part_size) {
        const ObjectRef ref{b, key};
        auto policy = meta->get_bucket(b)->config.storage_policy;
        auto data_key = crypto::new_data_key(*keys);
        EXPECT_TRUE(data_key);
        auto upload = meta->begin_upload(ref, data_key->encryption, now);
        EXPECT_TRUE(upload);
        std::uint32_t number = 0;
        for (std::size_t pos = 0; pos < plain.size(); pos += part_size) {
            std::span<const std::byte> piece(plain.data() + pos, std::min(part_size, plain.size() - pos));
            auto stored = crypto::seal_part(data_key->key, ++number, piece);
            auto extent = w.write(policy, stored);
            EXPECT_TRUE(extent) << extent.error().message;
            EXPECT_TRUE(meta->put_part(*upload, Part{number, piece.size(), crc32c(piece), *extent}, now));
        }
        auto committed = meta->commit_upload(*upload, Precondition{}, now);
        EXPECT_TRUE(committed) << committed.error().message;
        return ref;
    }

    // Reads an Object back through head + ChunkReader, decrypting every Part.
    std::vector<std::byte> get(const ObjectRef& ref) {
        rpc::ChunkReader reader(cluster, channels);
        auto meta_data = meta->head(ref);
        EXPECT_TRUE(meta_data);
        if (!meta_data)
            return {};
        auto data_key = crypto::unwrap(*keys, meta_data->encryption);
        EXPECT_TRUE(data_key);
        std::vector<std::byte> out;
        for (const Part& part : meta_data->parts) {
            auto stored = reader.read(part.extent);
            EXPECT_TRUE(stored) << stored.error().message;
            if (!stored)
                return {};
            auto plain = crypto::open_part(*data_key, part.number, *stored);
            EXPECT_TRUE(plain);
            if (!plain)
                return {};
            out.insert(out.end(), plain->begin(), plain->end());
        }
        return out;
    }

    std::vector<ChunkInfo> chunks(std::optional<ChunkState> state) {
        auto list = cluster->list_chunks(state, std::nullopt, 10000);
        EXPECT_TRUE(list);
        return list.value_or(std::vector<ChunkInfo>{});
    }

    // Seals every open Chunk holding data, as a writer does when one is full.
    std::vector<ChunkId> seal_written_chunks() {
        std::vector<ChunkId> sealed;
        for (ChunkInfo c : chunks(ChunkState::open)) {
            if (c.size <= kChunkHeaderSize)
                continue;
            c.state = ChunkState::sealed;
            c.sealed_at = now;
            c.writer.clear();
            EXPECT_TRUE(cluster->update_chunk(c));
            sealed.push_back(c.id);
        }
        return sealed;
    }

    pb::StatResponse stat(const NodeId& id, pb::FileRef file) {
        auto stub = pb::DataNode::NewStub(channels->get(node(id).info.address));
        grpc::ClientContext ctx;
        ctx.set_deadline(std::chrono::system_clock::now() + 5s);
        pb::StatResponse response;
        EXPECT_TRUE(stub->Stat(&ctx, file, &response).ok());
        return response;
    }

    pb::StatResponse stat_replica(const NodeId& id, ChunkId chunk) {
        pb::FileRef file;
        file.set_chunk_id(chunk);
        return stat(id, file);
    }

    std::set<std::string> domains(const ChunkInfo& c, std::string NodeInfo::*field) {
        std::set<std::string> out;
        for (const NodeId& id : c.nodes)
            out.insert(node(id).info.*field);
        return out;
    }
};

// Leases run on the real clock, renewed in the background every lead_ttl / 3.
TEST_F(ControllerTest, OnlyOneControllerLeadsAndAnotherTakesOverAfterTheTtl) {
    Options oa = options("a");
    Options ob = options("b");
    oa.lead_ttl = ob.lead_ttl = 1s;
    auto a = make(oa);
    auto b = make(ob);
    EXPECT_TRUE(pass(*a).leader);
    EXPECT_FALSE(pass(*b).leader);
    std::this_thread::sleep_for(1500ms);  // longer than the TTL: a's renewals keep it
    EXPECT_FALSE(pass(*b).leader);
    EXPECT_TRUE(pass(*a).leader);
    a.reset();
    std::this_thread::sleep_for(1200ms);  // a's lease runs out
    EXPECT_TRUE(pass(*b).leader);
}

TEST_F(ControllerTest, StaleLeaderIsFencedAndDeletesNothing) {
    add_six_nodes_in_three_zones();
    auto stallable = std::make_shared<StallableStore>(cluster);
    Options oa = options("a");
    oa.lead_ttl = 1s;
    oa.open_chunks_per_policy = 3;
    Options ob = options("b");
    ob.lead_ttl = 1s;
    ob.compaction_grace = 24h;  // b leaves the compactable chunk alone
    auto a = make(oa, nullptr, stallable);
    auto b = make(ob);
    ASSERT_TRUE(pass(*a).leader);
    {
        auto w = writer();
        put(*w, bucket, "dead", random_bytes(30000, 20), 30000);
        put(*w, bucket, "live", random_bytes(2000, 21), 2000);
    }
    const ChunkId old = meta->head({bucket, "live"})->parts.front().extent.chunk_id;
    ASSERT_TRUE(meta->remove({bucket, "dead"}, Precondition{}, now));
    EXPECT_EQ(seal_written_chunks(), std::vector<ChunkId>{old});  // a now wants one more open chunk
    const ChunkInfo before = *cluster->get_chunk(old);

    // a stalls right after confirming its lease; b takes over meanwhile.
    stallable->on_list_chunks = [&] {
        stallable->stall();
        std::this_thread::sleep_for(1500ms);
        EXPECT_TRUE(pass(*b).leader);
    };
    const int ok_before = stallable->fenced_ok;
    Report stale = pass(*a);
    stallable->resume();

    EXPECT_EQ(stallable->fenced_ok, ok_before);  // nothing a wrote after losing the lease took effect
    EXPECT_GE(stallable->fenced_rejected, 1);
    EXPECT_EQ(stale.chunks_created, 0u);
    EXPECT_EQ(stale.chunks_deleted, 0u);
    auto kept = cluster->get_chunk(old);
    ASSERT_TRUE(kept);
    EXPECT_EQ(kept->nodes, before.nodes);
    for (const NodeId& holder : before.nodes)
        EXPECT_TRUE(stat_replica(holder, old).exists());
    EXPECT_FALSE(pass(*a).leader);
}

TEST_F(ControllerTest, CompactionWaitsForTheGraceAfterSealing) {
    add_six_nodes_in_three_zones();
    Options o = options();
    o.compaction_grace = 600s;
    auto c = make(o);
    pass(*c);
    {
        auto w = writer();
        put(*w, bucket, "dead", random_bytes(5000, 22), 5000);
    }
    const ChunkId old = meta->head({bucket, "dead"})->parts.front().extent.chunk_id;
    ASSERT_TRUE(meta->remove({bucket, "dead"}, Precondition{}, now));
    seal_written_chunks();

    EXPECT_EQ(pass(*c).chunks_deleted, 0u);
    tick(599s);
    EXPECT_EQ(pass(*c).chunks_deleted, 0u);
    EXPECT_TRUE(cluster->get_chunk(old));
    tick(1s);
    EXPECT_EQ(pass(*c).chunks_deleted, 1u);
    EXPECT_EQ(cluster->get_chunk(old).error().code, Errc::not_found);
}

TEST_F(ControllerTest, PoolIsNotStarvedByAWriterHoardingLeases) {
    add_six_nodes_in_three_zones();
    auto c = make(options());
    EXPECT_EQ(pass(*c).chunks_created, 2u);
    for (int i = 0; i < 2; ++i)
        ASSERT_TRUE(cluster->claim_open_chunk(kReplicated3, "hoarder/" + std::to_string(i), 0, now + 1h, now));
    EXPECT_EQ(pass(*c).chunks_created, 2u);  // the leased ones do not count
    EXPECT_EQ(pass(*c).chunks_created, 0u);
    tick(2h);  // the hoarder's leases expired
    EXPECT_EQ(pass(*c).chunks_created, 0u);
    EXPECT_EQ(chunks(ChunkState::open).size(), 4u);
}

TEST_F(ControllerTest, ScrubRepairsAReplicaFileMissingOnAHealthyNode) {
    add_six_nodes_in_three_zones();
    auto c = make(options());
    pass(*c);
    std::vector<std::byte> data = random_bytes(4000, 23);
    {
        auto w = writer();
        put(*w, bucket, "obj", data, 4000);
    }
    const ChunkId id = meta->head({bucket, "obj"})->parts.front().extent.chunk_id;
    seal_written_chunks();
    const NodeId lost = cluster->get_chunk(id)->nodes.front();
    {
        pb::FileRef file;
        file.set_chunk_id(id);
        auto stub = pb::DataNode::NewStub(channels->get(node(lost).info.address));
        grpc::ClientContext ctx;
        pb::Empty empty;
        ASSERT_TRUE(stub->DeleteChunk(&ctx, file, &empty).ok());  // a disk replaced under the node
    }

    Report r = pass(*c);
    EXPECT_EQ(r.chunks_repaired, 1u);
    auto repaired = cluster->get_chunk(id);
    ASSERT_TRUE(repaired);
    EXPECT_FALSE(std::ranges::contains(repaired->nodes, lost));
    EXPECT_EQ(domains(*repaired, &NodeInfo::zone).size(), 3u);
    for (const NodeId& holder : repaired->nodes)
        EXPECT_GE(stat_replica(holder, id).size(), repaired->size);
    EXPECT_EQ(get({bucket, "obj"}), data);
    EXPECT_EQ(pass(*c).chunks_repaired, 0u);
}

TEST_F(ControllerTest, ScrubRestoresAMissingReplicaOnItsOwnHolderWhenNoOtherNodeFits) {
    add_nodes({{"a", "k0"}, {"b", "k1"}, {"c", "k2"}});  // a StatefulSet pod back with an empty PVC
    auto c = make(options());
    pass(*c);
    std::vector<std::byte> data = random_bytes(4000, 29);
    {
        auto w = writer();
        put(*w, bucket, "obj", data, 4000);
    }
    const ChunkId id = meta->head({bucket, "obj"})->parts.front().extent.chunk_id;
    seal_written_chunks();
    const NodeId lost = cluster->get_chunk(id)->nodes.front();
    {
        pb::FileRef file;
        file.set_chunk_id(id);
        auto stub = pb::DataNode::NewStub(channels->get(node(lost).info.address));
        grpc::ClientContext ctx;
        pb::Empty empty;
        ASSERT_TRUE(stub->DeleteChunk(&ctx, file, &empty).ok());
    }

    Report r = pass(*c);
    EXPECT_EQ(r.chunks_repaired, 1u);
    auto repaired = cluster->get_chunk(id);
    ASSERT_TRUE(repaired);
    EXPECT_TRUE(std::ranges::contains(repaired->nodes, lost));
    for (const NodeId& holder : repaired->nodes)
        EXPECT_GE(stat_replica(holder, id).size(), repaired->size);
    EXPECT_EQ(get({bucket, "obj"}), data);
    EXPECT_EQ(pass(*c).chunks_repaired, 0u);
}

TEST_F(ControllerTest, NonLeaderDoesNothing) {
    add_six_nodes_in_three_zones();
    auto a = make(options("a"));
    auto b = make(options("b"));
    ASSERT_TRUE(pass(*a).leader);
    const auto before = chunks(std::nullopt).size();
    Report r = pass(*b);
    EXPECT_FALSE(r.leader);
    EXPECT_EQ(r.chunks_created, 0u);
    EXPECT_EQ(chunks(std::nullopt).size(), before);
}

TEST_F(ControllerTest, PoolPlacesThreeReplicasInDistinctZones) {
    add_six_nodes_in_three_zones();
    ASSERT_TRUE(meta->create_bucket(ec_bucket, BucketConfig{false, kEc42}));
    auto c = make(options());
    Report r = pass(*c);
    EXPECT_TRUE(r.leader);
    EXPECT_EQ(r.chunks_created, 4u);  // two per Storage Policy in use

    std::map<StoragePolicy, int> per_policy;
    for (const ChunkInfo& chunk : chunks(ChunkState::open)) {
        ++per_policy[chunk.target_policy];
        EXPECT_EQ(chunk.nodes.size(), 3u);
        EXPECT_EQ(domains(chunk, &NodeInfo::zone).size(), 3u);
        EXPECT_EQ(chunk.size, kChunkHeaderSize);
        EXPECT_EQ(chunk.capacity, std::uint64_t{1} << 20);
        for (const NodeId& id : chunk.nodes)
            EXPECT_TRUE(stat_replica(id, chunk.id).exists());
    }
    EXPECT_EQ(per_policy[kReplicated3], 2);
    EXPECT_EQ(per_policy[kEc42], 2);
    EXPECT_EQ(pass(*c).chunks_created, 0u);  // the pool is full
}

TEST_F(ControllerTest, PoolFallsBackToK8sNodesWhenZonesAreFewerThanThree) {
    add_nodes({{"a", "k0"}, {"a", "k0"}, {"a", "k1"}, {"b", "k1"}, {"b", "k2"}, {"b", "k2"}});
    auto c = make(options());
    EXPECT_EQ(pass(*c).chunks_created, 2u);
    for (const ChunkInfo& chunk : chunks(ChunkState::open))
        EXPECT_EQ(domains(chunk, &NodeInfo::k8s_node).size(), 3u);
}

TEST_F(ControllerTest, PoolFallsBackToNodeIdsWithoutTopology) {
    add_nodes({{"", ""}, {"", ""}, {"", ""}});
    auto c = make(options());
    EXPECT_EQ(pass(*c).chunks_created, 2u);
    for (const ChunkInfo& chunk : chunks(ChunkState::open))
        EXPECT_EQ(std::set<NodeId>(chunk.nodes.begin(), chunk.nodes.end()).size(), 3u);
}

TEST_F(ControllerTest, SuspectNodesOpenChunksAreSealed) {
    add_six_nodes_in_three_zones();
    auto c = make(options());
    pass(*c);
    std::vector<std::byte> data = random_bytes(5000, 1);
    {
        auto w = writer();
        put(*w, bucket, "obj", data, 2000);
    }
    const ChunkInfo written = *cluster->get_chunk(meta->head({bucket, "obj"})->parts.front().extent.chunk_id);
    ASSERT_GT(written.size, kChunkHeaderSize);
    const NodeId suspect = written.nodes.front();
    node(suspect).heartbeating = false;

    tick(31s);
    Report r = pass(*c);
    EXPECT_GE(r.chunks_sealed, 1u);
    EXPECT_EQ(r.chunks_repaired, 0u);
    EXPECT_EQ(r.nodes_removed, 0u);

    auto sealed = cluster->get_chunk(written.id);
    ASSERT_TRUE(sealed);
    EXPECT_EQ(sealed->state, ChunkState::sealed);
    EXPECT_TRUE(sealed->writer.empty());
    EXPECT_EQ(sealed->size, written.size);
    for (const ChunkInfo& open : chunks(ChunkState::open))
        EXPECT_FALSE(std::ranges::contains(open.nodes, suspect));
    EXPECT_EQ(get({bucket, "obj"}), data);
}

TEST_F(ControllerTest, DeadNodesChunksAreRepairedAfterTheRepairDelayAndTheNodeRemoved) {
    add_six_nodes_in_three_zones();
    auto c = make(options());
    pass(*c);
    std::vector<std::byte> data = random_bytes(7000, 2);
    {
        auto w = writer();
        put(*w, bucket, "obj", data, 3000);
    }
    const ChunkId id = meta->head({bucket, "obj"})->parts.front().extent.chunk_id;
    seal_written_chunks();
    const NodeId dead = cluster->get_chunk(id)->nodes.front();
    stop(dead);

    tick(31s);
    EXPECT_EQ(pass(*c).chunks_repaired, 0u);
    tick(60s);  // 91 s silent, repair delay is 120 s
    Report early = pass(*c);
    EXPECT_EQ(early.chunks_repaired, 0u);
    EXPECT_EQ(cluster->list_nodes()->size(), 6u);
    EXPECT_TRUE(std::ranges::contains(cluster->get_chunk(id)->nodes, dead));

    tick(40s);
    Report r = pass(*c);
    EXPECT_GE(r.chunks_repaired, 1u);
    EXPECT_EQ(r.nodes_removed, 1u);
    EXPECT_EQ(cluster->list_nodes()->size(), 5u);
    for (const ChunkInfo& chunk : chunks(std::nullopt))
        EXPECT_FALSE(std::ranges::contains(chunk.nodes, dead)) << "chunk " << chunk.id;

    auto repaired = cluster->get_chunk(id);
    ASSERT_TRUE(repaired);
    EXPECT_EQ(repaired->state, ChunkState::sealed);
    ASSERT_EQ(repaired->nodes.size(), 3u);
    EXPECT_EQ(domains(*repaired, &NodeInfo::zone).size(), 3u);
    for (const NodeId& holder : repaired->nodes) {
        auto st = stat_replica(holder, id);
        EXPECT_TRUE(st.exists());
        EXPECT_GE(st.size(), repaired->size);
    }
    EXPECT_EQ(get({bucket, "obj"}), data);
}

TEST_F(ControllerTest, GarbageIsCountedIntoChunks) {
    add_six_nodes_in_three_zones();
    auto c = make(options());
    pass(*c);
    auto w = writer();
    put(*w, bucket, "obj", random_bytes(3000, 3), 1000);
    const auto old_parts = meta->head({bucket, "obj"})->parts;
    put(*w, bucket, "obj", random_bytes(500, 4), 1000);  // replaces it: the old Parts are garbage

    std::map<ChunkId, std::uint64_t> expected;
    for (const Part& part : old_parts)
        expected[part.extent.chunk_id] += part.extent.length;
    Report r = pass(*c);
    EXPECT_EQ(r.garbage_entries, 3u);
    for (const auto& [id, bytes] : expected)
        EXPECT_EQ(cluster->get_chunk(id)->garbage_bytes, bytes);
    EXPECT_TRUE(meta->peek_garbage(10)->empty());
    EXPECT_EQ(pass(*c).garbage_entries, 0u);
}

TEST_F(ControllerTest, ExpiredUploadsAreReclaimed) {
    add_six_nodes_in_three_zones();
    auto c = make(options());
    pass(*c);
    auto w = writer();
    auto data_key = crypto::new_data_key(*keys);
    auto upload = meta->begin_upload({bucket, "never-committed"}, data_key->encryption, now);
    ASSERT_TRUE(upload);
    auto plain = random_bytes(100, 5);
    auto extent = w->write(kReplicated3, crypto::seal_part(data_key->key, 1, plain));
    ASSERT_TRUE(extent);
    ASSERT_TRUE(meta->put_part(*upload, Part{1, plain.size(), crc32c(plain), *extent}, now));

    EXPECT_EQ(pass(*c).uploads_reclaimed, 0u);
    tick(2h);
    Report r = pass(*c);
    EXPECT_EQ(r.uploads_reclaimed, 1u);
    EXPECT_EQ(pass(*c).garbage_entries, 1u);
    EXPECT_EQ(cluster->get_chunk(extent->chunk_id)->garbage_bytes, extent->length);
}

TEST_F(ControllerTest, SealedChunkIsErasureCodedAndStaysReadable) {
    add_six_nodes_in_three_zones();
    ASSERT_TRUE(meta->create_bucket(ec_bucket, BucketConfig{false, kEc42}));
    auto c = make(options());
    pass(*c);
    std::vector<std::byte> data = random_bytes(30000, 6);
    {
        auto w = writer();
        put(*w, ec_bucket, "obj", data, 10000);
    }
    const ChunkId id = meta->head({ec_bucket, "obj"})->parts.front().extent.chunk_id;
    const ChunkInfo before = *cluster->get_chunk(id);
    EXPECT_EQ(seal_written_chunks(), std::vector<ChunkId>{id});

    Report r = pass(*c);
    EXPECT_EQ(r.chunks_encoded, 1u);
    auto coded = cluster->get_chunk(id);
    ASSERT_TRUE(coded);
    EXPECT_EQ(coded->state, ChunkState::erasure_coded);
    EXPECT_EQ(coded->data_units, 4);
    EXPECT_EQ(coded->parity_units, 2);
    ASSERT_EQ(coded->nodes.size(), 6u);
    EXPECT_EQ(std::set<NodeId>(coded->nodes.begin(), coded->nodes.end()).size(), 6u);
    EXPECT_EQ(coded->size, before.size);
    for (const NodeId& holder : before.nodes)
        EXPECT_FALSE(stat_replica(holder, id).exists());
    for (std::size_t i = 0; i < coded->nodes.size(); ++i) {
        pb::FileRef shard;
        shard.set_chunk_id(id);
        shard.set_shard(true);
        shard.set_shard_index(static_cast<std::uint32_t>(i));
        EXPECT_TRUE(stat(coded->nodes[i], shard).exists());
    }
    EXPECT_EQ(get({ec_bucket, "obj"}), data);

    stop(coded->nodes[0]);  // a data shard
    EXPECT_EQ(get({ec_bucket, "obj"}), data);
    EXPECT_EQ(pass(*c).chunks_encoded, 0u);
}

TEST_F(ControllerTest, DeadShardIsRebuiltElsewhere) {
    add_nodes({{"a", "k0"}, {"a", "k1"}, {"b", "k2"}, {"b", "k3"}, {"c", "k4"}, {"c", "k5"}, {"c", "k6"}});
    ASSERT_TRUE(meta->create_bucket(ec_bucket, BucketConfig{false, kEc42}));
    auto c = make(options());
    pass(*c);
    std::vector<std::byte> data = random_bytes(20000, 7);
    {
        auto w = writer();
        put(*w, ec_bucket, "obj", data, 10000);
    }
    const ChunkId id = meta->head({ec_bucket, "obj"})->parts.front().extent.chunk_id;
    seal_written_chunks();
    ASSERT_EQ(pass(*c).chunks_encoded, 1u);
    const ChunkInfo coded = *cluster->get_chunk(id);
    const NodeId dead = coded.nodes[1];
    stop(dead);

    tick(130s);
    Report r = pass(*c);
    EXPECT_GE(r.chunks_repaired, 1u);
    auto rebuilt = cluster->get_chunk(id);
    ASSERT_TRUE(rebuilt);
    EXPECT_FALSE(std::ranges::contains(rebuilt->nodes, dead));
    EXPECT_EQ(std::set<NodeId>(rebuilt->nodes.begin(), rebuilt->nodes.end()).size(), 6u);
    EXPECT_EQ(get({ec_bucket, "obj"}), data);
}

TEST_F(ControllerTest, CompactionRelocatesLivePartsAndDeletesTheOldChunk) {
    add_six_nodes_in_three_zones();
    auto c = make(options());
    pass(*c);
    std::vector<std::byte> live = random_bytes(9000, 8);
    {
        auto w = writer();
        put(*w, bucket, "dead", random_bytes(40000, 9), 20000);
        put(*w, bucket, "live", live, 4000);
    }
    const auto live_parts = meta->head({bucket, "live"})->parts;
    const ChunkId old = live_parts.front().extent.chunk_id;
    ASSERT_EQ(meta->head({bucket, "dead"})->parts.front().extent.chunk_id, old);
    ASSERT_TRUE(meta->remove({bucket, "dead"}, Precondition{}, now));
    EXPECT_EQ(seal_written_chunks(), std::vector<ChunkId>{old});
    const ChunkInfo before = *cluster->get_chunk(old);

    Report r = pass(*c);
    EXPECT_EQ(r.chunks_compacted, 1u);
    EXPECT_EQ(r.chunks_deleted, 1u);
    EXPECT_EQ(cluster->get_chunk(old).error().code, Errc::not_found);
    for (const NodeId& holder : before.nodes)
        EXPECT_FALSE(stat_replica(holder, old).exists());

    const auto moved = meta->head({bucket, "live"})->parts;
    ASSERT_EQ(moved.size(), live_parts.size());
    for (std::size_t i = 0; i < moved.size(); ++i) {
        EXPECT_NE(moved[i].extent.chunk_id, old);
        EXPECT_EQ(moved[i].extent.length, live_parts[i].extent.length);
        EXPECT_EQ(moved[i].extent.crc32c, live_parts[i].extent.crc32c);
    }
    EXPECT_EQ(get({bucket, "live"}), live);
}

TEST_F(ControllerTest, ChunkWithoutLiveExtentsIsDeletedDirectly) {
    add_six_nodes_in_three_zones();
    auto c = make(options());
    pass(*c);
    {
        auto w = writer();
        put(*w, bucket, "dead", random_bytes(5000, 10), 5000);
    }
    const ChunkId old = meta->head({bucket, "dead"})->parts.front().extent.chunk_id;
    ASSERT_TRUE(meta->remove({bucket, "dead"}, Precondition{}, now));
    EXPECT_EQ(seal_written_chunks(), std::vector<ChunkId>{old});

    Report r = pass(*c);
    EXPECT_EQ(r.chunks_deleted, 1u);
    EXPECT_EQ(r.chunks_compacted, 0u);
    EXPECT_EQ(cluster->get_chunk(old).error().code, Errc::not_found);
}

TEST_F(ControllerTest, ChunkHoldingAPendingPartIsCompactedOnlyAfterTheUploadCommits) {
    add_six_nodes_in_three_zones();
    auto c = make(options());
    pass(*c);
    const ObjectRef pending_ref{bucket, "pending"};
    std::vector<std::byte> plain = random_bytes(2000, 13);
    UploadId upload = 0;
    {
        auto w = writer();
        put(*w, bucket, "dead", random_bytes(30000, 14), 30000);
        auto data_key = crypto::new_data_key(*keys);
        auto begun = meta->begin_upload(pending_ref, data_key->encryption, now);
        ASSERT_TRUE(begun);
        upload = *begun;
        auto extent = w->write(kReplicated3, crypto::seal_part(data_key->key, 1, plain));
        ASSERT_TRUE(extent);
        ASSERT_TRUE(meta->put_part(upload, Part{1, plain.size(), crc32c(plain), *extent}, now));
    }
    const ChunkId old = meta->head({bucket, "dead"})->parts.front().extent.chunk_id;
    ASSERT_EQ(meta->get_upload(upload, now)->parts.at(1).extent.chunk_id, old);
    ASSERT_TRUE(meta->remove({bucket, "dead"}, Precondition{}, now));
    EXPECT_EQ(seal_written_chunks(), std::vector<ChunkId>{old});

    Report waiting = pass(*c);
    EXPECT_EQ(waiting.garbage_entries, 1u);
    EXPECT_EQ(waiting.chunks_deleted, 0u);
    EXPECT_TRUE(cluster->get_chunk(old));

    ASSERT_TRUE(meta->commit_upload(upload, Precondition{}, now));
    Report r = pass(*c);
    EXPECT_EQ(r.chunks_compacted, 1u);
    EXPECT_EQ(r.chunks_deleted, 1u);
    EXPECT_EQ(cluster->get_chunk(old).error().code, Errc::not_found);
    EXPECT_NE(meta->head(pending_ref)->parts.front().extent.chunk_id, old);
    EXPECT_EQ(get(pending_ref), plain);
}

TEST_F(ControllerTest, CompactionFindsOpenChunksOfAPolicyNoBucketUsesAnyMore) {
    add_six_nodes_in_three_zones();
    ASSERT_TRUE(meta->create_bucket(ec_bucket, BucketConfig{false, kEc42}));
    auto c = make(options());
    pass(*c);
    std::vector<std::byte> live = random_bytes(3000, 15);
    {
        auto w = writer();
        put(*w, ec_bucket, "dead", random_bytes(30000, 16), 30000);
        put(*w, ec_bucket, "live", live, 3000);
    }
    const ChunkId old = meta->head({ec_bucket, "live"})->parts.front().extent.chunk_id;
    ASSERT_TRUE(meta->remove({ec_bucket, "dead"}, Precondition{}, now));
    ASSERT_TRUE(meta->set_bucket_config(ec_bucket, BucketConfig{}));  // no Bucket uses 4+2 now
    for (ChunkInfo chunk : chunks(ChunkState::open)) {
        if (chunk.target_policy != kEc42)
            continue;
        chunk.state = ChunkState::sealed;
        chunk.writer.clear();
        ASSERT_TRUE(cluster->update_chunk(chunk));
    }

    Report r = pass(*c);
    EXPECT_EQ(r.chunks_compacted, 1u);
    EXPECT_EQ(cluster->get_chunk(old).error().code, Errc::not_found);
    auto moved = cluster->get_chunk(meta->head({ec_bucket, "live"})->parts.front().extent.chunk_id);
    ASSERT_TRUE(moved);
    EXPECT_EQ(moved->target_policy, kEc42);
    EXPECT_EQ(get({ec_bucket, "live"}), live);
}

TEST_F(ControllerTest, UnrecoverableChunksAreReportedAsAGauge) {
    add_six_nodes_in_three_zones();
    auto c = make(options());
    pass(*c);
    {
        auto w = writer();
        put(*w, bucket, "obj", random_bytes(2000, 17), 2000);
    }
    const ChunkId id = meta->head({bucket, "obj"})->parts.front().extent.chunk_id;
    seal_written_chunks();
    const std::vector<NodeId> lost = cluster->get_chunk(id)->nodes;
    for (const NodeId& holder : lost)
        stop(holder);

    tick(130s);
    pass(*c);
    std::size_t expected = 0;
    for (const ChunkInfo& chunk : chunks(std::nullopt))
        if (std::ranges::all_of(chunk.nodes, [&](const NodeId& n) { return std::ranges::contains(lost, n); }))
            ++expected;
    EXPECT_GE(expected, 1u);
    auto& gauge = metrics::global().gauge("rmk_controller_unrecoverable_chunks",
                                          "Chunks on dead nodes with no surviving source to repair from");
    EXPECT_EQ(gauge.value(), static_cast<double>(expected));
    pass(*c);
    EXPECT_EQ(gauge.value(), static_cast<double>(expected));  // a level, not a running count
}

TEST_F(ControllerTest, CompactionWaitsForTheGarbageThreshold) {
    add_six_nodes_in_three_zones();
    auto c = make(options());
    pass(*c);
    {
        auto w = writer();
        put(*w, bucket, "small-dead", random_bytes(1000, 11), 1000);
        put(*w, bucket, "big-live", random_bytes(20000, 12), 20000);
    }
    const ChunkId id = meta->head({bucket, "big-live"})->parts.front().extent.chunk_id;
    ASSERT_TRUE(meta->remove({bucket, "small-dead"}, Precondition{}, now));
    seal_written_chunks();
    Report r = pass(*c);
    EXPECT_EQ(r.garbage_entries, 1u);
    EXPECT_EQ(r.chunks_deleted, 0u);
    EXPECT_TRUE(cluster->get_chunk(id));
}

TEST_F(ControllerTest, MasterKeyRotationRewrapsEveryVersion) {
    const BucketRef versioned{"t1", "versioned"};
    ASSERT_TRUE(meta->create_bucket(versioned, BucketConfig{true, kReplicated3}));
    std::map<std::pair<std::string, Generation>, crypto::Key> data_keys;
    for (int i = 0; i < 5; ++i) {
        const ObjectRef ref{versioned, "obj-" + std::to_string(i % 3)};
        auto data_key = crypto::new_data_key(*keys);
        ASSERT_EQ(data_key->encryption.master_key_id, 1u);
        auto upload = meta->begin_upload(ref, data_key->encryption, now);
        ASSERT_TRUE(meta->put_part(*upload, Part{1, 10, 0, Extent{999, 64, 38, 0}}, now));
        auto generation = meta->commit_upload(*upload, Precondition{}, now);
        ASSERT_TRUE(generation);
        data_keys[{ref.key, *generation}] = data_key->key;
    }
    ASSERT_TRUE(meta->remove({versioned, "obj-0"}, Precondition{}, now));  // a delete marker

    auto rotated = std::make_shared<crypto::StaticKeyProvider>(
        std::map<std::uint32_t, crypto::Key>{{1, key_of(1)}, {2, key_of(2)}}, 2);
    Options o = options();
    o.rewrap_batch = 2;
    auto c = make(o, rotated);
    std::uint32_t total = 0;
    for (int i = 0; i < 4; ++i) {
        Report r = pass(*c);
        EXPECT_LE(r.versions_rewrapped, 2u);
        total += r.versions_rewrapped;
    }
    EXPECT_EQ(total, 5u);

    auto all = meta->scan_versions(std::nullopt, 100);
    ASSERT_TRUE(all);
    ASSERT_EQ(all->versions.size(), 6u);
    for (const ObjectMeta& version : all->versions) {
        if (version.delete_marker) {
            EXPECT_TRUE(version.encryption.wrapped_data_key.empty());
            continue;
        }
        EXPECT_EQ(version.encryption.master_key_id, 2u);
        auto key = crypto::unwrap(*rotated, version.encryption);
        ASSERT_TRUE(key);
        EXPECT_EQ(*key, (data_keys[{version.ref.key, version.generation}]));
    }
    EXPECT_EQ(pass(*c).versions_rewrapped, 0u);
}

TEST_F(ControllerTest, StartRunsPassesInTheBackground) {
    add_six_nodes_in_three_zones();
    Options o = options();
    o.interval = 1s;
    {
        auto c = make(o);
        c->start();
        for (int i = 0; i < 100 && chunks(ChunkState::open).size() < 2; ++i)
            std::this_thread::sleep_for(50ms);
    }
    EXPECT_EQ(chunks(ChunkState::open).size(), 2u);
}

}  // namespace
}  // namespace rmk::controller
