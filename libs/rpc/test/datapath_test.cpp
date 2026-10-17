#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "rmk/core/chunk_file.hpp"
#include "rmk/core/crc32c.hpp"
#include "rmk/datanode/datanode.hpp"
#include "rmk/ec/ec.hpp"
#include "rmk/stores/memory_stores.hpp"
#include "rmk/rpc/rpc.hpp"
#include "rmk/v1/datanode.grpc.pb.h"

namespace rmk {
namespace {

namespace fs = std::filesystem;
namespace pb = rmk::internal::v1;
using Bytes = std::vector<std::byte>;

constexpr std::uint64_t kMiB = std::uint64_t{1} << 20;

std::uint16_t free_port() {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    ::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof addr);
    socklen_t len = sizeof addr;
    ::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len);
    ::close(fd);
    return ntohs(addr.sin_port);
}

Bytes random_bytes(std::size_t n, std::uint64_t seed) {
    std::mt19937_64 rng(seed);
    Bytes out(n);
    for (auto& b : out)
        b = static_cast<std::byte>(rng());
    return out;
}

std::string hex_id(ChunkId id) {
    char buf[17];
    std::snprintf(buf, sizeof buf, "%016llx", static_cast<unsigned long long>(id));
    return buf;
}

Bytes file_bytes(const fs::path& path) {
    std::ifstream in(path, std::ios::binary);
    std::vector<char> raw((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    Bytes out(raw.size());
    std::memcpy(out.data(), raw.data(), raw.size());
    return out;
}

void flip_byte(const fs::path& path, std::uint64_t offset) {
    std::fstream f(path, std::ios::binary | std::ios::in | std::ios::out);
    f.seekg(static_cast<std::streamoff>(offset));
    char c = 0;
    f.read(&c, 1);
    c = static_cast<char>(c ^ 0x5A);
    f.seekp(static_cast<std::streamoff>(offset));
    f.write(&c, 1);
}

pb::FileRef replica(ChunkId id) {
    pb::FileRef ref;
    ref.set_chunk_id(id);
    return ref;
}

pb::FileRef shard(ChunkId id, std::uint32_t index) {
    pb::FileRef ref = replica(id);
    ref.set_shard(true);
    ref.set_shard_index(index);
    return ref;
}

struct TestNode {
    std::string id;
    std::string address;
    fs::path dir;
    std::unique_ptr<datanode::Node> node;
    std::unique_ptr<grpc::Server> server;

    void stop() {
        if (!server)
            return;
        server->Shutdown(std::chrono::system_clock::now());
        server->Wait();
        server.reset();
    }
    fs::path replica_path(ChunkId id) const { return dir / (hex_id(id) + ".r"); }
    fs::path shard_path(ChunkId id, int index) const { return dir / (hex_id(id) + ".s" + std::to_string(index)); }
};

// N in-process Data Nodes on loopback sharing one MemoryClusterStore.
class Cluster : public ::testing::Test {
protected:
    void start(int n) {
        static std::atomic<int> counter{0};
        root_ = fs::temp_directory_path() / ("rmk-rpc-" + std::to_string(::getpid()) + "-" + std::to_string(counter++));
        fs::remove_all(root_);
        store_ = std::make_shared<MemoryClusterStore>();
        channels_ = std::make_shared<rpc::Channels>(rpc::TlsConfig{.insecure = true, .cert_file = {}, .key_file = {}, .ca_file = {}});
        for (int i = 0; i < n; ++i) {
            auto node = std::make_unique<TestNode>();
            node->id = "dn-" + std::to_string(i);
            node->dir = root_ / node->id;
            node->address = "127.0.0.1:" + std::to_string(free_port());
            datanode::Options options;
            options.identity.id = node->id;
            options.identity.address = node->address;
            options.identity.zone = "z" + std::to_string(i);
            options.data_dir = node->dir;
            auto created = datanode::Node::create(options, store_, channels_);
            ASSERT_TRUE(created) << created.error().message;
            node->node = std::move(*created);
            grpc::ServerBuilder builder;
            int port = 0;
            builder.AddListeningPort(node->address, grpc::InsecureServerCredentials(), &port);
            builder.SetMaxReceiveMessageSize(32 << 20);
            builder.RegisterService(node->node->service());
            node->server = builder.BuildAndStart();
            ASSERT_TRUE(node->server && port != 0) << "cannot listen on " << node->address;
            ASSERT_TRUE(node->node->heartbeat());
            nodes_.push_back(std::move(node));
        }
    }

    void TearDown() override {
        for (auto& node : nodes_)
            node->stop();
        nodes_.clear();
        fs::remove_all(root_);
    }

    std::unique_ptr<pb::DataNode::Stub> stub(int node) {
        return pb::DataNode::NewStub(channels_->get(nodes_[node]->address));
    }

    // An open replicated Chunk with replicas on the given nodes.
    ChunkId make_chunk(std::vector<int> on, std::uint64_t capacity = 64 * kMiB) {
        ChunkId id = *store_->allocate_chunk_id();
        ChunkInfo info;
        info.id = id;
        info.capacity = capacity;
        info.size = kChunkHeaderSize;
        for (int n : on) {
            pb::CreateChunkRequest request;
            *request.mutable_file() = replica(id);
            request.set_capacity(capacity);
            request.set_data_units(1);
            request.set_parity_units(2);
            grpc::ClientContext ctx;
            pb::Empty empty;
            EXPECT_TRUE(stub(n)->CreateChunk(&ctx, request, &empty).ok());
            info.nodes.push_back(nodes_[n]->id);
        }
        EXPECT_TRUE(store_->put_chunk(info));
        return id;
    }

    grpc::Status raw_write(int node, const pb::FileRef& ref, std::uint64_t offset, const Bytes& bytes,
                           pb::WriteResponse* response) {
        grpc::ClientContext ctx;
        auto stream = stub(node)->Write(&ctx, response);
        pb::WriteRequest request;
        *request.mutable_header()->mutable_file() = ref;
        request.mutable_header()->set_offset(offset);
        stream->Write(request);
        request.set_data(reinterpret_cast<const char*>(bytes.data()), bytes.size());
        stream->Write(request);
        stream->WritesDone();
        return stream->Finish();
    }

    Result<Bytes> raw_read(int node, const pb::FileRef& ref, std::uint64_t offset, std::uint64_t length) {
        grpc::ClientContext ctx;
        pb::ReadRequest request;
        *request.mutable_file() = ref;
        request.set_offset(offset);
        request.set_length(length);
        auto stream = stub(node)->Read(&ctx, request);
        Bytes out;
        pb::ReadResponse response;
        while (stream->Read(&response)) {
            EXPECT_LE(response.data().size(), kMiB);
            auto p = reinterpret_cast<const std::byte*>(response.data().data());
            out.insert(out.end(), p, p + response.data().size());
        }
        grpc::Status status = stream->Finish();
        if (!status.ok())
            return std::unexpected(rpc::from_status(status));
        return out;
    }

    pb::StatResponse raw_stat(int node, const pb::FileRef& ref) {
        grpc::ClientContext ctx;
        pb::StatResponse response;
        EXPECT_TRUE(stub(node)->Stat(&ctx, ref, &response).ok());
        return response;
    }

    std::unique_ptr<rpc::ChunkWriter> writer(std::size_t per_policy = 1, std::chrono::seconds lease = std::chrono::seconds(30),
                                             std::string id = "test-writer") {
        rpc::WriterOptions options;
        options.writer_id = std::move(id);
        options.writers_per_policy = per_policy;
        options.lease = lease;
        return std::make_unique<rpc::ChunkWriter>(store_, channels_, options);
    }

    std::unique_ptr<rpc::ChunkReader> reader() { return std::make_unique<rpc::ChunkReader>(store_, channels_); }

    void seal(ChunkId id) {
        auto info = *store_->get_chunk(id);
        info.state = ChunkState::sealed;
        info.sealed_at = Clock::now();
        info.writer.clear();
        ASSERT_TRUE(store_->update_chunk(info));
    }

    grpc::Status encode(ChunkId id, int on) {
        pb::EncodeChunkRequest request;
        request.set_chunk_id(id);
        request.set_data_units(4);
        request.set_parity_units(2);
        for (int i = 0; i < 6; ++i)
            request.add_target_addresses(nodes_[i]->address);
        grpc::ClientContext ctx;
        pb::Empty empty;
        return stub(on)->EncodeChunk(&ctx, request, &empty);
    }

    grpc::Status rebuild(ChunkId id, int shard_index, int on, std::vector<int> unavailable = {}) {
        pb::RebuildShardRequest request;
        request.set_chunk_id(id);
        request.set_shard_index(static_cast<std::uint32_t>(shard_index));
        request.set_logical_size(store_->get_chunk(id)->size);
        request.set_data_units(4);
        request.set_parity_units(2);
        for (int i = 0; i < 6; ++i) {
            bool skip = i == shard_index || std::ranges::find(unavailable, i) != unavailable.end();
            request.add_source_addresses(skip ? "" : nodes_[i]->address);
        }
        grpc::ClientContext ctx;
        pb::Empty empty;
        return stub(on)->RebuildShard(&ctx, request, &empty);
    }

    struct Stored {
        Extent extent;
        Bytes bytes;
    };

    // A Chunk written through a ChunkWriter on nodes 0..2, sealed, then erasure coded 4+2 onto
    // nodes 0..5. Its logical size exceeds 4 * 4 MiB, so encoding takes several blocks.
    std::pair<ChunkId, std::vector<Stored>> erasure_coded_chunk() {
        ChunkId id = make_chunk({0, 1, 2});
        std::vector<Stored> stored;
        {
            auto w = writer();
            std::size_t sizes[] = {3 * kMiB + 12345, 77, 4 * kMiB + 1, 3 * kMiB, 5 * kMiB + 999, 4096, 2 * kMiB + 3};
            for (std::size_t i = 0; i < std::size(sizes); ++i) {
                Bytes bytes = random_bytes(sizes[i], 100 + i);
                auto extent = w->write(kReplicated3, bytes);
                EXPECT_TRUE(extent) << extent.error().message;
                stored.push_back({*extent, std::move(bytes)});
            }
        }
        seal(id);
        grpc::Status status = encode(id, 0);
        EXPECT_TRUE(status.ok()) << status.error_message();
        pb::Empty empty;

        auto info = *store_->get_chunk(id);
        info.state = ChunkState::erasure_coded;
        info.data_units = 4;
        info.parity_units = 2;
        info.nodes.clear();
        for (int i = 0; i < 6; ++i)
            info.nodes.push_back(nodes_[i]->id);
        EXPECT_TRUE(store_->update_chunk(info));
        for (int i = 0; i < 3; ++i) {
            grpc::ClientContext del;
            EXPECT_TRUE(stub(i)->DeleteChunk(&del, replica(id), &empty).ok());
        }
        return {id, stored};
    }

    void expect_readable(const std::vector<Stored>& stored) {
        auto r = reader();
        for (const Stored& s : stored) {
            auto data = r->read(s.extent);
            ASSERT_TRUE(data) << data.error().message;
            EXPECT_TRUE(*data == s.bytes) << "extent at " << s.extent.offset;
        }
    }

    fs::path root_;
    std::shared_ptr<MemoryClusterStore> store_;
    std::shared_ptr<rpc::Channels> channels_;
    std::vector<std::unique_ptr<TestNode>> nodes_;
};

TEST_F(Cluster, HeartbeatReportsAddressAndCapacity) {
    start(1);
    auto nodes = *store_->list_nodes();
    ASSERT_EQ(nodes.size(), 1u);
    EXPECT_EQ(nodes[0].id, "dn-0");
    EXPECT_EQ(nodes[0].address, nodes_[0]->address);
    EXPECT_EQ(nodes[0].zone, "z0");
    EXPECT_GT(nodes[0].capacity_bytes, 0u);
}

TEST_F(Cluster, FilesFollowTheNamingScheme) {
    start(1);
    ChunkId id = make_chunk({0});
    EXPECT_TRUE(fs::exists(nodes_[0]->replica_path(id)));
    auto st = raw_stat(0, replica(id));
    EXPECT_TRUE(st.exists());
    EXPECT_EQ(st.size(), kChunkHeaderSize);
    EXPECT_EQ(st.capacity(), 64 * kMiB);
    EXPECT_FALSE(raw_stat(0, replica(id + 1)).exists());

    auto below_header = raw_read(0, replica(id), 10, 4);
    ASSERT_FALSE(below_header);
    EXPECT_EQ(below_header.error().code, Errc::invalid_argument);

    pb::WriteResponse response;
    ASSERT_TRUE(raw_write(0, replica(id), 64, Bytes(10, std::byte{1}), &response).ok());
    grpc::Status overwrite = raw_write(0, replica(id), 70, Bytes(10, std::byte{2}), &response);
    EXPECT_EQ(rpc::from_status(overwrite).code, Errc::invalid_argument);
}

TEST_F(Cluster, WriterPlacesIdenticalBytesOnEveryReplica) {
    start(3);
    ChunkId id = make_chunk({0, 1, 2});
    auto w = writer();
    std::vector<Bytes> parts{random_bytes(1000, 1), random_bytes(2 * kMiB + 17, 2), random_bytes(1, 3)};
    std::vector<Extent> extents;
    std::uint64_t expected_offset = kChunkHeaderSize;
    for (const Bytes& part : parts) {
        auto extent = w->write(kReplicated3, part);
        ASSERT_TRUE(extent) << extent.error().message;
        EXPECT_EQ(extent->chunk_id, id);
        EXPECT_EQ(extent->offset, expected_offset);
        EXPECT_EQ(extent->length, part.size());
        EXPECT_EQ(extent->crc32c, crc32c(part));
        expected_offset += part.size();
        extents.push_back(*extent);
    }
    for (int n = 0; n < 3; ++n) {
        EXPECT_EQ(raw_stat(n, replica(id)).size(), expected_offset);
        for (std::size_t i = 0; i < parts.size(); ++i) {
            auto data = raw_read(n, replica(id), extents[i].offset, extents[i].length);
            ASSERT_TRUE(data);
            EXPECT_TRUE(*data == parts[i]);
        }
    }
    auto info = *store_->get_chunk(id);
    EXPECT_EQ(info.size, expected_offset);
    EXPECT_EQ(info.writer.rfind("test-writer", 0), 0u);

    auto r = reader();
    for (std::size_t i = 0; i < parts.size(); ++i) {
        auto data = r->read(extents[i]);
        ASSERT_TRUE(data) << data.error().message;
        EXPECT_TRUE(*data == parts[i]);
    }

    w.reset();
    info = *store_->get_chunk(id);
    EXPECT_EQ(info.state, ChunkState::open);
    EXPECT_TRUE(info.writer.empty());
    EXPECT_EQ(info.size, expected_offset);
}

TEST_F(Cluster, WriterSkipsChunkWithReplicaDownAtClaim) {
    start(4);
    ChunkId first = make_chunk({0, 1, 2});
    ChunkId second = make_chunk({0, 1, 3});
    nodes_[2]->stop();
    auto w = writer();
    Bytes bytes = random_bytes(5000, 4);
    auto extent = w->write(kReplicated3, bytes);
    ASSERT_TRUE(extent) << extent.error().message;
    EXPECT_EQ(extent->chunk_id, second);
    EXPECT_EQ(store_->get_chunk(first)->state, ChunkState::sealed);
    EXPECT_TRUE(store_->get_chunk(first)->writer.empty());
    EXPECT_TRUE(*reader()->read(*extent) == bytes);
}

TEST_F(Cluster, ReplicaFailingMidWriteSealsTheChunk) {
    start(4);
    ChunkId first = make_chunk({0, 1, 2});
    ChunkId second = make_chunk({0, 1, 3});
    auto w = writer();
    Bytes a = random_bytes(3000, 5);
    Bytes b = random_bytes(4000, 6);
    auto ea = w->write(kReplicated3, a);
    ASSERT_TRUE(ea);
    EXPECT_EQ(ea->chunk_id, first);

    nodes_[2]->stop();
    auto eb = w->write(kReplicated3, b);
    ASSERT_TRUE(eb) << eb.error().message;
    EXPECT_EQ(eb->chunk_id, second);
    auto sealed = *store_->get_chunk(first);
    EXPECT_EQ(sealed.state, ChunkState::sealed);
    EXPECT_EQ(sealed.size, ea->offset + ea->length);

    auto r = reader();
    EXPECT_TRUE(*r->read(*ea) == a);
    EXPECT_TRUE(*r->read(*eb) == b);
}

TEST_F(Cluster, CrashedWritersBytesAreNeverOverwritten) {
    start(3);
    ChunkId id = make_chunk({0, 1, 2});
    Bytes leftover = random_bytes(1000, 7);
    pb::WriteResponse response;
    ASSERT_TRUE(raw_write(1, replica(id), kChunkHeaderSize, leftover, &response).ok());
    ASSERT_EQ(store_->get_chunk(id)->size, kChunkHeaderSize);

    auto w = writer();
    Bytes bytes = random_bytes(2000, 8);
    auto extent = w->write(kReplicated3, bytes);
    ASSERT_TRUE(extent) << extent.error().message;
    EXPECT_EQ(extent->chunk_id, id);
    EXPECT_EQ(extent->offset, kChunkHeaderSize + leftover.size());
    EXPECT_TRUE(*raw_read(1, replica(id), kChunkHeaderSize, leftover.size()) == leftover);
    EXPECT_TRUE(*reader()->read(*extent) == bytes);
}

TEST_F(Cluster, ReaderFallsBackFromCorruptReplica) {
    start(3);
    ChunkId id = make_chunk({0, 1, 2});
    Bytes bytes = random_bytes(10000, 9);
    auto extent = writer()->write(kReplicated3, bytes);
    ASSERT_TRUE(extent);
    flip_byte(nodes_[1]->replica_path(id), extent->offset + 4321);

    auto r = reader();
    for (int i = 0; i < 20; ++i) {
        auto data = r->read(*extent);
        ASSERT_TRUE(data) << data.error().message;
        EXPECT_TRUE(*data == bytes);
    }
    flip_byte(nodes_[0]->replica_path(id), extent->offset + 1);
    flip_byte(nodes_[2]->replica_path(id), extent->offset + 2);
    auto broken = r->read(*extent);
    ASSERT_FALSE(broken);
    EXPECT_EQ(broken.error().code, Errc::corruption);
}

TEST_F(Cluster, ConcurrentWritesNeverOverlap) {
    start(3);
    for (int i = 0; i < 3; ++i)
        make_chunk({0, 1, 2}, 4 * kMiB);
    auto w = writer(2);
    std::mutex mu;
    std::vector<std::pair<Extent, Bytes>> written;
    std::vector<std::jthread> threads;
    for (int t = 0; t < 6; ++t)
        threads.emplace_back([&, t] {
            for (int i = 0; i < 5; ++i) {
                Bytes bytes = random_bytes(50000 + 1000 * static_cast<std::size_t>(i), 1000 + 10 * t + i);
                auto extent = w->write(kReplicated3, bytes);
                ASSERT_TRUE(extent) << extent.error().message;
                std::lock_guard lock(mu);
                written.emplace_back(*extent, std::move(bytes));
            }
        });
    threads.clear();
    ASSERT_EQ(written.size(), 30u);
    std::set<std::pair<ChunkId, std::uint64_t>> starts;
    for (auto& [extent, bytes] : written) {
        EXPECT_TRUE(starts.insert({extent.chunk_id, extent.offset}).second);
        for (auto& [other, unused] : written) {
            if (&other == &extent || other.chunk_id != extent.chunk_id)
                continue;
            EXPECT_TRUE(other.offset + other.length <= extent.offset || extent.offset + extent.length <= other.offset);
        }
    }
    auto r = reader();
    for (auto& [extent, bytes] : written)
        EXPECT_TRUE(*r->read(extent) == bytes);
}

TEST_F(Cluster, WriterReportsUnavailableWithoutOpenChunks) {
    start(3);
    auto extent = writer()->write(kReplicated3, random_bytes(10, 1));
    ASSERT_FALSE(extent);
    EXPECT_EQ(extent.error().code, Errc::unavailable);
}

TEST_F(Cluster, CopyChunkFromPullsTheWholeFile) {
    start(4);
    ChunkId id = make_chunk({0, 1, 2});
    Bytes bytes = random_bytes(5 * kMiB + 3, 10);
    auto extent = writer()->write(kReplicated3, bytes);
    ASSERT_TRUE(extent);

    pb::CopyChunkRequest request;
    *request.mutable_file() = replica(id);
    request.set_source_address(nodes_[0]->address);
    grpc::ClientContext ctx;
    pb::Empty empty;
    grpc::Status status = stub(3)->CopyChunkFrom(&ctx, request, &empty);
    ASSERT_TRUE(status.ok()) << status.error_message();
    EXPECT_TRUE(file_bytes(nodes_[3]->replica_path(id)) == file_bytes(nodes_[0]->replica_path(id)));
    EXPECT_EQ(raw_stat(3, replica(id)).size(), raw_stat(0, replica(id)).size());

    auto info = *store_->get_chunk(id);
    info.nodes = {nodes_[3]->id};
    ASSERT_TRUE(store_->update_chunk(info));
    nodes_[0]->stop();
    EXPECT_TRUE(*reader()->read(*extent) == bytes);
}

TEST_F(Cluster, ErasureCodedReadsSurviveParityLoss) {
    start(6);
    auto [id, stored] = erasure_coded_chunk();
    const std::uint64_t logical = store_->get_chunk(id)->size;
    const std::uint64_t shard_len = ec::shard_length(logical, 4);
    ASSERT_GT(shard_len, 4 * kMiB);
    for (int i = 0; i < 6; ++i) {
        auto st = raw_stat(i, shard(id, static_cast<std::uint32_t>(i)));
        EXPECT_TRUE(st.exists());
        EXPECT_EQ(st.size(), kChunkHeaderSize + shard_len);
        EXPECT_FALSE(raw_stat(i, replica(id)).exists());
    }
    expect_readable(stored);

    // A bad byte in a data shard is caught by the extent crc and rebuilt from the others.
    const std::uint64_t bad = shard_len + 100;  // logical position of shard 1, byte 100
    flip_byte(nodes_[1]->shard_path(id, 1), kChunkHeaderSize + 100);
    auto hit = std::ranges::find_if(stored, [&](const Stored& s) {
        return s.extent.offset <= bad && bad < s.extent.offset + s.extent.length;
    });
    ASSERT_NE(hit, stored.end());
    auto data = reader()->read(hit->extent);
    ASSERT_TRUE(data) << data.error().message;
    EXPECT_TRUE(*data == hit->bytes);
    flip_byte(nodes_[1]->shard_path(id, 1), kChunkHeaderSize + 100);

    nodes_[1]->stop();
    nodes_[4]->stop();
    expect_readable(stored);

    nodes_[0]->stop();
    auto lost = reader()->read(stored[0].extent);
    ASSERT_FALSE(lost);
    EXPECT_EQ(lost.error().code, Errc::unavailable);
}

TEST_F(Cluster, RebuildShardRestoresADeletedShard) {
    start(6);
    auto [id, stored] = erasure_coded_chunk();
    const Bytes original = file_bytes(nodes_[2]->shard_path(id, 2));
    pb::Empty empty;
    {
        grpc::ClientContext ctx;
        ASSERT_TRUE(stub(2)->DeleteChunk(&ctx, shard(id, 2), &empty).ok());
    }
    ASSERT_FALSE(fs::exists(nodes_[2]->shard_path(id, 2)));

    grpc::Status status = rebuild(id, 2, 2, {5});
    ASSERT_TRUE(status.ok()) << status.error_message();
    EXPECT_TRUE(file_bytes(nodes_[2]->shard_path(id, 2)) == original);

    // Only shards 2..5 remain, so every read now depends on the rebuilt one.
    nodes_[0]->stop();
    nodes_[1]->stop();
    expect_readable(stored);
}

TEST_F(Cluster, RebuildRefusesInconsistentSources) {
    start(6);
    auto [id, stored] = erasure_coded_chunk();
    const Bytes original = file_bytes(nodes_[2]->shard_path(id, 2));
    flip_byte(nodes_[0]->shard_path(id, 0), kChunkHeaderSize + 5000);

    // Five sources, one of them bad: the spare exposes the disagreement and nothing is written.
    grpc::Status status = rebuild(id, 2, 2);
    EXPECT_EQ(rpc::from_status(status).code, Errc::corruption) << status.error_message();
    EXPECT_TRUE(file_bytes(nodes_[2]->shard_path(id, 2)) == original);
}

TEST_F(Cluster, ErasureCodedReadSkipsABadRebuildSource) {
    start(6);
    auto [id, stored] = erasure_coded_chunk();
    const std::uint64_t shard_len = ec::shard_length(store_->get_chunk(id)->size, 4);
    const std::uint64_t at = shard_len + 100;  // shard 1, byte 100
    auto hit = std::ranges::find_if(stored, [&](const Stored& s) {
        return s.extent.offset <= at && at < s.extent.offset + s.extent.length;
    });
    ASSERT_NE(hit, stored.end());
    // Shard 1 is gone and shard 0, the first source used to rebuild it, is corrupt there.
    nodes_[1]->stop();
    flip_byte(nodes_[0]->shard_path(id, 0), kChunkHeaderSize + 100);
    auto data = reader()->read(hit->extent);
    ASSERT_TRUE(data) << data.error().message;
    EXPECT_TRUE(*data == hit->bytes);
}

TEST_F(Cluster, EncodeOnlyForSealedChunks) {
    start(6);
    auto [id, stored] = erasure_coded_chunk();
    const Bytes shard3 = file_bytes(nodes_[3]->shard_path(id, 3));
    EXPECT_EQ(rpc::from_status(encode(id, 0)).code, Errc::precondition_failed);
    EXPECT_TRUE(file_bytes(nodes_[3]->shard_path(id, 3)) == shard3);

    ChunkId open = make_chunk({0, 1, 2});
    ASSERT_TRUE(writer()->write(kReplicated3, random_bytes(1000, 1)));
    EXPECT_EQ(rpc::from_status(encode(open, 0)).code, Errc::precondition_failed);
}

TEST_F(Cluster, ShardsAppearOnlyWhenComplete) {
    start(1);
    pb::CreateChunkRequest create;
    *create.mutable_file() = shard(42, 1);
    create.set_capacity(kChunkHeaderSize + 1000);
    create.set_data_units(2);
    create.set_parity_units(1);
    pb::Empty empty;
    {
        grpc::ClientContext ctx;
        ASSERT_TRUE(stub(0)->CreateChunk(&ctx, create, &empty).ok());
    }
    EXPECT_FALSE(raw_stat(0, shard(42, 1)).exists());

    pb::WriteResponse response;
    grpc::Status partial = raw_write(0, shard(42, 1), kChunkHeaderSize, random_bytes(400, 1), &response);
    EXPECT_EQ(rpc::from_status(partial).code, Errc::invalid_argument);
    EXPECT_FALSE(raw_stat(0, shard(42, 1)).exists());

    // A new CreateChunk restarts the staging file; a complete stream is renamed into place.
    {
        grpc::ClientContext ctx;
        ASSERT_TRUE(stub(0)->CreateChunk(&ctx, create, &empty).ok());
    }
    const Bytes full = random_bytes(1000, 2);
    ASSERT_TRUE(raw_write(0, shard(42, 1), kChunkHeaderSize, full, &response).ok());
    EXPECT_EQ(raw_stat(0, shard(42, 1)).size(), kChunkHeaderSize + 1000);
    EXPECT_TRUE(*raw_read(0, shard(42, 1), kChunkHeaderSize, 1000) == full);

    // Staging a replacement leaves the existing shard untouched.
    {
        grpc::ClientContext ctx;
        ASSERT_TRUE(stub(0)->CreateChunk(&ctx, create, &empty).ok());
    }
    EXPECT_TRUE(*raw_read(0, shard(42, 1), kChunkHeaderSize, 1000) == full);
}

TEST_F(Cluster, IdleWriterReleasesItsChunk) {
    start(3);
    ChunkId id = make_chunk({0, 1, 2});
    auto w = writer(1, std::chrono::seconds(1));
    auto extent = w->write(kReplicated3, random_bytes(100, 1));
    ASSERT_TRUE(extent);
    EXPECT_FALSE(store_->get_chunk(id)->writer.empty());

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (!store_->get_chunk(id)->writer.empty() && std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    auto info = *store_->get_chunk(id);
    EXPECT_TRUE(info.writer.empty());
    EXPECT_EQ(info.state, ChunkState::open);
    EXPECT_EQ(info.size, extent->offset + extent->length);

    // Another writer can now use the Chunk; the first one claims afresh when it writes again.
    auto other = writer(1, std::chrono::seconds(30), "other-writer");
    auto next = other->write(kReplicated3, random_bytes(100, 2));
    ASSERT_TRUE(next) << next.error().message;
    EXPECT_EQ(next->chunk_id, id);
    EXPECT_EQ(next->offset, extent->offset + extent->length);
}

TEST_F(Cluster, OversizedWriteDoesNotSealThePool) {
    start(3);
    ChunkId id = make_chunk({0, 1, 2}, 4 * kMiB);
    auto w = writer();
    ASSERT_TRUE(w->write(kReplicated3, random_bytes(1000, 1)));
    auto huge = w->write(kReplicated3, random_bytes(4 * kMiB, 2));
    ASSERT_FALSE(huge);
    EXPECT_EQ(huge.error().code, Errc::unavailable);
    EXPECT_EQ(store_->get_chunk(id)->state, ChunkState::open);
    auto small = w->write(kReplicated3, random_bytes(1000, 3));
    ASSERT_TRUE(small);
    EXPECT_EQ(small->chunk_id, id);
}

TEST_F(Cluster, SealingRecordsTheTime) {
    start(4);
    ChunkId first = make_chunk({0, 1, 2});
    make_chunk({0, 1, 3});
    nodes_[2]->stop();
    const auto before = Clock::now();
    ASSERT_TRUE(writer()->write(kReplicated3, random_bytes(10, 1)));
    auto info = *store_->get_chunk(first);
    EXPECT_EQ(info.state, ChunkState::sealed);
    EXPECT_GE(info.sealed_at, before);
}

}  // namespace
}  // namespace rmk
