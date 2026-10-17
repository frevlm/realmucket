#pragma once

// The whole system in one process, for tests that talk to it only through rmk::client::Client:
// in-memory stores, Data Nodes on loopback, a Controller driven by run_once, a Gateway.

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <optional>
#include <random>
#include <span>
#include <string>
#include <thread>
#include <vector>

#include "rmk/client.hpp"
#include "rmk/controller.hpp"
#include "rmk/crc32c.hpp"
#include "rmk/crypto.hpp"
#include "rmk/datanode.hpp"
#include "rmk/gateway.hpp"
#include "rmk/memory_stores.hpp"
#include "rmk/rpc.hpp"

namespace rmk::e2e {

namespace fs = std::filesystem;
namespace rc = ::rmk::client;

inline constexpr std::uint64_t kKiB = 1024;
inline constexpr std::uint64_t kMiB = 1024 * 1024;

// ----- Helpers

inline std::string pattern(std::size_t size, std::uint64_t seed) {
    std::mt19937_64 rng(seed);
    std::string out(size, '\0');
    for (auto& c : out)
        c = static_cast<char>(rng() & 0xFF);
    return out;
}

inline std::uint32_t crc_of(std::string_view s) {
    return crc32c(std::as_bytes(std::span(s.data(), s.size())));
}

// Unwraps a client Result; a failure is reported and yields a default value.
template <class T>
T take(rc::Result<T> r) {
    if (!r) {
        ADD_FAILURE() << "client error " << static_cast<int>(r.error().code) << ": " << r.error().message;
        return T{};
    }
    return std::move(*r);
}

inline void take(const rc::Status& s) {
    if (!s)
        ADD_FAILURE() << "client error " << static_cast<int>(s.error().code) << ": " << s.error().message;
}

template <class R>
void expect_error(const R& r, rc::ErrorCode want) {
    if (r)
        ADD_FAILURE() << "expected error " << static_cast<int>(want) << " but the call succeeded";
    else
        EXPECT_EQ(r.error().code, want) << r.error().message;
}

inline std::uint16_t free_port() {
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

// ----- The cluster

struct Config {
    int nodes = 4;                                  // zones z1..z3 cycling
    std::uint64_t chunk_capacity = 1 * kMiB;        // small, so Chunks fill and seal quickly
    std::uint64_t gateway_part_size = 64 * kKiB;
    std::size_t writers_per_policy = 2;
    std::chrono::seconds repair_delay{60};          // also the suspect threshold
};

struct TestNode {
    std::string id;
    std::string address;
    fs::path dir;
    std::unique_ptr<datanode::Node> node;
    std::unique_ptr<grpc::Server> server;

    bool alive() const { return server != nullptr; }
};

class E2E : public ::testing::Test {
protected:
    virtual Config config() const { return {}; }

    void SetUp() override {
        cfg = config();
        std::string templ = (fs::temp_directory_path() / "rmk-e2e-XXXXXX").string();
        ASSERT_NE(mkdtemp(templ.data()), nullptr);
        root = templ;

        rpc::TlsConfig tls;
        tls.insecure = true;
        channels = std::make_shared<rpc::Channels>(tls);
        crypto::Key master = crypto::random_key();
        keys = std::make_shared<crypto::StaticKeyProvider>(std::map<std::uint32_t, crypto::Key>{{1, master}}, 1);

        for (int i = 0; i < cfg.nodes; ++i) {
            start_node(i);
            if (HasFatalFailure())
                return;
        }

        controller::Options co;
        co.id = "e2e-controller";
        co.chunk_capacity = cfg.chunk_capacity;
        co.suspect_after = cfg.repair_delay;
        co.repair_delay = cfg.repair_delay;
        co.compaction_grace = std::chrono::seconds{0};  // tests seal and compact within one run
        auto created = controller::Controller::create(co, controller::Dependencies{stores(), keys, channels});
        ASSERT_TRUE(created.has_value()) << created.error().message;
        controller = std::move(*created);
        pass();  // the open Chunk pool the first write needs

        gateway::Options go;
        go.part_size = cfg.gateway_part_size;
        go.cluster_config_cache = std::chrono::seconds{0};
        rpc::WriterOptions wo;
        wo.writer_id = "e2e-gateway";
        wo.writers_per_policy = cfg.writers_per_policy;
        gateway::Dependencies deps;
        deps.stores = stores();
        deps.keys = keys;
        deps.writer = std::make_shared<rpc::ChunkWriter>(cluster, channels, wo);
        deps.reader = std::make_shared<rpc::ChunkReader>(cluster, channels);
        auto gw = gateway::Gateway::create(go, std::move(deps));
        ASSERT_TRUE(gw.has_value()) << gw.error().message;
        gateway = std::move(*gw);

        grpc::ServerBuilder builder;
        int port = 0;
        builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
        builder.RegisterService(gateway->service());
        builder.SetMaxReceiveMessageSize(32 << 20);
        builder.SetMaxSendMessageSize(32 << 20);
        server = builder.BuildAndStart();
        ASSERT_NE(port, 0);
        endpoint = "127.0.0.1:" + std::to_string(port);

        admin = connect();
    }

    void TearDown() override {
        admin.reset();
        if (server)
            server->Shutdown(std::chrono::system_clock::now());
        server.reset();
        gateway.reset();  // releases the writer leases
        controller.reset();
        for (auto& n : nodes)
            stop_node(n);
        std::error_code ec;
        fs::remove_all(root, ec);
    }

    // ----- Clients: small multipart threshold and Part size so a few hundred KiB use many Parts.

    rc::Options client_options() const {
        rc::Options o;
        o.endpoint = endpoint;
        o.insecure = true;
        o.multipart_threshold = 100 * kKiB;
        o.part_size = 64 * kKiB;
        o.parallel_parts = 4;
        o.timeout = std::chrono::seconds{30};
        return o;
    }

    std::unique_ptr<rc::Client> connect() const {
        return connect(client_options());
    }

    std::unique_ptr<rc::Client> connect(rc::Options options) const {
        auto c = rc::Client::connect(std::move(options));
        if (!c) {
            ADD_FAILURE() << "connect: " << c.error().message;
            return nullptr;
        }
        return std::make_unique<rc::Client>(std::move(*c));
    }

    // Creates a Tenant and returns a fresh client for it.
    std::unique_ptr<rc::Client> tenant_admin(const std::string& tenant, const rc::Quota& quota = {}) {
        take(admin->create_tenant(tenant, quota));
        return connect();
    }

    // ----- Stores and the Controller

    rpc::Stores stores() const { return {meta, config_store, cluster}; }

    // A Data Node answers every heartbeat; the Controller judges liveness by them.
    void beat_live() {
        for (auto& n : nodes) {
            if (n.alive()) {
                EXPECT_TRUE(n.node->heartbeat().has_value());
            }
        }
    }

    controller::Report pass() {
        beat_live();
        return pass_at(Clock::now());
    }

    controller::Report pass_at(Clock::time_point now) {
        auto report = controller->run_once(now);
        if (!report) {
            ADD_FAILURE() << report.error().message;
            return {};
        }
        EXPECT_TRUE(report->leader);
        return *report;
    }

    // ----- Data Nodes

    void start_node(int i) {
        TestNode n;
        n.id = "dn" + std::to_string(i + 1);
        n.dir = root / n.id;
        fs::create_directories(n.dir);
        datanode::Options options;
        options.identity.id = n.id;
        options.identity.zone = "z" + std::to_string(i % 3 + 1);
        options.identity.k8s_node = "host-" + std::to_string(i);
        options.identity.address = "127.0.0.1:" + std::to_string(free_port());
        options.identity.capacity_bytes = std::uint64_t{1} << 30;
        options.data_dir = n.dir;
        n.address = options.identity.address;
        auto node = datanode::Node::create(options, cluster, channels);
        ASSERT_TRUE(node.has_value()) << node.error().message;
        n.node = std::move(*node);
        grpc::ServerBuilder builder;
        int port = 0;
        builder.AddListeningPort(n.address, grpc::InsecureServerCredentials(), &port);
        builder.SetMaxReceiveMessageSize(32 << 20);
        builder.SetMaxSendMessageSize(32 << 20);
        builder.RegisterService(n.node->service());
        n.server = builder.BuildAndStart();
        ASSERT_TRUE(n.server && port != 0) << "cannot listen on " << n.address;
        ASSERT_TRUE(n.node->heartbeat().has_value());
        nodes.push_back(std::move(n));
    }

    static void stop_node(TestNode& n) {
        if (n.server) {
            n.server->Shutdown(std::chrono::system_clock::now());
            n.server->Wait();
            n.server.reset();
        }
        n.node.reset();
    }

    TestNode* node_by_id(const std::string& id) {
        for (auto& n : nodes)
            if (n.id == id)
                return &n;
        return nullptr;
    }

    void stop_node(const std::string& id) {
        auto* n = node_by_id(id);
        ASSERT_NE(n, nullptr) << id;
        stop_node(*n);
    }

    // ----- Looking behind the API

    ObjectMeta stored(const std::string& tenant, const std::string& bucket, const std::string& key) {
        auto head = meta->head(ObjectRef{{tenant, bucket}, key});
        if (!head) {
            ADD_FAILURE() << "head " << tenant << "/" << bucket << "/" << key << ": " << head.error().message;
            return {};
        }
        return *head;
    }

    ChunkInfo chunk(ChunkId id) {
        auto c = cluster->get_chunk(id);
        if (!c) {
            ADD_FAILURE() << "chunk " << id << ": " << c.error().message;
            return {};
        }
        return *c;
    }

    std::vector<ChunkInfo> chunks_in(std::optional<ChunkState> state) {
        auto list = cluster->list_chunks(state, std::nullopt, 1000);
        if (!list) {
            ADD_FAILURE() << list.error().message;
            return {};
        }
        return *list;
    }

    Config cfg;
    fs::path root;
    std::shared_ptr<MemoryMetaStore> meta = std::make_shared<MemoryMetaStore>(std::chrono::hours{1});
    std::shared_ptr<MemoryConfigStore> config_store = std::make_shared<MemoryConfigStore>();
    std::shared_ptr<MemoryClusterStore> cluster = std::make_shared<MemoryClusterStore>();
    std::shared_ptr<rpc::Channels> channels;
    std::shared_ptr<crypto::KeyProvider> keys;
    std::vector<TestNode> nodes;
    std::unique_ptr<controller::Controller> controller;
    std::unique_ptr<gateway::Gateway> gateway;
    std::unique_ptr<grpc::Server> server;
    std::string endpoint;
    std::unique_ptr<rc::Client> admin;  // creates Tenants
};

// Six nodes: a 4+2 stripe gets one shard per node.
class E2EWide : public E2E {
protected:
    Config config() const override {
        Config c;
        c.nodes = 6;
        return c;
    }
};

}  // namespace rmk::e2e
