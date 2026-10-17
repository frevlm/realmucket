// rmk-dev: Gateway, Controller and N Data Nodes in one process on in-memory stores.
// Local development without FoundationDB or Docker; everything is lost when it exits.

#include <arpa/inet.h>
#include <netinet/in.h>
#include <pthread.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <filesystem>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include <grpcpp/grpcpp.h>

#include "rmk/controller.hpp"
#include "rmk/crypto.hpp"
#include "rmk/datanode.hpp"
#include "rmk/gateway.hpp"
#include "rmk/memory_stores.hpp"
#include "rmk/rpc.hpp"

namespace {

namespace fs = std::filesystem;

constexpr int kMaxMessageBytes = 32 << 20;
constexpr int kZones = 3;
constexpr std::uint64_t kMiB = std::uint64_t{1} << 20;

void log_line(const char* level, const std::string& message) {
    std::time_t now = std::time(nullptr);
    std::tm tm{};
    ::gmtime_r(&now, &tm);
    char stamp[32];
    std::strftime(stamp, sizeof stamp, "%Y-%m-%dT%H:%M:%SZ", &tm);
    std::fprintf(stderr, "%s %s dev %s\n", stamp, level, message.c_str());
}

int fatal(const std::string& what, const std::string& why) {
    log_line("fatal", what + ": " + why);
    return 1;
}

int fatal(const std::string& what, const rmk::Error& error) {
    return fatal(what, std::string(rmk::errc_name(error.code)) + ": " + error.message);
}

struct Flags {
    int nodes = 4;
    std::string listen = "127.0.0.1:7000";
    fs::path data_dir = "./.rmk-dev";
    std::uint64_t chunk_mib = 16;
};

const char* kUsage =
    "usage: rmk-dev [--nodes N] [--listen HOST:PORT] [--data-dir DIR]\n"
    "               [--chunk-mib N]\n"
    "  --nodes            Data Nodes to run, zones z1..z3 cycling (default 4)\n"
    "  --listen           Gateway address (default 127.0.0.1:7000)\n"
    "  --data-dir         where the Data Nodes keep chunk files (default ./.rmk-dev)\n"
    "  --chunk-mib        chunk capacity in MiB (default 16); a Part must fit in a chunk\n";

bool parse_flags(int argc, char** argv, Flags& flags, std::string& error) {
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        std::string name = arg;
        std::string value;
        bool inline_value = false;
        if (auto eq = arg.find('='); eq != std::string::npos && arg.starts_with("--")) {
            name = arg.substr(0, eq);
            value = arg.substr(eq + 1);
            inline_value = true;
        }
        if (name == "--help" || name == "-h") {
            error = "";
            return false;
        }
        if (!inline_value) {
            if (i + 1 >= argc) {
                error = name + " needs a value";
                return false;
            }
            value = argv[++i];
        }
        auto number = [&](std::uint64_t min, std::uint64_t max, std::uint64_t& out) {
            try {
                std::size_t used = 0;
                auto parsed = std::stoull(value, &used);
                if (used != value.size() || parsed < min || parsed > max)
                    throw std::out_of_range(name);
                out = parsed;
                return true;
            } catch (const std::exception&) {
                error = name + " needs a number between " + std::to_string(min) + " and " + std::to_string(max);
                return false;
            }
        };
        if (name == "--nodes") {
            std::uint64_t n = 0;
            if (!number(1, 64, n))
                return false;
            flags.nodes = static_cast<int>(n);
        } else if (name == "--listen") {
            flags.listen = value;
        } else if (name == "--data-dir") {
            flags.data_dir = value;
        } else if (name == "--chunk-mib") {
            if (!number(1, 1 << 20, flags.chunk_mib))
                return false;
        } else {
            error = "unknown flag " + name;
            return false;
        }
    }
    return true;
}

// A loopback port that is free right now; the Data Node must know its address before it starts.
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

struct DataNode {
    std::unique_ptr<rmk::datanode::Node> node;
    std::unique_ptr<grpc::Server> server;
};

int run(const Flags& flags) {
    // Signals are taken synchronously by sigwait below; every thread started later inherits the mask.
    sigset_t signals;
    sigemptyset(&signals);
    sigaddset(&signals, SIGTERM);
    sigaddset(&signals, SIGINT);
    // A shell that backgrounded us may have left SIGINT ignored, which sigwait would never see.
    std::signal(SIGINT, SIG_DFL);
    std::signal(SIGTERM, SIG_DFL);
    pthread_sigmask(SIG_BLOCK, &signals, nullptr);

    std::error_code ec;
    fs::create_directories(flags.data_dir, ec);
    if (ec)
        return fatal("data dir " + flags.data_dir.string(), ec.message());

    rmk::rpc::TlsConfig tls;
    tls.insecure = true;
    auto channels = std::make_shared<rmk::rpc::Channels>(tls);
    auto meta = std::make_shared<rmk::MemoryMetaStore>(std::chrono::hours{24 * 7});
    auto config = std::make_shared<rmk::MemoryConfigStore>();
    auto cluster = std::make_shared<rmk::MemoryClusterStore>();
    rmk::rpc::Stores stores{meta, config, cluster};

    rmk::crypto::Key master = rmk::crypto::random_key();
    auto keys = std::make_shared<rmk::crypto::StaticKeyProvider>(std::map<std::uint32_t, rmk::crypto::Key>{{1, master}}, 1);

    // ----- Data Nodes. Chunk ids restart with the in-memory stores, so old files must not survive.
    std::vector<DataNode> nodes;
    for (int i = 0; i < flags.nodes; ++i) {
        const std::string id = "dn" + std::to_string(i + 1);
        const fs::path dir = flags.data_dir / ("node-" + std::to_string(i + 1));
        fs::remove_all(dir, ec);
        fs::create_directories(dir, ec);
        if (ec)
            return fatal("data dir " + dir.string(), ec.message());

        rmk::datanode::Options options;
        options.identity.id = id;
        options.identity.address = "127.0.0.1:" + std::to_string(free_port());
        options.identity.zone = "z" + std::to_string(i % kZones + 1);
        options.identity.k8s_node = "dev";
        options.data_dir = dir;
        const std::string address = options.identity.address;
        auto created = rmk::datanode::Node::create(std::move(options), cluster, channels);
        if (!created)
            return fatal("create " + id, created.error());

        DataNode dn;
        dn.node = std::move(*created);
        grpc::ServerBuilder builder;
        int port = 0;
        builder.AddListeningPort(address, grpc::InsecureServerCredentials(), &port);
        builder.SetMaxReceiveMessageSize(kMaxMessageBytes);
        builder.SetMaxSendMessageSize(kMaxMessageBytes);
        builder.RegisterService(dn.node->service());
        dn.server = builder.BuildAndStart();
        if (!dn.server || port == 0)
            return fatal("serve " + id, "cannot listen on " + address);
        if (auto s = dn.node->heartbeat(); !s)
            return fatal("heartbeat " + id, s.error());
        dn.node->start_heartbeats();
        log_line("info", "data node " + id + " zone z" + std::to_string(i % kZones + 1) + " on " + address);
        nodes.push_back(std::move(dn));
    }

    // ----- Controller: one pass now so the first PUT finds open Chunks, then every second.
    rmk::controller::Options controller_options;
    controller_options.id = "rmk-dev";
    controller_options.interval = std::chrono::seconds{1};
    controller_options.chunk_capacity = flags.chunk_mib * kMiB;
    rmk::controller::Dependencies controller_deps{stores, keys, channels};
    auto controller = rmk::controller::Controller::create(controller_options, std::move(controller_deps));
    if (!controller)
        return fatal("create controller", controller.error());
    if (auto report = (*controller)->run_once(rmk::Clock::now()); !report)
        return fatal("first controller pass", report.error());
    (*controller)->start();

    // ----- Gateway.
    rmk::gateway::Options gateway_options;
    gateway_options.part_size = std::min<std::uint64_t>(4 * kMiB, flags.chunk_mib * kMiB / 4);
    gateway_options.cluster_config_cache = std::chrono::seconds{0};
    rmk::rpc::WriterOptions writer_options;
    writer_options.writer_id = "rmk-dev";
    writer_options.lease = std::chrono::seconds{30};
    rmk::gateway::Dependencies gateway_deps;
    gateway_deps.stores = stores;
    gateway_deps.keys = keys;
    gateway_deps.writer = std::make_shared<rmk::rpc::ChunkWriter>(cluster, channels, writer_options);
    gateway_deps.reader = std::make_shared<rmk::rpc::ChunkReader>(cluster, channels);
    auto gateway = rmk::gateway::Gateway::create(gateway_options, std::move(gateway_deps));
    if (!gateway)
        return fatal("create gateway", gateway.error());

    grpc::ServerBuilder builder;
    int port = 0;
    builder.AddListeningPort(flags.listen, grpc::InsecureServerCredentials(), &port);
    builder.RegisterService((*gateway)->service());
    builder.SetMaxReceiveMessageSize(kMaxMessageBytes);
    builder.SetMaxSendMessageSize(kMaxMessageBytes);
    std::unique_ptr<grpc::Server> server = builder.BuildAndStart();
    if (!server || port == 0)
        return fatal("serve gateway", "cannot listen on " + flags.listen);

    log_line("info", "gateway listening on " + flags.listen + " (port " + std::to_string(port) + "), " +
                         std::to_string(flags.nodes) + " data nodes, chunks of " + std::to_string(flags.chunk_mib) +
                         " MiB, parts of " + std::to_string(gateway_options.part_size / kMiB) + " MiB");
    std::cout << "\nrmk-dev is running on in-memory stores (insecure, nothing survives a restart).\n\n"
              << "  export RMK_ENDPOINT=" << flags.listen << " RMK_TENANT=demo\n"
              << "  rmkctl --insecure tenant create demo\n"
              << "  rmkctl --insecure bucket create photos\n"
              << "  rmkctl --insecure object put photos hello.txt ./hello.txt\n"
              << "  rmkctl --insecure object get photos hello.txt -\n\n"
              << "Ctrl-C stops everything.\n"
              << std::endl;

    int received = 0;
    sigwait(&signals, &received);
    log_line("info", std::string("received ") + (received == SIGTERM ? "SIGTERM" : "SIGINT") + ", shutting down");

    server->Shutdown(std::chrono::system_clock::now() + std::chrono::seconds{10});
    server->Wait();
    server.reset();
    gateway->reset();  // releases the writer leases with each Chunk's size
    controller->reset();
    for (DataNode& dn : nodes) {
        dn.server->Shutdown(std::chrono::system_clock::now() + std::chrono::seconds{5});
        dn.server->Wait();
        dn.server.reset();
        dn.node.reset();
    }
    log_line("info", "stopped");
    return 0;
}

}  // namespace

int main(int argc, char** argv) {
    Flags flags;
    std::string error;
    if (!parse_flags(argc, argv, flags, error)) {
        if (!error.empty())
            std::cerr << "error: " << error << '\n';
        std::cerr << kUsage;
        return error.empty() ? 0 : 2;
    }
    return run(flags);
}
