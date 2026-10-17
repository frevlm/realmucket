// rmk-gateway: the rmk.v1.Storage gRPC service (docs/IMPLEMENTATION.md §6, §7).

#include <unistd.h>

#include <csignal>
#include <cstdio>
#include <ctime>
#include <memory>
#include <string>

#include <grpcpp/grpcpp.h>

#include "rmk/crypto.hpp"
#include "rmk/env.hpp"
#include "rmk/gateway.hpp"
#include "rmk/http.hpp"
#include "rmk/rpc.hpp"

namespace {

using namespace rmk;

constexpr int kMaxMessageBytes = 32 << 20;
constexpr auto kShutdownGrace = std::chrono::seconds{30};

void log_line(const char* level, const std::string& message) {
    std::time_t now = Clock::to_time_t(Clock::now());
    std::tm utc{};
    gmtime_r(&now, &utc);
    char stamp[32];
    std::strftime(stamp, sizeof stamp, "%Y-%m-%dT%H:%M:%SZ", &utc);
    std::fprintf(stderr, "%s %s gateway %s\n", stamp, level, message.c_str());
}

[[noreturn]] void die(const std::string& what, const Error& error) {
    log_line("fatal", what + ": " + errc_name(error.code) + ": " + error.message);
    std::exit(1);
}

template <class T>
T must(Result<T> result, const std::string& what) {
    if (!result) die(what, result.error());
    return std::move(*result);
}

void must(const Status& status, const std::string& what) {
    if (!status) die(what, status.error());
}

std::string hostname() {
    char name[256] = {};
    if (gethostname(name, sizeof name - 1) != 0) return "gateway";
    return name;
}

}  // namespace

int main() {
    // Block termination signals before any thread starts, so only sigwait() below sees them.
    sigset_t signals;
    sigemptyset(&signals);
    sigaddset(&signals, SIGTERM);
    sigaddset(&signals, SIGINT);
    pthread_sigmask(SIG_BLOCK, &signals, nullptr);

    const EnvLookup env = process_env();
    gateway::Options options = must(gateway::load_options(env), "configuration");
    rpc::TlsConfig tls = must(rpc::load_tls(env), "TLS configuration");

    std::string key_dir;
    must(env_required(env, "RMK_MASTER_KEY_DIR", key_dir), "configuration");
    std::string listen = "0.0.0.0:7000";
    must(env_string(env, "RMK_LISTEN", false, listen), "configuration");
    std::uint64_t metrics_port = 9100;
    must(env_u64(env, "RMK_METRICS_PORT", 0, 65535, metrics_port), "configuration");

    rpc::WriterOptions writer_options;
    writer_options.writer_id = hostname();
    must(env_string(env, "RMK_GATEWAY_ID", false, writer_options.writer_id), "configuration");
    std::uint64_t writers = writer_options.writers_per_policy;
    must(env_u64(env, "RMK_WRITERS_PER_POLICY", 1, 64, writers), "configuration");
    writer_options.writers_per_policy = writers;
    must(env_seconds(env, "RMK_WRITER_LEASE_SECONDS", std::chrono::seconds{5}, writer_options.lease),
         "configuration");

    auto keys = std::make_shared<crypto::DirectoryKeyProvider>(
        must(crypto::DirectoryKeyProvider::load(key_dir), "master keys in " + key_dir));
    rpc::Stores stores = must(rpc::open_stores(env, options.pending_upload_retention), "metadata stores");

    auto channels = std::make_shared<rpc::Channels>(tls);
    gateway::Dependencies deps;
    deps.stores = stores;
    deps.keys = keys;
    deps.writer = std::make_shared<rpc::ChunkWriter>(stores.cluster, channels, writer_options);
    deps.reader = std::make_shared<rpc::ChunkReader>(stores.cluster, channels);
    std::unique_ptr<gateway::Gateway> service = must(gateway::Gateway::create(options, std::move(deps)), "gateway");

    auto metrics = must(metrics::serve(static_cast<std::uint16_t>(metrics_port)), "metrics endpoint");

    auto credentials = rpc::server_credentials(tls);
    if (!credentials) {
        log_line("fatal", "serving needs RMK_TLS_CERT and RMK_TLS_KEY (or RMK_INSECURE=true)");
        return 1;
    }
    grpc::ServerBuilder builder;
    int port = 0;
    builder.AddListeningPort(listen, credentials, &port);
    builder.RegisterService(service->service());
    builder.SetMaxReceiveMessageSize(kMaxMessageBytes);
    builder.SetMaxSendMessageSize(kMaxMessageBytes);
    std::unique_ptr<grpc::Server> server = builder.BuildAndStart();
    if (!server || port == 0) {
        log_line("fatal", "cannot listen on " + listen);
        return 1;
    }
    log_line("info", "listening on " + listen + " (port " + std::to_string(port) + "), writer id " +
                         writer_options.writer_id + (tls.insecure ? ", INSECURE" : ""));

    int received = 0;
    sigwait(&signals, &received);
    log_line("info", std::string("received ") + (received == SIGTERM ? "SIGTERM" : "SIGINT") + ", shutting down");

    server->Shutdown(std::chrono::system_clock::now() + kShutdownGrace);
    server.reset();
    service.reset();  // releases the writer leases with each Chunk's size
    log_line("info", "stopped");
    return 0;
}
