#include <pthread.h>

#include <chrono>
#include <csignal>
#include <cstdio>
#include <ctime>
#include <memory>
#include <string>

#include <grpcpp/grpcpp.h>

#include "rmk/core/config.hpp"
#include "rmk/datanode/datanode.hpp"
#include "rmk/core/env.hpp"
#include "rmk/http/http.hpp"
#include "rmk/rpc/rpc.hpp"

namespace {

constexpr int kMaxMessageBytes = 32 << 20;

void log_line(const char* level, const std::string& message) {
    std::time_t now = std::time(nullptr);
    std::tm tm{};
    ::gmtime_r(&now, &tm);
    char stamp[32];
    std::strftime(stamp, sizeof stamp, "%Y-%m-%dT%H:%M:%SZ", &tm);
    std::fprintf(stderr, "%s %s datanode %s\n", stamp, level, message.c_str());
}

int fatal(const std::string& what, const rmk::Error& error) {
    log_line("error", what + ": " + rmk::errc_name(error.code) + ": " + error.message);
    return 1;
}

int run() {
    // Signals are taken synchronously by sigwait below; every thread started later inherits the mask.
    sigset_t signals;
    sigemptyset(&signals);
    sigaddset(&signals, SIGTERM);
    sigaddset(&signals, SIGINT);
    pthread_sigmask(SIG_BLOCK, &signals, nullptr);

    const rmk::EnvLookup env = rmk::process_env();
    auto options = rmk::datanode::load_options(env);
    if (!options)
        return fatal("configuration", options.error());
    auto tls = rmk::rpc::load_tls(env);
    if (!tls)
        return fatal("configuration", tls.error());
    auto config = rmk::load_config(env);
    if (!config)
        return fatal("configuration", config.error());
    std::string listen = "0.0.0.0:7001";
    if (auto s = rmk::env_string(env, "RMK_LISTEN", false, listen); !s)
        return fatal("configuration", s.error());
    std::uint64_t metrics_port = 9100;
    if (auto s = rmk::env_u64(env, "RMK_METRICS_PORT", 0, 65535, metrics_port); !s)
        return fatal("configuration", s.error());
    if (!tls->insecure && tls->ca_file.empty())
        return fatal("configuration", rmk::Error{rmk::Errc::invalid_argument,
                                                 "RMK_TLS_CA is required to verify the callers of a Data Node"});
    auto credentials = rmk::rpc::server_credentials(*tls);
    if (!credentials)
        return fatal("configuration", rmk::Error{rmk::Errc::invalid_argument,
                                                 "serving needs RMK_TLS_CERT and RMK_TLS_KEY (or RMK_INSECURE=true)"});

    auto stores = rmk::rpc::open_stores(env, config->pending_upload_retention);
    if (!stores)
        return fatal("open stores", stores.error());
    auto channels = std::make_shared<rmk::rpc::Channels>(*tls);
    const std::string node_id = options->identity.id;
    auto node = rmk::datanode::Node::create(std::move(*options), stores->cluster, channels);
    if (!node)
        return fatal("start node", node.error());
    auto metrics = rmk::metrics::serve(static_cast<std::uint16_t>(metrics_port));
    if (!metrics)
        return fatal("serve metrics", metrics.error());

    grpc::ServerBuilder builder;
    int bound_port = 0;
    builder.AddListeningPort(listen, credentials, &bound_port);
    builder.SetMaxReceiveMessageSize(kMaxMessageBytes);
    builder.SetMaxSendMessageSize(kMaxMessageBytes);
    builder.RegisterService((*node)->service());
    std::unique_ptr<grpc::Server> server = builder.BuildAndStart();
    if (!server || bound_port == 0)
        return fatal("serve", rmk::Error{rmk::Errc::unavailable, "cannot listen on " + listen});
    log_line("info", "node " + node_id + " listening on " + listen + ", metrics on port " +
                         std::to_string((*metrics)->port()));

    if (auto s = (*node)->heartbeat(); !s)
        log_line("warn", "heartbeat failed: " + s.error().message);
    (*node)->start_heartbeats();

    int signal = 0;
    sigwait(&signals, &signal);
    log_line("info", std::string("received ") + (signal == SIGTERM ? "SIGTERM" : "SIGINT") + ", shutting down");
    server->Shutdown(std::chrono::system_clock::now() + std::chrono::seconds(10));
    server->Wait();
    node->reset();
    log_line("info", "stopped");
    return 0;
}

}  // namespace

int main() {
    return run();
}
