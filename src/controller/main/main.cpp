#include <pthread.h>

#include <csignal>
#include <cstdio>
#include <ctime>
#include <memory>
#include <string>

#include "rmk/config.hpp"
#include "rmk/controller.hpp"
#include "rmk/crypto.hpp"
#include "rmk/env.hpp"
#include "rmk/http.hpp"
#include "rmk/rpc.hpp"

namespace {

void log_line(const char* level, const std::string& message) {
    std::time_t now = std::time(nullptr);
    std::tm tm{};
    ::gmtime_r(&now, &tm);
    char stamp[32];
    std::strftime(stamp, sizeof stamp, "%Y-%m-%dT%H:%M:%SZ", &tm);
    std::fprintf(stderr, "%s %s controller %s\n", stamp, level, message.c_str());
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
    auto options = rmk::controller::load_options(env);
    if (!options)
        return fatal("configuration", options.error());
    auto tls = rmk::rpc::load_tls(env);
    if (!tls)
        return fatal("configuration", tls.error());
    auto config = rmk::load_config(env);
    if (!config)
        return fatal("configuration", config.error());
    std::string key_dir;
    if (auto s = rmk::env_required(env, "RMK_MASTER_KEY_DIR", key_dir); !s)
        return fatal("configuration", s.error());
    std::uint64_t metrics_port = 9100;
    if (auto s = rmk::env_u64(env, "RMK_METRICS_PORT", 0, 65535, metrics_port); !s)
        return fatal("configuration", s.error());

    auto keys = rmk::crypto::DirectoryKeyProvider::load(key_dir);
    if (!keys)
        return fatal("load master keys", keys.error());
    auto stores = rmk::rpc::open_stores(env, config->pending_upload_retention);
    if (!stores)
        return fatal("open stores", stores.error());
    auto metrics = rmk::metrics::serve(static_cast<std::uint16_t>(metrics_port));
    if (!metrics)
        return fatal("serve metrics", metrics.error());

    const std::string id = options->id;
    rmk::controller::Dependencies deps{
        std::move(*stores),
        std::make_shared<rmk::crypto::DirectoryKeyProvider>(std::move(*keys)),
        std::make_shared<rmk::rpc::Channels>(*tls),
    };
    auto controller = rmk::controller::Controller::create(std::move(*options), std::move(deps));
    if (!controller)
        return fatal("start controller", controller.error());
    log_line("info", "controller " + id + " started, metrics on port " + std::to_string((*metrics)->port()));
    (*controller)->start();

    int signal = 0;
    sigwait(&signals, &signal);
    log_line("info", std::string("received ") + (signal == SIGTERM ? "SIGTERM" : "SIGINT") + ", shutting down");
    controller->reset();  // finishes the current duty, then stops
    log_line("info", "stopped");
    return 0;
}

}  // namespace

int main() {
    return run();
}
