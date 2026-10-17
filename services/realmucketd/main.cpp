// realmucketd: the object store as one process on one machine (docs/IMPLEMENTATION.md §7).

#include <pthread.h>

#include <csignal>
#include <cstdio>
#include <ctime>
#include <memory>
#include <string>

#include "rmk/core/env.hpp"
#include "rmk/crypto/crypto.hpp"
#include "rmk/http/http.hpp"
#include "rmk/realmucketd/realmucketd.hpp"

namespace {

using namespace rmk;

void log_line(const char* level, const std::string& message) {
    std::time_t now = Clock::to_time_t(Clock::now());
    std::tm utc{};
    gmtime_r(&now, &utc);
    char stamp[32];
    std::strftime(stamp, sizeof stamp, "%Y-%m-%dT%H:%M:%SZ", &utc);
    std::fprintf(stderr, "%s %s realmucketd %s\n", stamp, level, message.c_str());
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

}  // namespace

int main() {
    // Block termination signals before any thread starts, so only sigwait() below sees them.
    sigset_t signals;
    sigemptyset(&signals);
    sigaddset(&signals, SIGTERM);
    sigaddset(&signals, SIGINT);
    pthread_sigmask(SIG_BLOCK, &signals, nullptr);

    realmucketd::Options options = must(realmucketd::load_options(process_env()), "configuration");
    auto keys = std::make_shared<crypto::DirectoryKeyProvider>(must(
        crypto::DirectoryKeyProvider::load(options.master_key_dir), "master keys in " + options.master_key_dir.string()));
    auto metrics = must(metrics::serve(static_cast<std::uint16_t>(options.metrics_port)), "metrics endpoint");
    auto daemon = must(realmucketd::Daemon::start(options, keys), "start");

    log_line("info", "listening on " + options.listen + " (port " + std::to_string(daemon->port()) + "), data in " +
                         options.data_dir.string() + (options.tls.insecure ? ", INSECURE" : ""));

    int received = 0;
    sigwait(&signals, &received);
    log_line("info", std::string("received ") + (received == SIGTERM ? "SIGTERM" : "SIGINT") + ", shutting down");
    daemon.reset();
    log_line("info", "stopped");
    return 0;
}
