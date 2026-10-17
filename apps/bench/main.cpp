// realmucket-bench: throughput and latency of a running realmucketd (or any Gateway), through the
// Client SDK. Two scenarios:
//   small  many small objects, PUT then GET, for a fixed time
//   large  one big object per worker, uploaded then downloaded, for the sustained transfer rate
// each at every requested concurrency.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <expected>
#include <filesystem>
#include <functional>
#include <memory>
#include <optional>
#include <iostream>
#include <mutex>
#include <random>
#include <sstream>
#include <streambuf>
#include <string>
#include <thread>
#include <vector>

#include "rmk/client.hpp"

namespace {

namespace rc = rmk::client;
using Steady = std::chrono::steady_clock;

constexpr double kMiB = 1024.0 * 1024.0;

struct Flags {
    std::string endpoint = "127.0.0.1:7000";
    std::filesystem::path ca_file;
    bool insecure = false;
    std::string tenant = "bench";
    std::vector<unsigned> concurrency{1, 16, 256};
    std::uint64_t small_size = 16 * 1024;
    unsigned duration = 10;  // seconds per small phase
    std::uint64_t large_mib = 32;
    std::uint64_t part_mib = 8;
    unsigned parallel_parts = 4;
    bool small = true;
    bool large = true;
    bool cleanup = true;
};

[[noreturn]] void usage(const char* why) {
    std::fprintf(stderr,
                 "%s\n\nusage: realmucket-bench [flags]\n"
                 "  --endpoint HOST:PORT     default 127.0.0.1:7000\n"
                 "  --insecure               plaintext\n"
                 "  --ca-file PATH           trust root for TLS\n"
                 "  --tenant NAME            default bench (created if missing)\n"
                 "  --concurrency 1,16,256   workers per phase\n"
                 "  --scenarios small,large  default both\n"
                 "  --small-size BYTES       default 16384\n"
                 "  --duration SECONDS       per small phase, default 10\n"
                 "  --large-mib N            object size per worker, default 32\n"
                 "  --part-mib N             upload Part size, default 8\n"
                 "  --parallel-parts N       Parts in flight per upload, default 4\n"
                 "  --keep                   leave the objects behind\n",
                 why);
    std::exit(2);
}

std::vector<std::string> split(const std::string& s) {
    std::vector<std::string> out;
    std::stringstream ss(s);
    for (std::string item; std::getline(ss, item, ',');)
        if (!item.empty())
            out.push_back(item);
    return out;
}

Flags parse(int argc, char** argv) {
    Flags f;
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        auto value = [&]() -> std::string {
            if (i + 1 >= argc)
                usage(("missing value for " + a).c_str());
            return argv[++i];
        };
        if (a == "--endpoint") f.endpoint = value();
        else if (a == "--insecure") f.insecure = true;
        else if (a == "--ca-file") f.ca_file = value();
        else if (a == "--tenant") f.tenant = value();
        else if (a == "--concurrency") {
            f.concurrency.clear();
            for (const auto& c : split(value()))
                f.concurrency.push_back(static_cast<unsigned>(std::stoul(c)));
        } else if (a == "--scenarios") {
            f.small = f.large = false;
            for (const auto& s : split(value())) {
                if (s == "small") f.small = true;
                else if (s == "large") f.large = true;
                else usage("unknown scenario");
            }
        } else if (a == "--small-size") f.small_size = std::stoull(value());
        else if (a == "--duration") f.duration = static_cast<unsigned>(std::stoul(value()));
        else if (a == "--large-mib") f.large_mib = std::stoull(value());
        else if (a == "--part-mib") f.part_mib = std::stoull(value());
        else if (a == "--parallel-parts") f.parallel_parts = static_cast<unsigned>(std::stoul(value()));
        else if (a == "--keep") f.cleanup = false;
        else usage(("unknown flag " + a).c_str());
    }
    if (f.concurrency.empty() || f.small_size == 0 || f.duration == 0 || f.large_mib == 0 || f.part_mib == 0)
        usage("bad flag values");
    if (!f.insecure && f.ca_file.empty())
        std::fprintf(stderr, "note: neither --insecure nor --ca-file given; using the system trust store\n");
    return f;
}

// ----- Content without holding it in memory.

class PatternBuf final : public std::streambuf {
public:
    explicit PatternBuf(std::uint64_t total) : remaining_(total) {
        std::mt19937_64 rng(42);
        for (std::size_t i = 0; i < buf_.size(); i += 8) {
            const std::uint64_t v = rng();
            std::memcpy(buf_.data() + i, &v, 8);
        }
    }

protected:
    int_type underflow() override {
        if (remaining_ == 0)
            return traits_type::eof();
        const std::size_t n = static_cast<std::size_t>(std::min<std::uint64_t>(buf_.size(), remaining_));
        remaining_ -= n;
        setg(buf_.data(), buf_.data(), buf_.data() + n);
        return traits_type::to_int_type(buf_[0]);
    }

private:
    std::vector<char> buf_ = std::vector<char>(1 << 20);
    std::uint64_t remaining_;
};

class CountBuf final : public std::streambuf {
public:
    std::uint64_t bytes() const { return bytes_; }

protected:
    std::streamsize xsputn(const char*, std::streamsize n) override {
        bytes_ += static_cast<std::uint64_t>(n);
        return n;
    }
    int_type overflow(int_type c) override {
        ++bytes_;
        return c;
    }

private:
    std::uint64_t bytes_ = 0;
};

// ----- Measuring.

struct PhaseResult {
    std::uint64_t ops = 0;
    std::uint64_t errors = 0;
    std::uint64_t bytes = 0;
    double seconds = 0;
    std::vector<double> latencies_ms;
    std::string first_error;
};

double percentile(const std::vector<double>& sorted, double p) {
    if (sorted.empty())
        return 0;
    const std::size_t at = std::min(sorted.size() - 1, static_cast<std::size_t>(p * static_cast<double>(sorted.size())));
    return sorted[at];
}

void report(const char* scenario, const char* op, unsigned conc, PhaseResult& r) {
    std::sort(r.latencies_ms.begin(), r.latencies_ms.end());
    const double mib_s = static_cast<double>(r.bytes) / kMiB / std::max(r.seconds, 1e-9);
    std::printf("%-6s %-4s %5u %9llu %6llu %8.2fs %10.0f %10.1f %9.2f %9.2f %9.2f %9.2f %9.2f\n", scenario, op, conc,
                static_cast<unsigned long long>(r.ops), static_cast<unsigned long long>(r.errors), r.seconds,
                static_cast<double>(r.ops) / std::max(r.seconds, 1e-9), mib_s, percentile(r.latencies_ms, 0.50),
                percentile(r.latencies_ms, 0.90), percentile(r.latencies_ms, 0.99), percentile(r.latencies_ms, 0.999),
                r.latencies_ms.empty() ? 0.0 : r.latencies_ms.back());
    if (!r.first_error.empty())
        std::printf("       first error: %s\n", r.first_error.c_str());
    std::fflush(stdout);
}

// Runs `body(worker, op_index)` on `conc` threads. With `deadline` set it loops until then, otherwise
// it runs once per worker. body returns the bytes moved, or an error message.
using Body = std::function<std::expected<std::uint64_t, std::string>(unsigned worker, std::uint64_t op)>;

PhaseResult run_phase(unsigned conc, std::optional<std::chrono::seconds> duration, const Body& body) {
    std::vector<PhaseResult> per(conc);
    std::atomic<unsigned> ready{0};
    std::atomic<bool> go{false};
    std::atomic<bool> stop{false};
    std::vector<std::thread> threads;
    for (unsigned w = 0; w < conc; ++w)
        threads.emplace_back([&, w] {
            ++ready;
            while (!go)
                std::this_thread::yield();
            PhaseResult& mine = per[w];
            for (std::uint64_t op = 0; !stop; ++op) {
                const auto begin = Steady::now();
                auto result = body(w, op);
                const double ms = std::chrono::duration<double, std::milli>(Steady::now() - begin).count();
                if (result) {
                    ++mine.ops;
                    mine.bytes += *result;
                    mine.latencies_ms.push_back(ms);
                } else {
                    ++mine.errors;
                    if (mine.first_error.empty())
                        mine.first_error = result.error();
                }
                if (!duration)
                    break;
            }
        });
    while (ready < conc)
        std::this_thread::yield();
    const auto begin = Steady::now();
    go = true;
    if (duration) {
        std::this_thread::sleep_for(*duration);
        stop = true;
    }
    for (auto& t : threads)
        t.join();
    PhaseResult total;
    total.seconds = std::chrono::duration<double>(Steady::now() - begin).count();
    for (auto& p : per) {
        total.ops += p.ops;
        total.errors += p.errors;
        total.bytes += p.bytes;
        total.latencies_ms.insert(total.latencies_ms.end(), p.latencies_ms.begin(), p.latencies_ms.end());
        if (total.first_error.empty())
            total.first_error = p.first_error;
    }
    return total;
}

std::vector<std::unique_ptr<rc::Client>> connect_all(const Flags& f, unsigned n) {
    std::vector<std::unique_ptr<rc::Client>> clients;
    for (unsigned i = 0; i < n; ++i) {
        rc::Options o;
        o.endpoint = f.endpoint;
        o.insecure = f.insecure;
        o.ca_file = f.ca_file;
        o.multipart_threshold = f.part_mib << 20;
        o.part_size = f.part_mib << 20;
        o.parallel_parts = f.parallel_parts;
        o.timeout = std::chrono::minutes{10};
        auto c = rc::Client::connect(o);
        if (!c) {
            std::fprintf(stderr, "connect: %s\n", c.error().message.c_str());
            std::exit(1);
        }
        clients.push_back(std::make_unique<rc::Client>(std::move(*c)));
    }
    return clients;
}

std::string message(const rc::Error& e) {
    return e.message;
}

void cleanup(const Flags& f, std::vector<std::unique_ptr<rc::Client>>& clients, const std::string& bucket,
             const std::vector<std::vector<std::string>>& keys) {
    if (!f.cleanup)
        return;
    std::vector<std::thread> threads;
    for (std::size_t w = 0; w < keys.size(); ++w)
        threads.emplace_back([&, w] {
            for (const auto& key : keys[w])
                (void)clients[w]->remove({f.tenant, bucket, key});
        });
    for (auto& t : threads)
        t.join();
}

void small_scenario(const Flags& f, rc::Client& admin, unsigned conc, const std::string& run) {
    const std::string bucket = "small-" + run + "-" + std::to_string(conc);
    if (auto b = admin.create_bucket(f.tenant, bucket); !b) {
        std::fprintf(stderr, "create bucket: %s\n", b.error().message.c_str());
        std::exit(1);
    }
    auto clients = connect_all(f, conc);
    const std::string payload(f.small_size, 'p');
    std::vector<std::vector<std::string>> keys(conc);
    const std::chrono::seconds duration{f.duration};

    PhaseResult put = run_phase(conc, duration, [&](unsigned w, std::uint64_t op) -> std::expected<std::uint64_t, std::string> {
        std::string key = "k" + std::to_string(w) + "-" + std::to_string(op);
        auto r = clients[w]->put({f.tenant, bucket, key}, payload);
        if (!r)
            return std::unexpected(message(r.error()));
        keys[w].push_back(std::move(key));
        return payload.size();
    });
    report("small", "PUT", conc, put);

    std::vector<std::string> all;
    for (const auto& per : keys)
        all.insert(all.end(), per.begin(), per.end());
    if (!all.empty()) {
        std::vector<std::mt19937_64> rng;
        for (unsigned w = 0; w < conc; ++w)
            rng.emplace_back(1000 + w);
        PhaseResult get = run_phase(conc, duration, [&](unsigned w, std::uint64_t) -> std::expected<std::uint64_t, std::string> {
            const std::string& key = all[rng[w]() % all.size()];
            auto r = clients[w]->get({f.tenant, bucket, key});
            if (!r)
                return std::unexpected(message(r.error()));
            if (r->size() != payload.size())
                return std::unexpected("short read");
            return r->size();
        });
        report("small", "GET", conc, get);
    }
    cleanup(f, clients, bucket, keys);
}

void large_scenario(const Flags& f, rc::Client& admin, unsigned conc, const std::string& run) {
    const std::string bucket = "large-" + run + "-" + std::to_string(conc);
    if (auto b = admin.create_bucket(f.tenant, bucket); !b) {
        std::fprintf(stderr, "create bucket: %s\n", b.error().message.c_str());
        std::exit(1);
    }
    auto clients = connect_all(f, conc);
    const std::uint64_t size = f.large_mib << 20;
    std::vector<std::vector<std::string>> keys(conc);
    for (unsigned w = 0; w < conc; ++w)
        keys[w].push_back("big-" + std::to_string(w));

    PhaseResult put = run_phase(conc, std::nullopt, [&](unsigned w, std::uint64_t) -> std::expected<std::uint64_t, std::string> {
        PatternBuf buf(size);
        std::istream in(&buf);
        auto r = clients[w]->put({f.tenant, bucket, keys[w][0]}, in);
        if (!r)
            return std::unexpected(message(r.error()));
        return size;
    });
    report("large", "PUT", conc, put);

    PhaseResult get = run_phase(conc, std::nullopt, [&](unsigned w, std::uint64_t) -> std::expected<std::uint64_t, std::string> {
        CountBuf sink;
        std::ostream out(&sink);
        auto r = clients[w]->get({f.tenant, bucket, keys[w][0]}, out);
        if (!r)
            return std::unexpected(message(r.error()));
        if (sink.bytes() != size)
            return std::unexpected("short read");
        return size;
    });
    report("large", "GET", conc, get);
    cleanup(f, clients, bucket, keys);
}

}  // namespace

int main(int argc, char** argv) {
    const Flags f = parse(argc, argv);
    auto admin_clients = connect_all(f, 1);
    rc::Client& admin = *admin_clients[0];
    if (auto t = admin.create_tenant(f.tenant); !t && t.error().code != rc::ErrorCode::already_exists) {
        std::fprintf(stderr, "create tenant: %s\n", t.error().message.c_str());
        return 1;
    }
    const std::string run = std::to_string(std::chrono::duration_cast<std::chrono::seconds>(
                                               std::chrono::system_clock::now().time_since_epoch())
                                               .count());
    std::printf("endpoint %s  small=%lluB x %us  large=%lluMiB (part %lluMiB x%u)\n\n", f.endpoint.c_str(),
                static_cast<unsigned long long>(f.small_size), f.duration, static_cast<unsigned long long>(f.large_mib),
                static_cast<unsigned long long>(f.part_mib), f.parallel_parts);
    std::printf("%-6s %-4s %5s %9s %6s %9s %10s %10s %9s %9s %9s %9s %9s\n", "scen", "op", "conc", "ops", "errs", "time",
                "ops/s", "MiB/s", "p50ms", "p90ms", "p99ms", "p99.9ms", "maxms");
    for (unsigned conc : f.concurrency) {
        if (f.small)
            small_scenario(f, admin, conc, run);
        if (f.large)
            large_scenario(f, admin, conc, run);
    }
    return 0;
}
