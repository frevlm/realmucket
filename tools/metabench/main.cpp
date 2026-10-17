// metabench: how fast can candidate metadata engines commit the transactions of an object store?
//
// One PUT is three transactions, as the Gateway issues them through MetaStore: begin_upload,
// put_part and commit_upload; only the last one has to be on disk when it returns. The workload
// writes the same kinds of keys the FoundationDB layout (docs/metadata-layout.md) does, with realistic
// key and value sizes, into a database that already holds --objects objects.
#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <format>
#include <map>
#include <random>
#include <sstream>
#include <thread>

#include "batcher.hpp"

namespace mb = metabench;
using Clock = std::chrono::steady_clock;

namespace {

// ----- Flags

struct Flags {
    std::filesystem::path dir = std::filesystem::temp_directory_path();
    std::vector<std::string> engines{"memory", "sqlite", "lmdb", "rocksdb"};
    std::vector<std::string> syncs{"on", "off"};
    std::vector<std::string> scenarios{"put", "put-durable-all", "get", "list", "mixed"};
    std::vector<unsigned> concurrency{1, 16, 256};
    std::uint64_t objects = 1'000'000;
    unsigned duration = 5;
    std::size_t max_batch = 1024;
    bool decouple = true;
    bool random_keys = false;
};

std::vector<std::string> split(const std::string& s) {
    std::vector<std::string> out;
    std::stringstream ss(s);
    for (std::string item; std::getline(ss, item, ',');)
        if (!item.empty())
            out.push_back(item);
    return out;
}

[[noreturn]] void usage(const char* why) {
    std::fprintf(stderr,
                 "%s\n\nusage: metabench [flags]\n"
                 "  --dir PATH            scratch directory (default: temp dir)\n"
                 "  --engines a,b         memory,sqlite,lmdb,rocksdb\n"
                 "  --sync on,off         flush to disk, or never flush\n"
                 "  --scenarios a,b       put,put-durable-all,get,list,mixed\n"
                 "  --concurrency 1,16,256\n"
                 "  --objects N           objects already stored (default 1000000)\n"
                 "  --duration SECONDS    per cell (default 5)\n"
                 "  --max-batch N         transactions per commit; 1 = no group commit (default 1024)\n"
                 "  --decouple on|off     flush on its own thread where the engine allows it (default on)\n"
                 "  --random-keys         spread object names over the key space, so inserts land all over the\n"
                 "                        tree instead of at its end\n",
                 why);
    std::exit(2);
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
        if (a == "--dir") f.dir = value();
        else if (a == "--engines") f.engines = split(value());
        else if (a == "--sync") f.syncs = split(value());
        else if (a == "--scenarios") f.scenarios = split(value());
        else if (a == "--concurrency") {
            f.concurrency.clear();
            for (const auto& c : split(value()))
                f.concurrency.push_back(static_cast<unsigned>(std::stoul(c)));
        } else if (a == "--objects") f.objects = std::stoull(value());
        else if (a == "--duration") f.duration = static_cast<unsigned>(std::stoul(value()));
        else if (a == "--max-batch") f.max_batch = std::stoull(value());
        else if (a == "--decouple") f.decouple = value() == "on";
        else if (a == "--random-keys") f.random_keys = true;
        else usage(("unknown flag " + a).c_str());
    }
    return f;
}

// ----- Keys and values (docs/metadata-layout.md)

std::string be64(std::uint64_t v) {
    std::string s(8, '\0');
    for (int i = 7; i >= 0; --i, v >>= 8)
        s[static_cast<std::size_t>(i)] = static_cast<char>(v & 0xFF);
    return s;
}

std::string be32(std::uint32_t v) {
    return std::string{static_cast<char>(v >> 24), static_cast<char>(v >> 16), static_cast<char>(v >> 8), static_cast<char>(v)};
}

constexpr unsigned kTenants = 16, kBuckets = 4;
bool g_random_keys = false;

std::uint64_t mix64(std::uint64_t x) {  // splitmix64
    x += 0x9e3779b97f4a7c15ull;
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ull;
    x = (x ^ (x >> 27)) * 0x94d049bb133111ebull;
    return x ^ (x >> 31);
}

struct Obj {
    std::uint64_t index;
    std::string tenant, bucket, key;
};

Obj object_of(std::uint64_t i) {
    const std::uint64_t spread = (i * 2654435761u) >> 3;
    return {i, std::format("tenant-{:02}", spread % kTenants), std::format("bucket-{}", (spread / kTenants) % kBuckets),
            g_random_keys ? std::format("obj/{:016x}/data/file-{}.bin", mix64(i), i % 977)
                          : std::format("obj/{:010}/data/file-{}.bin", i, i % 977)};
}

std::string name_of(const Obj& o) { return o.tenant + "/" + o.bucket + "/" + o.key; }
std::string head_key(const Obj& o) { return "H/" + name_of(o); }
std::string version_key(const Obj& o, std::uint64_t gen) { return "V/" + name_of(o) + "/" + be64(gen); }
std::string part_key(std::uint64_t gen, std::uint32_t n) { return "Y" + be64(gen) + be32(n); }
std::string index_key(std::uint64_t chunk, std::uint64_t gen, std::uint32_t n) { return "X" + be64(chunk) + be64(gen) + be32(n); }
std::string usage_key(const Obj& o, char kind) { return "u/" + o.tenant + "/" + kind; }
std::string upload_key(std::uint64_t id, char field) { return "P" + be64(id) + std::string(1, field); }
std::string upload_part_key(std::uint64_t id, std::uint32_t n) { return "P" + be64(id) + "\x02" + be32(n); }
std::string deadline_key(std::uint64_t deadline, std::uint64_t id) { return "D" + be64(deadline) + be64(id); }
std::string pending_key(const Obj& o, std::uint64_t id) { return "p/" + o.tenant + "/" + o.bucket + "/" + be64(id); }
std::string chunk_pending_key(std::uint64_t chunk, std::uint64_t id, std::uint32_t n) { return "Q" + be64(chunk) + be64(id) + be32(n); }

const std::string kFiller(256, 'v');
std::string_view filler(std::size_t n) { return std::string_view(kFiller).substr(0, n); }

std::uint64_t read_u64(const std::string& s) {
    std::uint64_t v = 0;
    std::memcpy(&v, s.data(), std::min<std::size_t>(8, s.size()));
    return v;
}
std::string u64_bytes(std::uint64_t v) { return std::string(reinterpret_cast<const char*>(&v), 8); }

constexpr std::uint64_t kDeadline = 4'000'000'000ull;

struct Shared {
    std::uint64_t prefilled = 0;
    std::atomic<std::uint64_t> next_object{0};  // offset past `prefilled`
    std::atomic<std::uint64_t> next_upload{1};
    std::atomic<std::uint64_t> next_generation{1};
};

// ----- The three transactions of a PUT

mb::TxBody begin_upload(const Obj& o, std::uint64_t id) {
    return [&o, id](mb::Tx& tx) {
        tx.put(upload_key(id, '\0'), filler(96));
        tx.put(deadline_key(kDeadline, id), "");
        tx.put(pending_key(o, id), "");
        return true;
    };
}

mb::TxBody put_part(const Obj&, std::uint64_t id, std::uint64_t chunk) {
    return [id, chunk](mb::Tx& tx) {
        tx.put(upload_part_key(id, 1), filler(48));
        tx.put(chunk_pending_key(chunk, id, 1), "");
        tx.del(deadline_key(kDeadline, id));
        tx.put(deadline_key(kDeadline + 1, id), "");
        return true;
    };
}

mb::TxBody commit_upload(const Obj& o, std::uint64_t id, std::uint64_t chunk, Shared& shared) {
    return [&o, id, chunk, &shared](mb::Tx& tx) {
        (void)tx.get(head_key(o));  // the Precondition check
        if (!tx.get(upload_key(id, '\0')) || !tx.get(upload_part_key(id, 1)))
            return false;
        const std::uint64_t gen = shared.next_generation.fetch_add(1);
        tx.put(version_key(o, gen), filler(104));
        tx.put(part_key(gen, 1), filler(48));
        tx.put(index_key(chunk, gen, 1), filler(32));
        tx.put(head_key(o), be64(gen) + std::string(1, '\0'));
        for (char kind : {'b', 'o'}) {
            auto cur = tx.get(usage_key(o, kind));
            tx.put(usage_key(o, kind), u64_bytes((cur ? read_u64(*cur) : 0) + 1));
        }
        tx.del(upload_key(id, '\0'));
        tx.del(upload_part_key(id, 1));
        tx.del(deadline_key(kDeadline + 1, id));
        tx.del(pending_key(o, id));
        tx.del(chunk_pending_key(chunk, id, 1));
        return true;
    };
}

// ----- Prefill

void prefill(mb::Engine& engine, Shared& shared, std::uint64_t n, bool sync) {
    constexpr std::uint64_t kPerBatch = 2000;
    for (std::uint64_t base = 0; base < n; base += kPerBatch) {
        std::vector<mb::TxBody> bodies;
        const std::uint64_t end = std::min(n, base + kPerBatch);
        for (std::uint64_t i = base; i < end; ++i)
            bodies.push_back([i](mb::Tx& tx) {
                const Obj o = object_of(i);
                const std::uint64_t gen = i + 1;
                tx.put(head_key(o), be64(gen) + std::string(1, '\0'));
                tx.put(version_key(o, gen), filler(104));
                tx.put(part_key(gen, 1), filler(48));
                tx.put(index_key(1 + i / 4096, gen, 1), filler(32));
                return true;
            });
        std::vector<const mb::TxBody*> ptrs;
        for (auto& b : bodies)
            ptrs.push_back(&b);
        std::vector<char> ok(ptrs.size());
        engine.apply_batch(ptrs, ok, false);
        if (n >= 4'000'000 && (base / kPerBatch) % 500 == 499)
            std::fprintf(stderr, "   ... %llu of %llu objects loaded\n", static_cast<unsigned long long>(end),
                         static_cast<unsigned long long>(n));
    }
    // One flush so the database is at rest before measuring.
    mb::TxBody marker = [](mb::Tx& tx) { tx.put("M", "prefilled"); return true; };
    std::vector<const mb::TxBody*> ptrs{&marker};
    std::vector<char> ok(1);
    engine.apply_batch(ptrs, ok, sync);
    shared.prefilled = n;
    shared.next_generation = n + 1;
}

// ----- Measuring

struct Latencies {
    std::vector<std::uint32_t> micros;
    void add(Clock::duration d) { micros.push_back(static_cast<std::uint32_t>(std::chrono::duration_cast<std::chrono::microseconds>(d).count())); }
};

struct Stats {
    std::uint64_t ops = 0;
    double p50 = 0, p99 = 0, p999 = 0, max = 0;
};

Stats summarize(std::vector<std::uint32_t> v, double seconds) {
    Stats s;
    s.ops = v.size();
    if (v.empty())
        return s;
    std::sort(v.begin(), v.end());
    auto at = [&](double p) { return v[std::min(v.size() - 1, static_cast<std::size_t>(p * static_cast<double>(v.size())))] / 1000.0; };
    s.p50 = at(0.50);
    s.p99 = at(0.99);
    s.p999 = at(0.999);
    s.max = v.back() / 1000.0;
    (void)seconds;
    return s;
}

enum Op { kPut, kGet, kList, kOps };
const char* kOpNames[] = {"PUT", "GET", "LIST"};

void do_put(mb::Batcher& batcher, Shared& shared, bool durable_all) {
    const std::uint64_t index = shared.prefilled + shared.next_object.fetch_add(1);
    const std::uint64_t id = shared.next_upload.fetch_add(1);
    const std::uint64_t chunk = 1 + index / 4096;
    const Obj o = object_of(index);
    mb::TxBody a = begin_upload(o, id), b = put_part(o, id, chunk), c = commit_upload(o, id, chunk, shared);
    batcher.submit(a, durable_all);
    batcher.submit(b, durable_all);
    batcher.submit(c, true);
}

void do_get(mb::Engine& engine, Shared& shared, std::mt19937_64& rng) {
    const Obj o = object_of(rng() % shared.prefilled);
    auto head = engine.read_get(head_key(o));
    if (head)
        (void)engine.read_get(version_key(o, [&] { std::uint64_t g = 0; for (int i = 0; i < 8; ++i) g = (g << 8) | static_cast<unsigned char>((*head)[static_cast<std::size_t>(i)]); return g; }()));
}

void do_list(mb::Engine& engine, Shared& shared, std::mt19937_64& rng) {
    const Obj o = object_of(rng() % shared.prefilled);
    const std::string prefix = "H/" + o.tenant + "/" + o.bucket + "/obj/";
    std::string end = prefix;
    end.back() = '0';  // '/' + 1
    (void)engine.read_scan("H/" + name_of(o), end, 100);
}

struct CellResult {
    Stats ops[kOps];
    double seconds = 0;
    double avg_batch = 0, flushes_per_s = 0;
    bool decoupled = false;
};

CellResult run_cell(mb::Engine& engine, mb::Batcher& batcher, Shared& shared, const std::string& scenario, unsigned conc,
                    unsigned duration) {
    std::vector<std::array<Latencies, kOps>> per(conc);
    std::atomic<unsigned> ready{0};
    std::atomic<bool> go{false}, stop{false};
    const std::uint64_t batches0 = batcher.batches(), flushed0 = batcher.flushes(), txs0 = batcher.transactions();
    std::vector<std::thread> threads;
    for (unsigned w = 0; w < conc; ++w)
        threads.emplace_back([&, w] {
            std::mt19937_64 rng(1234 + w);
            ++ready;
            while (!go)
                std::this_thread::yield();
            while (!stop) {
                Op op;
                if (scenario == "put" || scenario == "put-durable-all") op = kPut;
                else if (scenario == "get") op = kGet;
                else if (scenario == "list") op = kList;
                else op = rng() % 5 == 0 ? kPut : kGet;  // mixed: 80% GET, 20% PUT
                const auto begin = Clock::now();
                if (op == kPut) do_put(batcher, shared, scenario == "put-durable-all");
                else if (op == kGet) do_get(engine, shared, rng);
                else do_list(engine, shared, rng);
                per[w][op].add(Clock::now() - begin);
            }
        });
    while (ready < conc)
        std::this_thread::yield();
    const auto begin = Clock::now();
    go = true;
    std::this_thread::sleep_for(std::chrono::seconds(duration));
    stop = true;
    for (auto& t : threads)
        t.join();
    CellResult r;
    r.seconds = std::chrono::duration<double>(Clock::now() - begin).count();
    for (int op = 0; op < kOps; ++op) {
        std::vector<std::uint32_t> all;
        for (auto& p : per)
            all.insert(all.end(), p[op].micros.begin(), p[op].micros.end());
        r.ops[op] = summarize(std::move(all), r.seconds);
    }
    const auto batches = batcher.batches() - batches0;
    r.avg_batch = batches ? static_cast<double>(batcher.transactions() - txs0) / static_cast<double>(batches) : 0;
    r.flushes_per_s = static_cast<double>(batcher.flushes() - flushed0) / r.seconds;
    r.decoupled = batcher.decoupled();
    return r;
}

// ----- Does a "durable" commit really reach the disk?

double raw_flush_ms(const std::filesystem::path& dir, bool full) {
    const auto path = dir / "flush.probe";
    int fd = ::open(path.c_str(), O_RDWR | O_CREAT, 0644);
    std::string block(4096, 'x');
    constexpr int kRounds = 100;
    const auto begin = Clock::now();
    for (int i = 0; i < kRounds; ++i) {
        (void)!::pwrite(fd, block.data(), block.size(), i * 4096);
#ifdef __APPLE__
        if (full) ::fcntl(fd, F_FULLFSYNC); else ::fsync(fd);
#else
        (void)full;
        ::fdatasync(fd);
#endif
    }
    const double ms = std::chrono::duration<double, std::milli>(Clock::now() - begin).count() / kRounds;
    ::close(fd);
    std::filesystem::remove(path);
    return ms;
}

double single_commit_ms(mb::Engine& engine) {
    mb::Batcher batcher(engine, 1, false);
    constexpr int kRounds = 100;
    mb::TxBody body = [](mb::Tx& tx) { tx.put("probe", "x"); return true; };
    const auto begin = Clock::now();
    for (int i = 0; i < kRounds; ++i)
        batcher.submit(body, true);
    return std::chrono::duration<double, std::milli>(Clock::now() - begin).count() / kRounds;
}

std::unique_ptr<mb::Engine> make(const std::string& name, const mb::Options& o) {
    if (name == "memory") return mb::make_memory(o);
    if (name == "sqlite") return mb::make_sqlite(o);
#ifdef MB_HAVE_LMDB
    if (name == "lmdb") return mb::make_lmdb(o);
#endif
#ifdef MB_HAVE_ROCKSDB
    if (name == "rocksdb") return mb::make_rocksdb(o);
#endif
    return nullptr;
}

}  // namespace

int main(int argc, char** argv) {
    const Flags f = parse(argc, argv);
    g_random_keys = f.random_keys;
    const auto root = f.dir / ("metabench." + std::to_string(::getpid()));
    std::filesystem::create_directories(root);

    std::printf("objects=%llu  duration=%us  max-batch=%zu  keys=%s\n", static_cast<unsigned long long>(f.objects), f.duration,
                f.max_batch, f.random_keys ? "random" : "sequential");
    std::printf("raw flush of 4 KiB:  fsync %.3f ms,  full flush %.3f ms\n\n", raw_flush_ms(root, false), raw_flush_ms(root, true));
    const double full_flush = raw_flush_ms(root, true);

    for (const auto& name : f.engines)
        for (const auto& sync : f.syncs) {
            const bool on = sync == "on";
            const auto dir = root / (name + "-" + sync);
            std::unique_ptr<mb::Engine> engine;
            try {
                engine = make(name, {dir, on});
            } catch (const std::exception& e) {
                std::printf("== %s: cannot open: %s\n", name.c_str(), e.what());
                continue;
            }
            if (!engine) {
                std::printf("== %s: not built into this binary\n", name.c_str());
                continue;
            }
            try {
                Shared shared;
                const auto fill_begin = Clock::now();
                prefill(*engine, shared, f.objects, on);
                std::printf("== %s  sync=%s  prefilled %llu objects in %.1fs, %.0f MiB on disk\n", name.c_str(), sync.c_str(),
                            static_cast<unsigned long long>(f.objects),
                            std::chrono::duration<double>(Clock::now() - fill_begin).count(),
                            static_cast<double>(engine->disk_bytes()) / 1048576.0);
                if (on && name != "memory") {
                    const double ms = single_commit_ms(*engine);
                    std::printf("   one durable commit, one writer: %.2f ms  (%s)\n", ms,
                                ms >= 0.5 * full_flush ? "reaches the disk" : "WEAKER than a full flush: result not comparable");
                }
                std::printf("   (flush %s)\n", f.decouple && engine->can_decouple() ? "on its own thread" : "inside the commit");
                std::printf("   %-16s %-4s %5s %10s %9s %9s %9s %9s %8s %9s\n", "scenario", "op", "conc", "ops/s", "p50ms", "p99ms",
                            "p99.9ms", "maxms", "txs/bat", "flush/s");
                for (const auto& scenario : f.scenarios) {
                    if (scenario == "put-durable-all" && !on)
                        continue;
                    for (unsigned conc : f.concurrency) {
                        mb::Batcher batcher(*engine, f.max_batch, f.decouple);
                        CellResult r = run_cell(*engine, batcher, shared, scenario, conc, f.duration);
                        for (int op = 0; op < kOps; ++op) {
                            if (r.ops[op].ops == 0)
                                continue;
                            std::printf("   %-16s %-4s %5u %10.0f %9.2f %9.2f %9.2f %9.2f %8.1f %9.0f\n", scenario.c_str(),
                                        kOpNames[op], conc, static_cast<double>(r.ops[op].ops) / r.seconds, r.ops[op].p50,
                                        r.ops[op].p99, r.ops[op].p999, r.ops[op].max, op == kPut ? r.avg_batch : 0.0,
                                        op == kPut ? r.flushes_per_s : 0.0);
                        }
                        std::fflush(stdout);
                    }
                }
                std::printf("   disk after the run: %.0f MiB\n\n", static_cast<double>(engine->disk_bytes()) / 1048576.0);
            } catch (const std::exception& e) {
                std::printf("   FAILED: %s\n\n", e.what());
            }
            engine.reset();
            std::error_code ec;
            std::filesystem::remove_all(dir, ec);
        }
    std::error_code ec;
    std::filesystem::remove_all(root, ec);
    return 0;
}
