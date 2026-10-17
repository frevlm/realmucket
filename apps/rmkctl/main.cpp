#include <cstdlib>
#include <fstream>
#include <iostream>
#include <map>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include "rmk/client.hpp"

namespace c = rmk::client;

namespace {

struct Usage {
    std::string message;
};

struct Args {
    std::vector<std::string> positional;
    std::map<std::string, std::vector<std::string>> flags;

    bool has(const std::string& name) const { return flags.contains(name); }

    std::string get(const std::string& name, const std::string& fallback = {}) const {
        auto it = flags.find(name);
        return it == flags.end() ? fallback : it->second.back();
    }

    const std::vector<std::string>& all(const std::string& name) const {
        static const std::vector<std::string> none;
        auto it = flags.find(name);
        return it == flags.end() ? none : it->second;
    }

    std::uint64_t number(const std::string& name, std::uint64_t fallback = 0) const {
        if (!has(name)) {
            return fallback;
        }
        try {
            return std::stoull(get(name));
        } catch (...) {
            throw Usage{"--" + name + " needs a number"};
        }
    }

    std::string at(std::size_t i, const char* what) const {
        if (i >= positional.size()) {
            throw Usage{std::string("missing ") + what};
        }
        return positional[i];
    }
};

const std::set<std::string> kBoolFlags = {"insecure", "if-not-exists", "versioning"};

Args parse(int argc, char** argv) {
    Args args;
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a.size() > 2 && a.starts_with("--")) {
            std::string name = a.substr(2);
            std::string value;
            if (auto eq = name.find('='); eq != std::string::npos) {
                value = name.substr(eq + 1);
                name.resize(eq);
            } else if (kBoolFlags.contains(name)) {
                value = "true";
            } else if (i + 1 < argc) {
                value = argv[++i];
            } else {
                throw Usage{"--" + name + " needs a value"};
            }
            args.flags[name].push_back(value);
        } else {
            args.positional.push_back(a);
        }
    }
    return args;
}


const char* code_name(c::ErrorCode code) {
    switch (code) {
        case c::ErrorCode::not_found: return "not_found";
        case c::ErrorCode::already_exists: return "already_exists";
        case c::ErrorCode::precondition_failed: return "precondition_failed";
        case c::ErrorCode::not_empty: return "not_empty";
        case c::ErrorCode::invalid_argument: return "invalid_argument";
        case c::ErrorCode::expired: return "expired";
        case c::ErrorCode::no_space: return "no_space";
        case c::ErrorCode::quota_exceeded: return "quota_exceeded";
        case c::ErrorCode::permission_denied: return "permission_denied";
        case c::ErrorCode::unauthenticated: return "unauthenticated";
        case c::ErrorCode::unavailable: return "unavailable";
        case c::ErrorCode::aborted: return "aborted";
        case c::ErrorCode::corruption: return "corruption";
        case c::ErrorCode::io_error: return "io_error";
        case c::ErrorCode::unknown: return "unknown";
    }
    return "unknown";
}

struct Failure {
    c::Error error;
};

template <class T>
T take(c::Result<T> r) {
    if (!r) {
        throw Failure{r.error()};
    }
    return std::move(*r);
}

void take(c::Status s) {
    if (!s) {
        throw Failure{s.error()};
    }
}

std::string env_or(const char* name) {
    const char* v = std::getenv(name);
    return v ? v : "";
}

c::Precondition precondition(const Args& a) {
    if (a.has("if-not-exists")) {
        return c::Precondition::if_not_exists();
    }
    if (a.has("if-generation-match")) {
        return c::Precondition::if_generation(a.number("if-generation-match"));
    }
    return {};
}

long long ms(const std::chrono::system_clock::time_point& t) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(t.time_since_epoch()).count();
}

void print(const c::ObjectInfo& o) {
    std::cout << o.location.key << '\t' << o.generation << '\t' << o.size << '\t' << o.checksum << '\t'
              << (o.delete_marker ? "delete-marker" : "object") << '\t' << ms(o.created) << '\n';
}

void print(const c::TenantInfo& t) {
    std::cout << t.name << '\t' << t.quota.max_bytes << '\t' << t.quota.max_objects << '\t' << t.used_bytes
              << '\t' << t.used_objects << '\n';
}

void print(const c::BucketInfo& b) {
    std::cout << b.tenant << '\t' << b.bucket << '\t' << (b.config.versioning ? "versioned" : "unversioned")
              << '\n';
}

c::BucketConfig config_from(const Args& a, c::BucketConfig base) {
    if (a.has("versioning")) {
        base.versioning = a.get("versioning") != "false";
    }
    return base;
}

void run(c::Client& cl, const std::string& group, const std::string& cmd, const Args& a) {
    const std::string tenant = a.get("tenant", env_or("RMK_TENANT"));
    auto require_tenant = [&] {
        if (tenant.empty()) {
            throw Usage{"tenant is required (--tenant or RMK_TENANT)"};
        }
    };
    auto bucket = [&] { return a.at(2, "BUCKET"); };
    auto where = [&] { return c::Location{tenant, a.at(2, "BUCKET"), a.at(3, "KEY")}; };

    if (group == "tenant") {
        if (cmd == "create") {
            print(take(cl.create_tenant(a.at(2, "NAME"), {a.number("max-bytes"), a.number("max-objects")})));
        } else if (cmd == "get") {
            print(take(cl.get_tenant(a.at(2, "NAME"))));
        } else if (cmd == "quota") {
            print(take(cl.set_quota(a.at(2, "NAME"), {a.number("max-bytes"), a.number("max-objects")})));
        } else {
            throw Usage{"unknown tenant command"};
        }
    } else if (group == "bucket") {
        require_tenant();
        if (cmd == "create") {
            print(take(cl.create_bucket(tenant, bucket(), config_from(a, {}))));
        } else if (cmd == "delete") {
            take(cl.delete_bucket(tenant, bucket()));
        } else if (cmd == "get") {
            print(take(cl.get_bucket(tenant, bucket())));
        } else if (cmd == "list") {
            for (const auto& b : take(cl.list_buckets(tenant))) {
                print(b);
            }
        } else if (cmd == "config") {
            auto cur = take(cl.get_bucket(tenant, bucket()));
            print(take(cl.set_bucket_config(tenant, bucket(), config_from(a, cur.config))));
        } else {
            throw Usage{"unknown bucket command"};
        }
    } else if (group == "object") {
        require_tenant();
        if (cmd == "put") {
            auto src = a.at(4, "FILE");
            c::ProgressCallback progress = [](std::uint64_t id, std::uint32_t part, std::uint64_t done) {
                std::cerr << "upload " << id << " part " << part << " done, " << done << " bytes\n";
            };
            std::optional<std::uint64_t> resume;
            if (a.has("resume")) {
                resume = a.number("resume");
            }
            if (src == "-") {
                print(take(cl.put(where(), std::cin, precondition(a))));
            } else {
                print(take(cl.put_file(where(), src, precondition(a), resume, progress)));
            }
        } else if (cmd == "get") {
            auto dst = a.at(4, "FILE");
            c::GetOptions o{a.number("generation"), a.number("offset"), a.number("length")};
            if (dst == "-") {
                take(cl.get(where(), std::cout, o));
            } else {
                std::ofstream out(dst, std::ios::binary | std::ios::trunc);
                if (!out) {
                    throw Failure{{c::ErrorCode::io_error, "cannot write " + dst}};
                }
                take(cl.get(where(), out, o));
                out.flush();
                if (!out) {
                    throw Failure{{c::ErrorCode::io_error, "cannot write " + dst}};
                }
            }
        } else if (cmd == "head") {
            print(take(cl.head(where(), a.number("generation"))));
        } else if (cmd == "rm") {
            if (auto marker = take(cl.remove(where(), precondition(a))); marker != 0) {
                std::cout << marker << '\n';
            }
        } else if (cmd == "rm-version") {
            std::uint64_t gen = 0;
            try {
                gen = std::stoull(a.at(4, "GENERATION"));
            } catch (const std::invalid_argument&) {
                throw Usage{"GENERATION must be a number"};
            }
            take(cl.remove_version(where(), gen));
        } else if (cmd == "ls") {
            c::ListOptions o{a.get("prefix"), a.get("start-after"), static_cast<std::uint32_t>(a.number("limit"))};
            for (;;) {
                auto page = take(cl.list(tenant, bucket(), o));
                for (const auto& obj : page.objects) {
                    print(obj);
                }
                if (page.next_start_after.empty() || a.has("limit")) {
                    break;
                }
                o.start_after = page.next_start_after;
            }
        } else if (cmd == "versions") {
            for (const auto& v : take(cl.list_versions(where()))) {
                print(v);
            }
        } else {
            throw Usage{"unknown object command"};
        }
    } else if (group == "cluster" && cmd == "min-sdk-version") {
        take(cl.set_min_sdk_version(a.at(2, "VERSION")));
    } else {
        throw Usage{"unknown command"};
    }
}

const char* kUsage =
    "usage: rmkctl [--endpoint HOST:PORT] [--tenant T] [--ca-file F] [--insecure] GROUP COMMAND ...\n"
    "  (endpoint defaults to $RMK_ENDPOINT, tenant to $RMK_TENANT; bucket and object commands need a tenant)\n"
    "  tenant    create NAME | get NAME | quota NAME      [--max-bytes N] [--max-objects N]\n"
    "  bucket    create|delete|get|config BUCKET | list   [--versioning[=false]]\n"
    "  object    put BUCKET KEY FILE|- | get BUCKET KEY FILE|- | head|rm|versions BUCKET KEY |\n"
    "            rm-version BUCKET KEY GENERATION | ls BUCKET\n"
    "            [--if-not-exists] [--if-generation-match N] [--resume UPLOAD_ID]\n"
    "            [--generation N] [--offset N] [--length N] [--prefix P] [--start-after K] [--limit N]\n"
    "  cluster   min-sdk-version VERSION\n";

}  // namespace

int main(int argc, char** argv) {
    try {
        Args a = parse(argc, argv);
        if (a.positional.size() < 2) {
            std::cerr << kUsage;
            return 1;
        }
        c::Options o;
        if (const char* e = std::getenv("RMK_ENDPOINT")) {
            o.endpoint = e;
        }
        if (a.has("endpoint")) {
            o.endpoint = a.get("endpoint");
        }
        o.ca_file = a.get("ca-file");
        o.insecure = a.has("insecure");
        auto client = take(c::Client::connect(std::move(o)));
        run(client, a.positional[0], a.positional[1], a);
        std::cout.flush();
        return std::cout ? 0 : 1;
    } catch (const Failure& f) {
        std::cerr << "error: " << code_name(f.error.code) << ": " << f.error.message << '\n';
    } catch (const Usage& u) {
        std::cerr << "error: invalid_argument: " << u.message << '\n' << kUsage;
    }
    return 1;
}
