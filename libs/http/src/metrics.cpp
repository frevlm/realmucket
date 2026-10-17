#include <charconv>
#include <cmath>
#include <cstdio>
#include <cstdlib>

#include "rmk/http/http.hpp"

namespace rmk::metrics {
namespace {

void add(std::atomic<double>& v, double by) {
    double cur = v.load(std::memory_order_relaxed);
    while (!v.compare_exchange_weak(cur, cur + by, std::memory_order_relaxed)) {
    }
}

std::string escape(std::string_view in, bool quotes) {
    std::string out;
    for (char ch : in) {
        if (ch == '\\') out += "\\\\";
        else if (ch == '\n') out += "\\n";
        else if (ch == '"' && quotes) out += "\\\"";
        else out += ch;
    }
    return out;
}

std::string format_value(double v) {
    if (std::isnan(v)) return "NaN";
    if (std::isinf(v)) return v > 0 ? "+Inf" : "-Inf";
    char buf[32];
    auto r = std::to_chars(buf, buf + sizeof buf, v);
    return std::string(buf, r.ptr);
}

std::string format_labels(const Labels& labels) {
    if (labels.empty()) return {};
    std::string out = "{";
    bool first = true;
    for (const auto& [k, v] : labels) {
        if (!first) out += ',';
        first = false;
        out += k + "=\"" + escape(v, true) + "\"";
    }
    return out + "}";
}

[[noreturn]] void type_conflict(std::string_view name) {
    std::fprintf(stderr, "metrics: family %.*s registered with two different types\n",
                 static_cast<int>(name.size()), name.data());
    std::abort();
}

}  // namespace

void Counter::inc(double by) { add(value_, by); }
double Counter::value() const { return value_.load(std::memory_order_relaxed); }

void Gauge::set(double v) { value_.store(v, std::memory_order_relaxed); }
void Gauge::add(double by) { metrics::add(value_, by); }
double Gauge::value() const { return value_.load(std::memory_order_relaxed); }

struct Registry::Family {
    bool is_counter = true;
    std::string help;
    std::map<Labels, std::unique_ptr<Counter>> counters;
    std::map<Labels, std::unique_ptr<Gauge>> gauges;
};

Registry::Registry() = default;
Registry::~Registry() = default;

Counter& Registry::counter(std::string_view name, std::string_view help, const Labels& labels) {
    std::lock_guard lock(mu_);
    auto it = families_.find(name);
    if (it == families_.end()) {
        auto fam = std::make_unique<Family>();
        fam->is_counter = true;
        fam->help = help;
        it = families_.emplace(std::string(name), std::move(fam)).first;
    }
    if (!it->second->is_counter) type_conflict(name);
    auto& slot = it->second->counters[labels];
    if (!slot) slot = std::make_unique<Counter>();
    return *slot;
}

Gauge& Registry::gauge(std::string_view name, std::string_view help, const Labels& labels) {
    std::lock_guard lock(mu_);
    auto it = families_.find(name);
    if (it == families_.end()) {
        auto fam = std::make_unique<Family>();
        fam->is_counter = false;
        fam->help = help;
        it = families_.emplace(std::string(name), std::move(fam)).first;
    }
    if (it->second->is_counter) type_conflict(name);
    auto& slot = it->second->gauges[labels];
    if (!slot) slot = std::make_unique<Gauge>();
    return *slot;
}

std::string Registry::render() const {
    std::lock_guard lock(mu_);
    std::string out;
    for (const auto& [name, fam] : families_) {
        out += "# HELP " + name + " " + escape(fam->help, false) + "\n";
        out += std::string("# TYPE ") + name + (fam->is_counter ? " counter\n" : " gauge\n");
        if (fam->is_counter) {
            for (const auto& [labels, c] : fam->counters)
                out += name + format_labels(labels) + " " + format_value(c->value()) + "\n";
        } else {
            for (const auto& [labels, g] : fam->gauges)
                out += name + format_labels(labels) + " " + format_value(g->value()) + "\n";
        }
    }
    return out;
}

Registry& global() {
    static Registry registry;
    return registry;
}

Result<std::unique_ptr<http::Server>> serve(std::uint16_t port) {
    return http::Server::start(port, [](const http::Request& req) {
        if (req.path == "/metrics")
            return http::Response{200, "text/plain; version=0.0.4; charset=utf-8", global().render()};
        if (req.path == "/healthz") return http::Response{200, "text/plain; charset=utf-8", "ok"};
        return http::Response{404, "text/plain; charset=utf-8", "not found\n"};
    });
}

}  // namespace rmk::metrics
