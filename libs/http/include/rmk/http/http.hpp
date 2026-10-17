#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "rmk/core/error.hpp"

namespace rmk::http {

// ----- Minimal blocking HTTP/1.1 server for /metrics and /healthz. One thread, one request
// per connection ("Connection: close"); good enough for scrapes and probes, nothing more.
struct Request {
    std::string method;
    std::string path;  // without query string
};

struct Response {
    int status = 200;
    std::string content_type = "text/plain; charset=utf-8";
    std::string body;
};

using Handler = std::function<Response(const Request&)>;

class Server {
public:
    // port 0 picks a free port; see port().
    static Result<std::unique_ptr<Server>> start(std::uint16_t port, Handler handler);
    ~Server();  // stops and joins
    std::uint16_t port() const { return port_; }

private:
    Server() = default;
    int listen_fd_ = -1;
    std::uint16_t port_ = 0;
    std::atomic<bool> stopping_{false};
    Handler handler_;
    std::thread thread_;
};

// ----- Client (libcurl). Used for the k8s API (node labels).
struct GetOptions {
    std::string bearer_token;  // empty: no Authorization header
    std::string ca_file;       // empty: system trust store
    std::chrono::milliseconds timeout{5000};
};

// Body of a 2xx response; unavailable on transport errors, the status code in the message otherwise.
Result<std::string> get(std::string_view url, const GetOptions& options);

}  // namespace rmk::http

namespace rmk::metrics {

using Labels = std::map<std::string, std::string>;

// Prometheus text exposition. Thread-safe; metrics live as long as the Registry.
class Counter {
public:
    void inc(double by = 1);
    double value() const;

private:
    std::atomic<double> value_{0};
};

class Gauge {
public:
    void set(double v);
    void add(double by);
    double value() const;

private:
    std::atomic<double> value_{0};
};

class Registry {
public:
    Registry();
    ~Registry();
    Counter& counter(std::string_view name, std::string_view help, const Labels& labels = {});
    Gauge& gauge(std::string_view name, std::string_view help, const Labels& labels = {});
    std::string render() const;  // text format 0.0.4

private:
    struct Family;
    mutable std::mutex mu_;
    std::map<std::string, std::unique_ptr<Family>, std::less<>> families_;
};

Registry& global();

// Serves GET /metrics from global() and GET /healthz -> 200 "ok".
Result<std::unique_ptr<http::Server>> serve(std::uint16_t port);

}  // namespace rmk::metrics
