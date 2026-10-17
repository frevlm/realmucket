#include "rmk/http/http.hpp"

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace rmk {
namespace {

// "GET <path>" against the server; returns the whole reply (status line, headers, body).
std::string fetch(const http::Server& s, const std::string& path);
std::string status_line(const std::string& reply) { return reply.substr(0, reply.find("\r\n")); }
std::string body_of(const std::string& reply) {
    const auto at = reply.find("\r\n\r\n");
    return at == std::string::npos ? std::string() : reply.substr(at + 4);
}

std::string raw_exchange(std::uint16_t port, const std::string& request) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    ::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
    EXPECT_EQ(::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof addr), 0);
    EXPECT_EQ(::send(fd, request.data(), request.size(), 0), static_cast<ssize_t>(request.size()));
    std::string out;
    char buf[512];
    ssize_t n;
    while ((n = ::recv(fd, buf, sizeof buf, 0)) > 0) out.append(buf, static_cast<std::size_t>(n));
    ::close(fd);
    return out;
}

std::string fetch(const http::Server& s, const std::string& path) {
    return raw_exchange(s.port(), "GET " + path + " HTTP/1.1\r\nHost: x\r\n\r\n");
}

TEST(HttpServer, ServesHandlerAndGet) {
    auto server = http::Server::start(0, [](const http::Request& r) {
        return http::Response{200, "text/plain", r.method + " " + r.path};
    });
    ASSERT_TRUE(server.has_value());
    ASSERT_NE((*server)->port(), 0);
    const std::string reply = fetch(**server, "/a/b?x=1");
    EXPECT_EQ(status_line(reply), "HTTP/1.1 200 OK");
    EXPECT_EQ(body_of(reply), "GET /a/b");
}

TEST(HttpServer, HandlerExceptionIs500) {
    auto server = http::Server::start(0, [](const http::Request&) -> http::Response {
        throw std::runtime_error("boom");
    });
    ASSERT_TRUE(server.has_value());
    EXPECT_EQ(status_line(fetch(**server, "/")).rfind("HTTP/1.1 500", 0), 0u);
}

TEST(HttpServer, OversizedHeadersAre400) {
    auto server = http::Server::start(0, [](const http::Request&) { return http::Response{}; });
    ASSERT_TRUE(server.has_value());
    std::string req = "GET / HTTP/1.1\r\nX-Big: " + std::string(20000, 'a') + "\r\n\r\n";
    const std::string reply = raw_exchange((*server)->port(), req);
    EXPECT_EQ(reply.rfind("HTTP/1.1 400", 0), 0u) << reply;
}

TEST(HttpServer, ResponseCarriesLengthAndClose) {
    auto server = http::Server::start(0, [](const http::Request&) {
        return http::Response{200, "text/plain", "hello"};
    });
    ASSERT_TRUE(server.has_value());
    const std::string reply = raw_exchange((*server)->port(), "GET / HTTP/1.1\r\nHost: x\r\n\r\n");
    EXPECT_NE(reply.find("Content-Length: 5\r\n"), std::string::npos) << reply;
    EXPECT_NE(reply.find("Connection: close\r\n"), std::string::npos) << reply;
    EXPECT_EQ(reply.substr(reply.size() - 5), "hello");
}

TEST(HttpServer, DestructorReturnsPromptly) {
    auto server = http::Server::start(0, [](const http::Request&) { return http::Response{}; });
    ASSERT_TRUE(server.has_value());
    const auto start = std::chrono::steady_clock::now();
    server->reset();
    EXPECT_LT(std::chrono::steady_clock::now() - start, std::chrono::seconds(2));
}

TEST(Metrics, ServeHealthzAndMetrics) {
    metrics::global().counter("rmk_http_test_total", "Test counter.").inc(3);
    auto server = metrics::serve(0);
    ASSERT_TRUE(server.has_value());

    const std::string health = fetch(**server, "/healthz");
    EXPECT_EQ(status_line(health), "HTTP/1.1 200 OK");
    EXPECT_EQ(body_of(health), "ok");

    const std::string text = body_of(fetch(**server, "/metrics"));
    EXPECT_NE(text.find("rmk_http_test_total 3\n"), std::string::npos) << text;

    EXPECT_EQ(status_line(fetch(**server, "/nope")).rfind("HTTP/1.1 404", 0), 0u);
}

// Registry's header declares no constructor/destructor, so a Registry cannot be created outside
// metrics.cpp; these tests use global() with names that sort after every other family.
bool ends_with(const std::string& s, const std::string& suffix) {
    return s.size() >= suffix.size() && s.compare(s.size() - suffix.size(), suffix.size(), suffix) == 0;
}

TEST(Metrics, RenderExactText) {
    auto& reg = metrics::global();
    reg.gauge("zz_b_gauge", "A gauge.").set(-2.5);
    reg.counter("zz_a_total", "Requests\nwith \\ help.", {{"path", "/x"}, {"code", "200"}}).inc(2);
    reg.counter("zz_a_total", "ignored", {{"code", "500"}, {"path", "a\"b\\c\nd"}}).inc();

    const std::string expect =
        "# HELP zz_a_total Requests\\nwith \\\\ help.\n"
        "# TYPE zz_a_total counter\n"
        "zz_a_total{code=\"200\",path=\"/x\"} 2\n"
        "zz_a_total{code=\"500\",path=\"a\\\"b\\\\c\\nd\"} 1\n"
        "# HELP zz_b_gauge A gauge.\n"
        "# TYPE zz_b_gauge gauge\n"
        "zz_b_gauge -2.5\n";
    const std::string text = reg.render();
    EXPECT_TRUE(ends_with(text, expect)) << text;
}

TEST(Metrics, SameLabelsReturnSameMetric) {
    auto& reg = metrics::global();
    auto& a = reg.counter("zz_c_total", "help", {{"k", "v"}});
    auto& b = reg.counter("zz_c_total", "help", {{"k", "v"}});
    EXPECT_EQ(&a, &b);
}

TEST(Metrics, ConflictingTypeAborts) {
    EXPECT_DEATH(
        {
            metrics::global().counter("zz_x", "help");
            metrics::global().gauge("zz_x", "help");
        },
        "two different types");
}

TEST(Metrics, ConcurrentIncrementsSum) {
    auto& reg = metrics::global();
    auto& c = reg.counter("zz_n_total", "help");
    auto& g = reg.gauge("zz_level", "help");
    const double c0 = c.value();
    const double g0 = g.value();
    constexpr int kThreads = 8;
    constexpr int kPerThread = 20000;
    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; ++t)
        threads.emplace_back([&] {
            for (int i = 0; i < kPerThread; ++i) {
                c.inc();
                g.add(0.5);
            }
        });
    for (auto& t : threads) t.join();
    EXPECT_EQ(c.value() - c0, kThreads * kPerThread);
    EXPECT_EQ(g.value() - g0, kThreads * kPerThread * 0.5);
}

}  // namespace
}  // namespace rmk
