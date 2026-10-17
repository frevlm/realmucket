#include "rmk/http.hpp"

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

std::string url(const http::Server& s, const std::string& path) {
    return "http://127.0.0.1:" + std::to_string(s.port()) + path;
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

TEST(HttpServer, ServesHandlerAndGet) {
    auto server = http::Server::start(0, [](const http::Request& r) {
        return http::Response{200, "text/plain", r.method + " " + r.path};
    });
    ASSERT_TRUE(server.has_value());
    ASSERT_NE((*server)->port(), 0);
    auto body = http::get(url(**server, "/a/b?x=1"), {});
    ASSERT_TRUE(body.has_value()) << body.error().message;
    EXPECT_EQ(*body, "GET /a/b");
}

TEST(HttpServer, HandlerExceptionIs500) {
    auto server = http::Server::start(0, [](const http::Request&) -> http::Response {
        throw std::runtime_error("boom");
    });
    ASSERT_TRUE(server.has_value());
    auto body = http::get(url(**server, "/"), {});
    ASSERT_FALSE(body.has_value());
    EXPECT_EQ(body.error().code, Errc::unavailable);
    EXPECT_NE(body.error().message.find("500"), std::string::npos);
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

TEST(HttpClient, SendsBearerToken) {
    std::string seen;
    int lfd = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    ASSERT_EQ(::bind(lfd, reinterpret_cast<sockaddr*>(&addr), sizeof addr), 0);
    ASSERT_EQ(::listen(lfd, 1), 0);
    socklen_t len = sizeof addr;
    ::getsockname(lfd, reinterpret_cast<sockaddr*>(&addr), &len);
    std::thread t([&] {
        int fd = ::accept(lfd, nullptr, nullptr);
        char buf[2048];
        ssize_t n = ::recv(fd, buf, sizeof buf, 0);
        if (n > 0) seen.assign(buf, static_cast<std::size_t>(n));
        const char reply[] = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok";
        ::send(fd, reply, sizeof reply - 1, 0);
        ::close(fd);
    });
    http::GetOptions opt;
    opt.bearer_token = "tok123";
    auto r = http::get("http://127.0.0.1:" + std::to_string(ntohs(addr.sin_port)) + "/", opt);
    t.join();
    ::close(lfd);
    ASSERT_TRUE(r.has_value()) << r.error().message;
    EXPECT_EQ(*r, "ok");
    EXPECT_NE(seen.find("Authorization: Bearer tok123"), std::string::npos) << seen;
}

TEST(HttpClient, StatusMapping) {
    auto server = http::Server::start(0, [](const http::Request& r) {
        if (r.path == "/401") return http::Response{401, "text/plain", ""};
        if (r.path == "/403") return http::Response{403, "text/plain", ""};
        if (r.path == "/404") return http::Response{404, "text/plain", ""};
        if (r.path == "/400") return http::Response{400, "text/plain", ""};
        if (r.path == "/503") return http::Response{503, "text/plain", ""};
        return http::Response{200, "text/plain", "fine"};
    });
    ASSERT_TRUE(server.has_value());
    const std::pair<const char*, Errc> cases[] = {
        {"/401", Errc::permission_denied}, {"/403", Errc::permission_denied},
        {"/404", Errc::not_found},         {"/400", Errc::invalid_argument},
        {"/503", Errc::unavailable},
    };
    for (const auto& [path, code] : cases) {
        auto r = http::get(url(**server, path), {});
        ASSERT_FALSE(r.has_value()) << path;
        EXPECT_EQ(r.error().code, code) << path;
        EXPECT_NE(r.error().message.find(std::string(path + 1)), std::string::npos) << path;
    }
}

TEST(HttpClient, TransportErrorIsUnavailable) {
    std::uint16_t dead_port;
    {
        auto server = http::Server::start(0, [](const http::Request&) { return http::Response{}; });
        ASSERT_TRUE(server.has_value());
        dead_port = (*server)->port();
    }
    auto r = http::get("http://127.0.0.1:" + std::to_string(dead_port) + "/", {});
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error().code, Errc::unavailable);
}

TEST(Metrics, ServeHealthzAndMetrics) {
    metrics::global().counter("rmk_http_test_total", "Test counter.").inc(3);
    auto server = metrics::serve(0);
    ASSERT_TRUE(server.has_value());

    auto health = http::get(url(**server, "/healthz"), {});
    ASSERT_TRUE(health.has_value());
    EXPECT_EQ(*health, "ok");

    auto text = http::get(url(**server, "/metrics"), {});
    ASSERT_TRUE(text.has_value());
    EXPECT_NE(text->find("rmk_http_test_total 3\n"), std::string::npos) << *text;

    auto missing = http::get(url(**server, "/nope"), {});
    ASSERT_FALSE(missing.has_value());
    EXPECT_EQ(missing.error().code, Errc::not_found);
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
