#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <string>
#include <utility>

#include "rmk/http/http.hpp"

namespace rmk::http {
namespace {

constexpr std::size_t kMaxHeaderBytes = 8 * 1024;

#ifdef MSG_NOSIGNAL
constexpr int kSendFlags = MSG_NOSIGNAL;
#else
constexpr int kSendFlags = 0;
#endif

const char* reason(int status) {
    switch (status) {
        case 200: return "OK";
        case 400: return "Bad Request";
        case 404: return "Not Found";
        case 405: return "Method Not Allowed";
        case 500: return "Internal Server Error";
        case 503: return "Service Unavailable";
        default: return "Status";
    }
}

void send_all(int fd, const std::string& data) {
    std::size_t sent = 0;
    while (sent < data.size()) {
        ssize_t n = ::send(fd, data.data() + sent, data.size() - sent, kSendFlags);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) return;
        sent += static_cast<std::size_t>(n);
    }
}

void respond(int fd, const Response& r) {
    std::string out = "HTTP/1.1 " + std::to_string(r.status) + " " + reason(r.status) + "\r\n";
    out += "Content-Type: " + r.content_type + "\r\n";
    out += "Content-Length: " + std::to_string(r.body.size()) + "\r\n";
    out += "Connection: close\r\n\r\n";
    out += r.body;
    send_all(fd, out);
}

Response bad_request() { return Response{400, "text/plain; charset=utf-8", "bad request\n"}; }

// Reads up to the blank line ending the headers; false if the peer stalls, closes early or the
// head exceeds kMaxHeaderBytes.
bool read_head(int fd, std::string& head) {
    char buf[1024];
    while (head.find("\r\n\r\n") == std::string::npos) {
        if (head.size() > kMaxHeaderBytes) return false;
        ssize_t n = ::recv(fd, buf, sizeof buf, 0);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) return false;
        head.append(buf, static_cast<std::size_t>(n));
    }
    return head.find("\r\n\r\n") + 4 <= kMaxHeaderBytes;
}

bool parse_request_line(const std::string& head, Request& req) {
    const std::size_t eol = head.find("\r\n");
    const std::string line = head.substr(0, eol);
    const std::size_t sp1 = line.find(' ');
    if (sp1 == std::string::npos || sp1 == 0) return false;
    const std::size_t sp2 = line.find(' ', sp1 + 1);
    if (sp2 == std::string::npos) return false;
    std::string target = line.substr(sp1 + 1, sp2 - sp1 - 1);
    if (target.empty() || target[0] != '/') return false;
    req.method = line.substr(0, sp1);
    req.path = target.substr(0, target.find('?'));
    return true;
}

void serve_connection(int fd, const Handler& handler) {
    timeval tv{5, 0};
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
    ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof tv);
#ifdef SO_NOSIGPIPE
    int one = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof one);
#endif

    std::string head;
    Request req;
    if (!read_head(fd, head) || !parse_request_line(head, req)) {
        respond(fd, bad_request());
        return;
    }
    Response resp;
    try {
        resp = handler(req);
    } catch (...) {
        resp = Response{500, "text/plain; charset=utf-8", "internal error\n"};
    }
    respond(fd, resp);
}

}  // namespace

Result<std::unique_ptr<Server>> Server::start(std::uint16_t port, Handler handler) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return fail(Errc::io_error, std::string("socket: ") + std::strerror(errno));

    int one = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(port);
    if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof addr) != 0 || ::listen(fd, 64) != 0) {
        std::string msg = "listen on port " + std::to_string(port) + ": " + std::strerror(errno);
        ::close(fd);
        return fail(Errc::io_error, std::move(msg));
    }
    socklen_t len = sizeof addr;
    if (::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len) != 0) {
        std::string msg = std::string("getsockname: ") + std::strerror(errno);
        ::close(fd);
        return fail(Errc::io_error, std::move(msg));
    }

    std::unique_ptr<Server> server(new Server());
    server->listen_fd_ = fd;
    server->port_ = ntohs(addr.sin_port);
    server->handler_ = std::move(handler);
    Server* self = server.get();
    server->thread_ = std::thread([self] {
        while (!self->stopping_) {
            pollfd p{self->listen_fd_, POLLIN, 0};
            if (::poll(&p, 1, 100) <= 0 || !(p.revents & POLLIN)) continue;
            int conn = ::accept(self->listen_fd_, nullptr, nullptr);
            if (conn < 0) continue;
            serve_connection(conn, self->handler_);
            ::close(conn);
        }
    });
    return server;
}

Server::~Server() {
    stopping_ = true;
    if (thread_.joinable()) thread_.join();
    if (listen_fd_ >= 0) ::close(listen_fd_);
}

}  // namespace rmk::http
