#include "server.hpp"
#include "app.hpp"
#include "common.hpp"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <net/if.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <signal.h>
#include <string.h>
#include <sys/event.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

namespace shrt {

static constexpr int MAX_HEADERS_LINE = 64 << 10; // request head cap
static constexpr size_t READ_CAP = 64 << 10;

// ---------- listener ----------

int listen_socket(int port) {
    int fd = ::socket(AF_INET6, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    int on = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof on);
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &on, sizeof on);
    int off = 0;
    ::setsockopt(fd, IPPROTO_IPV6, IPV6_V6ONLY, &off, sizeof off); // dual-stack
    sockaddr_in6 addr{};
    addr.sin6_family = AF_INET6;
    addr.sin6_addr = in6addr_any;
    addr.sin6_port = htons((uint16_t)port);
    if (::bind(fd, (sockaddr*)&addr, sizeof addr) < 0) { ::close(fd); return -1; }
    if (::listen(fd, 1024) < 0) { ::close(fd); return -1; }
    return fd;
}

// ---------- http helpers ----------

static const char* status_line(int status) {
    switch (status) {
    case 200: return "HTTP/1.1 200 OK\r\n";
    case 201: return "HTTP/1.1 201 Created\r\n";
    case 204: return "HTTP/1.1 204 No Content\r\n";
    case 302: return "HTTP/1.1 302 Found\r\n";
    case 400: return "HTTP/1.1 400 Bad Request\r\n";
    case 404: return "HTTP/1.1 404 Not Found\r\n";
    case 409: return "HTTP/1.1 409 Conflict\r\n";
    case 413: return "HTTP/1.1 413 Payload Too Large\r\n";
    case 429: return "HTTP/1.1 429 Too Many Requests\r\n";
    case 503: return "HTTP/1.1 503 Service Unavailable\r\n";
    default:  return "HTTP/1.1 500 Internal Server Error\r\n";
    }
}

static const std::string& cors_block() {
    static const std::string b = [] {
        std::string s;
        s += "access-control-allow-origin: ";
        s += cors_origin();
        s += "\r\naccess-control-allow-methods: GET,POST,PATCH,DELETE,OPTIONS\r\n"
             "access-control-allow-headers: content-type,x-admin-token\r\n"
             "access-control-max-age: 86400\r\n";
        return s;
    }();
    return b;
}

static void write_reply(std::string& out, const Reply& r) {
    out += status_line(r.status);
    out += cors_block();
    if (r.location) {
        out += "location: ";
        out += *r.location;
        out += "\r\ncontent-length: 0\r\n\r\n";
        return;
    }
    out += "content-type: ";
    out += r.ctype ? r.ctype : "application/json";
    out += "\r\ncontent-length: ";
    itoa(out, (int64_t)r.body.size());
    out += "\r\n\r\n";
    out += r.body;
}

struct ReqInfo {
    size_t total = 0;        // bytes consumed (head+body), 0 = incomplete
    std::string_view method, path, admin, xff;
    size_t body_off = 0, body_len = 0;
    bool alive = true;
};

/// Remote IP of a connected socket ("" on failure).
static std::string peer_ip(int fd) {
    sockaddr_storage ss{};
    socklen_t sl = sizeof ss;
    if (::getpeername(fd, (sockaddr*)&ss, &sl) < 0) return {};
    char buf[INET6_ADDRSTRLEN]{};
    if (ss.ss_family == AF_INET)
        ::inet_ntop(AF_INET, &((sockaddr_in*)&ss)->sin_addr, buf, sizeof buf);
    else if (ss.ss_family == AF_INET6)
        ::inet_ntop(AF_INET6, &((sockaddr_in6*)&ss)->sin6_addr, buf, sizeof buf);
    return buf;
}

static bool trust_proxy() {
    static const bool v = getenv("TRUST_PROXY") != nullptr;
    return v;
}

/// Rate-limit key: first X-Forwarded-For hop under TRUST_PROXY, else peer.
static std::string client_ip(const ReqInfo& ri, const std::string& peer) {
    if (trust_proxy() && !ri.xff.empty()) {
        auto first = ri.xff.substr(0, ri.xff.find(','));
        while (!first.empty() && first.front() == ' ') first.remove_prefix(1);
        while (!first.empty() && first.back() == ' ') first.remove_suffix(1);
        if (!first.empty()) return std::string(first);
    }
    return peer;
}

/// Parse one request from buf[pos..]; fills ReqInfo. total==0 when incomplete.
static ReqInfo parse_req(std::string_view buf, size_t pos) {
    ReqInfo ri;
    std::string_view rest = buf.substr(pos);
    size_t he = rest.find("\r\n\r\n");
    if (he == std::string_view::npos) return ri; // need more head bytes
    std::string_view head = rest.substr(0, he);
    // request line: METHOD SP PATH SP VERSION
    size_t sp1 = head.find(' ');
    if (sp1 == std::string_view::npos) { ri.total = SIZE_MAX; return ri; }
    size_t sp2 = head.find(' ', sp1 + 1);
    if (sp2 == std::string_view::npos) { ri.total = SIZE_MAX; return ri; }
    ri.method = head.substr(0, sp1);
    size_t crlf = head.find("\r\n", sp2 + 1);
    ri.path = head.substr(sp1 + 1, sp2 - sp1 - 1);
    std::string_view ver = crlf == std::string_view::npos
        ? std::string_view{} : head.substr(sp2 + 1, crlf - sp2 - 1);
    ri.alive = ver == "HTTP/1.1"; // 1.1 default keep-alive
    size_t cl = 0;
    // header scan
    size_t hp = crlf == std::string_view::npos ? head.size() : crlf + 2;
    while (hp < head.size()) {
        size_t eol = head.find("\r\n", hp);
        std::string_view line = eol == std::string_view::npos
            ? head.substr(hp) : head.substr(hp, eol - hp); // last line has no CRLF
        hp = eol == std::string_view::npos ? head.size() : eol + 2;
        size_t colon = line.find(':');
        if (colon == std::string_view::npos) continue;
        std::string_view k = line.substr(0, colon);
        std::string_view v = line.substr(colon + 1);
        while (!v.empty() && (v.front() == ' ' || v.front() == '\t')) v.remove_prefix(1);
        while (!v.empty() && (v.back() == ' ' || v.back() == '\t')) v.remove_suffix(1);
        if (k.size() == 14 && (k == "content-length" || k == "Content-Length"))
            cl = (size_t)strtoll(std::string(v).c_str(), nullptr, 10);
        else if (k.size() == 10 && (k == "connection" || k == "Connection"))
            ri.alive = !(v == "close" || v == "Close");
        else if (k.size() == 13 && (k == "x-admin-token" || k == "X-Admin-Token"))
            ri.admin = v;
        else if (k.size() == 15 && (k == "x-forwarded-for" || k == "X-Forwarded-For"))
            ri.xff = v;
    }
    bool needs_body = ri.method == "POST" || ri.method == "PATCH";
    ri.body_off = pos + he + 4;
    ri.body_len = needs_body ? cl : 0;
    ri.total = he + 4 + ri.body_len;
    if (buf.size() - pos < ri.total) { ri.total = 0; return ri; } // body pending
    return ri;
}

/// Body-size guard mirroring the TS/Go/Rust builds.
static size_t body_limit(std::string_view path) {
    size_t q = path.find('?');
    std::string_view pn = path.substr(0, q);
    return pn == "/api/shorten/bulk" ? MAX_BULK_BODY : MAX_BODY;
}

// ---------- mini: thread-per-connection ----------

static void conn_loop(int fd, Store* st) {
    const std::string peer = peer_ip(fd);
    int on = 1;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &on, sizeof on);
    std::string buf, out;
    buf.reserve(READ_CAP);
    out.reserve(READ_CAP);
    size_t parsed = 0;
    bool close_after = false;
    for (;;) {
        // grow buffer if full
        if (buf.size() == buf.capacity() && buf.capacity() < (size_t)MAX_HEADERS_LINE + MAX_BULK_BODY)
            buf.reserve(buf.capacity() * 2);
        char tmp[64 << 10];
        ssize_t n = ::recv(fd, tmp, sizeof tmp, 0);
        if (n > 0) buf.append(tmp, n);
        if (n == 0 && parsed == buf.size()) { ::close(fd); return; } // EOF, nothing pending
        if (n < 0) {
            if (errno == EINTR) continue;
            ::close(fd);
            return;
        }
        for (;;) {
            ReqInfo ri = parse_req(buf, parsed);
            if (ri.total == 0) break;                       // incomplete
            if (ri.total == SIZE_MAX) { ::close(fd); return; } // malformed: drop conn
            bool needs_body = ri.method == "POST" || ri.method == "PATCH";
            if (needs_body && ri.body_len > body_limit(ri.path)) {
                Reply r{413, nullptr, "{\"error\":\"body too large\"}", nullptr};
                write_reply(out, r);
                (void)::send(fd, out.data(), out.size(), MSG_NOSIGNAL);
                ::close(fd);
                return; // destroy on oversize
            }
            std::string body = needs_body
                ? std::string(buf.substr(ri.body_off, ri.body_len)) : std::string();
            Reply r = handle(*st, std::string(ri.method), std::string(ri.path),
                             body, std::string(ri.admin), client_ip(ri, peer));
            write_reply(out, r);
            parsed += ri.total;
            if (!ri.alive) { close_after = true; break; }
        }
        if (parsed) { buf.erase(0, parsed); parsed = 0; }
        if (!out.empty()) {
            const char* p = out.data();
            size_t left = out.size();
            while (left) {
                ssize_t w = ::send(fd, p, left, MSG_NOSIGNAL);
                if (w <= 0) { if (errno == EINTR) continue; ::close(fd); return; }
                p += w; left -= (size_t)w;
            }
            out.clear();
        }
        if (close_after || n == 0) { ::close(fd); return; }
    }
}

int serve_mini(int listen_fd, Store& st) {
    for (;;) {
        int fd = ::accept(listen_fd, nullptr, nullptr);
        if (fd < 0) {
            if (errno == EINTR) continue;
            if (errno == EMFILE || errno == ENFILE) { usleep(1000); continue; }
            return -1;
        }
        std::thread(conn_loop, fd, &st).detach();
    }
}

// ---------- kq: single-thread kqueue event loop ----------

struct KqConn {
    std::string rbuf, wbuf;
    std::string peer; // accepted peer IP — the rate-limit key
    size_t parsed = 0;
    bool close_after = false;
    bool want_write = false;
};

int serve_kq(int listen_fd, Store& st) {
    ::fcntl(listen_fd, F_SETFL, O_NONBLOCK);
    int kq = ::kqueue();
    if (kq < 0) return -1;
    std::unordered_map<int, KqConn> conns;
    conns.reserve(1024);

    auto add_ev = [&](int fd, int16_t filt, uint16_t flags) {
        struct kevent ev;
        EV_SET(&ev, fd, filt, flags, 0, 0, nullptr);
        ::kevent(kq, &ev, 1, nullptr, 0, nullptr);
    };
    add_ev(listen_fd, EVFILT_READ, EV_ADD | EV_ENABLE);

    std::vector<struct kevent> evs(256);
    char tmp[64 << 10];
    for (;;) {
        int nev = ::kevent(kq, nullptr, 0, evs.data(), (int)evs.size(), nullptr);
        if (nev < 0) { if (errno == EINTR) continue; return -1; }
        for (int k = 0; k < nev; k++) {
            int fd = (int)evs[k].ident;
            if (fd == listen_fd) {
                for (;;) {
                    int c = ::accept(listen_fd, nullptr, nullptr);
                    if (c < 0) break;
                    int on = 1;
                    ::setsockopt(c, IPPROTO_TCP, TCP_NODELAY, &on, sizeof on);
                    ::fcntl(c, F_SETFL, O_NONBLOCK);
                    conns[c] = KqConn{};
                    conns[c].peer = peer_ip(c);
                    conns[c].rbuf.reserve(READ_CAP);
                    conns[c].wbuf.reserve(READ_CAP);
                    add_ev(c, EVFILT_READ, EV_ADD | EV_ENABLE);
                }
                continue;
            }
            auto it = conns.find(fd);
            if (it == conns.end()) continue;
            KqConn& cn = it->second;

            if (evs[k].flags & EV_EOF || evs[k].flags & EV_ERROR) {
                if (evs[k].filter == EVFILT_WRITE || cn.wbuf.empty()) {
                    ::close(fd); conns.erase(it); continue;
                }
            }
            if (evs[k].filter == EVFILT_READ) {
                ssize_t n;
                while ((n = ::recv(fd, tmp, sizeof tmp, 0)) > 0)
                    cn.rbuf.append(tmp, n);
                if (n == 0) cn.close_after = true;
                // process complete requests
                for (;;) {
                    ReqInfo ri = parse_req(cn.rbuf, cn.parsed);
                    if (ri.total == 0) break;
                    if (ri.total == SIZE_MAX) { cn.wbuf.clear(); cn.close_after = true; break; }
                    bool needs_body = ri.method == "POST" || ri.method == "PATCH";
                    if (needs_body && ri.body_len > body_limit(ri.path)) {
                        Reply r{413, nullptr, "{\"error\":\"body too large\"}", nullptr};
                        write_reply(cn.wbuf, r);
                        cn.close_after = true;
                        break;
                    }
                    std::string body = needs_body
                        ? std::string(cn.rbuf.substr(ri.body_off, ri.body_len)) : std::string();
                    Reply r = handle(st, std::string(ri.method), std::string(ri.path),
                                     body, std::string(ri.admin),
                                     client_ip(ri, cn.peer));
                    write_reply(cn.wbuf, r);
                    cn.parsed += ri.total;
                    if (!ri.alive) { cn.close_after = true; break; }
                }
                if (cn.parsed) { cn.rbuf.erase(0, cn.parsed); cn.parsed = 0; }
                if (!cn.wbuf.empty() && !cn.want_write) {
                    add_ev(fd, EVFILT_WRITE, EV_ADD | EV_ENABLE);
                    cn.want_write = true;
                }
            }
            if (evs[k].filter == EVFILT_WRITE && !cn.wbuf.empty()) {
                ssize_t w = ::send(fd, cn.wbuf.data(), cn.wbuf.size(), MSG_NOSIGNAL);
                if (w > 0) cn.wbuf.erase(0, (size_t)w);
                else if (w < 0 && errno != EAGAIN && errno != EINTR) {
                    ::close(fd); conns.erase(it); continue;
                }
                if (cn.wbuf.empty() && cn.want_write) {
                    add_ev(fd, EVFILT_WRITE, EV_DELETE);
                    cn.want_write = false;
                }
            }
            if (cn.close_after && cn.wbuf.empty()) {
                ::close(fd);
                conns.erase(it);
            }
        }
    }
}

} // namespace shrt
