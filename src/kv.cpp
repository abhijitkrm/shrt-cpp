#include "kv.hpp"
#include <arpa/inet.h>
#include <netdb.h>
#include <stdexcept>
#include <sys/socket.h>
#include <unistd.h>
#include <fcntl.h>

namespace shrt {

static int dial_tcp(const std::string& addr) {
    auto colon = addr.rfind(':');
    std::string host = colon == std::string::npos ? addr : addr.substr(0, colon);
    std::string port = colon == std::string::npos ? "6379" : addr.substr(colon + 1);
    addrinfo hints{}, *res = nullptr;
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    if (getaddrinfo(host.c_str(), port.c_str(), &hints, &res) != 0)
        return -1;
    int fd = -1;
    for (auto* rp = res; rp; rp = rp->ai_next) {
        fd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        if (fd < 0) continue;
        if (::connect(fd, rp->ai_addr, rp->ai_addrlen) == 0) break;
        ::close(fd);
        fd = -1;
    }
    freeaddrinfo(res);
    return fd;
}

Kv::Kv(const std::string& addr, int nconn) : addr_(addr) {
    for (int i = 0; i < nconn; i++) {
        auto c = std::make_unique<Conn>();
        c->fd = dial_tcp(addr);
        if (c->fd < 0) {
            if (i == 0) throw std::runtime_error("kv: connect " + addr + " failed");
            break;
        }
        conns_.push_back(std::move(c));
    }
    // verify
    Resp r = cmd({"PING"});
    if (r.kind != '+' || r.str != "PONG")
        throw std::runtime_error("kv: PING failed on " + addr);
}

Kv::~Kv() {
    for (auto& c : conns_)
        if (c->fd >= 0) ::close(c->fd);
}

Kv::Conn* Kv::conn() {
    size_t n = next_.fetch_add(1, std::memory_order_relaxed);
    return conns_[n % conns_.size()].get();
}

static void warg(std::string& b, const std::string& a) {
    b += '$';
    b += std::to_string(a.size());
    b += "\r\n";
    b += a;
    b += "\r\n";
}

// --- RESP reader: pull bytes into rbuf, parse incrementally ---
struct Reader {
    Kv::Conn& c;
    bool fill() {
        if (c.rpos == c.rbuf.size()) { c.rbuf.clear(); c.rpos = 0; }
        char tmp[64 * 1024];
        ssize_t n = ::recv(c.fd, tmp, sizeof(tmp), 0);
        if (n <= 0) return false;
        c.rbuf.append(tmp, (size_t)n);
        return true;
    }
    bool byte(char& b) {
        for (;;) {
            if (c.rpos < c.rbuf.size()) { b = c.rbuf[c.rpos++]; return true; }
            if (!fill()) return false;
        }
    }
    bool line(std::string& out) {
        for (;;) {
            size_t p = c.rbuf.find("\r\n", c.rpos);
            if (p != std::string::npos) {
                out.assign(c.rbuf, c.rpos, p - c.rpos);
                c.rpos = p + 2;
                return true;
            }
            if (!fill()) return false;
        }
    }
    bool exact(char* dst, size_t n) {
        for (size_t got = 0; got < n;) {
            size_t avail = c.rbuf.size() - c.rpos;
            if (avail) {
                size_t take = std::min(avail, n - got);
                memcpy(dst + got, c.rbuf.data() + c.rpos, take);
                c.rpos += take;
                got += take;
            } else if (!fill()) return false;
        }
        return true;
    }
};

static bool read_resp(Reader& rd, Resp& out) {
    char k;
    if (!rd.byte(k)) return false;
    out.kind = k;
    switch (k) {
    case '+': case '-':
        return rd.line(out.str);
    case ':': {
        std::string l;
        if (!rd.line(l)) return false;
        out.num = std::stoll(l);
        return true;
    }
    case '$': {
        std::string l;
        if (!rd.line(l)) return false;
        out.num = std::stoll(l);
        if (out.num < 0) return true;
        out.str.resize((size_t)out.num + 2);
        if (!rd.exact(out.str.data(), (size_t)out.num + 2)) return false;
        out.str.resize((size_t)out.num);
        return true;
    }
    case '*': {
        std::string l;
        if (!rd.line(l)) return false;
        out.num = std::stoll(l);
        if (out.num < 0) return true;
        out.arr.resize((size_t)out.num);
        for (auto& e : out.arr)
            if (!read_resp(rd, e)) return false;
        return true;
    }
    }
    return false;
}

Resp Kv::cmd(const std::vector<std::string>& args) {
    auto rs = pipe({args});
    return std::move(rs[0]);
}

std::vector<Resp> Kv::pipe(const std::vector<std::vector<std::string>>& cmds) {
    Conn& c = *conn();
    std::lock_guard<std::mutex> g(c.mu);
    std::string buf;
    for (auto& args : cmds) {
        buf += '*';
        buf += std::to_string(args.size());
        buf += "\r\n";
        for (auto& a : args) warg(buf, a);
    }
    size_t off = 0;
    while (off < buf.size()) {
        ssize_t n = ::send(c.fd, buf.data() + off, buf.size() - off, MSG_NOSIGNAL);
        if (n <= 0) throw std::runtime_error("kv: send failed");
        off += (size_t)n;
    }
    Reader rd{c};
    std::vector<Resp> rs(cmds.size());
    for (auto& r : rs)
        if (!read_resp(rd, r)) throw std::runtime_error("kv: read failed");
    return rs;
}

std::optional<std::string> Kv::get(const std::string& k) {
    Resp r = cmd({"GET", k});
    if (r.null()) return std::nullopt;
    return r.str;
}

bool Kv::set(const std::string& k, const std::string& v, int64_t px_ms, bool nx) {
    std::vector<std::string> args{"SET", k, v};
    if (px_ms > 0) { args.emplace_back("PX"); args.push_back(std::to_string(px_ms)); }
    if (nx) args.emplace_back("NX");
    Resp r = cmd(args);
    return r.kind == '+' && r.str == "OK";
}

int64_t Kv::del(const std::string& k) {
    Resp r = cmd({"DEL", k});
    return r.kind == ':' ? r.num : 0;
}

void Kv::incrby_many(const std::vector<std::pair<std::string, int64_t>>& deltas) {
    if (deltas.empty()) return;
    std::vector<std::vector<std::string>> cmds;
    cmds.reserve(deltas.size());
    for (auto& [k, d] : deltas)
        cmds.push_back({"INCRBY", k, std::to_string(d)});
    (void)pipe(cmds);
}

void Kv::scan_each(const std::string& pat, const std::function<void(std::string)>& cb) {
    std::string cursor = "0";
    for (;;) {
        Resp r = cmd({"SCAN", cursor, "MATCH", pat, "COUNT", "500"});
        if (r.kind != '*' || r.arr.size() != 2) return;
        cursor = r.arr[0].str;
        for (auto& k : r.arr[1].arr) cb(k.str);
        if (cursor == "0") return;
    }
}

std::optional<std::string> Kv::hget(const std::string& k, const std::string& f) {
    Resp r = cmd({"HGET", k, f});
    if (r.kind == '$' && !r.null()) return r.str;
    return std::nullopt;
}
bool Kv::hsetnx(const std::string& k, const std::string& f, const std::string& v) {
    Resp r = cmd({"HSETNX", k, f, v});
    return r.kind == ':' && r.num == 1;
}
bool Kv::hset(const std::string& k, const std::string& f, const std::string& v) {
    Resp r = cmd({"HSET", k, f, v});
    return r.kind == ':';
}
int64_t Kv::hdel(const std::string& k, const std::string& f) {
    Resp r = cmd({"HDEL", k, f});
    return r.kind == ':' ? r.num : 0;
}
void Kv::hincrby_many(const std::vector<std::tuple<std::string, std::string, int64_t>>& deltas) {
    if (deltas.empty()) return;
    std::vector<std::vector<std::string>> cmds;
    cmds.reserve(deltas.size());
    for (auto& [k, f, n] : deltas)
        cmds.push_back({"HINCRBY", k, f, std::to_string(n)});
    pipe(cmds);
}
void Kv::hscan_each(const std::string& k, const std::function<void(std::string, std::string)>& cb) {
    std::string cursor = "0";
    do {
        Resp r = cmd({"HSCAN", k, cursor, "COUNT", "1000"});
        if (r.kind != '*' || r.arr.size() != 2) return;
        cursor = r.arr[0].str;
        auto& items = r.arr[1].arr;
        for (size_t i = 0; i + 1 < items.size(); i += 2)
            cb(items[i].str, items[i + 1].str);
    } while (cursor != "0");
}

void Kv::flushdb() { (void)cmd({"FLUSHDB"}); }

} // namespace shrt
