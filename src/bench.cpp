// shrt-bench spawns real shrt servers and drives load with a small raw-TCP
// load generator (keep-alive + optional HTTP pipelining), mirroring the
// shrt-ts autocannon scenarios.
//
// Usage: ./shrt-bench   env: BENCH_DURATION=5 CONNECTIONS=64
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

using Clock = std::chrono::steady_clock;

static int64_t env_i64(const char* k, int64_t def) {
    const char* v = getenv(k);
    return v ? strtoll(v, nullptr, 10) : def;
}
static int64_t duration_secs() { return env_i64("BENCH_DURATION", 5); }
static int64_t connections() { return env_i64("CONNECTIONS", 64); }

static int tcp_connect(uint16_t port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    sockaddr_in a{};
    a.sin_family = AF_INET;
    a.sin_port = htons(port);
    inet_pton(AF_INET, "127.0.0.1", &a.sin_addr);
    if (::connect(fd, (sockaddr*)&a, sizeof a) < 0) {
        ::close(fd);
        return -1;
    }
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
    return fd;
}

// ---------- response reader ----------

struct RConn {
    int fd;
    std::string buf; // unconsumed response bytes
};

// Ensure at least `n` bytes buffered; returns false on EOF/error.
static bool fill(RConn& c, size_t n) {
    char tmp[64 << 10];
    while (c.buf.size() < n) {
        ssize_t r = ::recv(c.fd, tmp, sizeof tmp, 0);
        if (r <= 0) return false;
        c.buf.append(tmp, r);
    }
    return true;
}

// Parse one HTTP/1.1 response; returns status code, <0 on error.
static int read_resp(RConn& c) {
    for (;;) {
        if (!fill(c, 5)) return -1;
        size_t he = c.buf.find("\r\n\r\n");
        if (he == std::string::npos) continue;
        // status code = digits after first space
        size_t sp = c.buf.find(' ');
        int code = sp == std::string::npos ? 0 : atoi(c.buf.c_str() + sp + 1);
        size_t cl = 0;
        bool chunked = false;
        for (size_t p = c.buf.find("\r\n") + 2; p < he;) {
            size_t e = c.buf.find("\r\n", p);
            if (e == std::string::npos || e > he) e = he;
            std::string_view h(c.buf.data() + p, e - p);
            size_t colon = h.find(':');
            if (colon != std::string_view::npos) {
                auto key = h.substr(0, colon);
                auto val = h.substr(colon + 1);
                while (!val.empty() && (val.front() == ' ' || val.front() == '\t'))
                    val.remove_prefix(1);
                if (key.size() == 14 &&
                    (key == "content-length" || key == "Content-Length"))
                    cl = (size_t)strtoull(std::string(val).c_str(), nullptr, 10);
                else if ((key.size() == 17 &&
                          (key == "transfer-encoding" ||
                           key == "Transfer-Encoding")) &&
                         val.find("chunked") != std::string_view::npos)
                    chunked = true;
            }
            p = e + 2;
        }
        size_t body_start = he + 4;
        if (chunked) {
            // consume chunks until 0-size
            size_t p = body_start;
            for (;;) {
                size_t e;
                while ((e = c.buf.find("\r\n", p)) == std::string::npos)
                    if (!fill(c, c.buf.size() + 64)) return -1;
                size_t n = strtoull(c.buf.c_str() + p, nullptr, 16);
                if (n == 0) {
                    p = e + 4; // "0\r\n\r\n" (e points at first \r\n after 0)
                    break;
                }
                p = e + 2 + n + 2;
                if (!fill(c, p)) return -1;
            }
            c.buf.erase(0, p);
        } else {
            if (!fill(c, body_start + cl)) return -1;
            c.buf.erase(0, body_start + cl);
        }
        return code;
    }
}

// ---------- load generator ----------

struct Result {
    int64_t reqs = 0, non2xx = 0, errs = 0;
    double avg = 0, p99 = 0;
};

static Result blast(uint16_t port, int conns,
                    const std::vector<std::string>& reqs, int pipe,
                    std::chrono::milliseconds dur) {
    std::atomic<int64_t> total{0}, non2xx{0}, errs{0};
    std::mutex lmu;
    std::vector<double> lats;
    auto deadline = Clock::now() + dur;
    std::vector<std::thread> ths;
    ths.reserve(conns);
    for (int c = 0; c < conns; c++) {
        ths.emplace_back([&, c] {
            int fd = tcp_connect(port);
            if (fd < 0) {
                errs.fetch_add(1, std::memory_order_relaxed);
                return;
            }
            RConn rc{fd, {}};
            rc.buf.reserve(64 << 10);
            size_t i = (size_t)c % reqs.size();
            std::vector<double> my;
            while (Clock::now() < deadline) {
                auto start = Clock::now();
                int batch = 0;
                for (int k = 0; k < pipe; k++) {
                    const std::string& r = reqs[i];
                    ssize_t w = ::send(fd, r.data(), r.size(), 0);
                    if (w < 0) {
                        errs.fetch_add(1, std::memory_order_relaxed);
                        break;
                    }
                    batch++;
                    i = (i + 1) % reqs.size();
                }
                if (batch == 0) break;
                int got = 0;
                while (got < batch) {
                    int code = read_resp(rc);
                    if (code < 0) {
                        errs.fetch_add(1, std::memory_order_relaxed);
                        break;
                    }
                    got++;
                    if (code < 200 || code >= 400)
                        non2xx.fetch_add(1, std::memory_order_relaxed);
                }
                if (got == 0) break;
                double el = std::chrono::duration<double, std::milli>(
                                Clock::now() - start)
                                .count() /
                            got;
                total.fetch_add(got, std::memory_order_relaxed);
                my.push_back(el);
            }
            ::close(fd);
            std::lock_guard lk(lmu);
            lats.insert(lats.end(), my.begin(), my.end());
        });
    }
    for (auto& t : ths) t.join();
    std::sort(lats.begin(), lats.end());
    Result r;
    r.reqs = total.load();
    r.non2xx = non2xx.load();
    r.errs = errs.load();
    if (!lats.empty()) {
        double s = 0;
        for (double v : lats) s += v;
        r.avg = s / (double)lats.size();
        r.p99 = lats[(size_t)(lats.size() * 0.99)];
    }
    return r;
}

static void report(const char* name, const Result& r, double rows_per_req) {
    double rps = (double)r.reqs / (double)duration_secs();
    printf("%-28s %10.0f req/s  %10.0f rows/s  lat avg %6.2fms  p99 %6.2fms  "
           "non2xx/3xx %7lld  err %lld\n",
           name, rps, rps * rows_per_req, r.avg, r.p99,
           (long long)r.non2xx, (long long)r.errs);
}

// ---------- server lifecycle ----------

static bool wait_healthy(uint16_t port) {
    auto deadline = Clock::now() + std::chrono::seconds(15);
    const char* req = "GET /api/health HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n";
    while (Clock::now() < deadline) {
        int fd = tcp_connect(port);
        if (fd >= 0) {
            (void)::send(fd, req, strlen(req), 0);
            char buf[256];
            ssize_t n = ::recv(fd, buf, sizeof buf - 1, 0);
            ::close(fd);
            if (n > 0) {
                buf[n] = 0;
                if (strstr(buf, "200")) return true;
            }
        }
        usleep(100'000);
    }
    return false;
}

using Env = std::vector<std::pair<std::string, std::string>>;

static pid_t start_server(const Env& env, const std::string& bin) {
    pid_t pid = fork();
    if (pid == 0) {
        int devnull = open("/dev/null", O_WRONLY);
        dup2(devnull, STDOUT_FILENO);
        for (auto& [k, v] : env) setenv(k.c_str(), v.c_str(), 1);
        execl(bin.c_str(), bin.c_str(), (char*)nullptr);
        _exit(127);
    }
    uint16_t port = 0;
    for (auto& [k, v] : env)
        if (k == "PORT") port = (uint16_t)atoi(v.c_str());
    if (!wait_healthy(port)) {
        fprintf(stderr, "server :%d did not start\n", port);
        exit(1);
    }
    return pid;
}

static void stop_server(pid_t pid) {
    kill(pid, SIGTERM);
    int st;
    waitpid(pid, &st, 0);
    // give SO_REUSEPORT children a moment to exit
    usleep(100'000);
}

// ---------- request templates ----------

static std::string get_req(const std::string& path) {
    return "GET " + path + " HTTP/1.1\r\nHost: x\r\n\r\n";
}
static std::string post_req(const std::string& path, const std::string& body) {
    return "POST " + path +
           " HTTP/1.1\r\nHost: x\r\ncontent-type: application/json\r\ncontent-length: " +
           std::to_string(body.size()) + "\r\n\r\n" + body;
}

static std::vector<std::string> make_codes(uint16_t port, int n) {
    std::vector<std::string> codes;
    codes.reserve(n);
    int fd = tcp_connect(port);
    RConn rc{fd, {}};
    for (int i = 0; i < n; i++) {
        std::string code = "bk" + std::to_string(i);
        std::string body = "{\"url\":\"https://bench.example/" +
                           std::to_string(i) + "\",\"alias\":\"" + code + "\"}";
        std::string r = post_req("/api/shorten", body);
        (void)::send(fd, r.data(), r.size(), 0);
        int status = read_resp(rc);
        if (status != 201) {
            fprintf(stderr, "alias seed failed: %d\n", status);
            exit(1);
        }
        codes.push_back(code);
    }
    ::close(fd);
    return codes;
}

int main() {
    const int keyspace = 50'000;
    std::string tmp = getenv("TMPDIR") ? getenv("TMPDIR") : "/tmp/";
    std::string bin = tmp + "shrt-cpp-bench-bin";
    uint16_t port = 4600;

    // build once
    if (system("make shrt 2>/dev/null") != 0) {
        fprintf(stderr, "make shrt failed\n");
        return 1;
    }
    std::string cwd = std::string(getenv("PWD") ? getenv("PWD") : ".");
    std::string built = cwd + "/shrt";
    {
        std::string cp = "cp '" + built + "' '" + bin + "'";
        if (system(cp.c_str()) != 0) {
            fprintf(stderr, "copy bin failed\n");
            return 1;
        }
    }

    printf("bench: %lld conns x %llds, keyspace %d\n\n",
           (long long)connections(), (long long)duration_secs(), keyspace);

    std::string write_req =
        post_req("/api/shorten", "{\"url\":\"https://bench.example/write\"}");
    const int BULK_N = 1000;
    std::string sb = "{\"urls\":[";
    for (int i = 0; i < BULK_N; i++) {
        if (i) sb += ',';
        sb += "\"https://b.example/" + std::to_string(i) + "\"";
    }
    sb += "]}";
    std::string bulk_req = post_req("/api/shorten/bulk", sb);

    auto env_for = [&](const char* tag,
                       std::vector<std::pair<const char*, std::string>> extra) {
        port += 10;
        Env e;
        std::string dir = tmp + "bench-cpp-" + tag + "-" + std::to_string(port);
        std::string rm = "rm -rf '" + dir + "'";
        system(rm.c_str());
        e.emplace_back("PORT", std::to_string(port));
        e.emplace_back("DATA_DIR", dir);
        for (auto& [k, v] : extra) e.emplace_back(k, v);
        return e;
    };
    auto dur = std::chrono::seconds(duration_secs());
    int conns = (int)connections();

    // --- mini, single instance ---
    {
        Env e = env_for("mini1", {{"SERVER", "mini"}, {"SEED", std::to_string(keyspace)}});
        pid_t srv = start_server(e, bin);
        auto codes = make_codes(port, 100);
        std::vector<std::string> reqs;
        for (auto& c : codes) reqs.push_back(get_req("/" + c));
        report("redirect (mini)", blast(port, conns, reqs, 1, dur), 1.0);
        report("redirect (mini, p10)", blast(port, conns, reqs, 10, dur), 1.0);
        std::vector<std::string> mixed(reqs.begin(), reqs.begin() + 95);
        for (int i = 0; i < 5; i++) mixed.push_back(write_req);
        report("mixed 95/5 (mini)", blast(port, conns, mixed, 1, dur), 1.0);
        report("shorten (mini)", blast(port, conns, {write_req}, 1, dur), 1.0);
        report("bulk x1000 (mini)", blast(port, conns / 4, {bulk_req}, 1, dur), BULK_N);
        stop_server(srv);
    }

    // --- kq comparison ---
    {
        Env e = env_for("kq1", {{"SERVER", "kq"}, {"SEED", std::to_string(keyspace)}});
        pid_t srv = start_server(e, bin);
        auto codes = make_codes(port, 100);
        std::vector<std::string> reqs;
        for (auto& c : codes) reqs.push_back(get_req("/" + c));
        report("redirect (kq)", blast(port, conns, reqs, 1, dur), 1.0);
        report("shorten (kq)", blast(port, conns, {write_req}, 1, dur), 1.0);
        stop_server(srv);
    }

    // --- mini multi-instance: 4 procs sharing the port via SO_REUSEPORT ---
    {
        Env e = env_for("mini4", {{"SERVER", "mini"},
                                  {"WORKERS", "4"},
                                  {"SEED", std::to_string(keyspace)}});
        pid_t srv = start_server(e, bin);
        report("bulk x1000 (mini x4)", blast(port, conns / 2, {bulk_req}, 1, dur), BULK_N);
        auto codes = make_codes(port, 100);
        std::vector<std::string> reqs;
        for (auto& c : codes) reqs.push_back(get_req("/" + c));
        // warm once so lazy tailing merges aliases across instances
        for (auto& c : codes) {
            int fd = tcp_connect(port);
            if (fd >= 0) {
                std::string r = get_req("/" + c);
                (void)::send(fd, r.data(), r.size(), 0);
                char b[512];
                (void)::recv(fd, b, sizeof b, 0);
                ::close(fd);
            }
        }
        report("redirect (mini x4)", blast(port, conns, reqs, 1, dur), 1.0);
        stop_server(srv);
    }
    return 0;
}
