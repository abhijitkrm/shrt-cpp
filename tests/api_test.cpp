// API suite — runs against both HTTP frontends (mini + kq).
#include "../src/app.hpp"
#include "../src/ratelimit.hpp"
#include "../src/common.hpp"
#include "../src/server.hpp"
#include "../src/store.hpp"
#include "test.hpp"

#include <arpa/inet.h>
#include <fcntl.h>
#include <mutex>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <string.h>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace shrt;

// ADMIN_TOKEN is process-global; serialize tests that mutate it.
static std::mutex ADMIN_LOCK;

struct Resp {
    int status = 0;
    std::vector<std::pair<std::string, std::string>> headers;
    std::string body;
    std::string header(const std::string& k) const {
        for (auto& [h, v] : headers)
            if (strcasecmp(h.c_str(), k.c_str()) == 0) return v;
        return "";
    }
};

/// One request per fresh connection (Connection: close).
static Resp req(int port, const char* method, const std::string& path,
                const std::vector<std::pair<std::string, std::string>>& hdrs = {},
                const std::string& body = "") {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
    if (::connect(fd, (sockaddr*)&addr, sizeof addr) < 0) { ::close(fd); return {}; }
    std::string r = std::string(method) + " " + path +
                    " HTTP/1.1\r\nHost: x\r\nConnection: close\r\n";
    for (auto& [k, v] : hdrs) r += k + ": " + v + "\r\n";
    if (!body.empty()) r += "content-length: " + std::to_string(body.size()) + "\r\n";
    r += "\r\n";
    ::send(fd, r.data(), r.size(), 0);
    if (!body.empty()) ::send(fd, body.data(), body.size(), 0);
    std::string raw;
    char buf[8192];
    ssize_t n;
    while ((n = ::recv(fd, buf, sizeof buf, 0)) > 0) raw.append(buf, n);
    ::close(fd);

    Resp resp;
    size_t split = raw.find("\r\n\r\n");
    if (split == std::string::npos) return resp;
    resp.body = raw.substr(split + 4);
    std::string head = raw.substr(0, split);
    size_t eol = head.find("\r\n");
    std::string status_line = head.substr(0, eol);
    sscanf(status_line.c_str(), "HTTP/%*c.%*c %d", &resp.status);
    size_t p = eol == std::string::npos ? head.size() : eol + 2;
    while (p < head.size()) {
        size_t e = head.find("\r\n", p);
        std::string line = head.substr(p, e == std::string::npos ? e : e - p);
        p = e == std::string::npos ? head.size() : e + 2;
        size_t c = line.find(':');
        if (c != std::string::npos) {
            std::string v = line.substr(c + 1);
            while (!v.empty() && v.front() == ' ') v.erase(0, 1);
            resp.headers.emplace_back(line.substr(0, c), v);
        }
    }
    return resp;
}

static Resp get(int port, const std::string& path) { return req(port, "GET", path); }
static Resp post(int port, const std::string& path, const std::string& body) {
    return req(port, "POST", path, {{"content-type", "application/json"}}, body);
}

static bool contains(const std::string& s, const std::string& sub) {
    return s.find(sub) != std::string::npos;
}

static std::string json_get(const std::string& body, const std::string& key) {
    std::string k = "\"" + key + "\":";
    size_t i = body.find(k);
    if (i == std::string::npos) return "";
    i += k.size();
    if (body[i] == '"') {
        size_t e = body.find('"', i + 1);
        return body.substr(i + 1, e - i - 1);
    }
    size_t e = body.find_first_of(",}", i);
    return body.substr(i, e - i);
}

static std::string shorten(int port, const std::string& url) {
    Resp r = post(port, "/api/shorten", "{\"url\":\"" + url + "\"}");
    if (r.status != 201) {
        printf("  FAIL shorten %s -> %d\n", url.c_str(), r.status);
        ::tst::fails()++;
        return "";
    }
    return json_get(r.body, "code");
}

/// Boot the handler on each frontend and run fn against it.
static void run_suite(const std::function<void(int)>& fn) {
    for (const char* srv : {"mini", "kq"}) {
        Store st = Store::open(":memory:", -1);
        int lfd = listen_socket(0);
        CHECK(lfd >= 0);
        sockaddr_in6 a{};
        socklen_t al = sizeof a;
        getsockname(lfd, (sockaddr*)&a, &al);
        int port = ntohs(a.sin6_port);
        std::thread([srv, lfd, &st] {
            if (!strcmp(srv, "kq")) serve_kq(lfd, st);
            else serve_mini(lfd, st);
        }).detach();
        for (int i = 0; i < 200; i++) {
            if (get(port, "/api/health").status == 200) break;
            usleep(10'000);
        }
        fn(port);
    }
}

TEST(health) {
    run_suite([](int port) {
        Resp r = get(port, "/api/health");
        CHECK_EQ(r.status, 200);
        CHECK(contains(r.body, "\"ok\":true"));
    });
}

TEST(metrics) {
    run_suite([](int port) {
        Resp r = get(port, "/api/metrics");
        CHECK_EQ(r.status, 200);
        CHECK(contains(r.body, "\"req_s\":"));
        CHECK(contains(r.body, "\"total\":"));
        CHECK(contains(r.body, "\"per_second\":["));
    });
}

TEST(ui_served) {
    run_suite([](int port) {
        Resp r = get(port, "/");
        CHECK_EQ(r.status, 200);
        CHECK(contains(r.header("content-type"), "text/html"));
        CHECK(contains(r.body, "<title>shrt"));
    });
}

TEST(shorten_redirect_stats_flow) {
    run_suite([](int port) {
        Resp r = post(port, "/api/shorten", "{\"url\":\"https://example.com/some/path\"}");
        CHECK_EQ(r.status, 201);
        std::string code = json_get(r.body, "code");
        CHECK(!code.empty());
        CHECK(json_get(r.body, "short_url") == "/" + code);

        Resp redir = get(port, "/" + code);
        CHECK_EQ(redir.status, 302);
        CHECK_EQ(redir.header("location"), "https://example.com/some/path");

        Resp st = get(port, "/api/stats/" + code);
        CHECK(json_get(st.body, "url") == "https://example.com/some/path");
        CHECK(json_get(st.body, "hits") == "1");
    });
}

TEST(ttl_defaults_and_cap) {
    run_suite([](int port) {
        const int64_t DAY = 86'400'000;
        int64_t now = now_ms();
        std::string code = shorten(port, "https://ttl-default.example");
        Resp st = get(port, "/api/stats/" + code);
        int64_t e = strtoll(json_get(st.body, "expires_at").c_str(), nullptr, 10);
        CHECK(std::llabs(e - (now + DAY)) < 5000);

        std::string body = "{\"url\":\"https://ttl-cap.example\",\"ttl_ms\":" +
                           std::to_string(365 * DAY) + "}";
        std::string code2 = json_get(post(port, "/api/shorten", body).body, "code");
        Resp st2 = get(port, "/api/stats/" + code2);
        e = strtoll(json_get(st2.body, "expires_at").c_str(), nullptr, 10);
        CHECK(std::llabs(e - (now + DAY)) < 5000);

        Resp r3 = post(port, "/api/shorten",
                       "{\"url\":\"https://ttl-short.example\",\"ttl_ms\":5000}");
        std::string code3 = json_get(r3.body, "code");
        Resp st3 = get(port, "/api/stats/" + code3);
        e = strtoll(json_get(st3.body, "expires_at").c_str(), nullptr, 10);
        CHECK(std::llabs(e - (now + 5000)) < 5000);
    });
}

TEST(custom_alias) {
    run_suite([](int port) {
        Resp r = post(port, "/api/shorten", "{\"url\":\"https://a.com\",\"alias\":\"cool\"}");
        CHECK_EQ(r.status, 201);
        Resp redir = get(port, "/cool");
        CHECK_EQ(redir.header("location"), "https://a.com");
        Resp dup = post(port, "/api/shorten", "{\"url\":\"https://b.com\",\"alias\":\"cool\"}");
        CHECK_EQ(dup.status, 409);
    });
}

TEST(rejects_invalid_url) {
    run_suite([](int port) {
        for (const char* u : {"notaurl", "ftp://x.com", "javascript:alert(1)"}) {
            Resp r = post(port, "/api/shorten",
                          std::string("{\"url\":\"") + u + "\"}");
            CHECK_EQ(r.status, 400);
        }
    });
}

TEST(rejects_invalid_json_and_missing_url) {
    run_suite([](int port) {
        for (const char* b : {"{bad", "{}"}) {
            Resp r = post(port, "/api/shorten", b);
            CHECK_EQ(r.status, 400);
        }
    });
}

TEST(not_found) {
    run_suite([](int port) {
        for (const char* p : {"/zzz", "/api/stats/zzz", "/a/b/c"})
            CHECK_EQ(get(port, p).status, 404);
    });
}

TEST(bulk_shorten) {
    run_suite([](int port) {
        std::string body = "{\"urls\":[";
        for (int i = 0; i < 50; i++) {
            if (i) body += ',';
            body += "\"https://bulk.example/" + std::to_string(i) + "\"";
        }
        body += "]}";
        Resp r = post(port, "/api/shorten/bulk", body);
        CHECK_EQ(r.status, 201);
        CHECK(json_get(r.body, "count") == "50");
        // pull one code out and redirect-check it
        size_t ci = r.body.find("\"codes\":[\"") + 10;
        std::string first = r.body.substr(ci, r.body.find('"', ci) - ci);
        Resp redir = get(port, "/" + first);
        CHECK(redir.header("location").rfind("https://bulk.example/", 0) == 0);
    });
}

TEST(bulk_rejects_bad_input) {
    run_suite([](int port) {
        for (const char* urls :
             {"[]", "[\"ftp://x\"]", "[\"https://ok.com\",\"nope\"]", "\"notarray\""}) {
            Resp r = post(port, "/api/shorten/bulk",
                          std::string("{\"urls\":") + urls + "}");
            CHECK_EQ(r.status, 400);
        }
    });
}

TEST(rejects_oversized_body) {
    run_suite([](int port) {
        std::string big = "{\"url\":\"https://x.com/" + std::string(5000, 'a') + "\"}";
        Resp r = post(port, "/api/shorten", big);
        CHECK(r.status == 400 || r.status == 413);
    });
}

TEST(cors) {
    run_suite([](int port) {
        Resp pre = req(port, "OPTIONS", "/api/shorten");
        CHECK_EQ(pre.status, 204);
        CHECK_EQ(pre.header("access-control-allow-origin"), "*");
        CHECK(contains(pre.header("access-control-allow-methods"), "DELETE"));
        Resp r = get(port, "/api/health");
        CHECK_EQ(r.header("access-control-allow-origin"), "*");
    });
}

TEST(list_pagination_sort_search) {
    run_suite([](int port) {
        shorten(port, "https://list-one.example");
        shorten(port, "https://list-two.example");
        Resp m = get(port, "/api/links?limit=5&offset=0");
        CHECK_EQ(m.status, 200);
        CHECK(contains(m.body, "\"links\":["));
        CHECK(contains(m.body, "\"total\":"));
        Resp sm = get(port, "/api/links?q=list-one");
        CHECK(contains(sm.body, "list-one"));
        Resp tm = get(port, "/api/links?sort=hits&limit=3");
        CHECK_EQ(tm.status, 200);
    });
}

TEST(admin_mutations) {
    std::lock_guard lk(ADMIN_LOCK);
    run_suite([](int port) {
        unsetenv("ADMIN_TOKEN");
        std::string code = shorten(port, "https://before.example");

        Resp r = req(port, "DELETE", "/api/links/" + code);
        CHECK_EQ(r.status, 404);

        // empty ADMIN_TOKEN must also fail closed
        setenv("ADMIN_TOKEN", "", 1);
        Resp r0 = req(port, "DELETE", "/api/links/" + code,
                      {{"x-admin-token", ""}});
        CHECK_EQ(r0.status, 404);

        setenv("ADMIN_TOKEN", "secret", 1);
        Resp r2 = req(port, "DELETE", "/api/links/" + code,
                      {{"x-admin-token", "wrong"}});
        CHECK_EQ(r2.status, 404);

        Resp pr = req(port, "PATCH", "/api/links/" + code,
                      {{"content-type", "application/json"}, {"x-admin-token", "secret"}},
                      "{\"url\":\"https://after.example\",\"ttl_ms\":60000}");
        CHECK_EQ(pr.status, 200);
        Resp redir = get(port, "/" + code);
        CHECK_EQ(redir.header("location"), "https://after.example");
        Resp st = get(port, "/api/stats/" + code);
        CHECK(strtoll(json_get(st.body, "expires_at").c_str(), nullptr, 10) > now_ms());

        Resp br = req(port, "PATCH", "/api/links/" + code,
                      {{"content-type", "application/json"}, {"x-admin-token", "secret"}},
                      "{\"url\":\"notaurl\"}");
        CHECK_EQ(br.status, 400);

        Resp nr = req(port, "PATCH", "/api/links/nope",
                      {{"x-admin-token", "secret"}}, "{}");
        CHECK_EQ(nr.status, 400);
        unsetenv("ADMIN_TOKEN");
    });
}

TEST(delete_removes_and_frees_alias) {
    std::lock_guard lk(ADMIN_LOCK);
    run_suite([](int port) {
        setenv("ADMIN_TOKEN", "secret", 1);
        Resp r = post(port, "/api/shorten",
                      "{\"url\":\"https://del.example\",\"alias\":\"todelete\"}");
        CHECK_EQ(r.status, 201);
        Resp dr = req(port, "DELETE", "/api/links/todelete",
                      {{"x-admin-token", "secret"}});
        CHECK_EQ(dr.status, 204);
        CHECK_EQ(get(port, "/todelete").status, 404);
        Resp dr2 = req(port, "DELETE", "/api/links/todelete",
                       {{"x-admin-token", "secret"}});
        CHECK_EQ(dr2.status, 404);
        Resp reuse = post(port, "/api/shorten",
                          "{\"url\":\"https://new.example\",\"alias\":\"todelete\"}");
        CHECK_EQ(reuse.status, 201);
        unsetenv("ADMIN_TOKEN");
    });
}

TEST(prometheus_metrics) {
    run_suite([](int port) {
        shorten(port, "https://prom.example");
        Resp r = get(port, "/metrics");
        CHECK_EQ(r.status, 200);
        CHECK_EQ(r.header("content-type"), "text/plain; version=0.0.4");
        CHECK(contains(r.body, "# TYPE shrt_requests_total counter"));
        CHECK(contains(r.body, "shrt_requests_total{op=\"shorten\"}"));
        CHECK(contains(r.body, "shrt_links_total"));
        CHECK(contains(r.body, "shrt_uptime_seconds"));
        CHECK(contains(r.body, "shrt_rate_limited_total"));
    });
}

TEST(rate_limit_per_ip) {
    setenv("RATE_LIMIT", "1", 1);
    setenv("RATE_LIMIT_BURST", "4", 1);
    RateLimiter::global().reload_for_test();
    run_suite([](int port) {
        // run_suite drives mini+kq off one bucket — post until rejected,
        // assert it happens within burst+slack
        int oks = 0, last = 0;
        for (int i = 0; i < 10; i++) {
            last = post(port, "/api/shorten",
                        "{\"url\":\"https://rl.example\"}").status;
            if (last == 201) { oks++; continue; }
            break;
        }
        CHECK_EQ(last, 429);
        CHECK(oks <= 4);
        CHECK_EQ(get(port, "/nope").status, 404); // reads not limited
    });
    unsetenv("RATE_LIMIT");
    unsetenv("RATE_LIMIT_BURST");
    RateLimiter::global().reload_for_test();
}
