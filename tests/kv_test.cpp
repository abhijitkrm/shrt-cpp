// Live tests for the RESP-KV backend. Gated on SHRT_KV_ADDR — skipped
// when unset/unreachable so the suite stays hermetic.
//   SHRT_KV_ADDR=127.0.0.1:6379 ./shrt-test
#include "../src/store.hpp"
#include "../src/kv.hpp"
#include "test.hpp"
#include <cstdlib>
#include <mutex>
#include <optional>
#include <thread>

using namespace shrt;

// tests share one logical DB — serialize so flushdb isolates
static std::mutex kv_mu;

static std::optional<Store> kv_open(int inst = 0, size_t cache = 1000, int64_t cttl = 50) {
    const char* a = getenv("SHRT_KV_ADDR");
    if (!a || !*a) return std::nullopt;
    try {
        return Store::open_kv(a, inst, cache, cttl);
    } catch (...) {
        return std::nullopt;
    }
}

#define KV_GUARD(sv) \
    std::lock_guard<std::mutex> _g(kv_mu); \
    auto _os = kv_open(); \
    if (!_os) { printf("  skip %s: SHRT_KV_ADDR unset/unreachable\n", __func__); return; } \
    Store& sv = *_os; \
    { const char* _a = getenv("SHRT_KV_ADDR"); try { Kv _k(_a, 1); _k.flushdb(); } catch (...) {} }

TEST(kv_shorten_resolve) {
    KV_GUARD(s);
    std::string gh = "gh";
    auto c = s.shorten("https://a.com", &gh, 0);
    CHECK(c && *c == "gh");
    auto u = s.resolve("gh");
    CHECK(u && *u == "https://a.com");
    CHECK(!s.shorten("https://b.com", &gh, 0));
    auto g = s.shorten("https://c.com", nullptr, 0);
    CHECK(g);
    auto u2 = s.resolve(*g);
    CHECK(u2 && *u2 == "https://c.com");
}

TEST(kv_hits_batched) {
    KV_GUARD(s);
    std::string h = "h";
    s.shorten("https://a.com", &h, 0);
    for (int i = 0; i < 5; i++) s.resolve("h");
    s.flush();
    auto st = s.stats("h");
    CHECK(st && st->hits == 5);
}

TEST(kv_update_remove) {
    KV_GUARD(s);
    std::string u = "u";
    s.shorten("https://a.com", &u, 0);
    CHECK(s.update("u", "https://b.com", 0, false) == MutResult::Ok);
    auto r = s.resolve("u");
    CHECK(r && *r == "https://b.com");
    CHECK(s.update("missing", "https://x.com", 0, false) == MutResult::Missing);
    CHECK(s.remove("u") == MutResult::Ok);
    CHECK(!s.resolve("u"));
    CHECK(s.remove("u") == MutResult::Missing);
}

TEST(kv_ttl) {
    KV_GUARD(s);
    std::string t = "ttl";
    s.shorten("https://t.com", &t, 80);
    CHECK(s.resolve("ttl"));
    std::this_thread::sleep_for(std::chrono::milliseconds(120));
    CHECK(!s.resolve("ttl"));
}

TEST(kv_list_stats) {
    KV_GUARD(s);
    std::string a = "one", b = "two";
    s.shorten("https://one.com", &a, 0);
    s.shorten("https://two.com", &b, 0);
    s.resolve("one");
    s.flush();
    auto pr = s.list(10, 0, "", "");
    CHECK_EQ(pr.second, 2u);
    bool found = false;
    for (auto& l : pr.first)
        if (l.code == "one" && l.hits == 1) found = true;
    CHECK(found);
    auto st = s.stats("two");
    CHECK(st && st->url == "https://two.com" && st->created_at > 0);
}

TEST(kv_bulk) {
    KV_GUARD(s);
    std::vector<std::string> urls;
    for (int i = 0; i < 50; i++)
        urls.push_back("https://b.com/" + std::to_string(i));
    auto codes = s.shorten_many(urls, 0);
    CHECK_EQ(codes.size(), 50u);
    for (size_t i = 0; i < 50; i++) {
        auto u = s.resolve(codes[i]);
        CHECK(u && *u == urls[i]);
    }
}

TEST(kv_cold_miss) {
    KV_GUARD(s);
    std::string k = "stay";
    s.shorten("https://stay.com", &k, 0);
    auto s2 = kv_open(1, 100, 50);
    CHECK(s2.has_value());
    auto u = s2->resolve("stay");
    CHECK(u && *u == "https://stay.com");
}

TEST(kv_cache_bounded) {
    KV_GUARD(s);
    std::vector<std::string> codes;
    for (int i = 0; i < 200; i++) {
        auto c = s.shorten("https://x.com/" + std::to_string(i), nullptr, 0);
        CHECK(c);
        codes.push_back(*c);
    }
    for (auto& c : codes)
        CHECK(s.resolve(c));
}
