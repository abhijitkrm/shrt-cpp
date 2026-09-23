#include "metrics.hpp"
#include "common.hpp"
#include <atomic>
#include <cmath>
#include <cstdio>

namespace shrt::metrics {

static constexpr int N = 60;
static std::atomic<int64_t> BUCKETS[N];
static std::atomic<int64_t> CUR{0}, TOTAL{0}, STARTED{0};

static const char* OPS[] = {"redirect", "shorten", "shorten_bulk", "update",
                            "delete",  "list",    "stats",        "health",
                            "metrics", "ui",      "other"};
static std::atomic<int64_t> OP_COUNTS[11];
static std::atomic<int64_t> STATUS[4]; // 2xx 3xx 4xx 5xx
static std::atomic<int64_t> CACHE_HIT{0}, CACHE_MISS{0};
static std::atomic<int64_t> STORE_READS{0}, STORE_READ_US{0}, STORE_WRITES{0};
static std::atomic<int64_t> LINKS_TOTAL{0};

void op(int i) { OP_COUNTS[i].fetch_add(1, std::memory_order_relaxed); }
void status(int code) {
    int i = code >= 200 && code < 300 ? 0
          : code >= 300 && code < 400 ? 1
          : code >= 400 && code < 500 ? 2
                                      : 3;
    STATUS[i].fetch_add(1, std::memory_order_relaxed);
}
void cache_hit() { CACHE_HIT.fetch_add(1, std::memory_order_relaxed); }
void cache_miss() { CACHE_MISS.fetch_add(1, std::memory_order_relaxed); }
void store_read(int64_t us) {
    STORE_READS.fetch_add(1, std::memory_order_relaxed);
    STORE_READ_US.fetch_add(us, std::memory_order_relaxed);
}
void store_write() { STORE_WRITES.fetch_add(1, std::memory_order_relaxed); }
void links_delta(int64_t n) { LINKS_TOTAL.fetch_add(n, std::memory_order_relaxed); }

void init() {
    int64_t ms = now_ms();
    STARTED.store(ms, std::memory_order_relaxed);
    CUR.store(ms / 1000, std::memory_order_relaxed);
}

void tick() {
    TOTAL.fetch_add(1, std::memory_order_relaxed);
    int64_t s = now_ms() / 1000;
    int64_t c = CUR.load(std::memory_order_relaxed);
    int64_t sec = s;
    if (s != c) {
        if (CUR.compare_exchange_strong(c, s, std::memory_order_relaxed)) {
            for (int64_t t = c + 1; t <= s; t++)
                BUCKETS[t % N].store(0, std::memory_order_relaxed);
        } else {
            sec = CUR.load(std::memory_order_relaxed);
        }
    }
    BUCKETS[sec % N].fetch_add(1, std::memory_order_relaxed);
}

std::string snapshot() {
    int64_t s = now_ms() / 1000;
    int64_t c = CUR.load(std::memory_order_relaxed);
    int64_t window[31] = {};
    for (int i = 0; i < 31; i++) {
        int64_t t = s - 30 + i;
        if (t <= c && c - t < N)
            window[i] = BUCKETS[t % N].load(std::memory_order_relaxed);
    }
    int64_t last5 = 0;
    for (int i = 25; i < 30; i++) last5 += window[i];
    double req_s = std::round(last5 / 5.0 * 10.0) / 10.0;

    std::string b;
    b.reserve(320);
    b += "{\"req_s\":";
    char tmp[32];
    snprintf(tmp, sizeof tmp, "%g", req_s);
    b += tmp;
    b += ",\"total\":";
    itoa(b, TOTAL.load(std::memory_order_relaxed));
    b += ",\"uptime_s\":";
    itoa(b, (now_ms() - STARTED.load(std::memory_order_relaxed)) / 1000);
    b += ",\"per_second\":[";
    for (int i = 0; i < 31; i++) {
        if (i) b += ',';
        itoa(b, window[i]);
    }
    b += "]}";
    return b;
}

static void pline(std::string& b, const char* m, const std::string& labels,
                  int64_t v) {
    b += m;
    if (!labels.empty()) { b += '{'; b += labels; b += '}'; }
    b += ' ';
    itoa(b, v);
    b += '\n';
}

std::string prometheus(uint64_t rate_limited) {
    std::string b;
    b.reserve(1024);
    b += "# HELP shrt_requests_total Requests by operation\n";
    b += "# TYPE shrt_requests_total counter\n";
    for (int i = 0; i < 11; i++)
        pline(b, "shrt_requests_total", std::string("op=\"") + OPS[i] + "\"",
              OP_COUNTS[i].load(std::memory_order_relaxed));
    b += "# HELP shrt_responses_total Responses by status class\n";
    b += "# TYPE shrt_responses_total counter\n";
    const char* cls[] = {"2xx", "3xx", "4xx", "5xx"};
    for (int i = 0; i < 4; i++)
        pline(b, "shrt_responses_total", std::string("class=\"") + cls[i] + "\"",
              STATUS[i].load(std::memory_order_relaxed));
    b += "# HELP shrt_cache_lookups_total Local hot-cache lookups\n";
    b += "# TYPE shrt_cache_lookups_total counter\n";
    pline(b, "shrt_cache_lookups_total", "result=\"hit\"", CACHE_HIT.load(std::memory_order_relaxed));
    pline(b, "shrt_cache_lookups_total", "result=\"miss\"", CACHE_MISS.load(std::memory_order_relaxed));
    b += "# HELP shrt_store_reads_total Backing-store point reads (cache misses)\n";
    b += "# TYPE shrt_store_reads_total counter\n";
    pline(b, "shrt_store_reads_total", "", STORE_READS.load(std::memory_order_relaxed));
    b += "# HELP shrt_store_read_us_total Cumulative backing-store read latency (us)\n";
    b += "# TYPE shrt_store_read_us_total counter\n";
    pline(b, "shrt_store_read_us_total", "", STORE_READ_US.load(std::memory_order_relaxed));
    b += "# HELP shrt_store_writes_total Backing-store writes\n";
    b += "# TYPE shrt_store_writes_total counter\n";
    pline(b, "shrt_store_writes_total", "", STORE_WRITES.load(std::memory_order_relaxed));
    b += "# HELP shrt_rate_limited_total Requests rejected by the rate limiter\n";
    b += "# TYPE shrt_rate_limited_total counter\n";
    pline(b, "shrt_rate_limited_total", "", (int64_t)rate_limited);
    b += "# HELP shrt_links_total Live links created minus deleted\n";
    b += "# TYPE shrt_links_total gauge\n";
    pline(b, "shrt_links_total", "", LINKS_TOTAL.load(std::memory_order_relaxed));
    b += "# HELP shrt_uptime_seconds Process uptime\n";
    b += "# TYPE shrt_uptime_seconds gauge\n";
    pline(b, "shrt_uptime_seconds", "",
          (now_ms() - STARTED.load(std::memory_order_relaxed)) / 1000);
    return b;
}

} // namespace shrt::metrics
