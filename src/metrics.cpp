#include "metrics.hpp"
#include "common.hpp"
#include <atomic>
#include <cmath>
#include <cstdio>

namespace shrt::metrics {

static constexpr int N = 60;
static std::atomic<int64_t> BUCKETS[N];
static std::atomic<int64_t> CUR{0}, TOTAL{0}, STARTED{0};

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

} // namespace shrt::metrics
