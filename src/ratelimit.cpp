#include "ratelimit.hpp"
#include <chrono>
#include <cstdlib>

namespace shrt {

std::atomic<uint64_t> rate_limited{0};

static int64_t now_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

static uint64_t ip_key(const std::string& ip) {
    uint64_t h = 0xcbf29ce484222325ULL;
    for (char c : ip) { h ^= (uint8_t)c; h *= 0x100000001b3ULL; }
    return h;
}

RateLimiter::RateLimiter() { reload_for_test(); }

void RateLimiter::reload_for_test() {
    for (auto& sh : shards_) {
        std::lock_guard<std::mutex> g(sh.mu);
        sh.m.clear();
    }
    if (const char* v = getenv("RATE_LIMIT")) rate_ = std::max(0.0, atof(v));
    else rate_ = 0;
    if (const char* v = getenv("RATE_LIMIT_BURST")) burst_ = std::max(0.0, atof(v));
    else burst_ = rate_;
    if (burst_ < 1) burst_ = 1;
}

bool RateLimiter::allow(const std::string& ip, double cost) {
    if (rate_ <= 0) return true;
    uint64_t k = ip_key(ip);
    auto& sh = shards_[k & (SHARDS - 1)];
    int64_t now = now_ms();
    std::lock_guard<std::mutex> g(sh.mu);
    if (sh.m.size() >= MAX_KEYS) {
        for (auto it = sh.m.begin(); it != sh.m.end();)
            it = (now - (int64_t)it->second.last_ms > IDLE_MS) ? sh.m.erase(it) : std::next(it);
    }
    auto [it, _] = sh.m.try_emplace(k, Bucket{burst_, (double)now});
    Bucket& b = it->second;
    b.tokens = std::min(b.tokens + (now - b.last_ms) / 1000.0 * rate_, burst_);
    b.last_ms = now;
    if (b.tokens >= cost) { b.tokens -= cost; return true; }
    return false;
}

} // namespace shrt
