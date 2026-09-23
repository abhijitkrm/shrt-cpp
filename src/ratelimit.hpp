#pragma once
#include <array>
#include <atomic>
#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>

namespace shrt {

/// Per-IP token bucket for write endpoints. RATE_LIMIT=<req/s per IP>
/// enables; RATE_LIMIT_BURST sets capacity (default = RATE_LIMIT).
/// POST /api/shorten costs 1; /api/shorten/bulk costs urls.size().
class RateLimiter {
    struct Bucket { double tokens, last_ms; };
    static constexpr size_t SHARDS = 64;
    static constexpr size_t MAX_KEYS = 1 << 12; // per shard
    static constexpr int64_t IDLE_MS = 60'000;

    struct Shard { std::mutex mu; std::unordered_map<uint64_t, Bucket> m; };
    std::array<Shard, SHARDS> shards_;
    double rate_ = 0, burst_ = 0;

public:
    static RateLimiter& global() {
        static RateLimiter rl;
        return rl;
    }

    RateLimiter();

    /// cost tokens from ip's bucket; true when allowed. Disabled → true.
    bool allow(const std::string& ip, double cost);

    /// Re-read env + clear state — tests only (the global limiter
    /// initializes on first use, before test env may be set).
    void reload_for_test();
};

/// Total requests rejected (exported via /metrics).
extern std::atomic<uint64_t> rate_limited;

} // namespace shrt
