#pragma once
#include <string>

namespace shrt::metrics {

void init();
void tick();
/// Renders {"req_s","total","uptime_s","per_second":[31]}.
std::string snapshot();

// ---- counters for the Prometheus /metrics endpoint ----
// Ops indexes: redirect, shorten, shorten_bulk, update, delete, list,
// stats, health, metrics, ui, other.
inline constexpr int OP_REDIRECT = 0, OP_SHORTEN = 1, OP_BULK = 2,
                     OP_UPDATE = 3, OP_DELETE = 4, OP_LIST = 5,
                     OP_STATS = 6, OP_HEALTH = 7, OP_METRICS = 8,
                     OP_UI = 9, OP_OTHER = 10;
void op(int i);
void status(int code);
void cache_hit();
void cache_miss();
void store_read(int64_t us);
void store_write();
void links_delta(int64_t n);
/// Prometheus text exposition — /metrics endpoint. rate_limited is the
/// ratelimit::rate_limited counter value.
std::string prometheus(uint64_t rate_limited);

} // namespace shrt::metrics
