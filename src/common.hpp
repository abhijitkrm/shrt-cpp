#pragma once
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <string>

namespace shrt {

inline int64_t now_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

inline int64_t env_i64(const char* k, int64_t def) {
    const char* v = std::getenv(k);
    if (!v || !*v) return def;
    return std::strtoll(v, nullptr, 10);
}

inline std::string env_str(const char* k, const std::string& def) {
    const char* v = std::getenv(k);
    return (v && *v) ? std::string(v) : def;
}

inline bool track_hits() { return env_i64("TRACK_HITS", 1) != 0; }

// append n's decimal digits — no allocation
inline void itoa(std::string& b, int64_t n) {
    char tmp[24];
    int i = 24;
    uint64_t v = n < 0 ? (uint64_t)(-(n + 1)) + 1 : (uint64_t)n;
    do {
        tmp[--i] = '0' + (v % 10);
        v /= 10;
    } while (v);
    if (n < 0) tmp[--i] = '-';
    b.append(tmp + i, 24 - i);
}

} // namespace shrt
