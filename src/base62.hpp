#pragma once
#include <cstdint>
#include <string>

namespace shrt {

inline constexpr char ALPHABET[] =
    "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
inline constexpr int CODE_LEN = 8;
inline constexpr int MAX_INSTANCES = 62;

inline std::string base62_encode(uint64_t n) {
    if (n == 0) return "0";
    char buf[12];
    int i = 12;
    while (n) {
        buf[--i] = ALPHABET[n % 62];
        n /= 62;
    }
    return std::string(buf + i, 12 - i);
}

} // namespace shrt
