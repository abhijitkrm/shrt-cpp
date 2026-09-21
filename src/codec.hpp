#pragma once
#include <cstdint>
#include <string>
#include <string_view>

namespace shrt {

/// One parsed log line: row {"c","u","a","e","i","n"}, hit delta {"h","d","i"},
/// or tombstone {"x"}.
struct Op {
    std::string c, u, h, x;
    int64_t a = 0, e = 0, i = 0, n = 0, d = 0;
    bool has_e = false;
};

bool parse_op(std::string_view line, Op& o);

void row_line_into(std::string& b, std::string_view c, std::string_view u,
                   int64_t a, int64_t e, int64_t i, int64_t n);
void hit_line_into(std::string& b, std::string_view c, int64_t d, int64_t i);
void del_line_into(std::string& b, std::string_view c);

std::string row_line(std::string_view c, std::string_view u, int64_t a,
                     int64_t e, int64_t i, int64_t n);
std::string hit_line(std::string_view c, int64_t d, int64_t i);
std::string del_line(std::string_view c);

} // namespace shrt
