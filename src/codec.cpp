#include "codec.hpp"
#include "common.hpp"

namespace shrt {

// JSON-escape into dst (string values only — control chars + \ and ")
static void esc(std::string& dst, std::string_view s) {
    for (char ch : s) {
        switch (ch) {
        case '"':  dst += "\\\""; break;
        case '\\': dst += "\\\\"; break;
        case '\n': dst += "\\n"; break;
        case '\r': dst += "\\r"; break;
        case '\t': dst += "\\t"; break;
        default:
            if ((unsigned char)ch < 0x20) {
                char tmp[8];
                snprintf(tmp, sizeof tmp, "\\u%04x", ch);
                dst += tmp;
            } else {
                dst += ch;
            }
        }
    }
}

static std::string unescape(std::string_view b) {
    std::string out;
    out.reserve(b.size());
    for (size_t i = 0; i < b.size();) {
        if (b[i] != '\\' || i + 1 >= b.size()) { out += b[i++]; continue; }
        char e = b[i + 1];
        switch (e) {
        case 'n': out += '\n'; i += 2; break;
        case 'r': out += '\r'; i += 2; break;
        case 't': out += '\t'; i += 2; break;
        case 'u': {
            if (i + 5 < b.size()) {
                unsigned cp = std::strtoul(std::string(b.substr(i + 2, 4)).c_str(), nullptr, 16);
                if (cp < 0x80) out += (char)cp;
                else { // encode utf8
                    if (cp < 0x800) { out += (char)(0xc0 | cp >> 6); out += (char)(0x80 | (cp & 63)); }
                    else { out += (char)(0xe0 | cp >> 12); out += (char)(0x80 | ((cp >> 6) & 63)); out += (char)(0x80 | (cp & 63)); }
                }
                i += 6;
            } else i += 2;
            break;
        }
        default: out += e; i += 2; break;
        }
    }
    return out;
}

void row_line_into(std::string& b, std::string_view c, std::string_view u,
                   int64_t a, int64_t e, int64_t i, int64_t n) {
    b += "{\"c\":\"";
    b += c;
    b += "\",\"u\":\"";
    esc(b, u);
    b += "\",\"a\":";
    itoa(b, a);
    b += ",\"e\":";
    if (e == 0) b += "null";
    else itoa(b, e);
    b += ",\"i\":";
    itoa(b, i);
    b += ",\"n\":";
    itoa(b, n);
    b += '}';
}

void hit_line_into(std::string& b, std::string_view c, int64_t d, int64_t i) {
    b += "{\"h\":\"";
    b += c;
    b += "\",\"d\":";
    itoa(b, d);
    b += ",\"i\":";
    itoa(b, i);
    b += '}';
}

void del_line_into(std::string& b, std::string_view c) {
    b += "{\"x\":\"";
    b += c;
    b += "\"}";
}

std::string row_line(std::string_view c, std::string_view u, int64_t a,
                     int64_t e, int64_t i, int64_t n) {
    std::string b;
    b.reserve(u.size() + c.size() + 48);
    row_line_into(b, c, u, a, e, i, n);
    return b;
}
std::string hit_line(std::string_view c, int64_t d, int64_t i) {
    std::string b;
    b.reserve(c.size() + 24);
    hit_line_into(b, c, d, i);
    return b;
}
std::string del_line(std::string_view c) {
    std::string b;
    b.reserve(c.size() + 8);
    del_line_into(b, c);
    return b;
}

/// Decode one log line. Machine-generated input, but tolerates field
/// reordering and escapes in string values.
bool parse_op(std::string_view line, Op& o) {
    o = Op{};
    size_t pos = 0;
    const size_t L = line.size();
    auto& b = line;
    while (pos < L) {
        while (pos < L && b[pos] != '"') pos++;
        if (pos >= L) break;
        pos++;
        size_t ks = pos;
        while (pos < L && b[pos] != '"') pos++;
        if (pos >= L) return false;
        char key = pos > ks ? b[ks] : 0;
        pos++;
        while (pos < L && b[pos] != ':') pos++;
        if (pos >= L) return false;
        pos++;
        while (pos < L && (b[pos] == ' ' || b[pos] == '\t')) pos++;
        if (pos >= L) return false;
        if (b[pos] == '"') {
            pos++;
            size_t vs = pos;
            bool escapes = false;
            while (pos < L) {
                if (b[pos] == '\\') { escapes = true; pos += 2; continue; }
                if (b[pos] == '"') break;
                pos++;
            }
            std::string_view val = b.substr(vs, pos - vs);
            std::string s = escapes ? unescape(val) : std::string(val);
            pos++;
            switch (key) {
            case 'c': o.c = std::move(s); break;
            case 'u': o.u = std::move(s); break;
            case 'h': o.h = std::move(s); break;
            case 'x': o.x = std::move(s); break;
            default: break;
            }
        } else {
            size_t vs = pos;
            while (pos < L && b[pos] != ',' && b[pos] != '}') pos++;
            std::string_view num = b.substr(vs, pos - vs);
            while (!num.empty() && (num.back() == ' ' || num.back() == '\t')) num.remove_suffix(1);
            int64_t v = num == "null" ? 0 : std::strtoll(std::string(num).c_str(), nullptr, 10);
            switch (key) {
            case 'a': o.a = v; break;
            case 'e': if (num != "null") { o.e = v; o.has_e = true; } break;
            case 'i': o.i = v; break;
            case 'n': o.n = v; break;
            case 'd': o.d = v; break;
            default: break;
            }
        }
    }
    return !o.c.empty() || !o.h.empty() || !o.x.empty();
}

} // namespace shrt
