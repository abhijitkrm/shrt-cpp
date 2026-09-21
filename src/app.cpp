#include "app.hpp"
#include "common.hpp"
#include "metrics.hpp"

#include <cctype>
#include <cstdio>
#include <mutex>
#include <unordered_map>

namespace shrt {

int64_t link_ttl_ms() { return env_i64("LINK_TTL_MS", 86'400'000); }
const char* cors_origin() {
    static std::string v = env_str("CORS_ORIGIN", "*");
    return v.c_str();
}

// ---------- minimal JSON (enough for our request bodies) ----------

struct J {
    enum T { NUL, NUM, STR, ARR, OBJ, BOOL } t = NUL;
    double num = 0;
    std::string str;
    std::vector<J> arr;
    std::vector<std::pair<std::string, J>> obj;
    const J* find(const std::string& k) const {
        if (t != OBJ) return nullptr;
        for (auto& [key, v] : obj)
            if (key == k) return &v;
        return nullptr;
    }
};

struct Jp {
    const char* p;
    const char* end;
    bool ok = true;
    void ws() { while (p < end && (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r')) p++; }
    bool lit(const char* s) {
        size_t n = strlen(s);
        if ((size_t)(end - p) < n || memcmp(p, s, n)) return false;
        p += n;
        return true;
    }
    J val() {
        ws();
        if (p >= end) { ok = false; return {}; }
        switch (*p) {
        case '{': return obj();
        case '[': return arr();
        case '"': { J j; j.t = J::STR; j.str = str(); return j; }
        case 't': if (lit("true")) { J j; j.t = J::BOOL; j.num = 1; return j; } break;
        case 'f': if (lit("false")) { J j; j.t = J::BOOL; return j; } break;
        case 'n': if (lit("null")) return J{}; break;
        default: return num();
        }
        ok = false;
        return {};
    }
    std::string str() {
        std::string out;
        if (p >= end || *p != '"') { ok = false; return out; }
        p++;
        while (p < end && *p != '"') {
            if (*p == '\\' && p + 1 < end) {
                p++;
                switch (*p) {
                case 'n': out += '\n'; break;
                case 'r': out += '\r'; break;
                case 't': out += '\t'; break;
                case 'u': {
                    if (p + 4 < end) {
                        out += '?'; // \uXXXX rare in urls; skip decode
                        p += 4;
                    }
                    break;
                }
                default: out += *p;
                }
                p++;
                continue;
            }
            out += *p++;
        }
        if (p >= end) { ok = false; return out; }
        p++;
        return out;
    }
    J num() {
        char* e2 = nullptr;
        double d = strtod(p, &e2);
        if (e2 == p) { ok = false; return {}; }
        p = e2;
        J j; j.t = J::NUM; j.num = d;
        return j;
    }
    J arr() {
        J j; j.t = J::ARR;
        p++; ws();
        if (p < end && *p == ']') { p++; return j; }
        for (;;) {
            j.arr.push_back(val());
            if (!ok) return j;
            ws();
            if (p < end && *p == ',') { p++; continue; }
            if (p < end && *p == ']') { p++; return j; }
            ok = false;
            return j;
        }
    }
    J obj() {
        J j; j.t = J::OBJ;
        p++; ws();
        if (p < end && *p == '}') { p++; return j; }
        for (;;) {
            ws();
            std::string k = str();
            if (!ok) return j;
            ws();
            if (p >= end || *p != ':') { ok = false; return j; }
            p++;
            j.obj.emplace_back(std::move(k), val());
            if (!ok) return j;
            ws();
            if (p < end && *p == ',') { p++; continue; }
            if (p < end && *p == '}') { p++; return j; }
            ok = false;
            return j;
        }
    }
};

static bool jparse(const std::string& s, J& out) {
    Jp p{s.data(), s.data() + s.size()};
    out = p.val();
    p.ws();
    return p.ok && p.p == p.end;
}

// ---------- validation ----------

static bool code_ok(const std::string& s) {
    if (s.empty() || s.size() > 64) return false;
    for (unsigned char c : s)
        if (!isalnum(c) && c != '_' && c != '-') return false;
    return true;
}

static bool is_valid_url(const std::string& u) {
    if (u.empty() || u.size() > 2048) return false;
    size_t pos;
    if (u.rfind("http://", 0) == 0) pos = 7;
    else if (u.rfind("https://", 0) == 0) pos = 8;
    else return false;
    size_t host_end = u.find_first_of("/?#", pos);
    std::string_view host = std::string_view(u).substr(
        pos, host_end == std::string::npos ? std::string::npos : host_end - pos);
    if (host.empty()) return false;
    for (char c : host)
        if (c == ' ' || c == '"' || c == '\\' || (unsigned char)c < 0x21) return false;
    return true;
}

// ---------- responses ----------

static Reply mk(int status, std::string body) {
    return Reply{status, nullptr, std::move(body), nullptr};
}
static Reply bad(const std::string& err) {
    return mk(400, "{\"error\":\"" + err + "\"}");
}
static Reply not_found() { return mk(404, "{\"error\":\"not found\"}"); }

/// ui/index.html loaded once; empty when absent.
static const std::string& ui_html() {
    static std::string html = [] {
        std::string s;
        if (FILE* f = fopen("ui/index.html", "rb")) {
            char buf[8192];
            size_t n;
            while ((n = fread(buf, 1, sizeof buf, f))) s.append(buf, n);
            fclose(f);
        }
        return s;
    }();
    return html;
}

static void link_json(std::string& b, const Link& l) {
    b += "{\"code\":\"";
    b += l.code;
    b += "\",\"url\":\"";
    for (char ch : l.url) { // escape
        if (ch == '"' || ch == '\\') b += '\\';
        b += ch;
    }
    b += "\",\"hits\":";
    itoa(b, l.hits);
    b += ",\"created_at\":";
    itoa(b, l.created_at);
    b += ",\"expires_at\":";
    if (l.expires_at) itoa(b, l.expires_at);
    else b += "null";
    b += '}';
}

static bool admin_ok(const std::string& token) {
    const char* want = getenv("ADMIN_TOKEN");
    return want && *want && token == want;
}

// ---------- handlers ----------

static Reply shorten_one(Store& st, const J& p) {
    const J* u = p.find("url");
    if (!u || u->t != J::STR || !is_valid_url(u->str)) return bad("invalid url");
    const J* a = p.find("alias");
    const std::string* alias = nullptr;
    if (a && a->t == J::STR) {
        if (!code_ok(a->str)) return bad("invalid alias");
        alias = &a->str;
    }
    int64_t ttl = link_ttl_ms();
    if (const J* t = p.find("ttl_ms")) {
        if (t->t != J::NUM || t->num <= 0) return bad("invalid ttl_ms");
        ttl = std::min((int64_t)t->num, link_ttl_ms());
    }
    auto code = st.shorten(u->str, alias, ttl);
    if (!code) return mk(409, "{\"error\":\"alias taken\"}");
    std::string b;
    b.reserve(code->size() + 32);
    b += "{\"code\":\"";
    b += *code;
    b += "\",\"short_url\":\"/";
    b += *code;
    b += "\"}";
    return mk(201, std::move(b));
}

/// bulk fast-path: prefix + length + no chars that could break the log line
static bool ok_bulk_url(const std::string& u) {
    if (u.size() <= 7 || u.size() > 2048) return false;
    if (u.rfind("http://", 0) != 0 && u.rfind("https://", 0) != 0) return false;
    for (char c : u)
        if (c == '"' || c == '\\' || c == '\n' || c == '\r') return false;
    return true;
}

static Reply shorten_bulk(Store& st, const J& p) {
    const J* urls = p.find("urls");
    std::string msg = "urls must be 1-" + std::to_string(MAX_BULK_URLS) + " valid http(s) urls";
    if (!urls || urls->t != J::ARR || urls->arr.empty() ||
        urls->arr.size() > MAX_BULK_URLS)
        return bad(msg);
    std::vector<std::string> us;
    us.reserve(urls->arr.size());
    for (auto& j : urls->arr) {
        if (j.t != J::STR || !ok_bulk_url(j.str)) return bad(msg);
        us.push_back(j.str);
    }
    auto codes = st.shorten_many(us, link_ttl_ms());
    std::string b;
    b.reserve(codes.size() * 10 + 24);
    b += "{\"count\":";
    itoa(b, (int64_t)codes.size());
    b += ",\"codes\":[";
    for (size_t i = 0; i < codes.size(); i++) {
        if (i) b += ',';
        b += '"';
        b += codes[i];
        b += '"';
    }
    b += "]}";
    return mk(201, std::move(b));
}

static std::string url_decode(std::string_view s) {
    std::string out;
    out.reserve(s.size());
    for (size_t i = 0; i < s.size(); i++) {
        if (s[i] == '%' && i + 2 < s.size()) {
            auto hv = [](char c) -> int {
                if (c >= '0' && c <= '9') return c - '0';
                if (c >= 'a' && c <= 'f') return c - 'a' + 10;
                if (c >= 'A' && c <= 'F') return c - 'A' + 10;
                return -1;
            };
            int hi = hv(s[i + 1]), lo = hv(s[i + 2]);
            if (hi >= 0 && lo >= 0) {
                out += (char)(hi * 16 + lo);
                i += 2;
                continue;
            }
        }
        out += s[i] == '+' ? ' ' : s[i];
    }
    return out;
}

static std::unordered_map<std::string, std::string> parse_query(const std::string& q) {
    std::unordered_map<std::string, std::string> m;
    size_t i = 0;
    while (i < q.size()) {
        size_t amp = q.find('&', i);
        std::string_view kv = std::string_view(q).substr(
            i, amp == std::string::npos ? std::string::npos : amp - i);
        if (size_t eq = kv.find('='); eq != std::string::npos)
            m[url_decode(kv.substr(0, eq))] = url_decode(kv.substr(eq + 1));
        i = amp == std::string::npos ? q.size() : amp + 1;
    }
    return m;
}

// ---------- handler ----------

Reply handle(Store& st, const std::string& method, const std::string& path,
             const std::string& body, const std::string& admin_token) {
    size_t qi = path.find('?');
    std::string pathname = path.substr(0, qi);
    std::string query = qi == std::string::npos ? "" : path.substr(qi + 1);

    metrics::tick();

    if (method == "OPTIONS") return Reply{204, nullptr, {}, nullptr};

    if (method == "GET") {
        if (pathname == "/api/health") return mk(200, "{\"ok\":true}");
        if (pathname == "/api/metrics") return mk(200, metrics::snapshot());
        if (pathname == "/") {
            const std::string& html = ui_html();
            if (html.empty()) return not_found();
            return Reply{200, nullptr, html, "text/html; charset=utf-8"};
        }
        if (pathname == "/api/links") {
            auto p = parse_query(query);
            int64_t limit = 50;
            if (auto it = p.find("limit"); it != p.end()) {
                int64_t n = strtoll(it->second.c_str(), nullptr, 10);
                if (n != 0) limit = n;
            }
            limit = std::clamp(limit, (int64_t)1, MAX_LIST_LIMIT);
            int64_t offset = 0;
            if (auto it = p.find("offset"); it != p.end()) {
                int64_t n = strtoll(it->second.c_str(), nullptr, 10);
                if (n > 0) offset = n;
            }
            std::string sort = p.count("sort") && p["sort"] == "hits" ? "hits" : "created";
            std::string q = p.count("q") ? p["q"] : "";
            auto [links, total] = st.list((size_t)limit, (size_t)offset, sort, q);
            std::string b;
            b.reserve(links.size() * 96 + 32);
            b += "{\"links\":[";
            for (size_t i = 0; i < links.size(); i++) {
                if (i) b += ',';
                link_json(b, links[i]);
            }
            b += "],\"total\":";
            itoa(b, (int64_t)total);
            b += '}';
            return mk(200, std::move(b));
        }
        if (pathname.rfind("/api/stats/", 0) == 0) {
            auto link = st.stats(pathname.substr(11));
            if (!link) return not_found();
            std::string b;
            link_json(b, *link);
            return mk(200, std::move(b));
        }
        std::string code = pathname.substr(1);
        if (code_ok(code))
            if (auto target = st.resolve(code))
                return Reply{302, target, {}, nullptr};
        return not_found();
    }

    if (method == "POST") {
        if (pathname != "/api/shorten" && pathname != "/api/shorten/bulk")
            return not_found();
        J p;
        if (!jparse(body, p)) return bad("invalid json");
        return pathname == "/api/shorten" ? shorten_one(st, p) : shorten_bulk(st, p);
    }

    if (method == "PATCH" || method == "DELETE") {
        if (pathname.rfind("/api/links/", 0) != 0 || !admin_ok(admin_token))
            return not_found();
        std::string code = pathname.substr(11);
        if (!code_ok(code)) return bad("invalid code");
        if (method == "DELETE") {
            switch (st.remove(code)) {
            case MutResult::Ok: return Reply{204, nullptr, {}, nullptr};
            case MutResult::Missing: return not_found();
            case MutResult::Remote:
                return mk(409, "{\"error\":\"owned by another instance\"}");
            }
        }
        J p;
        if (!jparse(body, p)) return bad("invalid json");
        if (const J* u = p.find("url")) {
            if (u->t != J::STR || !is_valid_url(u->str)) return bad("invalid url");
            int64_t ttl = 0;
            bool has_ttl = false;
            if (const J* t = p.find("ttl_ms")) {
                if (t->t == J::NUM) {
                    ttl = std::min((int64_t)t->num, link_ttl_ms());
                    has_ttl = true;
                }
            }
            switch (st.update(code, u->str, ttl, has_ttl)) {
            case MutResult::Ok: return mk(200, "{\"ok\":true}");
            case MutResult::Missing: return not_found();
            case MutResult::Remote:
                return mk(409, "{\"error\":\"owned by another instance\"}");
            }
        }
        return bad("nothing to update");
    }
    return not_found();
}

} // namespace shrt
