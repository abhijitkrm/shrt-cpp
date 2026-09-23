// store_kv.cpp — external-KV backend (DragonflyDB / Redis / any RESP
// server). The corpus lives in the KV store; this process keeps only a
// bounded hot FIFO cache + batched hit counters — memory stays flat as
// links grow.
//
// Keys:  l:{code} -> "{expires_ms}|{created_ms}|{url}"  (PX self-evicts)
//        h:{code} -> hit counter (INCRBY, flushed in 5ms batches)
//
// Multi-instance: the KV IS the shared state — no tailing, no convergence,
// admin mutations work on any node.

#include "store_kv.hpp"
#include "kv.hpp"
#include "common.hpp"
#include "base62.hpp"
#include <algorithm>
#include <atomic>
#include <deque>
#include <random>
#include <thread>
#include <unordered_map>

namespace shrt {

static constexpr int KV_SHARDS = 256;

struct KvEntry {
    std::shared_ptr<const std::string> u;
    int64_t e = 0;  // expires_at ms (0 = never)
    int64_t at = 0; // cached_at ms (staleness bound)
};

struct KvShard {
    std::mutex mu;
    std::unordered_map<std::string, KvEntry> m;
    std::deque<std::string> order; // FIFO
};

struct KvInnerImpl {
    Kv kv;
    std::array<KvShard, KV_SHARDS> cache;
    size_t cap_per_shard;
    int64_t cache_ttl_ms;
    std::array<std::mutex, KV_SHARDS> dirty_mu;
    std::array<std::unordered_map<std::string, int64_t>, KV_SHARDS> dirty;
    int instance = 0;
    char prefix = '0';
    bool layout_hash = false;   // KV_LAYOUT=hash: fields in l:{shard%buckets} hashes
    uint32_t buckets = 1000000; // KV_BUCKETS — keep fields/bucket < hash-max-listpack-entries
    std::atomic<bool> stop{false};
    std::thread flusher;
    std::thread janitor;
    std::mt19937_64 rng{std::random_device{}()};

    KvInnerImpl(const std::string& addr, int inst, size_t cache_entries, int64_t cttl)
        : kv(addr, 16),
          cap_per_shard(std::max<size_t>(16, cache_entries / KV_SHARDS)),
          cache_ttl_ms(cttl),
          instance(std::clamp(inst, 0, 61)) {
        prefix = ALPHABET[instance];
        if (const char* v = std::getenv("KV_LAYOUT"))
            layout_hash = std::string(v) == "hash";
        if (const char* v = std::getenv("KV_BUCKETS"))
            buckets = (uint32_t)std::max<uint64_t>(1, std::stoull(v));
        flusher = std::thread([this] {
            while (!stop.load(std::memory_order_relaxed)) {
                std::this_thread::sleep_for(std::chrono::milliseconds(5));
                try { flush_hits(); } catch (...) {}
            }
        });
        if (layout_hash) {
            uint64_t sweep_ms = 3600000;
            if (const char* v = std::getenv("KV_SWEEP_MS"))
                sweep_ms = std::max<uint64_t>(50, std::stoull(v));
            janitor = std::thread([this, sweep_ms] {
                // slice-sleep so shutdown doesn't block on a full interval
                uint64_t waited = 0;
                while (!stop.load(std::memory_order_relaxed)) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(50));
                    waited += 50;
                    if (waited >= sweep_ms) {
                        waited = 0;
                        try { sweep_expired(); } catch (...) {}
                    }
                }
            });
        }
    }
    ~KvInnerImpl() {
        stop.store(true);
        if (flusher.joinable()) flusher.join();
        if (janitor.joinable()) janitor.join();
        try { flush_hits(); } catch (...) {}
    }

    static int shard(std::string_view code) {
        uint32_t h = 2166136261u;
        for (unsigned char b : code) { h ^= b; h *= 16777619u; }
        return (int)(h & (KV_SHARDS - 1));
    }

    static std::string lkey(const std::string& c) { return "l:" + c; }
    static std::string hkey(const std::string& c) { return "h:" + c; }
    // hash mode: bucket = l:{shard(code) % buckets}, hit field = "h:"+code
    std::string bkey(const std::string& c) const {
        return "l:" + std::to_string((uint32_t)shard(c) % buckets);
    }
    static std::string hfield(const std::string& c) { return "h:" + c; }

    // "{e}|{c}|{u}" — legacy "{e}|{u}" decodes with c=0
    static std::string enc(int64_t e, int64_t c, const std::string& u) {
        return std::to_string(e) + "|" + std::to_string(c) + "|" + u;
    }
    static bool dec(const std::string& v, int64_t& e, int64_t& c, std::string_view& u) {
        size_t p = v.find('|');
        if (p == std::string::npos) return false;
        try { e = std::stoll(v.substr(0, p)); } catch (...) { return false; }
        std::string_view rest = std::string_view(v).substr(p + 1);
        size_t q = rest.find('|');
        if (q != std::string_view::npos) {
            try { c = std::stoll(std::string(rest.substr(0, q))); }
            catch (...) { return false; }
            u = rest.substr(q + 1);
        } else {
            c = 0;
            u = rest;
        }
        return true;
    }

    void flush_hits() {
        if (layout_hash) {
            std::vector<std::tuple<std::string, std::string, int64_t>> deltas;
            for (int i = 0; i < KV_SHARDS; i++) {
                std::lock_guard<std::mutex> g(dirty_mu[i]);
                for (auto& [code, n] : dirty[i])
                    deltas.emplace_back(bkey(code), hfield(code), n);
                dirty[i].clear();
            }
            kv.hincrby_many(deltas);
        } else {
            std::vector<std::pair<std::string, int64_t>> deltas;
            for (int i = 0; i < KV_SHARDS; i++) {
                std::lock_guard<std::mutex> g(dirty_mu[i]);
                for (auto& [code, n] : dirty[i])
                    deltas.emplace_back("h:" + code, n);
                dirty[i].clear();
            }
            kv.incrby_many(deltas);
        }
    }

    // hash-mode janitor: HSCAN each l:* bucket, HDEL expired fields
    void sweep_expired() {
        std::vector<std::string> buckets_list;
        kv.scan_each("l:*", [&](std::string k) { buckets_list.push_back(std::move(k)); });
        int64_t now = now_ms();
        std::vector<std::vector<std::string>> dels;
        for (auto& b : buckets_list) {
            std::vector<std::string> dead;
            kv.hscan_each(b, [&](std::string f, std::string v) {
                if (f.rfind("h:", 0) == 0) return;
                int64_t e, c;
                std::string_view u;
                if (dec(v, e, c, u) && e != 0 && e <= now) dead.push_back(f);
            });
            for (auto& f : dead) dels.push_back({"HDEL", b, f});
        }
        if (!dels.empty()) kv.pipe(dels);
    }

    std::optional<std::string> kv_get(const std::string& code) {
        if (layout_hash) return kv.hget(bkey(code), code);
        return kv.get(lkey(code));
    }

    void bump(const std::string& code) {
        if (!track_hits()) return;
        int i = shard(code);
        std::lock_guard<std::mutex> g(dirty_mu[i]);
        dirty[i][code]++;
    }

    bool cache_get(const std::string& code, std::shared_ptr<const std::string>& u) {
        KvShard& sh = cache[shard(code)];
        std::lock_guard<std::mutex> g(sh.mu);
        auto it = sh.m.find(code);
        if (it == sh.m.end()) return false;
        int64_t now = now_ms();
        if (cache_ttl_ms > 0 && now - it->second.at > cache_ttl_ms) {
            sh.m.erase(it);
            return false;
        }
        if (it->second.e != 0 && it->second.e <= now) return false;
        u = it->second.u;
        return true;
    }
    void cache_put(const std::string& code, std::shared_ptr<const std::string> u, int64_t e) {
        KvShard& sh = cache[shard(code)];
        std::lock_guard<std::mutex> g(sh.mu);
        auto it = sh.m.find(code);
        if (it != sh.m.end()) {
            it->second = {u, e, now_ms()};
            return;
        }
        while (sh.m.size() >= cap_per_shard && !sh.order.empty()) {
            sh.m.erase(sh.order.front());
            sh.order.pop_front();
        }
        sh.order.push_back(code);
        sh.m.emplace(code, KvEntry{u, e, now_ms()});
    }
    void cache_del(const std::string& code) {
        KvShard& sh = cache[shard(code)];
        std::lock_guard<std::mutex> g(sh.mu);
        sh.m.erase(code);
    }

    std::string gen_code() {
        std::string c(8, '0');
        c[0] = prefix;
        std::uniform_int_distribution<int> d(0, 61);
        for (int i = 1; i < 8; i++) c[i] = ALPHABET[d(rng)];
        return c;
    }

    // ---- API impl ----

    std::shared_ptr<const std::string> resolve(const std::string& code) {
        std::shared_ptr<const std::string> u;
        if (cache_get(code, u)) {
            bump(code);
            return u;
        }
        auto v = kv_get(code);
        if (!v) return nullptr;
        int64_t e, c;
        std::string_view us;
        if (!dec(*v, e, c, us) || (e != 0 && e <= now_ms())) return nullptr;
        auto ua = std::make_shared<const std::string>(us);
        cache_put(code, ua, e);
        bump(code);
        return ua;
    }

    std::shared_ptr<std::string> shorten(const std::string& url,
                                         const std::string* alias, int64_t ttl_ms) {
        int64_t now = now_ms();
        int64_t exp = ttl_ms > 0 ? now + ttl_ms : 0;
        if (alias) {
            bool ok = layout_hash
                ? kv.hsetnx(bkey(*alias), *alias, enc(exp, now, url))
                : kv.set(lkey(*alias), enc(exp, now, url), ttl_ms, true);
            if (ok) return std::make_shared<std::string>(*alias);
            return nullptr;
        }
        for (;;) {
            std::string c = gen_code();
            try {
                bool ok = layout_hash
                    ? kv.hsetnx(bkey(c), c, enc(exp, now, url))
                    : kv.set(lkey(c), enc(exp, now, url), ttl_ms, true);
                if (ok) return std::make_shared<std::string>(std::move(c));
            } catch (...) {
                return nullptr;
            }
        }
    }

    std::vector<std::string> shorten_many(const std::vector<std::string>& urls,
                                          int64_t ttl_ms) {
        int64_t now = now_ms();
        int64_t exp = ttl_ms > 0 ? now + ttl_ms : 0;
        std::vector<std::string> codes(urls.size());
        std::vector<std::vector<std::string>> cmds;
        cmds.reserve(urls.size());
        for (size_t i = 0; i < urls.size(); i++) {
            codes[i] = gen_code();
            if (layout_hash) {
                cmds.push_back({"HSETNX", bkey(codes[i]), codes[i], enc(exp, now, urls[i])});
            } else {
                std::vector<std::string> args{"SET", lkey(codes[i]), enc(exp, now, urls[i])};
                if (ttl_ms > 0) { args.emplace_back("PX"); args.push_back(std::to_string(ttl_ms)); }
                args.emplace_back("NX");
                cmds.push_back(std::move(args));
            }
        }
        std::vector<Resp> rs;
        try { rs = kv.pipe(cmds); }
        catch (...) { rs.clear(); }
        for (size_t i = 0; i < urls.size(); i++) {
            bool ok = layout_hash
                ? (i < rs.size() && rs[i].kind == ':' && rs[i].num == 1)
                : (i < rs.size() && rs[i].kind == '+' && rs[i].str == "OK");
            if (!ok) {
                auto c2 = shorten(urls[i], nullptr, ttl_ms);
                if (c2) codes[i] = *c2;
            }
        }
        return codes;
    }

    MutResult update(const std::string& code, const std::string& url,
                     int64_t ttl_ms, bool has_ttl) {
        auto v = kv_get(code);
        if (!v) return MutResult::Missing;
        int64_t e, c;
        std::string_view us;
        if (!dec(*v, e, c, us)) return MutResult::Missing;
        int64_t exp = has_ttl ? (ttl_ms > 0 ? now_ms() + ttl_ms : 0) : e;
        bool ok = layout_hash
            ? kv.hset(bkey(code), code, enc(exp, c, url))
            : kv.set(lkey(code), enc(exp, c, url),
                     exp > 0 ? exp - now_ms() : 0, false);
        if (!ok) return MutResult::Missing;
        cache_del(code);
        return MutResult::Ok;
    }

    MutResult remove(const std::string& code) {
        if (layout_hash) {
            std::string b = bkey(code);
            if (kv.hdel(b, code) <= 0) return MutResult::Missing;
            kv.hdel(b, hfield(code));
        } else {
            if (kv.del(lkey(code)) <= 0) return MutResult::Missing;
            kv.del(hkey(code));
        }
        cache_del(code);
        return MutResult::Ok;
    }

    std::pair<std::vector<Link>, size_t> list(size_t limit, size_t offset,
                                              const std::string& sort,
                                              const std::string& q) {
        std::vector<Link> items;
        int64_t now = now_ms();
        if (layout_hash) {
            std::vector<std::string> buckets_list;
            kv.scan_each("l:*", [&](std::string k) { buckets_list.push_back(std::move(k)); });
            std::unordered_map<std::string, int64_t> hits;
            for (auto& b : buckets_list) {
                kv.hscan_each(b, [&](std::string f, std::string v) {
                    if (f.rfind("h:", 0) == 0) {
                        try { hits[f.substr(2)] = std::stoll(v); } catch (...) {}
                        return;
                    }
                    int64_t e, c;
                    std::string_view u;
                    if (!dec(v, e, c, u)) return;
                    if (e != 0 && e <= now) return;
                    if (!q.empty() && f.find(q) == std::string::npos &&
                        std::string(u).find(q) == std::string::npos)
                        return;
                    Link l;
                    l.code = f;
                    l.url = std::string(u);
                    l.hits = 0;
                    l.created_at = c;
                    l.expires_at = e;
                    items.push_back(std::move(l));
                });
            }
            for (auto& l : items) {
                auto it = hits.find(l.code);
                if (it != hits.end()) l.hits = it->second;
            }
        } else {
        std::vector<std::string> keys;
        kv.scan_each("l:*", [&](std::string k) { keys.push_back(std::move(k)); });
        std::vector<std::vector<std::string>> cmds;
        cmds.reserve(keys.size() * 2);
        for (auto& k : keys) {
            cmds.push_back({"GET", k});
            cmds.push_back({"GET", hkey(k.substr(2))});
        }
        std::vector<Resp> rs;
        try { rs = kv.pipe(cmds); } catch (...) { rs.clear(); }
        for (size_t i = 0; i < keys.size(); i++) {
            std::string code = keys[i].substr(2);
            if (2 * i >= rs.size() || rs[2 * i].null()) continue;
            int64_t e, c;
            std::string_view u;
            if (!dec(rs[2 * i].str, e, c, u)) continue;
            if (e != 0 && e <= now) continue;
            std::string us(u);
            if (!q.empty() && code.find(q) == std::string::npos &&
                us.find(q) == std::string::npos)
                continue;
            int64_t hits = 0;
            if (2 * i + 1 < rs.size() && !rs[2 * i + 1].null()) {
                try { hits = std::stoll(rs[2 * i + 1].str); } catch (...) {}
            }
            Link l;
            l.code = code;
            l.url = std::move(us);
            l.hits = hits;
            l.created_at = c;
            l.expires_at = e;
            items.push_back(std::move(l));
        }
        }
        if (sort == "hits")
            std::sort(items.begin(), items.end(),
                      [](const Link& a, const Link& b) { return a.hits > b.hits; });
        size_t total = items.size();
        if (offset > total) offset = total;
        size_t end = std::min(total, offset + limit);
        return {std::vector<Link>(items.begin() + offset, items.begin() + end), total};
    }

    std::unique_ptr<Link> stats(const std::string& code) {
        std::optional<std::string> hv;
        auto v = kv_get(code);
        if (layout_hash)
            hv = kv.hget(bkey(code), hfield(code));
        else
            hv = kv.get(hkey(code));
        if (!v) return nullptr;
        int64_t e, c;
        std::string_view u;
        if (!dec(*v, e, c, u)) return nullptr;
        auto l = std::make_unique<Link>();
        l->code = code;
        l->url = std::string(u);
        l->created_at = c;
        l->expires_at = e;
        if (hv) {
            try { l->hits = std::stoll(*hv); } catch (...) {}
        }
        int i = shard(code);
        {
            std::lock_guard<std::mutex> g(dirty_mu[i]);
            auto it = dirty[i].find(code);
            if (it != dirty[i].end()) l->hits += it->second;
        }
        return l;
    }

    size_t seed(const std::vector<std::string>& urls) {
        shorten_many(urls, 0);
        flush_hits();
        return urls.size();
    }

    bool empty() {
        bool any = false;
        kv.scan_each("l:*", [&](std::string) { any = true; });
        return !any;
    }

    void flush() { flush_hits(); }
    void poll_tails() {}
    void compact() {}

};

// ---- pimpl wrapper ----

KvInner::KvInner(const std::string& addr, int inst, size_t cache_entries,
                 int64_t cache_ttl_ms)
    : p(std::make_unique<KvInnerImpl>(addr, inst, cache_entries, cache_ttl_ms)),
      instance(p->instance) {}

KvInner::~KvInner() = default;

std::shared_ptr<KvInner> KvInner::open(const std::string& addr, int inst,
                                       size_t cache_entries, int64_t cache_ttl_ms) {
    return std::make_shared<KvInner>(addr, inst, cache_entries, cache_ttl_ms);
}

std::shared_ptr<const std::string> KvInner::resolve(const std::string& code) {
    return p->resolve(code);
}
std::shared_ptr<std::string> KvInner::shorten(const std::string& url,
                                              const std::string* alias, int64_t ttl_ms) {
    return p->shorten(url, alias, ttl_ms);
}
std::vector<std::string> KvInner::shorten_many(const std::vector<std::string>& urls,
                                               int64_t ttl_ms) {
    return p->shorten_many(urls, ttl_ms);
}
MutResult KvInner::update(const std::string& code, const std::string& url,
                          int64_t ttl_ms, bool has_ttl) {
    return p->update(code, url, ttl_ms, has_ttl);
}
MutResult KvInner::remove(const std::string& code) { return p->remove(code); }
std::pair<std::vector<Link>, size_t> KvInner::list(size_t limit, size_t offset,
                                                 const std::string& sort,
                                                 const std::string& q) {
    return p->list(limit, offset, sort, q);
}
std::unique_ptr<Link> KvInner::stats(const std::string& code) {
    return p->stats(code);
}
size_t KvInner::seed(const std::vector<std::string>& urls) { return p->seed(urls); }
bool KvInner::empty() { return p->empty(); }
void KvInner::flush() { p->flush(); }
void KvInner::poll_tails() {}
void KvInner::compact() {}

} // namespace shrt
