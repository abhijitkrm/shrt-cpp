#include "store.hpp"
#include "metrics.hpp"
#include "store_rocks.hpp"
#include "store_kv.hpp"
#include "base62.hpp"
#include "codec.hpp"
#include "common.hpp"

#include <algorithm>
#include <array>
#include <cstdio>
#include <random>
#include <stdexcept>
#include <sys/stat.h>
#include <unistd.h>

namespace shrt {

static constexpr uint64_t FLUSH_MS = 5;
static constexpr uint64_t FSYNC_MS = 500;
static constexpr int64_t TAIL_MIN_INTERVAL = 200; // ms, rate limit lazy polls
static constexpr size_t FLUSH_BYTES = 256 << 10;
static constexpr int NUM_SHARDS = 256;
static constexpr size_t MAX_STRAY_HITS = 10'000;

static int64_t tail_ms() { return env_i64("TAIL_MS", 0); }

inline int shard_of(std::string_view code) {
    // FNV-1a — cheap and well-spread
    uint32_t h = 2166136261u;
    for (unsigned char b : code) {
        h ^= b;
        h *= 16777619u;
    }
    return (int)(h & (NUM_SHARDS - 1));
}

static int alpha_idx(char c) {
    for (int i = 0; i < 62; i++)
        if (ALPHABET[i] == c) return i;
    return -1;
}

static std::string snap_name(int i) { return "data-" + std::to_string(i) + ".snap"; }

struct Inner {
    std::array<Shard, NUM_SHARDS> shards;
    std::mutex tail_mu;
    Tail tail;
    std::mutex aof_mu;
    std::unique_ptr<Aof> aof;
    int instance = 0;
    char prefix = ALPHABET[0];
    std::string dir, own_name, lock_path;
    std::shared_mutex gate;       // read: mutations; write: compact
    std::atomic<bool> stop{false}, closed{false};
    std::mutex thr_mu;
    std::vector<std::thread> threads;

    void flush_public();
    void flush_locked();
    void poll_tails_pub();
};

/// Fold one parsed log line into the index.
static void apply(Inner& inner, const Op& o, StrayMap& stray) {
    if (!o.x.empty()) {
        auto& sh = inner.shards[shard_of(o.x)];
        std::unique_lock lk(sh.mu);
        sh.data.erase(o.x);
        stray.erase(o.x);
        return;
    }
    if (!o.h.empty()) {
        bool own = inner.instance == (int)o.i;
        auto& sh = inner.shards[shard_of(o.h)];
        {
            std::shared_lock lk(sh.mu);
            auto it = sh.data.find(o.h);
            if (it != sh.data.end()) {
                it->second.h.fetch_add(o.d, std::memory_order_relaxed);
                if (own) it->second.oh.fetch_add(o.d, std::memory_order_relaxed);
                return;
            }
        }
        if (stray.size() >= MAX_STRAY_HITS) stray.erase(stray.begin());
        auto& st = stray[o.h];
        st.t += o.d;
        if (own) st.o += o.d;
        return;
    }
    if (o.c.empty()) return;
    int64_t st_t = 0, st_o = 0;
    if (auto it = stray.find(o.c); it != stray.end()) {
        st_t = it->second.t;
        st_o = it->second.o;
        stray.erase(it);
    }
    auto& sh = inner.shards[shard_of(o.c)];
    std::unique_lock lk(sh.mu);
    sh.data.erase(o.c); // log rows are last-write-wins
    auto [it, _] = sh.data.try_emplace(
        o.c, std::make_shared<const std::string>(o.u), o.a,
        o.has_e ? o.e : 0, (int)o.i);
    it->second.h.store(o.n + st_t, std::memory_order_relaxed);
    it->second.oh.store(o.n + st_o, std::memory_order_relaxed);
}

static void apply_line(Inner& inner, std::string_view line, StrayMap& stray) {
    Op o;
    if (parse_op(line, o)) apply(inner, o, stray);
}

/// Discover new sibling shards and pull newly appended lines into `tail`.
static void poll_locked(Inner& inner, Tail& tail) {
    for (auto& f : shard_files(inner.dir, inner.own_name))
        if (!tail.readers.count(f))
            tail.readers.emplace(f, std::make_unique<TailReader>(inner.dir + "/" + f));
    for (auto& [_, t] : tail.readers)
        t->read_new([&](std::string_view l) { apply_line(inner, l, tail.stray); });
    tail.last_poll = now_ms();
}

Store Store::open_kv(const std::string& addr, int instance,
                     size_t cache_entries, int64_t cache_ttl_ms) {
    Store st;
    st.kvin = KvInner::open(addr, instance, cache_entries, cache_ttl_ms);
    return st;
}

Store Store::open_env(const std::string& dir, int instance) {
    std::string mode = env_str("STORE", "aof");
    if (mode == "rocksdb" || mode == "rocks") {
        std::string path = env_str("ROCKSDB_PATH", "");
        if (path.empty()) path = dir + "/rocks";
        size_t cache = (size_t)env_i64("CACHE", 100000);
        int64_t ttl = env_i64("CACHE_TTL_MS", 5000);
        return open_rocks(path, instance, cache, ttl);
    }
    if (mode == "dragonfly" || mode == "redis" || mode == "kv") {
        std::string addr = env_str("DRAGONFLY_ADDR", "");
        if (addr.empty()) addr = env_str("KV_ADDR", "127.0.0.1:6379");
        size_t cache = (size_t)env_i64("CACHE", 100000);
        int64_t ttl = env_i64("CACHE_TTL_MS", 5000);
        return open_kv(addr, instance, cache, ttl);
    }
    return open(dir, instance);
}

Store Store::open_rocks(const std::string& path, int instance,
                        size_t cache_entries, int64_t cache_ttl_ms) {
    Store st;
    st.rkin = RocksInner::open(path, instance, cache_entries, cache_ttl_ms);
    return st;
}

Store Store::open(const std::string& dir, int instance) {
    auto inner = std::make_shared<Inner>();

    if (dir != ":memory:") {
        int id;
        std::string lock;
        if (instance >= 0) {
            id = instance;
        } else {
            auto [cid, l] = claim_instance(dir);
            id = cid;
            lock = std::move(l);
        }
        if (id < 0 || id >= MAX_INSTANCES)
            throw std::runtime_error("no free instance id");
        inner->instance = id;
        inner->prefix = ALPHABET[id];
        inner->dir = dir;
        inner->own_name = "data-" + std::to_string(id) + ".log";
        inner->lock_path = std::move(lock);
        auto a = std::make_unique<Aof>();
        if (!a->open_at(inner->dir, inner->own_name))
            throw std::runtime_error("open aof failed");
        inner->aof = std::move(a);
    }

    Store st;
    st.in = std::move(inner);
    Inner& in = *st.in;

    if (in.aof) {
        // replay own snapshot + own log + all sibling logs present at boot
        auto& tail = in.tail;
        replay_file(in.dir + "/" + snap_name(in.instance),
                    [&](std::string_view l) { apply_line(in, l, tail.stray); });
        replay_file(in.dir + "/" + in.own_name,
                    [&](std::string_view l) { apply_line(in, l, tail.stray); });
        for (auto& f : shard_files(in.dir, in.own_name)) {
            std::string snap = f.substr(0, f.size() - 4) + ".snap";
            replay_file(in.dir + "/" + snap,
                        [&](std::string_view l) { apply_line(in, l, tail.stray); });
            tail.readers.emplace(f, std::make_unique<TailReader>(in.dir + "/" + f));
        }
    }

    { // flush thread
        std::weak_ptr<Inner> wk = st.in;
        std::lock_guard lk(in.thr_mu);
        in.threads.emplace_back([wk] {
            for (;;) {
                std::this_thread::sleep_for(std::chrono::milliseconds(FLUSH_MS));
                auto p = wk.lock();
                if (!p || p->stop.load(std::memory_order_relaxed)) break;
                p->flush_public();
            }
        });
    }
    if (in.aof) {
        std::weak_ptr<Inner> wk = st.in;
        { // fsync thread (checks stop every 50ms so close() stays snappy)
            std::lock_guard lk(in.thr_mu);
            in.threads.emplace_back([wk] {
                for (;;) {
                    for (int k = 0; k < (int)(FSYNC_MS / 50); k++) {
                        std::this_thread::sleep_for(std::chrono::milliseconds(50));
                        auto p = wk.lock();
                        if (!p || p->stop.load(std::memory_order_relaxed)) return;
                    }
                    auto p = wk.lock();
                    if (!p) return;
                    std::lock_guard lk2(p->aof_mu);
                    if (p->aof) p->aof->sync();
                }
            });
        }
        int64_t tms = tail_ms();
        if (tms > 0) { // periodic tail thread
            std::lock_guard lk(in.thr_mu);
            in.threads.emplace_back([wk, tms] {
                for (;;) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(tms));
                    auto p = wk.lock();
                    if (!p || p->stop.load(std::memory_order_relaxed)) break;
                    p->poll_tails_pub();
                }
            });
        }
    }
    return st;
}

int Store::instance() const { return kvin ? kvin->instance : rkin ? rkin->instance : in->instance; }
bool Store::persistent() const { return kvin || rkin ? true : (bool)in->aof; }

void Store::poll_tails() {
    if (kvin) { kvin->poll_tails(); return; }
    if (rkin) { rkin->poll_tails(); return; }
    if (!in->aof) return;
    std::lock_guard lk(in->tail_mu);
    poll_locked(*in, in->tail);
}

/// Tail the shard that owns code's prefix (generated codes only), then a
/// rate-limited full poll for aliases/strays.
static void tail_missed(Inner& inner, const std::string& code) {
    if (code.empty() || !inner.aof) return;
    std::lock_guard lk(inner.tail_mu);
    Tail& tail = inner.tail;
    int owner = alpha_idx(code[0]);
    if (owner >= 0 && owner != inner.instance) {
        std::string name = "data-" + std::to_string(owner) + ".log";
        auto& t = tail.readers[name];
        if (!t) t = std::make_unique<TailReader>(inner.dir + "/" + name);
        t->read_new([&](std::string_view l) { apply_line(inner, l, tail.stray); });
        tail.last_poll = now_ms();
        return;
    }
    if (now_ms() - tail.last_poll >= TAIL_MIN_INTERVAL)
        poll_locked(inner, tail);
}

// xorshift64* — fast per-thread RNG for code suffixes
static uint64_t rng_next() {
    static thread_local uint64_t st =
        (uint64_t)now_ms() ^ (uint64_t)(uintptr_t)&st ^ 0x9e3779b97f4a7c15ull;
    st ^= st >> 12;
    st ^= st << 25;
    st ^= st >> 27;
    return st * 2685821657736338717ull;
}

static std::string gen_code(const Inner& inner) {
    std::string c;
    c.reserve(CODE_LEN);
    c += inner.prefix;
    for (int k = 0; k < CODE_LEN - 1; k++)
        c += ALPHABET[rng_next() % MAX_INSTANCES];
    return c;
}

std::shared_ptr<std::string> Store::shorten(const std::string& url,
                                            const std::string* alias,
                                            int64_t ttl_ms) {
    if (kvin) {
        auto r = kvin->shorten(url, alias, ttl_ms);
        if (r) metrics::links_delta(1);
        return r;
    }
    if (rkin) {
        auto r = rkin->shorten(url, alias, ttl_ms);
        if (r) metrics::links_delta(1);
        return r;
    }
    Inner& inner = *in;
    std::shared_lock g(inner.gate);
    int64_t now = now_ms();
    int64_t exp = ttl_ms > 0 ? now + ttl_ms : 0;
    auto urlsp = std::make_shared<const std::string>(url);
    std::string code;
    if (alias) {
        code = *alias;
        auto& sh = inner.shards[shard_of(code)];
        std::unique_lock lk(sh.mu);
        if (sh.data.count(code)) return nullptr;
        sh.data.try_emplace(code, urlsp, now, exp, inner.instance);
    } else {
        for (;;) {
            code = gen_code(inner);
            auto& sh = inner.shards[shard_of(code)];
            std::unique_lock lk(sh.mu);
            if (sh.data.count(code)) continue;
            sh.data.try_emplace(code, urlsp, now, exp, inner.instance);
            break;
        }
    }
    if (inner.aof) {
        std::lock_guard lk(inner.aof_mu);
        inner.aof->push(row_line(code, url, now, exp, inner.instance, 0));
        if (inner.aof->pending_bytes() > FLUSH_BYTES) inner.aof->flush();
    }
    metrics::links_delta(1);
    return std::make_shared<std::string>(std::move(code));
}

std::vector<std::string> Store::shorten_many(const std::vector<std::string>& urls,
                                             int64_t ttl_ms) {
    if (kvin) {
        auto r = kvin->shorten_many(urls, ttl_ms);
        metrics::links_delta((int64_t)r.size());
        return r;
    }
    if (rkin) {
        auto r = rkin->shorten_many(urls, ttl_ms);
        metrics::links_delta((int64_t)r.size());
        return r;
    }
    Inner& inner = *in;
    std::shared_lock g(inner.gate);
    int64_t now = now_ms();
    int64_t exp = ttl_ms > 0 ? now + ttl_ms : 0;
    std::vector<std::string> codes;
    codes.reserve(urls.size());
    // all rows encoded into one scratch buffer -> single log write below
    std::string lines;
    lines.reserve(urls.size() * 48);
    for (auto& u : urls) {
        std::string cb = gen_code(inner);
        for (;;) {
            auto& sh = inner.shards[shard_of(cb)];
            std::unique_lock lk(sh.mu);
            if (!sh.data.count(cb)) {
                sh.data.try_emplace(cb, std::make_shared<const std::string>(u),
                                    now, exp, inner.instance);
                break;
            }
            cb = gen_code(inner);
        }
        if (inner.aof) {
            row_line_into(lines, cb, u, now, exp, inner.instance, 0);
            lines += NL;
        }
        codes.push_back(std::move(cb));
    }
    if (inner.aof) {
        std::lock_guard lk(inner.aof_mu);
        inner.aof->push_raw(lines);
        if (inner.aof->pending_bytes() > FLUSH_BYTES) inner.aof->flush();
    }
    metrics::links_delta((int64_t)codes.size());
    return codes;
}

/// Returns the target url, or nullptr for miss/expired. Counts a hit.
std::shared_ptr<const std::string> Store::resolve(const std::string& code) {
    if (kvin) return kvin->resolve(code);
    if (rkin) return rkin->resolve(code);
    Inner& inner = *in;
    auto& sh = inner.shards[shard_of(code)];
    bool found = false;
    std::shared_ptr<const std::string> url;
    {
        std::shared_lock lk(sh.mu);
        auto it = sh.data.find(code);
        if (it != sh.data.end()) {
            Entry& e = it->second;
            if (e.e == 0 || e.e > now_ms()) {
                if (track_hits()) {
                    e.h.fetch_add(1, std::memory_order_relaxed);
                    e.oh.fetch_add(1, std::memory_order_relaxed);
                }
                url = e.u;
                found = true;
            } else {
                return nullptr; // expired
            }
        }
    }
    if (found) {
        if (track_hits()) {
            std::lock_guard lk(sh.dmu);
            sh.dirty[code] += 1;
        }
        return url;
    }
    if (!inner.aof) return nullptr;
    // maybe a sibling wrote it and we haven't tailed yet
    tail_missed(inner, code);
    std::shared_lock lk(sh.mu);
    auto it = sh.data.find(code);
    if (it == sh.data.end()) return nullptr;
    Entry& e = it->second;
    if (e.e != 0 && e.e <= now_ms()) return nullptr;
    if (track_hits()) {
        e.h.fetch_add(1, std::memory_order_relaxed);
        e.oh.fetch_add(1, std::memory_order_relaxed);
        url = e.u;
    } else {
        url = e.u;
    }
    lk.unlock();
    if (track_hits()) {
        std::lock_guard dl(sh.dmu);
        sh.dirty[code] += 1;
    }
    return url;
}

bool Store::healthy() {
    if (kvin) return kvin->healthy();
    if (rkin) return rkin->healthy();
    return true; // the in-process engine is healthy when the process is
}

bool Store::empty() const {
    if (kvin) return kvin->empty();
    if (rkin) return rkin->empty();
    for (auto& sh : in->shards) {
        std::shared_lock lk(sh.mu);
        if (!sh.data.empty()) return false;
    }
    return true;
}

MutResult Store::update(const std::string& code, const std::string& url,
                        int64_t ttl_ms, bool has_ttl) {
    if (kvin) return kvin->update(code, url, ttl_ms, has_ttl);
    if (rkin) return rkin->update(code, url, ttl_ms, has_ttl);
    Inner& inner = *in;
    std::shared_lock g(inner.gate);
    auto& sh = inner.shards[shard_of(code)];
    int64_t a, exp;
    {
        std::unique_lock lk(sh.mu);
        auto it = sh.data.find(code);
        if (it == sh.data.end()) return MutResult::Missing;
        if (it->second.i != inner.instance) return MutResult::Remote;
        exp = has_ttl ? (ttl_ms > 0 ? now_ms() + ttl_ms : 0) : it->second.e;
        it->second.u = std::make_shared<const std::string>(url);
        it->second.e = exp;
        a = it->second.a;
    }
    if (inner.aof) {
        std::lock_guard lk(inner.aof_mu);
        inner.aof->push(row_line(code, url, a, exp, inner.instance, 0));
    }
    return MutResult::Ok;
}

MutResult Store::remove(const std::string& code) {
    if (kvin) {
        auto r = kvin->remove(code);
        if (r == MutResult::Ok) metrics::links_delta(-1);
        return r;
    }
    if (rkin) {
        auto r = rkin->remove(code);
        if (r == MutResult::Ok) metrics::links_delta(-1);
        return r;
    }
    Inner& inner = *in;
    std::shared_lock g(inner.gate);
    auto& sh = inner.shards[shard_of(code)];
    {
        std::unique_lock lk(sh.mu);
        auto it = sh.data.find(code);
        if (it == sh.data.end()) return MutResult::Missing;
        if (it->second.i != inner.instance) return MutResult::Remote;
        sh.data.erase(it);
    }
    if (inner.aof) {
        std::lock_guard lk(inner.aof_mu);
        inner.aof->push(del_line(code));
    }
    metrics::links_delta(-1);
    return MutResult::Ok;
}

/// O(n) scan for UI listing — admin path, not the hot path.
std::pair<std::vector<Link>, size_t> Store::list(size_t limit, size_t offset,
                                               const std::string& sort,
                                               const std::string& q) {
    if (kvin) return kvin->list(limit, offset, sort, q);
    if (rkin) return rkin->list(limit, offset, sort, q);
    std::vector<Link> items;
    for (auto& sh : in->shards) {
        std::shared_lock lk(sh.mu);
        for (auto& [code, e] : sh.data) {
            if (!q.empty() && code.find(q) == std::string::npos &&
                e.u->find(q) == std::string::npos)
                continue;
            items.push_back(Link{code, *e.u,
                                 e.h.load(std::memory_order_relaxed), e.a, e.e});
        }
    }
    if (sort == "hits")
        std::sort(items.begin(), items.end(),
                  [](const Link& x, const Link& y) { return x.hits > y.hits; });
    else
        std::sort(items.begin(), items.end(),
                  [](const Link& x, const Link& y) { return x.created_at > y.created_at; });
    size_t total = items.size();
    if (offset >= total) return {{}, total};
    size_t end = std::min(offset + limit, total);
    return {std::vector<Link>(items.begin() + offset, items.begin() + end), total};
}

std::unique_ptr<Link> Store::stats(const std::string& code) {
    if (kvin) return kvin->stats(code);
    if (rkin) return rkin->stats(code);
    auto& sh = in->shards[shard_of(code)];
    std::shared_lock lk(sh.mu);
    auto it = sh.data.find(code);
    if (it == sh.data.end()) return nullptr;
    Entry& e = it->second;
    return std::make_unique<Link>(Link{code, *e.u,
                                       e.h.load(std::memory_order_relaxed), e.a, e.e});
}

size_t Store::seed(const std::vector<std::string>& urls) {
    if (kvin) return kvin->seed(urls);
    if (rkin) return rkin->seed(urls);
    shorten_many(urls, 0);
    flush();
    return urls.size();
}

void Store::flush() {
    if (kvin) { kvin->flush(); return; }
    if (rkin) { rkin->flush(); return; }
    in->flush_public();
}

void Inner::flush_public() {
    std::shared_lock g(gate);
    flush_locked();
}

/// Flush for callers already holding the gate.
void Inner::flush_locked() {
    if (!aof) return;
    std::string lines;
    lines.reserve(256);
    for (auto& sh : shards) {
        std::lock_guard lk(sh.dmu);
        if (sh.dirty.empty()) continue;
        for (auto& [code, d] : sh.dirty) {
            hit_line_into(lines, code, d, instance);
            lines += NL;
        }
        sh.dirty.clear();
    }
    std::lock_guard lk(aof_mu);
    aof->push_raw(lines);
    aof->flush();
}

void Inner::poll_tails_pub() {
    if (!aof) return;
    std::lock_guard lk(tail_mu);
    poll_locked(*this, tail);
}

/// Rewrite own rows as a compact snapshot, then truncate own log.
void Store::compact() {
    if (kvin) { kvin->compact(); return; }
    if (rkin) { rkin->compact(); return; }
    Inner& inner = *in;
    if (!inner.aof) return;
    std::unique_lock g(inner.gate);
    inner.flush_locked();
    std::string snap_path = inner.dir + "/" + snap_name(inner.instance);
    std::string tmp = snap_path + ".tmp";
    if (FILE* f = fopen(tmp.c_str(), "w")) {
        for (auto& sh : inner.shards) {
            std::shared_lock lk(sh.mu);
            for (auto& [code, e] : sh.data) {
                if (e.i == inner.instance) {
                    std::string l = row_line(code, *e.u, e.a, e.e, e.i,
                                             e.oh.load(std::memory_order_relaxed));
                    fwrite(l.data(), 1, l.size(), f);
                    fputc('\n', f);
                }
            }
        }
        fflush(f);
        fsync(fileno(f));
        fclose(f);
        rename(tmp.c_str(), snap_path.c_str());
    }
    std::lock_guard lk(inner.aof_mu);
    inner.aof->truncate();
}

/// Stop timers, flush, fsync, and release the instance lock.
void Store::close() {
    if (kvin) { kvin.reset(); return; }
    if (rkin) { rkin.reset(); return; }
    if (!in) return;
    Inner& inner = *in;
    if (inner.closed.exchange(true, std::memory_order_relaxed)) return;
    inner.stop.store(true, std::memory_order_relaxed);
    {
        std::lock_guard lk(inner.thr_mu);
        for (auto& t : inner.threads)
            if (t.joinable()) t.join();
        inner.threads.clear();
    }
    inner.flush_public();
    if (inner.aof) {
        std::lock_guard lk(inner.aof_mu);
        inner.aof->close();
    }
    if (!inner.lock_path.empty()) ::unlink(inner.lock_path.c_str());
}

} // namespace shrt
