// store_rocks.cpp — embedded RocksDB backend (STORE=rocksdb).
//
// Schema: two column families — "links" (code -> {exp}|{created}|{url})
// and "hits" (code -> u64 via the UInt64Add merge operator, so counters
// accumulate without read-modify-write). Expiry is embedded in the value
// and enforced on read; a CompactionFilter drops expired keys during
// compaction, and compact() triggers a full CompactRange to reclaim
// space on the sweep cadence. Reads go through a bounded hot FIFO
// (same shape as the KV backend) so the DB only sees cache misses.
//
// Embedded means single-writer: RocksDB holds an exclusive LOCK on the
// DB dir, so exactly one process may open a given ROCKSDB_PATH.
#include "store_rocks.hpp"
#include "common.hpp"
#include "base62.hpp"

#include <rocksdb/db.h>
#include <rocksdb/merge_operator.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/table.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/compaction_filter.h>

#include <algorithm>
#include <array>
#include <filesystem>
#include <atomic>
#include <deque>
#include <random>
#include <thread>
#include <unordered_map>

namespace shrt {

static constexpr int KV_SHARDS = 256;

// ---- value codec: "{e}|{c}|{u}" (legacy "{e}|{u}" decodes with c=0) ----
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

// hit counters accumulate via merge: operands are little-endian u64 deltas
class U64AddMerge : public rocksdb::AssociativeMergeOperator {
public:
    bool Merge(const rocksdb::Slice& /*key*/, const rocksdb::Slice* existing,
               const rocksdb::Slice& value, std::string* new_value,
               rocksdb::Logger* /*logger*/) const override {
        uint64_t base = 0, delta = 0;
        if (existing && existing->size() >= 8) memcpy(&base, existing->data(), 8);
        if (value.size() >= 8) memcpy(&delta, value.data(), 8);
        base += delta;
        char buf[8];
        memcpy(buf, &base, 8);
        new_value->assign(buf, 8);
        return true;
    }
    const char* Name() const override { return "u64add"; }
};

// drops expired links during compaction — free TTL reclamation, no janitor
class ExpiryFilter : public rocksdb::CompactionFilter {
public:
    bool Filter(int /*level*/, const rocksdb::Slice& /*key*/,
                const rocksdb::Slice& value, std::string* /*new_value*/,
                bool* /*value_changed*/) const override {
        int64_t e, c;
        std::string_view u;
        std::string v = value.ToString();
        return dec(v, e, c, u) && e != 0 && e <= now_ms();
    }
    const char* Name() const override { return "shrt-expiry"; }
};

struct KvEntry {
    std::shared_ptr<const std::string> u;
    int64_t e = 0;
    int64_t at = 0;
};
struct KvShard {
    std::mutex mu;
    std::unordered_map<std::string, KvEntry> m;
    std::deque<std::string> order;
};

struct RocksInnerImpl {
    std::unique_ptr<rocksdb::DB> db;
    rocksdb::ColumnFamilyHandle* links = nullptr;
    rocksdb::ColumnFamilyHandle* hits = nullptr;
    std::array<KvShard, KV_SHARDS> cache;
    size_t cap_per_shard;
    int64_t cache_ttl_ms;
    std::array<std::mutex, KV_SHARDS> dirty_mu;
    std::array<std::unordered_map<std::string, int64_t>, KV_SHARDS> dirty;
    int instance = 0;
    char prefix = '0';
    std::atomic<bool> stop{false};
    std::thread flusher;
    std::mt19937_64 rng{std::random_device{}()};

    RocksInnerImpl(const std::string& path, int inst, size_t cache_entries,
                   int64_t cttl)
        : cap_per_shard(std::max<size_t>(16, cache_entries / KV_SHARDS)),
          cache_ttl_ms(cttl), instance(std::clamp(inst, 0, 61)) {
        prefix = ALPHABET[instance];

        std::error_code ec;
        std::filesystem::create_directories(path, ec); // rocksdb needs the dir chain

        rocksdb::BlockBasedTableOptions tbl;
        tbl.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10));
        tbl.block_cache = rocksdb::NewLRUCache(64 << 20);

        rocksdb::Options opt;
        opt.create_if_missing = true;
        opt.create_missing_column_families = true;
        opt.compression = rocksdb::kLZ4Compression;
        opt.table_factory.reset(rocksdb::NewBlockBasedTableFactory(tbl));

        rocksdb::ColumnFamilyOptions links_opt(opt);
        static ExpiryFilter expiry_filter; // stateless — static storage outlives the DB
        links_opt.compaction_filter = &expiry_filter;
        rocksdb::ColumnFamilyOptions hits_opt(opt);
        hits_opt.merge_operator = std::make_shared<U64AddMerge>();

        // reopen with existing CF list if the DB already exists
        std::vector<std::string> cfs;
        rocksdb::Status s = rocksdb::DB::ListColumnFamilies(opt, path, &cfs);
        if (!s.ok()) cfs = {rocksdb::kDefaultColumnFamilyName, "links", "hits"};

        std::vector<rocksdb::ColumnFamilyDescriptor> desc;
        for (auto& n : cfs) {
            if (n == "hits") desc.emplace_back(n, hits_opt);
            else desc.emplace_back(n, links_opt);
        }
        std::unique_ptr<rocksdb::DB> raw;
        std::vector<rocksdb::ColumnFamilyHandle*> hs;
        s = rocksdb::DB::Open(rocksdb::DBOptions(opt), path, desc, &hs, &raw);
        if (!s.ok()) throw std::runtime_error("rocksdb open: " + s.ToString());
        db = std::move(raw);
        for (auto* h : hs) {
            std::string n = h->GetName();
            if (n == "hits") hits = h;
            else if (n == "links") links = h;
            else db->DestroyColumnFamilyHandle(h);
        }
        if (!links || !hits) throw std::runtime_error("rocksdb: missing CFs");

        flusher = std::thread([this] {
            while (!stop.load()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(5));
                try { flush_hits(); } catch (...) {}
            }
        });
    }
    ~RocksInnerImpl() {
        stop.store(true);
        if (flusher.joinable()) flusher.join();
        try { flush_hits(); } catch (...) {}
        if (db) {
            if (links) db->DestroyColumnFamilyHandle(links);
            if (hits) db->DestroyColumnFamilyHandle(hits);
            db->Close();
        }
    }

    static int shard(std::string_view code) {
        uint32_t h = 2166136261u;
        for (unsigned char b : code) { h ^= b; h *= 16777619u; }
        return (int)(h & (KV_SHARDS - 1));
    }

    void flush_hits() {
        rocksdb::WriteBatch wb;
        bool any = false;
        for (int i = 0; i < KV_SHARDS; i++) {
            std::lock_guard<std::mutex> g(dirty_mu[i]);
            for (auto& [code, n] : dirty[i]) {
                std::string d(8, '\0');
                uint64_t u = (uint64_t)n;
                memcpy(d.data(), &u, 8);
                wb.Merge(hits, code, d);
                any = true;
            }
            dirty[i].clear();
        }
        if (any) db->Write(rocksdb::WriteOptions(), &wb);
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
        if (it != sh.m.end()) { it->second = {u, e, now_ms()}; return; }
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

    bool db_get(const std::string& code, std::string& v) {
        return db->Get(rocksdb::ReadOptions(), links, code, &v).ok();
    }

    int64_t db_hits(const std::string& code) {
        std::string v;
        if (!db->Get(rocksdb::ReadOptions(), hits, code, &v).ok() || v.size() < 8)
            return 0;
        uint64_t u;
        memcpy(&u, v.data(), 8);
        return (int64_t)u;
    }

    // ---- API impl ----

    std::shared_ptr<const std::string> resolve(const std::string& code) {
        std::shared_ptr<const std::string> u;
        if (cache_get(code, u)) {
            bump(code);
            return u;
        }
        std::string v;
        if (!db_get(code, v)) return nullptr;
        int64_t e, c;
        std::string_view us;
        if (!dec(v, e, c, us) || (e != 0 && e <= now_ms())) return nullptr;
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
            std::string v;
            if (db_get(*alias, v)) return nullptr;
            rocksdb::Status s = db->Put(rocksdb::WriteOptions(), links, *alias,
                                        enc(exp, now, url));
            if (!s.ok()) return nullptr;
            cache_put(*alias, std::make_shared<const std::string>(url), exp);
            return std::make_shared<std::string>(*alias);
        }
        for (;;) {
            std::string c = gen_code();
            std::string v;
            if (db_get(c, v)) continue;
            rocksdb::Status s = db->Put(rocksdb::WriteOptions(), links, c,
                                        enc(exp, now, url));
            if (!s.ok()) return nullptr;
            cache_put(c, std::make_shared<const std::string>(url), exp);
            return std::make_shared<std::string>(c);
        }
    }

    std::vector<std::string> shorten_many(const std::vector<std::string>& urls,
                                          int64_t ttl_ms) {
        int64_t now = now_ms();
        int64_t exp = ttl_ms > 0 ? now + ttl_ms : 0;
        std::vector<std::string> codes(urls.size());
        rocksdb::WriteBatch wb;
        std::vector<size_t> pending;
        for (size_t i = 0; i < urls.size(); i++) {
            std::string c = gen_code();
            std::string v;
            if (db_get(c, v)) { pending.push_back(i); continue; }
            codes[i] = c;
            wb.Put(links, c, enc(exp, now, urls[i]));
        }
        rocksdb::Status s = db->Write(rocksdb::WriteOptions(), &wb);
        for (size_t i = 0; i < urls.size(); i++) {
            if (codes[i].empty() || !s.ok()) {
                auto c2 = shorten(urls[i], nullptr, ttl_ms);
                if (c2) codes[i] = *c2;
            } else {
                cache_put(codes[i],
                          std::make_shared<const std::string>(urls[i]), exp);
            }
        }
        return codes;
    }

    MutResult update(const std::string& code, const std::string& url,
                     int64_t ttl_ms, bool has_ttl) {
        std::string v;
        if (!db_get(code, v)) return MutResult::Missing;
        int64_t e, c;
        std::string_view us;
        if (!dec(v, e, c, us)) return MutResult::Missing;
        int64_t exp = !has_ttl ? e : ttl_ms > 0 ? now_ms() + ttl_ms : 0;
        rocksdb::Status s = db->Put(rocksdb::WriteOptions(), links, code,
                                    enc(exp, c, url));
        if (!s.ok()) return MutResult::Missing;
        cache_del(code);
        return MutResult::Ok;
    }

    MutResult remove(const std::string& code) {
        std::string v;
        if (!db_get(code, v)) return MutResult::Missing;
        rocksdb::WriteBatch wb;
        wb.Delete(links, code);
        wb.Delete(hits, code);
        db->Write(rocksdb::WriteOptions(), &wb);
        cache_del(code);
        return MutResult::Ok;
    }

    std::pair<std::vector<Link>, size_t> list(size_t limit, size_t offset,
                                              const std::string& sort,
                                              const std::string& q) {
        std::vector<Link> items;
        int64_t now = now_ms();
        std::unique_ptr<rocksdb::Iterator> it(
            db->NewIterator(rocksdb::ReadOptions(), links));
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
            std::string code = it->key().ToString();
            std::string v = it->value().ToString();
            int64_t e, c;
            std::string_view u;
            if (!dec(v, e, c, u) || (e != 0 && e <= now)) continue;
            if (!q.empty() && code.find(q) == std::string::npos &&
                u.find(q) == std::string_view::npos)
                continue;
            items.push_back({code, std::string(u), db_hits(code), c,
                             e != 0 ? e : 0});
        }
        bool by_hits = sort == "hits";
        std::sort(items.begin(), items.end(), [by_hits](const Link& a, const Link& b) {
            return by_hits ? a.hits > b.hits : a.created_at > b.created_at;
        });
        size_t total = items.size();
        if (offset >= items.size()) items.clear();
        else {
            auto b = items.begin() + (ptrdiff_t)offset;
            auto en = items.begin() + (ptrdiff_t)std::min(offset + limit, items.size());
            items = std::vector<Link>(b, en);
        }
        return {items, total};
    }

    std::unique_ptr<Link> stats(const std::string& code) {
        std::string v;
        if (!db_get(code, v)) return nullptr;
        int64_t e, c;
        std::string_view u;
        if (!dec(v, e, c, u) || (e != 0 && e <= now_ms())) return nullptr;
        int64_t h = db_hits(code);
        {
            int i = shard(code);
            std::lock_guard<std::mutex> g(dirty_mu[i]);
            h += dirty[i][code];
        }
        return std::make_unique<Link>(
            Link{code, std::string(u), h, c, e != 0 ? e : 0});
    }

    size_t seed(const std::vector<std::string>& urls) {
        shorten_many(urls, 0);
        try { flush_hits(); } catch (...) {}
        return urls.size();
    }

    bool empty() {
        std::unique_ptr<rocksdb::Iterator> it(
            db->NewIterator(rocksdb::ReadOptions(), links));
        it->SeekToFirst();
        return !it->Valid();
    }

    void compact() {
        db->CompactRange(rocksdb::CompactRangeOptions(), links, nullptr, nullptr);
    }
};

// ---- RocksInner wrapper ----

RocksInner::RocksInner(const std::string& path, int inst, size_t ce, int64_t ct)
    : p(std::make_unique<RocksInnerImpl>(path, inst, ce, ct)) {
    instance = p->instance;
}
RocksInner::~RocksInner() = default;

std::shared_ptr<RocksInner> RocksInner::open(const std::string& path, int inst,
                                             size_t ce, int64_t ct) {
    return std::shared_ptr<RocksInner>(new RocksInner(path, inst, ce, ct));
}

std::shared_ptr<const std::string> RocksInner::resolve(const std::string& c) {
    return p->resolve(c);
}
std::shared_ptr<std::string> RocksInner::shorten(const std::string& u,
                                               const std::string* a, int64_t t) {
    return p->shorten(u, a, t);
}
std::vector<std::string> RocksInner::shorten_many(const std::vector<std::string>& u,
                                                  int64_t t) {
    return p->shorten_many(u, t);
}
MutResult RocksInner::update(const std::string& c, const std::string& u,
                             int64_t t, bool h) {
    return p->update(c, u, t, h);
}
MutResult RocksInner::remove(const std::string& c) { return p->remove(c); }
std::pair<std::vector<Link>, size_t> RocksInner::list(size_t l, size_t o,
                                                    const std::string& s,
                                                    const std::string& q) {
    return p->list(l, o, s, q);
}
std::unique_ptr<Link> RocksInner::stats(const std::string& c) { return p->stats(c); }
size_t RocksInner::seed(const std::vector<std::string>& u) { return p->seed(u); }
bool RocksInner::empty() { return p->empty(); }
void RocksInner::flush() { p->flush_hits(); }
void RocksInner::poll_tails() {}
void RocksInner::compact() { p->compact(); }

} // namespace shrt
