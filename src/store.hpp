#pragma once
#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "aof.hpp"

namespace shrt {

/// Outcome of update/remove.
enum class MutResult { Ok, Missing, Remote };

/// Public view of a stored entry.
struct Link {
    std::string code, url;
    int64_t hits = 0, created_at = 0;
    int64_t expires_at = 0; // 0 = none
};

struct Entry {
    std::shared_ptr<const std::string> u;
    int64_t a, e; // created_at ms; expires_at ms (0 = never)
    int i;        // owning instance
    std::atomic<int64_t> h{0}, oh{0}; // total hits; own hits (persisted)
    Entry(std::shared_ptr<const std::string> u_, int64_t a_, int64_t e_, int i_)
        : u(std::move(u_)), a(a_), e(e_), i(i_) {}
    Entry(const Entry&) = delete;
};

struct StrayHit { int64_t t = 0, o = 0; };
using StrayMap = std::unordered_map<std::string, StrayHit>;

struct Shard {
    std::shared_mutex mu;
    std::unordered_map<std::string, Entry> data;
    std::mutex dmu;
    std::unordered_map<std::string, int64_t> dirty;
};

struct Tail {
    std::unordered_map<std::string, std::unique_ptr<TailReader>> readers;
    StrayMap stray;
    int64_t last_poll = 0;
};

struct Inner;
struct KvInner;
struct RocksInner;
class Store {
    std::shared_ptr<Inner> in;
    std::shared_ptr<KvInner> kvin;   // set when STORE=dragonfly|redis
    std::shared_ptr<RocksInner> rkin; // set when STORE=rocksdb

public:
    /// dir ":memory:" disables persistence. instance < 0 auto-claims.
    static Store open(const std::string& dir, int instance);
    /// External RESP backend (DragonflyDB / Redis). cache_entries bounds
    /// the local hot FIFO; cache_ttl_ms bounds cross-node staleness.
    static Store open_kv(const std::string& addr, int instance,
                         size_t cache_entries, int64_t cache_ttl_ms);
    /// Embedded RocksDB backend. ROCKSDB_PATH (default {dir}/rocks).
    /// Single-writer: the DB dir is exclusively locked by one process.
    static Store open_rocks(const std::string& dir, int instance,
                            size_t cache_entries, int64_t cache_ttl_ms);
    /// STORE env dispatch: aof|local (default) | dragonfly|redis|kv | rocksdb.
    /// DRAGONFLY_ADDR/KV_ADDR, ROCKSDB_PATH, CACHE, CACHE_TTL_MS.
    static Store open_env(const std::string& dir, int instance);

    int instance() const;
    bool persistent() const;

    std::shared_ptr<const std::string> resolve(const std::string& code);
    std::shared_ptr<std::string> shorten(const std::string& url,
                                       const std::string* alias, int64_t ttl_ms);
    std::vector<std::string> shorten_many(const std::vector<std::string>& urls,
                                          int64_t ttl_ms);
    MutResult update(const std::string& code, const std::string& url,
                     int64_t ttl_ms, bool has_ttl);
    MutResult remove(const std::string& code);
    std::pair<std::vector<Link>, size_t> list(size_t limit, size_t offset,
                                              const std::string& sort,
                                              const std::string& q);
    std::unique_ptr<Link> stats(const std::string& code);
    size_t seed(const std::vector<std::string>& urls);
    bool empty() const;
    /// /api/health probe — RESP PING / rocksdb point read / true for aof.
    bool healthy();

    void flush();
    void poll_tails();
    void compact();
    void close();
    ~Store() { close(); }
};

} // namespace shrt
