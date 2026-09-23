// store_kv.hpp — declaration of the external-KV store backend.
// Implementation (KvInnerImpl) is private to store_kv.cpp.
#pragma once
#include "store.hpp"
#include <memory>

namespace shrt {

struct KvInnerImpl;

struct KvInner {
    std::unique_ptr<KvInnerImpl> p;
    int instance = 0;

    KvInner(const std::string& addr, int inst, size_t cache_entries,
            int64_t cache_ttl_ms);
    ~KvInner();

    static std::shared_ptr<KvInner> open(const std::string& addr, int inst,
                                         size_t cache_entries,
                                         int64_t cache_ttl_ms);

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
    bool empty();
    bool healthy();
    void flush();
    void poll_tails();
    void compact();
};

} // namespace shrt
