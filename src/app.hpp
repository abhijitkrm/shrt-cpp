#pragma once
#include <memory>
#include <string>
#include "store.hpp"

namespace shrt {

inline constexpr size_t MAX_BODY = 4096;
inline constexpr size_t MAX_BULK_BODY = 1 << 20;
inline constexpr size_t MAX_BULK_URLS = 10'000;
inline constexpr int64_t MAX_LIST_LIMIT = 1000;

int64_t link_ttl_ms();     // LINK_TTL_MS env, default & cap = 1 day
const char* cors_origin(); // CORS_ORIGIN env, default "*"

struct Reply {
    int status;
    std::shared_ptr<const std::string> location; // redirect target
    std::string body;
    const char* ctype = nullptr; // nullptr -> application/json
};

/// Transport-agnostic request handler. `path` includes the query string;
/// `body` is the raw request body (empty when absent).
Reply handle(Store& st, const std::string& method, const std::string& path,
             const std::string& body, const std::string& admin_token,
             const std::string& client);

} // namespace shrt
