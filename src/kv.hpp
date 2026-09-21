// kv.hpp — minimal RESP client (Redis / DragonflyDB / KeyDB). Zero deps.
// A Kv is a fixed set of connections, each with its own mutex; commands
// round-robin onto a conn, write, read, release. Pipelines collapse N
// round-trips into one flush + one read stream.
#pragma once
#include "common.hpp"
#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

namespace shrt {

struct Resp {
    char kind = 0;                 // '+', '-', ':', '$', '*'
    std::string str;               // '+'/'-'/'$' payload
    int64_t num = 0;               // ':'
    std::vector<Resp> arr;         // '*'
    bool null() const {
        return (kind == '$' && num < 0) || (kind == '*' && num < 0);
    }
};

class Kv {
public:
    struct Conn {
        int fd = -1;
        std::mutex mu;
        std::string rbuf; // read scratch
        size_t rpos = 0;
    };
private:
    std::string addr_;
    std::vector<std::unique_ptr<Conn>> conns_;
    std::atomic<size_t> next_{0};

    Conn* conn();
    bool cmd_on(Conn& c, const std::vector<std::string>& args, Resp& out);

public:
    explicit Kv(const std::string& addr, int nconn = 16);
    ~Kv();

    /// one command; throws std::runtime_error on transport failure
    Resp cmd(const std::vector<std::string>& args);
    /// N commands, one round-trip
    std::vector<Resp> pipe(const std::vector<std::vector<std::string>>& cmds);

    std::optional<std::string> get(const std::string& k);
    bool set(const std::string& k, const std::string& v, int64_t px_ms, bool nx);
    int64_t del(const std::string& k);
    void incrby_many(const std::vector<std::pair<std::string, int64_t>>& deltas);
    void scan_each(const std::string& pat, const std::function<void(std::string)>& cb);
    void flushdb();
};

} // namespace shrt
