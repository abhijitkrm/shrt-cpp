#pragma once
#include <cstdint>
#include <functional>
#include <string>
#include <vector>

namespace shrt {

inline constexpr char NL = '\n';
using LineCb = const std::function<void(std::string_view)>&;

/// Append-only log: queued line writes flushed in one syscall batch.
struct Aof {
    std::string buf;
    int fd = -1;
    std::string path;

    ~Aof();
    bool open_at(const std::string& dir, const std::string& name);

    inline void push(std::string_view line) { buf += line; buf += NL; }
    inline void push_raw(std::string_view bytes) { buf += bytes; }
    size_t pending_bytes() const { return buf.size(); }

    void flush();   // append queued bytes in one write (page cache only)
    void sync();    // flush + fsync
    void truncate();// discard contents (after snapshot written)
    void close();
};

/// Read `path` and invoke cb for each complete line (torn tail ignored).
void replay_file(const std::string& path, LineCb cb);

/// Tracks a sibling log's read offset; read_new yields new complete lines.
struct TailReader {
    std::string path;
    uint64_t offset = 0;
    std::string leftover;

    explicit TailReader(std::string p) : path(std::move(p)) {}
    void read_new(LineCb cb);
};

/// Claim the lowest free instance index via lock files in dir; stale locks
/// (dead pid) are stolen. Returns (id, lock path) — caller removes the file
/// on shutdown. id < 0 on failure.
std::pair<int, std::string> claim_instance(const std::string& dir);

/// List shard log names (data-*.log) present in dir, excluding `own`.
std::vector<std::string> shard_files(const std::string& dir, const std::string& own);

} // namespace shrt
