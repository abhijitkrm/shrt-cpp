#include "aof.hpp"
#include <fcntl.h>
#include <signal.h>
#include <string_view>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <dirent.h>
#include <algorithm>
#include <cstdio>

namespace shrt {

static uint64_t file_size(const std::string& p) {
    struct stat st{};
    return ::stat(p.c_str(), &st) == 0 ? (uint64_t)st.st_size : 0;
}

Aof::~Aof() { close(); }

bool Aof::open_at(const std::string& dir, const std::string& name) {
    ::mkdir(dir.c_str(), 0755);
    path = dir + "/" + name;
    fd = ::open(path.c_str(), O_WRONLY | O_APPEND | O_CREAT, 0644);
    buf.reserve(1 << 16);
    return fd >= 0;
}

void Aof::flush() {
    if (buf.empty() || fd < 0) return;
    const char* p = buf.data();
    size_t left = buf.size();
    while (left) {
        ssize_t n = ::write(fd, p, left);
        if (n <= 0) break;
        p += n;
        left -= (size_t)n;
    }
    buf.clear();
}

void Aof::sync() {
    flush();
    if (fd >= 0) ::fsync(fd);
}

void Aof::truncate() {
    flush();
    if (fd >= 0) ::close(fd);
    fd = ::open(path.c_str(), O_WRONLY | O_TRUNC | O_CREAT, 0644);
}

void Aof::close() {
    if (fd < 0) return;
    flush();
    ::fsync(fd);
    ::close(fd);
    fd = -1;
}

void replay_file(const std::string& path, LineCb cb) {
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) return;
    std::string buf;
    buf.resize(file_size(path));
    size_t got = 0;
    while (got < buf.size()) {
        ssize_t n = ::read(fd, buf.data() + got, buf.size() - got);
        if (n <= 0) break;
        got += (size_t)n;
    }
    ::close(fd);
    buf.resize(got);
    size_t start = 0;
    for (size_t i = 0; i < buf.size(); i++) {
        if (buf[i] == NL) {
            if (i > start) cb(std::string_view(buf).substr(start, i - start));
            start = i + 1;
        }
    }
}

void TailReader::read_new(LineCb cb) {
    uint64_t size = file_size(path);
    if (size < offset) { offset = 0; leftover.clear(); }
    if (size <= offset) return;
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) return;
    ::lseek(fd, (off_t)offset, SEEK_SET);
    std::string buf;
    buf.resize(size - offset);
    size_t got = 0;
    while (got < buf.size()) {
        ssize_t n = ::read(fd, buf.data() + got, buf.size() - got);
        if (n <= 0) break;
        got += (size_t)n;
    }
    ::close(fd);
    buf.resize(got);
    offset += got;

    leftover += buf;
    size_t start = 0;
    for (size_t i = 0; i < leftover.size(); i++) {
        if (leftover[i] == NL) {
            if (i > start) cb(std::string_view(leftover).substr(start, i - start));
            start = i + 1;
        }
    }
    leftover.erase(0, start);
}

static bool pid_alive(pid_t pid) {
    return ::kill(pid, 0) == 0 || errno == EPERM;
}

static bool try_lock(const std::string& lock) {
    int fd = ::open(lock.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0644);
    if (fd < 0) return false;
    std::string s = std::to_string(::getpid());
    ::write(fd, s.data(), s.size());
    ::close(fd);
    return true;
}

std::pair<int, std::string> claim_instance(const std::string& dir) {
    ::mkdir(dir.c_str(), 0755);
    for (int i = 0; i < 1024; i++) {
        std::string lock = dir + "/instance-" + std::to_string(i) + ".lock";
        if (try_lock(lock)) return {i, lock};
        // lock exists: steal if holder is dead
        int fd = ::open(lock.c_str(), O_RDONLY);
        if (fd < 0) continue;
        char buf[32];
        ssize_t n = ::read(fd, buf, sizeof buf - 1);
        ::close(fd);
        if (n <= 0) continue;
        buf[n] = 0;
        pid_t pid = (pid_t)std::strtol(buf, nullptr, 10);
        if (pid <= 0 || pid_alive(pid)) continue;
        ::unlink(lock.c_str());
        if (try_lock(lock)) return {i, lock};
    }
    return {-1, {}};
}

std::vector<std::string> shard_files(const std::string& dir, const std::string& own) {
    std::vector<std::string> out;
    DIR* d = ::opendir(dir.c_str());
    if (!d) return out;
    while (dirent* e = ::readdir(d)) {
        std::string n = e->d_name;
        if (n.rfind("data-", 0) == 0 && n.size() > 4 &&
            n.compare(n.size() - 4, 4, ".log") == 0 && n != own)
            out.push_back(std::move(n));
    }
    ::closedir(d);
    std::sort(out.begin(), out.end());
    return out;
}

} // namespace shrt
