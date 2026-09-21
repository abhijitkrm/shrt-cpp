//! shrt — high-performance URL shortener.
//!
//! Env config (same contract as shrt-ts / shrt-go / shrt-rust):
//!   PORT        3000    base listen port
//!   DATA_DIR    data    shard log directory (data-<i>.log, data-<i>.snap)
//!   SERVER      mini    "mini" (thread-per-conn) or "kq" (kqueue event loop)
//!   WORKERS     1       processes; all share PORT via SO_REUSEPORT
//!   INSTANCE    auto    instance id (auto-claimed via instance-<i>.lock files)
//!   SEED        0       bulk-insert N links if empty (random codes)
//!   HITS        1       "0" disables hit counting
//!   TAIL_MS     0       >0 enables periodic sibling-log polling
//!   CORS_ORIGIN *       value of Access-Control-Allow-Origin
//!   ADMIN_TOKEN unset   enables PATCH/DELETE; requests need x-admin-token: <value>
//!   LINK_TTL_MS 86400000  default AND max link lifetime

#include "app.hpp"
#include "common.hpp"
#include "metrics.hpp"
#include "server.hpp"
#include "store.hpp"

#include <csignal>
#include <cstdio>
#include <cstring>
#include <spawn.h>
#include <mach-o/dyld.h>
#include <string>
#include <sys/wait.h>
#include <unistd.h>
#include <vector>

extern char** environ;

using namespace shrt;

static int64_t port() { return env_i64("PORT", 3000); }
static std::string data_dir() { return env_str("DATA_DIR", "data"); }

/// Bulk-insert N links if the store is empty (through the write path).
static void seed(int64_t n) {
    try {
        Store s = Store::open(data_dir(), 0);
        if (s.empty()) {
            std::vector<std::string> urls;
            urls.reserve((size_t)n);
            for (int64_t i = 0; i < n; i++)
                urls.push_back("https://example.com/" + std::to_string(i));
            s.seed(urls);
        }
        s.close();
    } catch (const std::exception& e) {
        fprintf(stderr, "seed: %s\n", e.what());
    }
}

static std::vector<pid_t> g_children;
static volatile sig_atomic_t g_done = 0;
static void on_sig(int) {
    for (pid_t p : g_children) ::kill(p, SIGTERM);
    g_done = 1;
}

/// Spawn `workers` child processes sharing PORT via SO_REUSEPORT.
static int run_supervisor(int n) {
    char exe[4096];
    uint32_t sz = sizeof exe;
    if (_NSGetExecutablePath(exe, &sz) != 0) {
        fprintf(stderr, "cannot resolve exe path\n");
        return 1;
    }
    for (int i = 0; i < n; i++) {
        std::string inst = "INSTANCE=" + std::to_string(i);
        std::vector<std::string> envbuf;
        for (char** e = environ; *e; e++) {
            std::string v = *e;
            if (v.rfind("SHRT_CHILD=", 0) && v.rfind("INSTANCE=", 0) &&
                v.rfind("WORKERS=", 0) && v.rfind("SEED=", 0))
                envbuf.push_back(v);
        }
        envbuf.push_back("SHRT_CHILD=1");
        envbuf.push_back(inst);
        envbuf.push_back("WORKERS=1");
        envbuf.push_back("SEED=0");
        std::vector<char*> envp;
        for (auto& s : envbuf) envp.push_back(s.data());
        envp.push_back(nullptr);
        char* argv[] = {exe, nullptr};
        pid_t pid;
        if (posix_spawn(&pid, exe, nullptr, nullptr, argv, envp.data()) != 0)
            fprintf(stderr, "spawn worker %d failed\n", i);
        else
            g_children.push_back(pid);
    }
    printf("supervisor: %zu workers sharing :%lld (pid %d)\n",
           g_children.size(), (long long)port(), getpid());
    fflush(stdout);
    signal(SIGTERM, on_sig);
    signal(SIGINT, on_sig);
    while (!g_done) usleep(50'000);
    for (pid_t p : g_children) ::waitpid(p, nullptr, 0);
    return 0;
}

static Store* g_st = nullptr;
static void on_sig_serve(int) {
    if (g_st) g_st->close();
    _exit(0);
}

static int serve() {
    int64_t inst = env_i64("INSTANCE", -1);
    Store st;
    try {
        st = Store::open(data_dir(), (int)inst);
    } catch (const std::exception& e) {
        fprintf(stderr, "%s\n", e.what());
        return 1;
    }
    int fd = listen_socket((int)port());
    if (fd < 0) {
        fprintf(stderr, "failed to bind :%lld: %s\n", (long long)port(),
                strerror(errno));
        return 1;
    }
    g_st = &st;
    signal(SIGTERM, on_sig_serve);
    signal(SIGINT, on_sig_serve);
    signal(SIGPIPE, SIG_IGN);

    std::string srv = env_str("SERVER", "mini");
    if (srv == "kq") {
        printf("kq listening on :%lld (pid %d)\n", (long long)port(), getpid());
        fflush(stdout);
        serve_kq(fd, st);
    } else {
        printf("mini listening on :%lld (pid %d)\n", (long long)port(), getpid());
        fflush(stdout);
        serve_mini(fd, st);
    }
    st.close();
    return 0;
}

int main() {
    metrics::init();
    int64_t workers = env_i64("WORKERS", 1);
    int64_t seed_n = env_i64("SEED", 0);
    if (workers > 1 && !getenv("SHRT_CHILD")) {
        if (seed_n > 0) seed(seed_n);
        return run_supervisor((int)workers);
    }
    if (seed_n > 0) seed(seed_n);
    return serve();
}
