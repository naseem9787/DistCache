// Diagnostics implementation. Whole file is compiled out of the normal build.
#ifdef DISTCACHE_DIAGNOSTICS

#include "diag.hpp"

#include <dlfcn.h>
#include <execinfo.h>
#include <link.h>
#include <signal.h>
#include <sys/time.h>

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <unordered_set>

namespace diag {

Counters& c() {
    static Counters counters;
    return counters;
}

std::string report() {
    auto line = [](const char* name, const Counter& k) {
        const uint64_t n = k.n.load(), ns = k.ns.load();
        return std::string("diag_") + name + "_calls:" + std::to_string(n) + "\r\ndiag_" + name + "_total_ns:" +
               std::to_string(ns) + "\r\ndiag_" + name + "_avg_ns:" + std::to_string(n ? ns / n : 0) +
               "\r\ndiag_" + name + "_max_ns:" + std::to_string(k.max_ns.load()) + "\r\n";
    };
    Counters& k = c();
    std::string out = "# Diagnostics\r\n";
    out += line("encode", k.encode);
    out += line("append_wait", k.append_wait);
    out += line("append_locked", k.append_locked);
    out += line("flush_swap", k.flush_swap);
    out += line("flush_write", k.flush_write);
    out += line("flush_sync", k.flush_sync);
    out += line("shard_wait", k.shard_wait);
    out += line("shard_hold", k.shard_hold);
    out += line("repl_encode", k.repl_encode);
    out += line("repl_wait", k.repl_wait);
    out += line("repl_locked", k.repl_locked);
    out += "diag_flush_cycles:" + std::to_string(k.flush_cycles.load()) + "\r\n";
    out += "diag_flush_bytes:" + std::to_string(k.flush_bytes.load()) + "\r\n";
    out += "diag_buf_grow_events:" + std::to_string(k.buf_grow_events.load()) + "\r\n";
    out += "diag_buf_peak_bytes:" + std::to_string(k.buf_peak_bytes.load()) + "\r\n";
    return out;
}

bool sink_is_null() {
    static const bool v = [] {
        const char* e = std::getenv("DISTCACHE_WAL_SINK");
        return e && std::strcmp(e, "null") == 0;
    }();
    return v;
}

// ---- in-process sampling profiler -------------------------------------------------------
// SIGPROF fires on process CPU time (user + system). The handler records a stack trace and the
// name of the interrupted thread. It is not strictly async-signal-safe (backtrace()), which is
// acceptable for a diagnostics tool: backtrace is warmed up once before sampling starts.
namespace {

constexpr int kMaxFrames = 20;
constexpr size_t kMaxSamples = 400000;

struct Sample {
    const char* tag;
    int n;
    void* frames[kMaxFrames];
};

Sample* g_samples = nullptr;
std::atomic<size_t> g_count{0};
thread_local const char* tl_tag = "other";
std::string g_out_path;

void on_sigprof(int) {
    size_t i = g_count.fetch_add(1, std::memory_order_relaxed);
    if (i >= kMaxSamples) return;
    Sample& s = g_samples[i];
    s.tag = tl_tag;
    s.n = backtrace(s.frames, kMaxFrames);
}

int phdr_cb(struct dl_phdr_info* info, size_t, void* data) {
    *static_cast<uintptr_t*>(data) = static_cast<uintptr_t>(info->dlpi_addr);  // first object = main program
    return 1;
}

}  // namespace

void tag_thread(const char* name) { tl_tag = name; }

void profiler_start(const std::string& out_path, int hz) {
    g_out_path = out_path;
    g_samples = new Sample[kMaxSamples];
    void* warm[4];
    backtrace(warm, 4);  // load libgcc's unwinder now, not inside the signal handler
    struct sigaction sa;
    std::memset(&sa, 0, sizeof sa);
    sa.sa_handler = on_sigprof;
    sa.sa_flags = SA_RESTART;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGPROF, &sa, nullptr);
    itimerval it;
    const long usec = 1000000L / (hz > 0 ? hz : 250);
    it.it_interval.tv_sec = 0;
    it.it_interval.tv_usec = usec;
    it.it_value = it.it_interval;
    setitimer(ITIMER_PROF, &it, nullptr);
}

void profiler_stop() {
    if (!g_samples) return;
    itimerval off;
    std::memset(&off, 0, sizeof off);
    setitimer(ITIMER_PROF, &off, nullptr);
    uintptr_t base = 0;
    dl_iterate_phdr(phdr_cb, &base);
    const size_t n = std::min(g_count.load(), kMaxSamples);
    FILE* f = std::fopen(g_out_path.c_str(), "w");
    if (!f) return;
    std::fprintf(f, "# base=%lx samples=%zu\n", static_cast<unsigned long>(base), n);
    for (size_t i = 0; i < n; ++i) {
        const Sample& s = g_samples[i];
        std::fprintf(f, "%s %d", s.tag, s.n);
        for (int k = 0; k < s.n; ++k) std::fprintf(f, " %lx", reinterpret_cast<unsigned long>(s.frames[k]));
        std::fputc('\n', f);
    }
    // Resolve shared-library frames (libc, libstdc++) here, where dladdr is safe to call. The main
    // program's own frames are resolved offline with addr2line, which needs the debug info.
    std::unordered_set<void*> seen;
    for (size_t i = 0; i < n; ++i) {
        for (int k = 0; k < g_samples[i].n; ++k) {
            void* a = g_samples[i].frames[k];
            if (!seen.insert(a).second) continue;
            Dl_info info;
            if (dladdr(a, &info) && info.dli_fbase != nullptr &&
                reinterpret_cast<uintptr_t>(info.dli_fbase) != base && info.dli_sname) {
                const char* mod = info.dli_fname ? std::strrchr(info.dli_fname, '/') : nullptr;
                std::fprintf(f, "@ %lx %s %s\n", reinterpret_cast<unsigned long>(a), mod ? mod + 1 : "lib", info.dli_sname);
            }
        }
    }
    std::fclose(f);
}

}  // namespace diag

#endif  // DISTCACHE_DIAGNOSTICS
