// Compaction micro-benchmark — links the storage engine directly (no server).
// Built twice by bench/run_compaction_bench.sh: against the pre-change tree
// ("before": full rebuild under an exclusive lock) and the current tree
// ("after": tiered compaction). Uses only API common to both versions.
//
//   compaction_bench insert            insert throughput (page accounting cost)
//   compaction_bench ordered           delete oldest 50% (TTL-like) → compact
//   compaction_bench scattered         delete 3 of every 4 rows     → compact
#include "storage/table.h"
#include "parser/ast.h"
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

using Clock = std::chrono::steady_clock;
static double ms_since(Clock::time_point t) {
    return std::chrono::duration<double, std::milli>(Clock::now() - t).count();
}
static double rss_mb() {
    long pages = 0, rss = 0;
    if (FILE* f = std::fopen("/proc/self/statm", "r")) {
        if (std::fscanf(f, "%ld %ld", &pages, &rss) != 2) rss = 0;
        std::fclose(f);
    }
    return rss * (double)sysconf(_SC_PAGESIZE) / (1024.0 * 1024.0);
}

static TableSchema schema() {
    TableSchema s;
    s.table_name = "BENCH";
    s.columns = {{"ID", ColumnType::INT, true, false},
                 {"GRP", ColumnType::INT, false, false},
                 {"NAME", ColumnType::VARCHAR, false, false},
                 {"DATA", ColumnType::TEXT, false, false}};
    s.pk_col_index = 0;
    return s;
}

// Bulk-load n rows (~150 B of strings per row) through the batch insert path.
static void load(Table& t, int n) {
    const int B = 25000;
    std::vector<std::string> store;
    std::vector<std::string_view> flat;
    for (int base = 0; base < n; base += B) {
        const int m = std::min(B, n - base);
        store.clear(); flat.clear();
        store.reserve((size_t)m * 4);
        for (int i = base; i < base + m; ++i) {
            store.push_back(std::to_string(i));
            store.push_back(std::to_string(i % 4));
            store.push_back("user_" + std::to_string(i));
            store.push_back("payload-" + std::to_string(i) + std::string(120, 'x'));
        }
        for (auto& s : store) flat.push_back(s);
        t.insert_flat(flat.data(), (size_t)m, 4, 0);
    }
}

static void bench_insert() {
    const int N = 2000000;
    double best = 0;
    for (int run = 0; run < 3; ++run) {
        Table t(schema());
        auto t0 = Clock::now();
        load(t, N);
        best = std::max(best, N / (ms_since(t0) / 1000.0));
    }
    std::printf("insert: %.2f M rows/s (best of 3, 2M rows, PK table)\n", best / 1e6);
}

// Compact while a reader thread (PK point lookups) and a writer thread (PK
// updates) run; report how long each was stalled at worst during compaction.
static void bench_churn(bool ordered) {
    const int N = 1000000;
    Table t(schema());
    load(t, N);

    if (ordered) {
        auto p = Predicate::leaf("ID", "<", std::to_string(N / 2));   // oldest half
        t.delete_rows(&p);
    } else {
        auto p = Predicate::leaf("GRP", "!=", "0");                  // 3 of every 4
        t.delete_rows(&p);
    }
    const double rss_before = rss_mb();

    // Survivors: ordered → ID >= N/2 ; scattered → ID % 4 == 0
    auto live_id = [&](uint32_t r) {
        return ordered ? (int)(N / 2 + r % (N / 2)) : (int)((r % (N / 4)) * 4);
    };

    // Every operation is timestamped; afterwards we keep those that OVERLAP the
    // compaction window [c0, c1] — including one blocked for the whole window.
    struct Op { double start, dur; };
    std::atomic<bool> stop{false};
    std::vector<Op> rops, wops;
    const auto T0 = Clock::now();
    auto now_ms = [&] { return std::chrono::duration<double, std::milli>(Clock::now() - T0).count(); };

    std::thread reader([&] {
        uint32_t seed = 7;
        while (!stop) {
            seed = seed * 1103515245u + 12345u;
            auto p = Predicate::leaf("ID", "=", std::to_string(live_id(seed >> 4)));
            const double s0 = now_ms();
            t.scan({"NAME"}, &p, {});
            rops.push_back({s0, now_ms() - s0});
        }
    });
    std::thread writer([&] {
        uint32_t seed = 99;
        while (!stop) {
            seed = seed * 1103515245u + 12345u;
            auto p = Predicate::leaf("ID", "=", std::to_string(live_id(seed >> 4)));
            const double s0 = now_ms();
            t.update_rows({{"NAME", "upd"}}, &p);
            wops.push_back({s0, now_ms() - s0});
        }
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(200));   // warm up
    const double c0 = now_ms();
    const bool did = t.compact();
    const double c1 = now_ms();
    const double compact_ms = c1 - c0;
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    stop = true;
    reader.join();
    writer.join();

    auto window = [&](const std::vector<Op>& ops, double& mx, long& n, std::vector<double>& d) {
        mx = 0; n = 0;
        for (const Op& o : ops)
            if (o.start < c1 && o.start + o.dur > c0) { mx = std::max(mx, o.dur); ++n; d.push_back(o.dur); }
    };
    double max_read, max_write; long reads_during, writes_during;
    std::vector<double> reads, writes;
    window(rops, max_read, reads_during, reads);
    window(wops, max_write, writes_during, writes);
    std::sort(reads.begin(), reads.end());
    const double p99 = reads.empty() ? 0 : reads[(size_t)(reads.size() * 0.99)];
    std::printf("%s: compact=%s  time=%.0f ms  RSS %.0f → %.0f MB  "
                "reader: max %.1f ms p99 %.3f ms (%ld reads)  writer: max %.1f ms (%ld writes)\n",
                ordered ? "ordered  " : "scattered", did ? "yes" : "no", compact_ms,
                rss_before, rss_mb(), max_read, p99, reads_during, max_write, writes_during);
}

int main(int argc, char** argv) {
    const char* mode = argc > 1 ? argv[1] : "insert";
    if (!std::strcmp(mode, "insert"))         bench_insert();
    else if (!std::strcmp(mode, "ordered"))   bench_churn(true);
    else if (!std::strcmp(mode, "scattered")) bench_churn(false);
    return 0;
}
