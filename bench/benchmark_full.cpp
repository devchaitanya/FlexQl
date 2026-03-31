#include <iostream>
#include <chrono>
#include <string>
#include <cstdio>
#include <vector>
#include <thread>
#include <atomic>
#include <functional>
#include <cstring>
#include "flexql.h"

using namespace std;
using namespace std::chrono;

// ── Helpers ──────────────────────────────────────────────────────────────────

struct RowCount { long long n = 0; };

static int count_cb(void *d, int, char**, char**) {
    static_cast<RowCount*>(d)->n++;
    return 0;
}

static int noop_cb(void*, int, char**, char**) { return 0; }

static bool exec(FlexQL *db, const char *sql) {
    char *err = nullptr;
    int rc = flexql_exec(db, sql, nullptr, nullptr, &err);
    if (rc != FLEXQL_OK) {
        cerr << "[ERR] " << (err ? err : "?") << "\n";
        if (err) flexql_free(err);
        return false;
    }
    return true;
}

static long long timed_exec(FlexQL *db, const char *sql) {
    char *err = nullptr;
    auto t0 = high_resolution_clock::now();
    int rc = flexql_exec(db, sql, noop_cb, nullptr, &err);
    auto t1 = high_resolution_clock::now();
    if (rc != FLEXQL_OK) {
        cerr << "[ERR] " << (err ? err : "?") << "\n";
        if (err) flexql_free(err);
        return -1;
    }
    return duration_cast<microseconds>(t1 - t0).count();
}

static long long timed_count(FlexQL *db, const char *sql, long long &rows) {
    char *err = nullptr;
    RowCount rc_data;
    auto t0 = high_resolution_clock::now();
    int rc = flexql_exec(db, sql, count_cb, &rc_data, &err);
    auto t1 = high_resolution_clock::now();
    if (rc != FLEXQL_OK) {
        cerr << "[ERR] " << (err ? err : "?") << "\n";
        if (err) flexql_free(err);
        return -1;
    }
    rows = rc_data.n;
    return duration_cast<microseconds>(t1 - t0).count();
}

static void print_result(const char *label, long long us, long long rows = -1) {
    if (us < 0) { cout << label << ": FAILED\n"; return; }
    if (rows >= 0)
        printf("%-55s %8lld rows  %8lld.%03lld ms\n", label, rows, us/1000, us%1000);
    else
        printf("%-55s            %8lld.%03lld ms\n", label, us/1000, us%1000);
}

// ── Seed data ────────────────────────────────────────────────────────────────

static const int SEED_ROWS = 1000000;
static const int BATCH = 25000;

static bool seed_table(FlexQL *db) {
    exec(db, "DROP TABLE IF EXISTS BENCH;");
    if (!exec(db, "CREATE TABLE BENCH(ID DECIMAL, NAME VARCHAR(64), "
                   "DEPT VARCHAR(32), SALARY DECIMAL, EXPIRES_AT DECIMAL);"))
        return false;

    string sql;
    sql.reserve(BATCH * 90 + 40);
    char buf[64];
    const char *depts[] = {"ENG","HR","SALES","OPS","LEGAL","FIN","MKT","QA"};
    long long inserted = 0;

    auto t0 = high_resolution_clock::now();
    while (inserted < SEED_ROWS) {
        sql = "INSERT INTO BENCH VALUES ";
        int b = 0;
        while (b < BATCH && inserted < SEED_ROWS) {
            long long id = inserted + 1;
            int salary = 30000 + (int)(id % 70000);
            const char *dept = depts[id % 8];
            int n = snprintf(buf, sizeof(buf), "(%lld,'user%lld','%s',%d,1893456000)",
                             id, id, dept, salary);
            sql.append(buf, n);
            inserted++; b++;
            if (b < BATCH && inserted < SEED_ROWS) sql += ',';
        }
        sql += ';';
        if (!exec(db, sql.c_str())) return false;
    }
    auto t1 = high_resolution_clock::now();
    long long ms = duration_cast<milliseconds>(t1 - t0).count();
    printf("Seeded %d rows in %lld ms (%.0f rows/sec)\n\n", SEED_ROWS, ms,
           ms > 0 ? (double)SEED_ROWS * 1000.0 / ms : 0.0);
    return true;
}

// ── SELECT benchmarks ────────────────────────────────────────────────────────

static void bench_selects(FlexQL *db) {
    cout << "=== SELECT Benchmarks (1M-row table) ===\n\n";
    long long rows;

    // Full scan - COUNT(*)
    auto us = timed_count(db, "SELECT COUNT(*) FROM BENCH;", rows);
    print_result("SELECT COUNT(*) FROM BENCH", us, rows);

    // Full scan - all rows
    us = timed_count(db, "SELECT * FROM BENCH;", rows);
    print_result("SELECT * FROM BENCH (full scan)", us, rows);

    // Filtered scan - equality
    us = timed_count(db, "SELECT * FROM BENCH WHERE ID = 500000;", rows);
    print_result("SELECT * WHERE ID = 500000", us, rows);

    // Filtered scan - range
    us = timed_count(db, "SELECT * FROM BENCH WHERE SALARY > 90000;", rows);
    print_result("SELECT * WHERE SALARY > 90000", us, rows);

    // Filtered scan - compound AND
    us = timed_count(db,
        "SELECT * FROM BENCH WHERE DEPT = 'ENG' AND SALARY > 80000;", rows);
    print_result("SELECT * WHERE DEPT='ENG' AND SALARY>80000", us, rows);

    // Filtered scan - compound OR
    us = timed_count(db,
        "SELECT * FROM BENCH WHERE DEPT = 'HR' OR DEPT = 'SALES';", rows);
    print_result("SELECT * WHERE DEPT='HR' OR DEPT='SALES'", us, rows);

    // BETWEEN
    us = timed_count(db,
        "SELECT * FROM BENCH WHERE SALARY BETWEEN 50000 AND 60000;", rows);
    print_result("SELECT * WHERE SALARY BETWEEN 50000 AND 60000", us, rows);

    // IN
    us = timed_count(db,
        "SELECT * FROM BENCH WHERE DEPT IN ('ENG','HR','FIN');", rows);
    print_result("SELECT * WHERE DEPT IN ('ENG','HR','FIN')", us, rows);

    // LIKE
    us = timed_count(db,
        "SELECT * FROM BENCH WHERE NAME LIKE 'user1%';", rows);
    print_result("SELECT * WHERE NAME LIKE 'user1%'", us, rows);

    // ORDER BY + LIMIT
    us = timed_count(db,
        "SELECT * FROM BENCH ORDER BY SALARY DESC LIMIT 10;", rows);
    print_result("SELECT * ORDER BY SALARY DESC LIMIT 10", us, rows);

    // ORDER BY + LIMIT + OFFSET
    us = timed_count(db,
        "SELECT * FROM BENCH ORDER BY ID DESC LIMIT 100 OFFSET 1000;", rows);
    print_result("SELECT * ORDER BY ID DESC LIMIT 100 OFFSET 1000", us, rows);

    // DISTINCT
    us = timed_count(db, "SELECT DISTINCT DEPT FROM BENCH;", rows);
    print_result("SELECT DISTINCT DEPT FROM BENCH", us, rows);

    cout << "\n";
}

// ── Aggregate benchmarks ─────────────────────────────────────────────────────

static void bench_aggregates(FlexQL *db) {
    cout << "=== Aggregate Benchmarks ===\n\n";
    long long rows;

    auto us = timed_count(db, "SELECT SUM(SALARY) FROM BENCH;", rows);
    print_result("SELECT SUM(SALARY)", us, rows);

    us = timed_count(db, "SELECT AVG(SALARY) FROM BENCH;", rows);
    print_result("SELECT AVG(SALARY)", us, rows);

    us = timed_count(db, "SELECT MIN(SALARY) FROM BENCH;", rows);
    print_result("SELECT MIN(SALARY)", us, rows);

    us = timed_count(db, "SELECT MAX(SALARY) FROM BENCH;", rows);
    print_result("SELECT MAX(SALARY)", us, rows);

    us = timed_count(db,
        "SELECT DEPT, COUNT(*), AVG(SALARY) FROM BENCH GROUP BY DEPT;", rows);
    print_result("GROUP BY DEPT (8 groups)", us, rows);

    us = timed_count(db,
        "SELECT DEPT, AVG(SALARY) FROM BENCH GROUP BY DEPT "
        "HAVING AVG(SALARY) > 60000;", rows);
    print_result("GROUP BY DEPT HAVING AVG(SALARY)>60000", us, rows);

    cout << "\n";
}

// ── JOIN benchmarks ──────────────────────────────────────────────────────────

static void bench_joins(FlexQL *db) {
    cout << "=== JOIN Benchmarks ===\n\n";

    // Create a smaller lookup table (DEPARTMENTS)
    exec(db, "DROP TABLE IF EXISTS DEPARTMENTS;");
    exec(db, "CREATE TABLE DEPARTMENTS(DNAME VARCHAR(32), BUDGET DECIMAL, "
             "EXPIRES_AT DECIMAL);");
    exec(db, "INSERT INTO DEPARTMENTS VALUES "
             "('ENG',5000000,1893456000),('HR',2000000,1893456000),"
             "('SALES',3000000,1893456000),('OPS',1500000,1893456000),"
             "('LEGAL',1000000,1893456000),('FIN',2500000,1893456000),"
             "('MKT',1800000,1893456000),('QA',1200000,1893456000);");

    long long rows;

    // INNER JOIN - full
    auto us = timed_count(db,
        "SELECT BENCH.NAME, DEPARTMENTS.BUDGET FROM BENCH "
        "INNER JOIN DEPARTMENTS ON BENCH.DEPT = DEPARTMENTS.DNAME;", rows);
    print_result("INNER JOIN (1M x 8, all match)", us, rows);

    // INNER JOIN with WHERE filter
    us = timed_count(db,
        "SELECT BENCH.NAME, DEPARTMENTS.BUDGET FROM BENCH "
        "INNER JOIN DEPARTMENTS ON BENCH.DEPT = DEPARTMENTS.DNAME "
        "WHERE BENCH.SALARY > 90000;", rows);
    print_result("INNER JOIN + WHERE SALARY>90000", us, rows);

    // LEFT JOIN
    us = timed_count(db,
        "SELECT BENCH.NAME, DEPARTMENTS.BUDGET FROM BENCH "
        "LEFT JOIN DEPARTMENTS ON BENCH.DEPT = DEPARTMENTS.DNAME;", rows);
    print_result("LEFT JOIN (1M x 8)", us, rows);

    exec(db, "DROP TABLE IF EXISTS DEPARTMENTS;");
    cout << "\n";
}

// ── UPDATE benchmarks ────────────────────────────────────────────────────────

static void bench_updates(FlexQL *db) {
    cout << "=== UPDATE Benchmarks ===\n\n";

    // Update single row
    auto us = timed_exec(db, "UPDATE BENCH SET SALARY = 99999 WHERE ID = 1;");
    print_result("UPDATE single row (WHERE ID=1)", us);

    // Update range of rows
    us = timed_exec(db, "UPDATE BENCH SET SALARY = 55555 WHERE ID <= 1000;");
    print_result("UPDATE 1000 rows (WHERE ID<=1000)", us);

    // Update by department (filtered)
    us = timed_exec(db,
        "UPDATE BENCH SET SALARY = 75000 WHERE DEPT = 'QA' AND SALARY < 50000;");
    print_result("UPDATE filtered (DEPT='QA' AND SALARY<50000)", us);

    cout << "\n";
}

// ── DELETE benchmarks ────────────────────────────────────────────────────────

static void bench_deletes(FlexQL *db) {
    cout << "=== DELETE Benchmarks ===\n\n";

    // Delete small range
    auto us = timed_exec(db, "DELETE FROM BENCH WHERE ID <= 100;");
    print_result("DELETE 100 rows (WHERE ID<=100)", us);

    // Delete by filter
    us = timed_exec(db, "DELETE FROM BENCH WHERE DEPT = 'LEGAL' AND SALARY < 40000;");
    print_result("DELETE filtered (DEPT='LEGAL' AND SALARY<40000)", us);

    // Verify remaining
    long long rows;
    us = timed_count(db, "SELECT COUNT(*) FROM BENCH;", rows);
    print_result("SELECT COUNT(*) after deletes", us, rows);

    cout << "\n";
}

// ── Concurrency benchmarks ───────────────────────────────────────────────────

struct ThreadResult {
    long long total_us = 0;
    long long ops = 0;
};

static void bench_concurrent_reads(int num_threads) {
    printf("=== Concurrent READ Benchmark (%d threads) ===\n\n", num_threads);

    vector<thread> threads;
    vector<ThreadResult> results(num_threads);
    const int QUERIES_PER_THREAD = 20;

    for (int t = 0; t < num_threads; t++) {
        threads.emplace_back([t, &results, QUERIES_PER_THREAD]() {
            FlexQL *db = nullptr;
            if (flexql_open("127.0.0.1", 9000, &db) != FLEXQL_OK) {
                cerr << "Thread " << t << " connect failed\n";
                return;
            }

            long long total = 0;
            for (int i = 0; i < QUERIES_PER_THREAD; i++) {
                long long rows;
                char sql[128];
                snprintf(sql, sizeof(sql),
                    "SELECT * FROM BENCH WHERE ID = %d;", (t * 10000 + i + 1));
                long long us = timed_count(db, sql, rows);
                if (us >= 0) total += us;
            }
            results[t].total_us = total;
            results[t].ops = QUERIES_PER_THREAD;
            flexql_close(db);
        });
    }
    for (auto &th : threads) th.join();

    long long total_ops = 0, total_us = 0;
    for (auto &r : results) { total_ops += r.ops; total_us += r.total_us; }
    double avg_ms = (double)total_us / total_ops / 1000.0;
    printf("  Total queries: %lld across %d threads\n", total_ops, num_threads);
    printf("  Avg latency:   %.3f ms per query\n", avg_ms);

    // Also measure wall-clock for all threads doing COUNT(*)
    auto t0 = high_resolution_clock::now();
    vector<thread> threads2;
    for (int t = 0; t < num_threads; t++) {
        threads2.emplace_back([t]() {
            FlexQL *db = nullptr;
            if (flexql_open("127.0.0.1", 9000, &db) != FLEXQL_OK) return;
            long long rows;
            timed_count(db, "SELECT COUNT(*) FROM BENCH;", rows);
            flexql_close(db);
        });
    }
    for (auto &th : threads2) th.join();
    auto t1 = high_resolution_clock::now();
    long long wall_ms = duration_cast<milliseconds>(t1 - t0).count();
    printf("  %d concurrent COUNT(*):  %lld ms wall-clock\n\n", num_threads, wall_ms);
}

static void bench_concurrent_mixed(int num_readers, int num_writers) {
    printf("=== Concurrent MIXED Benchmark (%d readers + %d writers) ===\n\n",
           num_readers, num_writers);

    atomic<bool> done{false};
    atomic<long long> read_ops{0}, write_ops{0};
    vector<thread> threads;

    auto t0 = high_resolution_clock::now();

    // Readers
    for (int t = 0; t < num_readers; t++) {
        threads.emplace_back([&done, &read_ops, t]() {
            FlexQL *db = nullptr;
            if (flexql_open("127.0.0.1", 9000, &db) != FLEXQL_OK) return;
            while (!done.load(memory_order_relaxed)) {
                char sql[128];
                snprintf(sql, sizeof(sql),
                    "SELECT * FROM BENCH WHERE ID = %lld;",
                    (long long)(t * 100000 + (read_ops.load() % 100000) + 1));
                long long rows;
                timed_count(db, sql, rows);
                read_ops.fetch_add(1, memory_order_relaxed);
            }
            flexql_close(db);
        });
    }

    // Writers
    for (int t = 0; t < num_writers; t++) {
        threads.emplace_back([&done, &write_ops, t]() {
            FlexQL *db = nullptr;
            if (flexql_open("127.0.0.1", 9000, &db) != FLEXQL_OK) return;
            while (!done.load(memory_order_relaxed)) {
                char sql[256];
                long long wn = write_ops.load();
                snprintf(sql, sizeof(sql),
                    "UPDATE BENCH SET SALARY = %lld WHERE ID = %lld;",
                    50000 + (wn % 50000), (long long)(t * 50000 + (wn % 50000) + 1));
                exec(db, sql);
                write_ops.fetch_add(1, memory_order_relaxed);
            }
            flexql_close(db);
        });
    }

    // Run for 3 seconds
    this_thread::sleep_for(seconds(3));
    done.store(true, memory_order_relaxed);
    for (auto &th : threads) th.join();

    auto t1 = high_resolution_clock::now();
    long long wall_ms = duration_cast<milliseconds>(t1 - t0).count();

    printf("  Duration:     %lld ms\n", wall_ms);
    printf("  Read ops:     %lld  (%.0f ops/sec)\n",
           read_ops.load(), (double)read_ops.load() * 1000.0 / wall_ms);
    printf("  Write ops:    %lld  (%.0f ops/sec)\n",
           write_ops.load(), (double)write_ops.load() * 1000.0 / wall_ms);
    printf("  Total ops:    %lld  (%.0f ops/sec)\n",
           read_ops.load() + write_ops.load(),
           (double)(read_ops.load() + write_ops.load()) * 1000.0 / wall_ms);
    cout << "\n";
}

static void bench_concurrent_inserts(int num_threads) {
    printf("=== Concurrent INSERT Benchmark (%d threads x 50K rows each) ===\n\n",
           num_threads);

    // Each thread inserts into its own table
    vector<thread> threads;
    vector<long long> elapsed_ms(num_threads);

    auto wall_t0 = high_resolution_clock::now();

    for (int t = 0; t < num_threads; t++) {
        threads.emplace_back([t, &elapsed_ms]() {
            FlexQL *db = nullptr;
            if (flexql_open("127.0.0.1", 9000, &db) != FLEXQL_OK) {
                cerr << "Thread " << t << " connect failed\n";
                return;
            }

            char tbl[64];
            snprintf(tbl, sizeof(tbl), "CONC_T%d", t);

            char sql_buf[128];
            snprintf(sql_buf, sizeof(sql_buf), "DROP TABLE IF EXISTS %s;", tbl);
            exec(db, sql_buf);
            snprintf(sql_buf, sizeof(sql_buf),
                "CREATE TABLE %s(ID DECIMAL, VAL VARCHAR(32), EXPIRES_AT DECIMAL);", tbl);
            exec(db, sql_buf);

            const int ROWS = 50000;
            const int BATCH_SZ = 5000;
            string sql;
            sql.reserve(BATCH_SZ * 50 + 40);
            char buf[64];

            auto t0 = high_resolution_clock::now();
            long long inserted = 0;
            while (inserted < ROWS) {
                sql.clear();
                sql += "INSERT INTO ";
                sql += tbl;
                sql += " VALUES ";
                int b = 0;
                while (b < BATCH_SZ && inserted < ROWS) {
                    int n = snprintf(buf, sizeof(buf),
                        "(%lld,'val%lld',1893456000)", inserted+1, inserted+1);
                    sql.append(buf, n);
                    inserted++; b++;
                    if (b < BATCH_SZ && inserted < ROWS) sql += ',';
                }
                sql += ';';
                exec(db, sql.c_str());
            }
            auto t1 = high_resolution_clock::now();
            elapsed_ms[t] = duration_cast<milliseconds>(t1 - t0).count();

            // Cleanup
            snprintf(sql_buf, sizeof(sql_buf), "DROP TABLE IF EXISTS %s;", tbl);
            exec(db, sql_buf);
            flexql_close(db);
        });
    }
    for (auto &th : threads) th.join();

    auto wall_t1 = high_resolution_clock::now();
    long long wall = duration_cast<milliseconds>(wall_t1 - wall_t0).count();

    long long total_rows = (long long)num_threads * 50000;
    printf("  Wall-clock:       %lld ms\n", wall);
    printf("  Total rows:       %lld across %d tables\n", total_rows, num_threads);
    printf("  Agg. throughput:  %.0f rows/sec\n",
           wall > 0 ? (double)total_rows * 1000.0 / wall : 0.0);
    for (int t = 0; t < num_threads; t++)
        printf("  Thread %d:         %lld ms (%.0f rows/sec)\n",
               t, elapsed_ms[t],
               elapsed_ms[t] > 0 ? 50000.0 * 1000.0 / elapsed_ms[t] : 0.0);
    cout << "\n";
}

// ── LRU Cache benchmark ─────────────────────────────────────────────────────

static void bench_cache(FlexQL *db) {
    cout << "=== LRU Cache Benchmark (repeated query) ===\n\n";

    // Cold query
    long long rows;
    auto us1 = timed_count(db, "SELECT * FROM BENCH WHERE ID = 42;", rows);
    print_result("First  (cold cache)", us1, rows);

    // Warm query (should hit cache)
    auto us2 = timed_count(db, "SELECT * FROM BENCH WHERE ID = 42;", rows);
    print_result("Second (warm cache)", us2, rows);

    auto us3 = timed_count(db, "SELECT * FROM BENCH WHERE ID = 42;", rows);
    print_result("Third  (warm cache)", us3, rows);

    if (us1 > 0 && us3 > 0)
        printf("  Cache speedup: %.1fx\n", (double)us1 / us3);

    cout << "\n";
}

// ── Memory measurement ────────────────────────────────────────────────────────

static long long get_server_rss_kb() {
    // Find the flexql-server PID and read /proc/<pid>/status
    FILE *fp = popen("pgrep -x flexql-server | head -1", "r");
    if (!fp) return -1;
    char pidbuf[32];
    if (!fgets(pidbuf, sizeof(pidbuf), fp)) { pclose(fp); return -1; }
    pclose(fp);
    int pid = atoi(pidbuf);
    if (pid <= 0) return -1;

    char path[64];
    snprintf(path, sizeof(path), "/proc/%d/status", pid);
    FILE *sf = fopen(path, "r");
    if (!sf) return -1;
    char line[256];
    long long rss_kb = -1;
    while (fgets(line, sizeof(line), sf)) {
        if (strncmp(line, "VmRSS:", 6) == 0) {
            rss_kb = atoll(line + 6);
            break;
        }
    }
    fclose(sf);
    return rss_kb;
}

static void print_memory(const char *label) {
    long long rss = get_server_rss_kb();
    if (rss > 0)
        printf("%-45s %lld KB  (%.1f MB)\n", label, rss, rss / 1024.0);
    else
        printf("%-45s (could not read)\n", label);
}

// ── Main ─────────────────────────────────────────────────────────────────────

int main() {
    FlexQL *db = nullptr;
    if (flexql_open("127.0.0.1", 9000, &db) != FLEXQL_OK) {
        cerr << "Cannot connect to FlexQL server at 127.0.0.1:9000\n";
        return 1;
    }
    cout << "Connected to FlexQL\n\n";

    cout << "=== Memory Footprint ===\n\n";
    print_memory("Server RSS (idle, no tables)");

    // Seed 1M rows
    if (!seed_table(db)) { flexql_close(db); return 1; }
    print_memory("Server RSS (after 1M-row seed)");
    cout << "\n";

    // Run all benchmark suites
    bench_selects(db);
    bench_aggregates(db);
    bench_joins(db);
    bench_cache(db);
    bench_updates(db);
    bench_deletes(db);

    // Concurrency benchmarks (use separate connections)
    bench_concurrent_reads(1);
    bench_concurrent_reads(4);
    bench_concurrent_reads(8);

    bench_concurrent_mixed(4, 1);   // 4 readers 1 writer
    bench_concurrent_mixed(4, 4);   // 4 readers 4 writers

    bench_concurrent_inserts(1);
    bench_concurrent_inserts(4);
    bench_concurrent_inserts(8);

    // Final memory after all operations
    cout << "=== Final Memory ===\n\n";
    print_memory("Server RSS (after all benchmarks)");
    cout << "\n";

    // Cleanup
    exec(db, "DROP TABLE IF EXISTS BENCH;");
    flexql_close(db);

    cout << "=== All benchmarks complete ===\n";
    return 0;
}
