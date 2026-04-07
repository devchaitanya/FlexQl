#include <iostream>
#include <chrono>
#include <string>
#include <cstdio>
#include <vector>
#include <thread>
#include <atomic>
#include <cstring>
#include "flexql.h"

using namespace std;
using namespace std::chrono;

static const long long DEFAULT_INSERT_ROWS = 1000000LL;
static const int INSERT_BATCH_SIZE = 5000;

struct QueryStats {
    long long rows = 0;
};

static int count_rows_callback(void *data, int argc, char **argv, char **azColName) {
    (void)argc;
    
    
    (void)argv;
    (void)azColName;
    QueryStats *stats = static_cast<QueryStats*>(data);
    if (stats) {
        stats->rows++;
    }
    return 0;
}

static bool run_exec(FlexQL *db, const string &sql, const string &label) {
    char *errMsg = nullptr;
    auto start = high_resolution_clock::now();
    int rc = flexql_exec(db, sql.c_str(), nullptr, nullptr, &errMsg);
    auto end = high_resolution_clock::now();
    long long elapsed = duration_cast<milliseconds>(end - start).count();

    if (rc != FLEXQL_OK) {
        cout << "[FAIL] " << label << " -> " << (errMsg ? errMsg : "unknown error") << "\n";
        if (errMsg) {
            flexql_free(errMsg);
        }
        return false;
    }

    cout << "[PASS] " << label << " (" << elapsed << " ms)\n";
    return true;
}

static bool run_query(FlexQL *db, const string &sql, const string &label) {
    QueryStats stats;
    char *errMsg = nullptr;
    auto start = high_resolution_clock::now();
    int rc = flexql_exec(db, sql.c_str(), count_rows_callback, &stats, &errMsg);
    auto end = high_resolution_clock::now();
    long long elapsed = duration_cast<milliseconds>(end - start).count();

    if (rc != FLEXQL_OK) {
        cout << "[FAIL] " << label << " -> " << (errMsg ? errMsg : "unknown error") << "\n";
        if (errMsg) {
            flexql_free(errMsg);
        }
        return false;
    }

    cout << "[PASS] " << label << " | rows=" << stats.rows << " | " << elapsed << " ms\n";
    return true;
}

static bool query_rows(FlexQL *db, const string &sql, vector<string> &out_rows) {
    struct Collector {
        vector<string> rows;
    } collector;

    auto cb = [](void *data, int argc, char **argv, char **azColName) -> int {
        (void)azColName;
        Collector *c = static_cast<Collector*>(data);
        string row;
        for (int i = 0; i < argc; ++i) {
            if (i > 0) {
                row += "|";
            }
            row += (argv[i] ? argv[i] : "NULL");
        }
        c->rows.push_back(row);
        return 0;
    };

    char *errMsg = nullptr;
    int rc = flexql_exec(db, sql.c_str(), cb, &collector, &errMsg);
    if (rc != FLEXQL_OK) {
        cout << "[FAIL] " << sql << " -> " << (errMsg ? errMsg : "unknown error") << "\n";
        if (errMsg) {
            flexql_free(errMsg);
        }
        return false;
    }

    out_rows = collector.rows;
    return true;
}

static bool assert_rows_equal(const string &label, const vector<string> &actual, const vector<string> &expected) {
    if (actual == expected) {
        cout << "[PASS] " << label << "\n";
        return true;
    }

    cout << "[FAIL] " << label << "\n";
    cout << "Expected (" << expected.size() << "):\n";
    for (const auto &r : expected) {
        cout << "  " << r << "\n";
    }
    cout << "Actual (" << actual.size() << "):\n";
    for (const auto &r : actual) {
        cout << "  " << r << "\n";
    }
    return false;
}

static bool expect_query_failure(FlexQL *db, const string &sql, const string &label) {
    char *errMsg = nullptr;
    int rc = flexql_exec(db, sql.c_str(), nullptr, nullptr, &errMsg);
    if (rc == FLEXQL_OK) {
        cout << "[FAIL] " << label << " (expected failure, got success)\n";
        return false;
    }
    if (errMsg) {
        flexql_free(errMsg);
    }
    cout << "[PASS] " << label << "\n";
    return true;
}

static bool assert_row_count(const string &label, const vector<string> &rows, size_t expected_count) {
    if (rows.size() == expected_count) {
        cout << "[PASS] " << label << "\n";
        return true;
    }

    cout << "[FAIL] " << label << " (expected " << expected_count << ", got " << rows.size() << ")\n";
    return false;
}

static bool run_data_level_unit_tests(FlexQL *db) {
    cout << "\n\n[[ Running Unit Tests ]]\n\n";

    bool all_ok = true;
    int total_tests = 0;
    int failed_tests = 0;

    auto record = [&](bool result) {
        total_tests++;
        if (!result) {
            all_ok = false;
            failed_tests++;
        }
    };

    record(run_exec(
            db,
            "CREATE TABLE IF NOT EXISTS TEST_USERS(ID DECIMAL, NAME VARCHAR(64), BALANCE DECIMAL, EXPIRES_AT DECIMAL);",
            "CREATE TABLE TEST_USERS"));

    record(run_exec(db, "DELETE FROM TEST_USERS;", "RESET TEST_USERS"));

    record(run_exec(
            db,
            "INSERT INTO TEST_USERS VALUES "
            "(1, 'Alice', 1200, 1893456000),"
            "(2, 'Bob', 450, 1893456000),"
            "(3, 'Carol', 2200, 1893456000),"
            "(4, 'Dave', 800, 1893456000);",
            "INSERT TEST_USERS"));

    vector<string> rows;

    bool q1 = query_rows(db, "SELECT NAME, BALANCE FROM TEST_USERS WHERE ID = 2;", rows);
    record(q1);
    if (q1) {
        record(assert_rows_equal("Single-row value validation", rows, {"Bob|450"}));
    }

    bool q2 = query_rows(db, "SELECT NAME FROM TEST_USERS WHERE BALANCE > 1000 ORDER BY NAME;", rows);
    record(q2);
    if (q2) {
        record(assert_rows_equal("Filtered rows validation", rows, {"Alice", "Carol"}));
    }


    bool q4 = query_rows(db, "SELECT ID FROM TEST_USERS WHERE BALANCE > 5000;", rows);
    record(q4);
    if (q4) {
        record(assert_row_count("Empty result-set validation", rows, 0));
    }

    record(run_exec(
            db,
            "CREATE TABLE IF NOT EXISTS TEST_ORDERS(ORDER_ID DECIMAL, USER_ID DECIMAL, AMOUNT DECIMAL, EXPIRES_AT DECIMAL);",
            "CREATE TABLE TEST_ORDERS"));

    record(run_exec(db, "DELETE FROM TEST_ORDERS;", "RESET TEST_ORDERS"));

    record(run_exec(
            db,
            "INSERT INTO TEST_ORDERS VALUES "
            "(101, 1, 50, 1893456000),"
            "(102, 1, 150, 1893456000),"
            "(103, 3, 500, 1893456000);",
            "INSERT TEST_ORDERS"));


    bool q7 = query_rows(
            db,
            "SELECT TEST_USERS.NAME, TEST_ORDERS.AMOUNT "
            "FROM TEST_USERS INNER JOIN TEST_ORDERS ON TEST_USERS.ID = TEST_ORDERS.USER_ID "
            "WHERE TEST_ORDERS.AMOUNT > 900;",
            rows);
    record(q7);
    if (q7) {
        record(assert_row_count("Join with no matches validation", rows, 0));
    }

    record(expect_query_failure(db, "SELECT UNKNOWN_COLUMN FROM TEST_USERS;", "Invalid SQL should fail"));
    record(expect_query_failure(db, "SELECT * FROM MISSING_TABLE;", "Missing table should fail"));

    int passed_tests = total_tests - failed_tests;
    cout << "\nUnit Test Summary: " << passed_tests << "/" << total_tests << " passed, "
         << failed_tests << " failed.\n\n";

    return all_ok;
}

static bool run_pk_benchmark(FlexQL *db, long long target_rows) {
    cout << "\n\n[[ Primary Key Table Benchmark ]]\n\n";

    run_exec(db, "DROP TABLE IF EXISTS PK_USERS;", "DROP TABLE PK_USERS (if exists)");
    if (!run_exec(db,
            "CREATE TABLE PK_USERS(ID INT PRIMARY KEY NOT NULL, NAME VARCHAR(64), BALANCE DECIMAL, EXPIRES_AT DECIMAL);",
            "CREATE TABLE PK_USERS (with PK)")) return false;

    // INSERT benchmark with PK uniqueness enforcement
    long long pk_rows = min(target_rows, (long long)100000); // 100K rows for PK test
    cout << "Inserting " << pk_rows << " rows into PK table...\n";
    auto t0 = high_resolution_clock::now();

    string sql;
    sql.reserve(INSERT_BATCH_SIZE * 80 + 32);
    char ibuf[32];
    long long inserted = 0;
    while (inserted < pk_rows) {
        sql = "INSERT INTO PK_USERS VALUES ";
        int in_batch = 0;
        while (in_batch < INSERT_BATCH_SIZE && inserted < pk_rows) {
            long long id = inserted + 1;
            int n = snprintf(ibuf, sizeof(ibuf), "%lld", id);
            sql += '(';
            sql.append(ibuf, n);
            sql += ",'pk_user";
            sql.append(ibuf, n);
            sql += "',";
            int bal = (int)(1000 + (id % 10000));
            n = snprintf(ibuf, sizeof(ibuf), "%d", bal);
            sql.append(ibuf, n);
            sql += ",1893456000)";
            inserted++;
            in_batch++;
            if (in_batch < INSERT_BATCH_SIZE && inserted < pk_rows) sql += ',';
        }
        sql += ';';
        char *errMsg = nullptr;
        if (flexql_exec(db, sql.c_str(), nullptr, nullptr, &errMsg) != FLEXQL_OK) {
            cout << "[FAIL] INSERT PK_USERS -> " << (errMsg ? errMsg : "unknown") << "\n";
            if (errMsg) flexql_free(errMsg);
            return false;
        }
    }
    auto t1 = high_resolution_clock::now();
    long long ins_ms = duration_cast<milliseconds>(t1 - t0).count();
    long long tput = (ins_ms > 0) ? (pk_rows * 1000LL / ins_ms) : pk_rows;
    cout << "[PASS] PK INSERT: " << pk_rows << " rows in " << ins_ms << " ms (" << tput << " rows/sec)\n";

    // PK point lookup: WHERE ID = X  (should be O(1) hash)
    run_query(db, "SELECT * FROM PK_USERS WHERE ID = 50000;", "PK point lookup (ID=50000)");

    // PK IN lookup
    run_query(db, "SELECT * FROM PK_USERS WHERE ID IN (1, 1000, 50000, 99999);", "PK IN lookup (4 keys)");

    // PK range scan (falls back to full scan since hashmap, not B+ tree)
    run_query(db, "SELECT * FROM PK_USERS WHERE ID > 99900;", "PK range scan (ID>99900)");

    // PK UPDATE (O(1) hash)
    run_exec(db, "UPDATE PK_USERS SET BALANCE = 9999 WHERE ID = 50000;", "PK UPDATE (ID=50000)");

    // PK DELETE (O(1) hash)
    run_exec(db, "DELETE FROM PK_USERS WHERE ID = 99999;", "PK DELETE (ID=99999)");

    // Verify PK uniqueness enforcement: duplicate insert should fail
    bool dup_failed = expect_query_failure(db, "INSERT INTO PK_USERS VALUES (1, 'dup', 0, 0);",
                                           "PK duplicate INSERT rejected");
    if (!dup_failed)
        cout << "[WARN] PK uniqueness enforcement not working!\n";

    // Full table scan on PK table
    run_query(db, "SELECT * FROM PK_USERS;", "PK full table scan (" + to_string(pk_rows) + " rows)");

    // SELECT * validation on PK table
    vector<string> rows;
    bool q = query_rows(db, "SELECT * FROM PK_USERS WHERE ID = 1;", rows);
    if (q && !rows.empty()) {
        cout << "[PASS] PK SELECT validation: " << rows[0] << "\n";
    }

    return true;
}

static bool run_insert_benchmark(FlexQL *db, long long target_rows) {
    // Drop the table first if it already exists so re-runs start fresh
    run_exec(db, "DROP TABLE BIG_USERS;", "DROP TABLE BIG_USERS (if exists)");

    if (!run_exec(
            db,
            "CREATE TABLE BIG_USERS(ID DECIMAL, NAME VARCHAR(64), EMAIL VARCHAR(64), BALANCE DECIMAL, EXPIRES_AT DECIMAL);",
            "CREATE TABLE BIG_USERS")) {
        return false;
    }

    cout << "\nStarting insertion benchmark for " << target_rows << " rows...\n";
    auto bench_start = high_resolution_clock::now();

    long long inserted = 0;
    long long progress_step = target_rows / 10;
    if (progress_step <= 0) {
        progress_step = 1;
    }
    long long next_progress = progress_step;

    // Pre-allocate SQL buffer: 25K rows × ~85 bytes/row ≈ 2.1 MB
    string sql;
    sql.reserve(INSERT_BATCH_SIZE * 90 + 32);

    // Reusable char buffer for integer-to-string conversion (avoids std::to_string allocs)
    char ibuf[32];

    while (inserted < target_rows) {
        sql = "INSERT INTO BIG_USERS VALUES ";

        int in_batch = 0;
        while (in_batch < INSERT_BATCH_SIZE && inserted < target_rows) {
            long long id = inserted + 1;
            int n = snprintf(ibuf, sizeof(ibuf), "%lld", id);
            sql += '(';
            sql.append(ibuf, n);                        // ID
            sql += ",'user";
            sql.append(ibuf, n);                        // NAME
            sql += "','user";
            sql.append(ibuf, n);                        // EMAIL
            sql += "@mail.com',";
            // BALANCE: 1000 + (id % 10000)
            int bal = (int)(1000 + (id % 10000));
            n = snprintf(ibuf, sizeof(ibuf), "%d", bal);
            sql.append(ibuf, n);
            sql += ",1893456000)";
            inserted++;
            in_batch++;
            if (in_batch < INSERT_BATCH_SIZE && inserted < target_rows)
                sql += ',';
        }
        sql += ';';

        char *errMsg = nullptr;
        if (flexql_exec(db, sql.c_str(), nullptr, nullptr, &errMsg) != FLEXQL_OK) {
            cout << "[FAIL] INSERT BIG_USERS batch -> " << (errMsg ? errMsg : "unknown error") << "\n";
            if (errMsg) {
                flexql_free(errMsg);
            }
            return false;
        }

        if (inserted >= next_progress || inserted == target_rows) {
            cout << "Progress: " << inserted << "/" << target_rows << "\n";
            next_progress += progress_step;
        }
    }

    auto bench_end = high_resolution_clock::now();
    long long elapsed = duration_cast<milliseconds>(bench_end - bench_start).count();
    long long throughput = (elapsed > 0) ? (target_rows * 1000LL / elapsed) : target_rows;

    cout << "[PASS] INSERT benchmark complete\n";
    cout << "Rows inserted: " << target_rows << "\n";
    cout << "Elapsed: " << elapsed << " ms\n";
    cout << "Throughput: " << throughput << " rows/sec\n";

    return true;
}

// ── Timed helpers (microsecond precision) ─────────────────────────────────────

static long long timed_query(FlexQL *db, const char *sql, long long &rows) {
    QueryStats stats;
    char *errMsg = nullptr;
    auto t0 = high_resolution_clock::now();
    int rc = flexql_exec(db, sql, count_rows_callback, &stats, &errMsg);
    auto t1 = high_resolution_clock::now();
    if (rc != FLEXQL_OK) {
        cout << "[FAIL] " << sql << " -> " << (errMsg ? errMsg : "?") << "\n";
        if (errMsg) flexql_free(errMsg);
        return -1;
    }
    rows = stats.rows;
    return duration_cast<microseconds>(t1 - t0).count();
}

static long long timed_exec_us(FlexQL *db, const char *sql) {
    char *errMsg = nullptr;
    auto t0 = high_resolution_clock::now();
    int rc = flexql_exec(db, sql, nullptr, nullptr, &errMsg);
    auto t1 = high_resolution_clock::now();
    if (rc != FLEXQL_OK) {
        if (errMsg) flexql_free(errMsg);
        return -1;
    }
    return duration_cast<microseconds>(t1 - t0).count();
}

static void print_bench(const char *label, long long us, long long rows = -1) {
    if (us < 0) { printf("[FAIL] %s\n", label); return; }
    if (rows >= 0)
        printf("[PASS] %-55s %8lld rows  %8lld.%03lld ms\n",
               label, rows, us / 1000, us % 1000);
    else
        printf("[PASS] %-55s            %8lld.%03lld ms\n",
               label, us / 1000, us % 1000);
}

// ── SELECT query benchmarks on existing BIG_USERS table ──────────────────────

static void run_select_benchmarks(FlexQL *db, long long total_rows) {
    cout << "\n\n[[ SELECT Query Benchmarks on " << total_rows << " rows ]]\n\n";
    long long rows;

    // COUNT(*)
    auto us = timed_query(db, "SELECT COUNT(*) FROM BIG_USERS;", rows);
    print_bench("SELECT COUNT(*)", us, rows);

    // Point lookup (equality on ID)
    long long mid = total_rows / 2;
    char sql[512];
    snprintf(sql, sizeof(sql),
        "SELECT * FROM BIG_USERS WHERE ID = %lld;", mid);
    us = timed_query(db, sql, rows);
    print_bench("SELECT WHERE ID = <mid>", us, rows);

    // Range scan (BALANCE > threshold)
    us = timed_query(db,
        "SELECT * FROM BIG_USERS WHERE BALANCE > 10000;", rows);
    print_bench("SELECT WHERE BALANCE > 10000", us, rows);

    // BETWEEN
    us = timed_query(db,
        "SELECT * FROM BIG_USERS WHERE BALANCE BETWEEN 5000 AND 6000;", rows);
    print_bench("SELECT WHERE BALANCE BETWEEN 5000 AND 6000", us, rows);

    // IN (string column)
    us = timed_query(db,
        "SELECT NAME FROM BIG_USERS WHERE ID IN (1, 1000, 50000, 999999);", rows);
    print_bench("SELECT WHERE ID IN (4 values)", us, rows);

    // LIKE
    us = timed_query(db,
        "SELECT * FROM BIG_USERS WHERE NAME LIKE 'user1%';", rows);
    print_bench("SELECT WHERE NAME LIKE 'user1%'", us, rows);

    // ORDER BY + LIMIT
    us = timed_query(db,
        "SELECT * FROM BIG_USERS ORDER BY BALANCE DESC LIMIT 10;", rows);
    print_bench("SELECT ORDER BY BALANCE DESC LIMIT 10", us, rows);

    // ORDER BY + LIMIT + OFFSET
    us = timed_query(db,
        "SELECT * FROM BIG_USERS ORDER BY ID DESC LIMIT 100 OFFSET 1000;", rows);
    print_bench("SELECT ORDER BY ID DESC LIMIT 100 OFFSET 1000", us, rows);

    // DISTINCT (limited unique values due to BALANCE = 1000 + id%10000)
    us = timed_query(db,
        "SELECT DISTINCT BALANCE FROM BIG_USERS LIMIT 100;", rows);
    print_bench("SELECT DISTINCT BALANCE LIMIT 100", us, rows);

    // Aggregate: SUM
    us = timed_query(db, "SELECT SUM(BALANCE) FROM BIG_USERS;", rows);
    print_bench("SELECT SUM(BALANCE)", us, rows);

    // Aggregate: AVG
    us = timed_query(db, "SELECT AVG(BALANCE) FROM BIG_USERS;", rows);
    print_bench("SELECT AVG(BALANCE)", us, rows);

    // Aggregate: MIN
    us = timed_query(db, "SELECT MIN(BALANCE) FROM BIG_USERS;", rows);
    print_bench("SELECT MIN(BALANCE)", us, rows);

    // Aggregate: MAX
    us = timed_query(db, "SELECT MAX(BALANCE) FROM BIG_USERS;", rows);
    print_bench("SELECT MAX(BALANCE)", us, rows);

    // UPDATE single row
    snprintf(sql, sizeof(sql),
        "UPDATE BIG_USERS SET BALANCE = 99999 WHERE ID = %lld;", mid);
    us = timed_exec_us(db, sql);
    print_bench("UPDATE single row (WHERE ID = <mid>)", us);

    // DELETE single row
    snprintf(sql, sizeof(sql),
        "DELETE FROM BIG_USERS WHERE ID = %lld;", total_rows);
    us = timed_exec_us(db, sql);
    print_bench("DELETE single row (last row)", us);
}

// ── Concurrency benchmarks ───────────────────────────────────────────────────

static bool conc_exec(FlexQL *db, const char *sql) {
    char *err = nullptr;
    int rc = flexql_exec(db, sql, nullptr, nullptr, &err);
    if (rc != FLEXQL_OK && err) flexql_free(err);
    return rc == FLEXQL_OK;
}

static void run_concurrent_benchmarks() {
    cout << "\n\n[[ Concurrency Benchmarks ]]\n";

    // --- Concurrent reads (1, 4, 8 threads) ---
    for (int nthreads : {1, 4, 8}) {
        printf("\n--- Concurrent READ (%d threads x 20 queries) ---\n", nthreads);

        vector<thread> threads;
        vector<long long> latencies(nthreads, 0);
        const int QPS = 20;

        auto wall_t0 = high_resolution_clock::now();
        for (int t = 0; t < nthreads; t++) {
            threads.emplace_back([t, &latencies, QPS]() {
                FlexQL *db = nullptr;
                if (flexql_open("127.0.0.1", 9000, &db) != FLEXQL_OK) return;
                long long total_us = 0;
                for (int i = 0; i < QPS; i++) {
                    char sql[128];
                    snprintf(sql, sizeof(sql),
                        "SELECT * FROM BIG_USERS WHERE ID = %d;",
                        t * 10000 + i + 1);
                    long long rows;
                    long long us = timed_query(db, sql, rows);
                    if (us >= 0) total_us += us;
                }
                latencies[t] = total_us;
                flexql_close(db);
            });
        }
        for (auto &th : threads) th.join();
        auto wall_t1 = high_resolution_clock::now();
        long long wall_ms = duration_cast<milliseconds>(wall_t1 - wall_t0).count();

        long long total_us = 0;
        for (auto l : latencies) total_us += l;
        int total_ops = nthreads * QPS;
        printf("  Total queries: %d  |  Avg latency: %.3f ms  |  Wall: %lld ms\n",
               total_ops, (double)total_us / total_ops / 1000.0, wall_ms);

        // Also measure wall-clock for all threads doing COUNT(*)
        threads.clear();
        auto t0 = high_resolution_clock::now();
        for (int t = 0; t < nthreads; t++) {
            threads.emplace_back([]() {
                FlexQL *db = nullptr;
                if (flexql_open("127.0.0.1", 9000, &db) != FLEXQL_OK) return;
                long long rows;
                timed_query(db, "SELECT COUNT(*) FROM BIG_USERS;", rows);
                flexql_close(db);
            });
        }
        for (auto &th : threads) th.join();
        auto t1 = high_resolution_clock::now();
        printf("  %d concurrent COUNT(*): %lld ms wall-clock\n",
               nthreads,
               (long long)duration_cast<milliseconds>(t1 - t0).count());
    }

    // --- Mixed read/write workload ---
    for (auto [nr, nw] : vector<pair<int,int>>{{4, 1}, {4, 4}}) {
        printf("\n--- Mixed workload (%d readers + %d writers, 3 sec) ---\n", nr, nw);

        atomic<bool> done{false};
        atomic<long long> read_ops{0}, write_ops{0};
        vector<thread> threads;

        auto t0 = high_resolution_clock::now();

        for (int t = 0; t < nr; t++) {
            threads.emplace_back([&done, &read_ops, t]() {
                FlexQL *db = nullptr;
                if (flexql_open("127.0.0.1", 9000, &db) != FLEXQL_OK) return;
                while (!done.load(memory_order_relaxed)) {
                    char sql[128];
                    snprintf(sql, sizeof(sql),
                        "SELECT * FROM BIG_USERS WHERE ID = %lld;",
                        (long long)(t * 100000 + (read_ops.load() % 100000) + 1));
                    long long rows;
                    timed_query(db, sql, rows);
                    read_ops.fetch_add(1, memory_order_relaxed);
                }
                flexql_close(db);
            });
        }
        for (int t = 0; t < nw; t++) {
            threads.emplace_back([&done, &write_ops, t]() {
                FlexQL *db = nullptr;
                if (flexql_open("127.0.0.1", 9000, &db) != FLEXQL_OK) return;
                while (!done.load(memory_order_relaxed)) {
                    char sql[256];
                    long long wn = write_ops.load();
                    snprintf(sql, sizeof(sql),
                        "UPDATE BIG_USERS SET BALANCE = %lld WHERE ID = %lld;",
                        5000 + (wn % 5000),
                        (long long)(t * 50000 + (wn % 50000) + 1));
                    conc_exec(db, sql);
                    write_ops.fetch_add(1, memory_order_relaxed);
                }
                flexql_close(db);
            });
        }

        this_thread::sleep_for(seconds(3));
        done.store(true, memory_order_relaxed);
        for (auto &th : threads) th.join();

        auto t1 = high_resolution_clock::now();
        long long wall_ms = duration_cast<milliseconds>(t1 - t0).count();
        printf("  Read ops:  %lld (%.0f/sec)  |  Write ops: %lld (%.0f/sec)  |  Total: %.0f ops/sec\n",
               read_ops.load(), (double)read_ops.load() * 1000.0 / wall_ms,
               write_ops.load(), (double)write_ops.load() * 1000.0 / wall_ms,
               (double)(read_ops.load() + write_ops.load()) * 1000.0 / wall_ms);
    }

    // --- Concurrent inserts into separate tables ---
    for (int nthreads : {1, 4, 8}) {
        printf("\n--- Concurrent INSERT (%d threads x 50K rows) ---\n", nthreads);

        vector<thread> threads;
        vector<long long> elapsed(nthreads);

        auto wall_t0 = high_resolution_clock::now();
        for (int t = 0; t < nthreads; t++) {
            threads.emplace_back([t, &elapsed]() {
                FlexQL *db = nullptr;
                if (flexql_open("127.0.0.1", 9000, &db) != FLEXQL_OK) return;

                char tbl[64];
                snprintf(tbl, sizeof(tbl), "CONC_T%d", t);
                char buf[128];
                snprintf(buf, sizeof(buf), "DROP TABLE IF EXISTS %s;", tbl);
                conc_exec(db, buf);
                snprintf(buf, sizeof(buf),
                    "CREATE TABLE %s(ID DECIMAL, VAL VARCHAR(32), EXPIRES_AT DECIMAL);", tbl);
                conc_exec(db, buf);

                const int ROWS = 50000, BSZ = 5000;
                string sql;
                sql.reserve(BSZ * 50 + 40);
                char vbuf[64];

                auto t0 = high_resolution_clock::now();
                long long ins = 0;
                while (ins < ROWS) {
                    sql.clear();
                    sql += "INSERT INTO ";
                    sql += tbl;
                    sql += " VALUES ";
                    int b = 0;
                    while (b < BSZ && ins < ROWS) {
                        int n = snprintf(vbuf, sizeof(vbuf),
                            "(%lld,'val%lld',1893456000)", ins + 1, ins + 1);
                        sql.append(vbuf, n);
                        ins++; b++;
                        if (b < BSZ && ins < ROWS) sql += ',';
                    }
                    sql += ';';
                    conc_exec(db, sql.c_str());
                }
                auto t1 = high_resolution_clock::now();
                elapsed[t] = duration_cast<milliseconds>(t1 - t0).count();

                snprintf(buf, sizeof(buf), "DROP TABLE IF EXISTS %s;", tbl);
                conc_exec(db, buf);
                flexql_close(db);
            });
        }
        for (auto &th : threads) th.join();
        auto wall_t1 = high_resolution_clock::now();
        long long wall = duration_cast<milliseconds>(wall_t1 - wall_t0).count();
        long long total_rows = (long long)nthreads * 50000;
        printf("  Wall: %lld ms  |  Total: %lld rows  |  Throughput: %.0f rows/sec\n",
               wall, total_rows,
               wall > 0 ? (double)total_rows * 1000.0 / wall : 0.0);
    }
}

// ── Main ─────────────────────────────────────────────────────────────────────

int main(int argc, char **argv) {
    FlexQL *db = nullptr;
    long long insert_rows = DEFAULT_INSERT_ROWS;
    bool run_unit_tests_only = false;

    if (argc > 1) {
        string arg1 = argv[1];
        if (arg1 == "--unit-test") {
            run_unit_tests_only = true;
        } else {
            insert_rows = atoll(argv[1]);
            if (insert_rows <= 0) {
                cout << "Invalid row count. Use a positive integer or --unit-test.\n";
                return 1;
            }
        }
    }

    if (flexql_open("127.0.0.1", 9000, &db) != FLEXQL_OK) {
        cout << "Cannot open FlexQL\n";
        return 1;
    }

    cout << "Connected to FlexQL\n";

    if (run_unit_tests_only) {
        bool ok = run_data_level_unit_tests(db);
        flexql_close(db);
        return ok ? 0 : 1;
    }

    cout << "Running SQL subset checks plus insertion benchmark...\n";
    cout << "Target insert rows: " << insert_rows << "\n\n";

    if (!run_insert_benchmark(db, insert_rows)) {
        flexql_close(db);
        return 1;
    }

    // SELECT query benchmarks on the dataset (filtered, aggregates, etc.)
    run_select_benchmarks(db, insert_rows);

    if (!run_pk_benchmark(db, insert_rows)) {
        flexql_close(db);
        return 1;
    }

    // Concurrency benchmarks (multi-threaded readers/writers)
    run_concurrent_benchmarks();

    if (!run_data_level_unit_tests(db)) {
        flexql_close(db);
        return 1;
    }

    flexql_close(db);
    return 0;
}
