#include "flexql.h"
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <iostream>
#include <time.h>

// ── Row collector ─────────────────────────────────────────────────────────────
struct Collector {
    std::vector<std::string>              col_names;
    std::vector<std::vector<std::string>> rows;
};

static int collect_callback(void* arg, int ncols, char** vals, char** names) {
    auto* c = static_cast<Collector*>(arg);
    if (c->col_names.empty()) {
        for (int i = 0; i < ncols; ++i)
            c->col_names.push_back(names[i] ? names[i] : "");
    }
    c->rows.push_back({});
    for (int i = 0; i < ncols; ++i)
        c->rows.back().push_back(vals[i] ? vals[i] : "NULL");
    return 0;
}

// ── SQLite-style bordered table ───────────────────────────────────────────────
static void print_table(const std::vector<std::string>& col_names,
                         const std::vector<std::vector<std::string>>& rows) {
    int ncols = (int)col_names.size();
    if (ncols == 0) return;

    std::vector<size_t> widths(ncols);
    for (int i = 0; i < ncols; ++i)
        widths[i] = col_names[i].size();
    for (const auto& row : rows)
        for (int i = 0; i < ncols && i < (int)row.size(); ++i)
            widths[i] = std::max(widths[i], row[i].size());

    auto print_sep = [&]() {
        printf("+");
        for (int i = 0; i < ncols; ++i) {
            for (size_t j = 0; j < widths[i] + 2; ++j) printf("-");
            printf("+");
        }
        printf("\n");
    };

    print_sep();
    printf("|");
    for (int i = 0; i < ncols; ++i)
        printf(" %-*s |", (int)widths[i], col_names[i].c_str());
    printf("\n");
    print_sep();

    for (const auto& row : rows) {
        printf("|");
        for (int i = 0; i < ncols; ++i) {
            const char* v = (i < (int)row.size()) ? row[i].c_str() : "";
            printf(" %-*s |", (int)widths[i], v);
        }
        printf("\n");
    }
    print_sep();
}

// ── Main REPL ─────────────────────────────────────────────────────────────────
int main(int argc, char* argv[]) {
    const char* host = "127.0.0.1";
    int         port = 9000;

    if (argc > 1) host = argv[1];
    if (argc > 2) port = std::atoi(argv[2]);

    FlexQL* db     = nullptr;
    char*   errmsg = nullptr;

    int rc = flexql_open(host, port, &db);
    if (rc != FLEXQL_OK) {
        fprintf(stderr, "Cannot connect to FlexQL server at %s:%d\n", host, port);
        return 1;
    }
    printf("Connected to FlexQL server at %s:%d\n", host, port);

    std::string line;
    std::string buffer;

    while (true) {
        printf(buffer.empty() ? "flexql> " : "     -> ");
        fflush(stdout);

        if (!std::getline(std::cin, line)) break; // EOF / Ctrl-D

        // Trim leading/trailing whitespace from the line for dot-command detection
        std::string ltrimmed = line;
        while (!ltrimmed.empty() && std::isspace((unsigned char)ltrimmed.front()))
            ltrimmed.erase(ltrimmed.begin());
        while (!ltrimmed.empty() && std::isspace((unsigned char)ltrimmed.back()))
            ltrimmed.pop_back();

        // Dot-commands always take priority (even mid-buffer)
        if (ltrimmed == ".exit" || ltrimmed == ".quit") break;
        if (ltrimmed == ".help") {
            printf("Commands: .exit  .quit  .help  .tables  .clear\n");
            printf("SQL:      CREATE TABLE, INSERT, SELECT [DISTINCT] [LIMIT n]\n");
            printf("          UPDATE ... SET, DELETE, TRUNCATE TABLE\n");
            printf("          SELECT COUNT(*) / SUM / AVG / MIN / MAX\n");
            printf("          WHERE col LIKE 'pattern%%'  (%%=any, _=one char)\n");
            printf("          ORDER BY, GROUP BY, HAVING\n");
            printf("          INNER JOIN / LEFT JOIN, SHOW TABLES, DESCRIBE\n");
            continue;
        }
        if (ltrimmed == ".tables") {
            // Shortcut for SHOW TABLES
            line = "SHOW TABLES;";
        }
        if (ltrimmed == ".clear") {
            if (!buffer.empty()) {
                buffer.clear();
                printf("Buffer cleared.\n");
            }
            continue;
        }

        buffer += line;
        // Trim trailing whitespace to find semicolon
        std::string trimmed = buffer;
        while (!trimmed.empty() && std::isspace((unsigned char)trimmed.back()))
            trimmed.pop_back();

        if (trimmed.empty()) { buffer.clear(); continue; }

        if (trimmed.back() == ';') {
            Collector collector;
            errmsg = nullptr;

            struct timespec t0, t1;
            clock_gettime(CLOCK_MONOTONIC, &t0);
            rc = flexql_exec(db, trimmed.c_str(), collect_callback, &collector, &errmsg);
            clock_gettime(CLOCK_MONOTONIC, &t1);
            double elapsed = (t1.tv_sec - t0.tv_sec) + (t1.tv_nsec - t0.tv_nsec) * 1e-9;

            if (rc != FLEXQL_OK) {
                fprintf(stderr, "ERROR: %s\n", errmsg ? errmsg : "(unknown)");
                flexql_free(errmsg);
            } else if (flexql_last_was_query(db)) {
                // Build column name list: prefer callback-captured names (non-empty result),
                // fall back to metadata from the COLS header (empty result set).
                std::vector<std::string> col_names = collector.col_names;
                if (col_names.empty()) {
                    int n = flexql_last_col_count(db);
                    for (int i = 0; i < n; ++i) {
                        const char* cn = flexql_last_col_name(db, i);
                        col_names.push_back(cn ? cn : "");
                    }
                }

                printf("\n"); // move table off the prompt line
                if (collector.rows.empty()) {
                    if (!col_names.empty()) print_table(col_names, {});
                    printf("Empty set (%.3f sec)\n", elapsed);
                } else {
                    print_table(col_names, collector.rows);
                    size_t n = collector.rows.size();
                    printf("%zu row%s in set (%.3f sec)\n", n, n == 1 ? "" : "s", elapsed);
                }
            } else {
                printf("Query OK (%.3f sec)\n", elapsed);
            }

            buffer.clear();
        } else {
            buffer += ' '; // multi-line continuation
        }
    }

    flexql_close(db);
    printf("Connection closed\n");
    return 0;
}
