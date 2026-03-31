# FlexQL — A Flexible SQL-like Database Driver

A client-server database engine implemented in C++17 from scratch. Supports a rich SQL subset with compound predicates, aggregates, GROUP BY, LEFT JOIN, TTL expiration, LRU query caching, Write-Ahead Log (WAL) persistence, binary snapshots, and multi-threaded concurrency.

---

## Table of Contents

1. [Quick Start](#quick-start)
2. [Project Structure](#project-structure)
3. [Architecture & Design Decisions](#architecture--design-decisions)
   - [Storage Design](#storage-design)
   - [Persistence — WAL + Snapshots](#persistence--wal--snapshots)
   - [Indexing](#indexing)
   - [Caching Strategy](#caching-strategy)
   - [TTL / Expiration](#ttl--expiration)
   - [Multithreading](#multithreading)
4. [Supported SQL Commands](#supported-sql-commands)
5. [Client C/C++ API](#client-cc-api)
6. [Building](#building)
7. [Running](#running)
8. [Performance](#performance)

---

## Quick Start

```bash
# Build both binaries
make all -j$(nproc)

# Terminal 1 — start the server on port 9000
./bin/flexql-server 9000

# Terminal 2 — connect with the interactive REPL
./bin/flexql-client 127.0.0.1 9000
```

```
Connected to FlexQL server at 127.0.0.1:9000
flexql> CREATE TABLE STUDENT (ID INT PRIMARY KEY NOT NULL, NAME TEXT NOT NULL);
Query OK (0.001 sec)
flexql> INSERT INTO STUDENT VALUES (1, 'Alice', 1900000000);
Query OK (0.000 sec)
flexql> INSERT INTO STUDENT VALUES (2, 'Bob', 1900000000);
Query OK (0.000 sec)
flexql> SELECT * FROM STUDENT WHERE ID >= 1 ORDER BY NAME DESC;

+----+-------+
| ID | NAME  |
+----+-------+
| 2  | Bob   |
| 1  | Alice |
+----+-------+
2 rows in set (0.000 sec)
flexql> .exit
Connection closed
```

---

## Project Structure

```
flexql/
├── Makefile                          # -O3 -march=native -std=c++17 build
├── .clangd                           # IDE include-path hints
├── config/
│   └── server.conf                   # port, threads, WAL path, snapshot dir, cache size
├── scripts/
│   ├── build.sh
│   ├── run_server.sh
│   └── run_client.sh
├── include/
│   ├── common/types.h                # TableSchema, ColumnDef, QueryResult
│   ├── storage/
│   │   ├── arena.h                   # StringArena — bump-pointer slab allocator
│   │   ├── table.h                   # Table (columnar storage, insert/scan/delete/compact)
│   │   ├── database.h                # Database singleton (table registry)
│   │   ├── wal.h                     # WalWriter / WalReader (zero-copy writev, hw CRC32c)
│   │   └── snapshot.h                # Snapshot::write / load / find_latest
│   ├── index/pk_index.h              # PrimaryIndex — FNV-64 flat open-addressing hash map
│   ├── parser/
│   │   ├── token.h                   # Tokenizer, Token, TokenType
│   │   ├── ast.h                     # Statement AST (Predicate tree, InsertStmt flat layout)
│   │   └── parser.h                  # Recursive descent Parser
│   ├── query/executor.h              # Executor — routes statements to handlers
│   ├── network/
│   │   ├── server.h                  # TcpServer
│   │   └── protocol.h                # Wire encode/decode
│   ├── cache/lru_cache.h             # LRUCache<K,V> template
│   ├── expiration/ttl_manager.h      # Background TTL sweeper
│   ├── concurrency/thread_pool.h     # Fixed-size thread pool
│   └── utils/string_utils.h
├── src/
│   ├── server/main.cpp               # startup recovery (snapshot + WAL replay)
│   ├── network/
│   │   ├── server.cpp                # TCP accept loop, WAL append, TCP_NODELAY
│   │   └── protocol.cpp              # recv_sql (65KB buffer), send_response
│   ├── parser/
│   │   ├── token.cpp                 # Lexer
│   │   └── parser.cpp                # Fast INSERT path + full recursive descent
│   ├── query/executor.cpp            # exec_select, exec_group_by, exec_join, …
│   ├── storage/
│   │   ├── table.cpp                 # insert_flat (hot path), scan, matches_pred
│   │   ├── database.cpp
│   │   ├── wal.cpp                   # writev + SSE4.2 CRC32c
│   │   └── snapshot.cpp              # binary checkpoint
│   ├── cache/lru_cache.cpp
│   ├── expiration/ttl_manager.cpp
│   ├── concurrency/thread_pool.cpp
│   ├── utils/string_utils.cpp
│   └── client/
│       ├── flexql.h                  # Public C API
│       ├── flexql.cpp                # flexql_open / close / exec / free
│       └── client.cpp                # REPL main()
├── data/
│   ├── wal/                          # wal.log (created at first server start)
│   └── snapshots/                    # snap_<lsn>.bin files
└── bench/                            # pre-compiled benchmark suite (submodule)
    ├── benchmark                     # 1M-row INSERT benchmark + 22 unit tests
    ├── benchmark_flexql.cpp
    ├── flexql.h / flexql.cpp         # C client library used by benchmark
    └── ref_server                    # SQLite reference server
```

---

## Architecture & Design Decisions

### Storage Design

**Choice: Columnar in-memory layout — flat parallel arrays + StringArena**

Each table stores data in two flat vectors instead of per-row heap allocations:

```
rows_meta_[i]         →  RowMeta { int64_t expires_at; bool deleted; }
rows_vals_[i*ncols+c] →  string_view into StringArena (stable pointer)
```

All string content lives in a `StringArena` — a bump-pointer slab allocator (4 MB slabs) that never moves memory. `insert_flat()` is the hot path: it takes a flat array of `string_view`s pointing directly into the received SQL buffer, computes CRC32c for WAL, calls `writev(header, sql)`, then interns each value into the arena in a single pass.

**Why columnar over `vector<Row>`?**
Eliminates 1M per-row `vector<string>` heap allocations for a 1M-row insert. All 3M string_views for a 1M×3-col table live in one contiguous pre-allocated array — cache-line friendly sequential access during scan.

---

### Persistence — WAL + Snapshots

**Write-Ahead Log (WAL):**
Every mutation (INSERT, UPDATE, DELETE, CREATE, DROP, TRUNCATE) is appended to `data/wal/wal.log` before execution. Each record is: `[magic:4][lsn:8][len:4][crc32c:4][sql:len]`.

Key implementation details:
- **Zero-copy write**: `writev(header_on_stack, sql_buffer)` — no memcpy of the SQL payload
- **Hardware CRC32c**: SSE4.2 `_mm_crc32_u64` instruction — ~10 GB/s throughput (~9ms for 200 batches of 275KB)
- **No `fdatasync` in hot path**: `write()` to the kernel page cache is sufficient for process-crash safety. `fdatasync()` is called only at shutdown/checkpoint via `sync()`
- WAL overhead over pure RAM: **~55ms** for a 1M-row benchmark

**Snapshots:**
Binary checkpoints (`snap_<lsn>.bin`) capture the full table state at a specific LSN. On restart: load latest snapshot → replay WAL records with LSN > snapshot LSN. Snapshot interval is configurable (`snapshot_interval = 100000` records).

---

### Indexing

**Choice: FNV-64 flat open-addressing hash map (`include/index/pk_index.h`)**

Keys are `string_view`s into the StringArena (stable pointers). Uses linear probing with tombstones, max load factor 0.6, doubles capacity on rehash.

```
PK value (string_view into arena) → row index (size_t)   O(1) average
```

**Why a custom flat map over `unordered_map`?**
`unordered_map` allocates a linked-list node per entry — 1M inserts = 1M `new` calls. The flat open-addressing array keeps all slots contiguous, eliminating pointer chasing and reducing allocator pressure by ~80%.

---

### Caching Strategy

**Choice: LRU query result cache, 1024 entries, per-table invalidation**

The cache stores the full `QueryResult` keyed by the normalized SQL string plus table name tag.

```
key:   "BIG_USERS|SELECT * FROM BIG_USERS WHERE ID = 5|"
value: QueryResult { column_names, rows }
```

Doubly-linked list (access order) + `unordered_map` (O(1) lookup). On every INSERT/UPDATE/DELETE, all cache entries tagged with that table are evicted.

---

### TTL / Expiration

**Choice: Lazy expiry during scan + background sweeper**

The last value in a multi-value INSERT is interpreted as a Unix expiration timestamp if it exceeds the schema column count:

```sql
-- 2 schema columns + implicit expires_at
INSERT INTO T VALUES (1, 'Alice', 1900000000);
```

During scan, rows where `expires_at > 0 && expires_at <= now()` are silently skipped — zero background work on the read path. A `TtlManager` thread wakes every 60 seconds to call `Table::compact()`, which physically removes tombstoned and expired rows and rebuilds the PK index.

---

### Multithreading

**Choice: Fixed thread pool + per-table `shared_mutex` + single WAL mutex**

```
accept() loop (main thread)
    └── submit(client_fd) → ThreadPool (N = hardware_concurrency workers)
            ├── Thread 0: handle_client(fd_A) — parse → WAL append → execute
            ├── Thread 1: handle_client(fd_B)
            └── ...
```

- **SELECT** acquires `shared_lock` — multiple concurrent readers
- **INSERT/UPDATE/DELETE/compact** acquire `unique_lock` — exclusive writer
- **WAL**: single `std::mutex` guards the `writev` call; one writer at a time, no contention issue since WAL appends are fast (~57µs each)
- **JOIN**: two-table locks always acquired in alphabetical order to prevent deadlock

---

## Supported SQL Commands

### CREATE TABLE

```sql
CREATE TABLE T (ID INT PRIMARY KEY NOT NULL, NAME TEXT NOT NULL, AGE INT);
CREATE TABLE IF NOT EXISTS T (...);
-- Supported types: INT, DECIMAL, VARCHAR(n), TEXT, DATETIME
```

### INSERT

```sql
-- Single row (expires at Unix timestamp, or 0 = never expires)
INSERT INTO T VALUES (1, 'Alice', 30, 1900000000);

-- Multi-row batch
INSERT INTO T VALUES (1,'Alice',30,ts), (2,'Bob',25,ts), ...;
```

### SELECT

```sql
-- Basic
SELECT * FROM T;
SELECT ID, NAME FROM T;

-- Compound WHERE (AND / OR / NOT)
SELECT * FROM T WHERE AGE >= 18 AND DEPT = 'Eng';
SELECT * FROM T WHERE NOT (AGE < 18 OR STATUS = 'inactive');

-- BETWEEN / IN / IS NULL
SELECT * FROM T WHERE AGE BETWEEN 20 AND 30;
SELECT * FROM T WHERE DEPT IN ('Eng', 'HR', 'Sales');
SELECT * FROM T WHERE MANAGER IS NULL;
SELECT * FROM T WHERE MANAGER IS NOT NULL;

-- LIKE
SELECT * FROM T WHERE NAME LIKE 'Ali%';
SELECT * FROM T WHERE EMAIL LIKE '%@example.com';

-- ORDER BY / LIMIT / OFFSET
SELECT * FROM T ORDER BY AGE DESC LIMIT 10 OFFSET 5;

-- DISTINCT
SELECT DISTINCT DEPT FROM T;

-- Aggregate functions
SELECT COUNT(*) FROM T WHERE AGE > 18;
SELECT SUM(SALARY), AVG(SALARY), MIN(SALARY), MAX(SALARY) FROM T;

-- GROUP BY + HAVING
SELECT DEPT, COUNT(*), AVG(SALARY) FROM T GROUP BY DEPT HAVING AVG(SALARY) > 70000;

-- INNER JOIN
SELECT * FROM A INNER JOIN B ON A.ID = B.AID WHERE B.STATUS = 'active' ORDER BY A.ID;

-- LEFT OUTER JOIN
SELECT * FROM A LEFT JOIN B ON A.ID = B.AID;
```

### UPDATE / DELETE / TRUNCATE

```sql
UPDATE T SET NAME = 'Bob', AGE = 26 WHERE ID = 2;
DELETE FROM T WHERE AGE < 18;
TRUNCATE TABLE T;
```

### DDL

```sql
DROP TABLE T;
DROP TABLE IF EXISTS T;
ALTER TABLE T ADD COLUMN SCORE INT;
ALTER TABLE T DROP COLUMN SCORE;
USE mydb;          -- acknowledged (single-database server)
```

### Introspection

```sql
SHOW TABLES;
SHOW DATABASES;
DESCRIBE T;
```

### REPL Commands

| Command | Description |
|---|---|
| `.exit` / `.quit` | Disconnect and exit |
| `.help` | Show available commands |

---

## Client C/C++ API

Include `src/client/flexql.h` and link against `src/client/flexql.cpp` + `src/network/protocol.cpp`.

```c
#include "flexql.h"

// Error codes
#define FLEXQL_OK    0
#define FLEXQL_ERROR 1

// Opaque connection handle
typedef struct FlexQL FlexQL;

int  flexql_open (const char *host, int port, FlexQL **db);
int  flexql_close(FlexQL *db);
int  flexql_exec (FlexQL *db, const char *sql,
                  int (*callback)(void *arg, int ncols,
                                  char **values, char **col_names),
                  void *arg, char **errmsg);
void flexql_free (void *ptr);
```

**Example:**

```c
#include <stdio.h>
#include "flexql.h"

int print_row(void *arg, int n, char **vals, char **cols) {
    for (int i = 0; i < n; i++)
        printf("%s = %s\n", cols[i], vals[i] ? vals[i] : "NULL");
    printf("\n");
    return 0; // return 1 to abort
}

int main(void) {
    FlexQL *db;
    char *errmsg = NULL;

    if (flexql_open("127.0.0.1", 9000, &db) != FLEXQL_OK) {
        fprintf(stderr, "connection failed\n");
        return 1;
    }

    flexql_exec(db,
        "CREATE TABLE STUDENT (ID INT PRIMARY KEY NOT NULL, NAME TEXT NOT NULL);",
        NULL, NULL, &errmsg);

    flexql_exec(db,
        "INSERT INTO STUDENT VALUES (1,'Alice',1900000000),(2,'Bob',1900000000);",
        NULL, NULL, &errmsg);

    flexql_exec(db, "SELECT * FROM STUDENT ORDER BY NAME;", print_row, NULL, &errmsg);

    if (errmsg) { fprintf(stderr, "Error: %s\n", errmsg); flexql_free(errmsg); }
    flexql_close(db);
    return 0;
}
```

---

## Building

**Requirements:** g++ with C++17 + SSE4.2 support (any x86-64 CPU since ~2009), Linux/POSIX.

```bash
# Build server + client (parallel)
make all -j$(nproc)

# Clean
make clean
```

Compiles with `-O3 -march=native -std=c++17 -pthread`. No external libraries required.

---

## Running

### Server

```bash
# Default: port 9000, threads = hardware_concurrency
./bin/flexql-server

# Custom port
./bin/flexql-server 5432

# Via script
./scripts/run_server.sh 9000
```

Server output on startup:
```
[FlexQL server] threads=8
[recovery] WAL open, next LSN=1
[FlexQL server] listening on port 9000
```

### Client (interactive REPL)

```bash
./bin/flexql-client 127.0.0.1 9000
# or
./scripts/run_client.sh 127.0.0.1 9000
```

### Running the Benchmark

```bash
# Start server in one terminal
./bin/flexql-server 9000

# Run 1M-row INSERT benchmark in another terminal
./bench/benchmark 1000000

# Or run with a custom row count
./bench/benchmark 10000000   # 10M rows
./bench/benchmark 20000000   # 20M rows
```

Expected output:
```
Connected to FlexQL
Running SQL subset checks plus insertion benchmark...
Target insert rows: 1000000

[PASS] DROP TABLE BIG_USERS (if exists) (0 ms)
[PASS] CREATE TABLE BIG_USERS (0 ms)

Starting insertion benchmark for 1000000 rows...
Progress: 100000/1000000
...
Progress: 1000000/1000000
[PASS] INSERT benchmark complete
Rows inserted: 1000000
Elapsed: 334 ms
Throughput: 2994011 rows/sec
```

---

## Performance

### Key design choices for INSERT throughput

| Layer | Optimization | Impact |
|---|---|---|
| Parser | `fast_parse_insert()` — single-pass char scanner, no tokenizer | Eliminates 65K Token objects per batch |
| AST | Flat `string_view` layout — one `vector<string_view>` per batch | Eliminates 5K inner-vector heap allocs per batch |
| Storage | `insert_flat()` — single lock, single pass validate+intern+store | No per-row allocation |
| Arena | `StringArena` bump-pointer, 4 MB slabs, `current_` pointer cache | ~10ns per string intern |
| PK index | FNV-64 flat open-addressing hash map | No linked-node allocations |
| WAL | `writev(header_on_stack, sql_buf)` — zero memcpy | +~55ms over RAM-only |
| WAL CRC | SSE4.2 `_mm_crc32_u64` hardware instruction | 9ms vs 166ms software for 200 batches |
| Network | `TCP_NODELAY` on client sockets | No Nagle buffering delay |
| Compiler | `-O3 -march=native` | Auto-vectorisation of arena memcpy |

### Measured Results (i5-1135G7 @ 2.40 GHz, 14 GB RAM)

**INSERT throughput — batch size 25,000:**

| Dataset | Elapsed | Throughput |
|---|---|---|
| 1M rows | 264 ms | 3,787,878 rows/sec |
| 10M rows | 2,748 ms | 3,639,010 rows/sec |

**INSERT throughput — batch size 5,000:**

| Dataset | Elapsed | Throughput |
|---|---|---|
| 1M rows | 334 ms | 2,994,011 rows/sec |
| 10M rows | 3,065 ms | 3,262,642 rows/sec |
| 20M rows | 6,385 ms | 3,132,341 rows/sec |

Larger batches reduce TCP round-trips and lock acquisitions, yielding ~10–21% higher throughput. Throughput degrades slightly at scale due to hash-table rehashing and memory pressure.

WAL adds only ~50ms per 1M rows because writes go to the kernel page cache (no `fdatasync` in the hot path). Data is safe against process crashes; the trade-off is that an OS crash could lose the last few un-fsynced records.

### SELECT Performance

| Query type | Mechanism | Complexity |
|---|---|---|
| PK equality (`WHERE id = X`) | FNV-64 hash map lookup | O(1) |
| Non-PK scan | Sequential `rows_vals_` traversal | O(n) cache-friendly |
| Repeated SELECT | LRU cache hit | O(1), no table lock |
| Concurrent SELECTs | `shared_lock` — readers run in parallel | Linear scaling |
