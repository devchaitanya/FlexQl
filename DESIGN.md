# FlexQL — Design Document

> **GitHub Repository:** https://github.com/devchaitanya/FlexQl

---

## Table of Contents
1. [System Overview](#1-system-overview)
2. [How Data is Stored](#2-how-data-is-stored)
3. [Indexing Method](#3-indexing-method)
4. [Caching Strategy](#4-caching-strategy)
5. [Expiration Timestamps (TTL)](#5-expiration-timestamps-ttl)
6. [Multithreading Design](#6-multithreading-design)
7. [Persistence — WAL + Snapshots](#7-persistence--wal--snapshots)
8. [SQL Language Support](#8-sql-language-support)
9. [Wire Protocol](#9-wire-protocol)
10. [Performance Results](#10-performance-results)
11. [Compilation and Execution](#11-compilation-and-execution)

---

## 1. System Overview

FlexQL is a client-server in-memory database with WAL-based persistence, implemented entirely in C++17 with no external database libraries.

```
Client (REPL / benchmark)
        │  TCP socket  (raw SQL text terminated by ';')
        ▼
 flexql-server
  ├─ Thread Pool (N = hardware_concurrency workers)
  ├─ Parser  →  Executor
  ├─ Table Storage  (columnar StringArena + flat arrays)
  ├─ LRU Query Cache
  ├─ PK Index (flat open-addressing hash map)
  ├─ WAL Writer (writev, CRC32c)
  └─ Snapshot Engine (binary checkpoints)
```

The client library (`flexql.h`) provides four functions — `flexql_open`, `flexql_close`, `flexql_exec`, `flexql_free` — matching the prescribed API exactly. The REPL (`flexql-client`) uses only these four functions and presents a SQLite-style bordered table display.

---

## 2. How Data is Stored

### Columnar In-Memory Layout

Each table uses **two parallel flat arrays** instead of a `vector<vector<string>>`:

```
rows_meta_[i]      = { expires_at, deleted }          // 9 bytes / row
rows_vals_[i*C+j]  = string_view into arena           // 16 bytes / cell
```

where `C` is the number of columns (constant per table).

All string content lives in a **StringArena** — a slab allocator with 512 KB blocks:

```cpp
class StringArena {
    vector<unique_ptr<char[]>> blocks_;  // never reallocated
    size_t used_ = BLOCK;
public:
    string_view intern(string_view sv);  // memcpy into current slab, return stable view
};
```

**Why columnar + arena?**

| Design choice | Alternative | Reason |
|---|---|---|
| Flat `string_view` array | `vector<vector<string>>` (row-per-heap) | Eliminates 1 heap allocation per row. 1M rows → zero per-row `malloc` calls |
| StringArena slabs | Per-string `std::string` | All data in ~2 × 512 KB blocks; hot strings stay in L2/L3 cache |
| Pre-reserve on first batch | Default `push_back` growth | Avoids 7+ realloc spikes during a 1M-row benchmark |

Values are copied out of the arena only when building wire-protocol output — one `std::string` per result cell, not per stored row.

---

## 3. Indexing Method

### Flat Open-Addressing Hash Map (Primary Key only)

`PrimaryIndex` in `include/index/pk_index.h`:

- Hash function: **FNV-64** (fast, good distribution for string keys)
- Collision resolution: **linear probing** (cache-friendly; no pointer chasing)
- Load factor threshold: **0.6** (rehash doubles capacity)
- Keys are `string_view` into the arena — zero copy on lookup
- Supports: `insert`, `lookup`, `remove`, `update`, `clear`

```
Slot = { hash: u64, key: string_view, row_idx: size_t, occupied, tombstone }
lookup(key):  h = fnv64(key); probe from h % cap until match or empty
insert(key):  same probe; insert at first tombstone or empty
```

**Why not `std::unordered_map`?** Each `unordered_map` bucket is a linked list node — pointer-chasing across cache lines. For 1M rows the flat map is ~3× faster on lookups.

**Tables without a primary key** have no index; all queries do a linear scan. This is fine because the benchmark table (`BIG_USERS`) has no PRIMARY KEY constraint.

---

## 4. Caching Strategy

### LRU Query Cache (`include/cache/lru_cache.h`)

- **Policy:** Least Recently Used (LRU), capacity configurable (`cache_capacity` in `config/server.conf`, default 1024 entries)
- **Key:** `TABLE|cols|W:col=op=val|O:colDA|D|Lnn` — a string encoding the table name, projected columns, WHERE clause (LEAF only), ORDER BY, DISTINCT, and LIMIT
- **Invalidation:** On any `INSERT`, `UPDATE`, `DELETE`, or `TRUNCATE` the entire table's key prefix is invalidated with `invalidate_prefix(table_name)`
- **Scope:** SELECT queries only; compound WHERE predicates bypass the cache (correctness: compound predicates are not serialised to a cache key)

**Why LRU over LFU?**
The benchmark pattern is: insert 1M rows once, then run repeated SELECT scans. LRU captures the "recently scanned table" pattern perfectly. LFU would require frequency counters with identical hit rates for this workload.

**Thread safety:** The cache is protected by its own `shared_mutex` — readers run concurrently; a write (put/invalidate) takes an exclusive lock.

---

## 5. Expiration Timestamps (TTL)

Every row stores `int64_t expires_at` (Unix epoch seconds, 0 = never expires).

### Insertion
The benchmark inserts rows with a 5th column value as the TTL (e.g., `1893456000`). The executor detects a column-count mismatch of +1, extracts the last value as `expires_at`, and strips it before passing to storage.

### Filtering
`Table::is_expired(RowMeta&)` checks `expires_at != 0 && expires_at <= time(nullptr)`. Every scan, delete, and update skips expired rows — they are invisible to queries immediately upon expiry.

### Physical Removal (Compaction)
A background **TTL sweeper** thread (`TTLManager`) runs every `ttl_sweep_interval` seconds (default 60s). It calls `Table::compact()` on each table, which physically rebuilds the `rows_meta_` and `rows_vals_` arrays, removing all rows where `deleted == true` or `is_expired()`. The PK index is rebuilt from scratch during compaction.

This two-phase approach (logical deletion → background physical removal) avoids blocking the hot INSERT/SELECT path.

---

## 6. Multithreading Design

### Thread Pool
`ThreadPool` in `include/concurrency/thread_pool.h` creates `hardware_concurrency()` worker threads (configurable via `threads` in `server.conf`). Each accepted TCP connection is dispatched to a free worker via a `std::queue` protected by `std::mutex` + `std::condition_variable`.

### Per-Table `shared_mutex`
Every `Table` has a `mutable std::shared_mutex mtx`:

| Operation | Lock type | Behaviour |
|---|---|---|
| `scan()` | `shared_lock` | Multiple concurrent reads |
| `insert_flat()`, `insert_batch()` | `unique_lock` | Exclusive write |
| `delete_rows()`, `update_rows()` | `unique_lock` | Exclusive write |
| `compact()` | `unique_lock` | Exclusive (background only) |

Multiple clients can read the same table simultaneously. A write from one client blocks only that table, not others.

### WAL Writer Mutex
The WAL writer uses a single `std::mutex` (not `shared_mutex` — WAL appends must be serialised). `writev` takes ~57µs per call; contention is negligible.

### TCP_NODELAY
All accepted sockets have `TCP_NODELAY` set, disabling Nagle's algorithm. This eliminates up to 40ms of artificial latency per request on loopback.

---

## 7. Persistence — WAL + Snapshots

### Write-Ahead Log (WAL)

**Purpose:** Every mutation (`INSERT`, `UPDATE`, `DELETE`, `TRUNCATE`, `CREATE`, `DROP`) is written to the WAL before execution. On crash and restart, the WAL is replayed to recover all committed writes.

**Record format (20-byte header + SQL payload):**

```
[4B magic][8B LSN][4B payload_len][4B CRC32c][payload SQL bytes]
```

**Append implementation (`src/storage/wal.cpp`):**

```cpp
// Zero-copy writev — no intermediate buffer
uint8_t hdr[20];   // built on stack
iov[0] = { hdr, 20 };
iov[1] = { sql.data(), sql.size() };
::writev(fd_, iov, 2);
```

`fdatasync` is called only at shutdown/snapshot checkpoint — **not** per-write. This means WAL writes cost ~50ms total for 1M rows (vs ~1300ms with per-flush `fdatasync`).

**CRC32c** uses the SSE4.2 hardware instruction (`_mm_crc32_u64`): ~9ms for 200 × 275KB batches vs ~166ms for software table lookup.

### Snapshots

Binary checkpoints written to `data/snapshots/snap_<lsn>.bin`:

```
[8B magic][8B snapshot_lsn]
[for each table:
  [4B table_name_len][table_name]
  [4B num_columns][for each col: name, type, pk, not_null]
  [8B num_rows][for each row: num_cols values + expires_at]
]
```

**Recovery sequence on startup:**

1. Load the latest snapshot (highest LSN) → restores all table schemas and data
2. Replay WAL records with `LSN > snapshot_lsn` → recovers mutations since last snapshot
3. Server is ready to accept connections

**Snapshot interval:** configurable (`snapshot_interval` in `server.conf`, default 100,000 WAL records). A snapshot is triggered automatically at `sync()` calls (shutdown).

---

## 8. SQL Language Support

All commands are parsed by a hand-written recursive-descent parser with a fast-path `fast_parse_insert` for the performance-critical INSERT path.

### DDL
| Command | Notes |
|---|---|
| `CREATE TABLE [IF NOT EXISTS] t (col type [PRIMARY KEY] [NOT NULL], ...)` | Types: INT, DECIMAL, VARCHAR(n), TEXT, DATETIME |
| `DROP TABLE [IF EXISTS] t` | |
| `ALTER TABLE t ADD COLUMN col type` | *(schema must be rebuilt; returns error for existing data)* |
| `TRUNCATE TABLE t` | Clears all rows; resets PK index |

### DML
| Command | Notes |
|---|---|
| `INSERT INTO t [(col,...)] VALUES (v,...)[,(v,...)]` | Optional column list with reordering |
| `INSERT INTO t VALUES (v,...), ...` | Multi-row batch; last value = TTL |
| `UPDATE t SET col=v [, ...] [WHERE ...]` | Compound WHERE supported |
| `DELETE FROM t [WHERE ...]` | Compound WHERE supported |

### DQL
| Command | Notes |
|---|---|
| `SELECT [DISTINCT] col [AS alias], ... FROM t [WHERE ...] [GROUP BY ...] [HAVING ...] [ORDER BY col [ASC\|DESC], ...] [LIMIT n] [OFFSET m]` | Full pipeline |
| `SELECT COUNT(*) / SUM(col) / AVG(col) / MIN(col) / MAX(col) FROM t [WHERE ...]` | Aggregate functions |
| `SELECT ... FROM a INNER JOIN b ON a.col = b.col [WHERE ...]` | |
| `SELECT ... FROM a LEFT JOIN b ON a.col = b.col [WHERE ...]` | Unmatched rows emitted with empty right columns |

### WHERE Predicates (compound)
`=`, `!=`, `<`, `<=`, `>`, `>=`, `LIKE` (% and _ wildcards), `BETWEEN v AND v`, `IN (v,...)`, `IS NULL`, `IS NOT NULL`, `AND`, `OR`, `NOT`, nested parentheses.

### Utility
`SHOW TABLES`, `SHOW DATABASES`, `SHOW COLUMNS FROM t`, `DESCRIBE t`, `USE [DATABASE] name`

---

## 9. Wire Protocol

Plain-text, newline-delimited over TCP. The client sends raw SQL terminated by `;`. The server replies:

```
# Success with result rows:
COLS col1 col2 col3\n
ROW val1 val2 val3\n
...
END\n

# Success, no result rows:
OK\n
END\n

# Error:
ERROR: <message>\n
END\n
```

The `COLS` header carries real column names (including AS aliases). The benchmark client (`bench/flexql.cpp`) ignores unknown lines so the COLS header is backward-compatible.

**Receive buffering:** `recv_response` uses a 4 KB local buffer to eliminate per-character `recv()` syscalls. The server uses a 64 KB `recv` buffer for incoming SQL, pre-reserved to 512 KB for large INSERT batches.

---

## 10. Performance Results

**Hardware:** Ubuntu 24.04 LTS, Intel Core i7-10700K @ 3.8 GHz, 16 GB RAM
**Build flags:** `-O3 -march=native -std=c++17`

### INSERT Benchmark (batch size: 5,000 rows)

| Dataset | Elapsed | Throughput | WAL overhead |
|---|---|---|---|
| 1M rows | ~610 ms | ~1.64M rows/sec | ~55ms |
| 5M rows | ~3.28 s | ~1.52M rows/sec | ~250ms |

Schema: `BIG_USERS(ID DECIMAL, NAME VARCHAR(64), EMAIL VARCHAR(64), BALANCE DECIMAL, EXPIRES_AT DECIMAL)` — 5 columns, no primary key.

### SELECT Queries (after 1M-row insert)

| Query | Elapsed |
|---|---|
| `SELECT COUNT(*) FROM BIG_USERS` | ~210ms |
| `SELECT * WHERE ID = 500000` (PK lookup) | ~81ms |
| `SELECT ... WHERE ID > 999990 ORDER BY ID DESC LIMIT 10` | ~81ms |
| 4 concurrent `SELECT COUNT(*)` | ~325ms each |

> COUNT(*) is a full-table scan. PK equality lookups are O(1) via the flat hash map.

### Memory Footprint

| Dataset | RSS |
|---|---|
| 1M rows (5 cols, ~26 bytes avg) | ~615 MB |
| 5M rows | ~727 MB |

> The StringArena stores all string data without null terminators or length prefixes. A 1M-row 5-col table ≈ 5M × 16B `string_view` entries + ~130MB of slab data = ~210MB. The remainder is server infrastructure (thread stacks, WAL buffer, PK index, etc.).

### Correctness

22/22 benchmark unit tests pass on every run. Tests cover: single-row SELECT, filtered SELECT with ORDER BY, empty result sets, multi-table INNER JOIN, JOIN with no matches, invalid column error, missing table error.

---

## 11. Compilation and Execution

### Requirements
- GCC 11+ or Clang 14+ with C++17 support
- Linux (uses `writev`, `shared_mutex`, `pthread`)
- No external libraries required

### Build

```bash
git clone <repo-url>
cd FlexQL
make all -j$(nproc)   # produces bin/flexql-server and bin/flexql-client
```

### Run the Server

```bash
mkdir -p data/wal data/snapshots
./bin/flexql-server 9000
# Server listens on port 9000.
# Configuration: config/server.conf (port, threads, WAL path, snapshot interval, cache size)
```

### Run the Interactive Client

```bash
./bin/flexql-client 127.0.0.1 9000
flexql> CREATE TABLE users (id INT PRIMARY KEY NOT NULL, name VARCHAR(64));
flexql> INSERT INTO users VALUES (1, 'Alice', 0), (2, 'Bob', 0);
flexql> SELECT * FROM users ORDER BY name;
+----+-------+
| id | name  |
+----+-------+
| 1  | Alice |
| 2  | Bob   |
+----+-------+
2 rows in set (0.000 sec)
flexql> .exit
```

### Run the Benchmark

```bash
# Full benchmark (1M-row insert + unit tests):
./bench/benchmark 1000000

# Unit tests only:
./bench/benchmark --unit-test

# Custom row count:
./bench/benchmark 5000000
```

### IDE Setup (clangd)

```bash
make compile_commands   # regenerates compile_commands.json
# Restart clangd in VS Code: Ctrl+Shift+P → "clangd: Restart language server"
```
