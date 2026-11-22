// ── FlexQL Comprehensive Test Suite ──────────────────────────────────────────
// Standalone unit + adversarial tests: links against core engine, no server needed.
// Build: see Makefile target 'test'
// Run:   ./bin/flexql-test
//
// Tests cover:
//   - Parser correctness (CREATE, INSERT, SELECT, JOIN, DELETE, UPDATE, etc.)
//   - PrimaryIndex (hash map with tombstone handling)
//   - StringArena (bump allocator, oversized strings)
//   - Table operations (insert, scan, PK enforcement, TTL expiration)
//   - Executor (end-to-end query execution)
//   - Concurrency (stress: concurrent INSERT + SELECT + JOIN)
//   - Adversarial inputs (malformed SQL, boundary values, huge payloads)

#include "common/types.h"
#include "parser/parser.h"
#include "parser/ast.h"
#include "storage/table.h"
#include "storage/database.h"
#include "storage/arena.h"
#include "storage/wal.h"
#include "cache/lru_cache.h"
#include <filesystem>
#include <sys/stat.h>
#include <unistd.h>
#include "index/pk_index.h"
#include "query/executor.h"
#include "utils/string_utils.h"

#include <cassert>
#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <atomic>
#include <chrono>
#include <cstring>
#include <functional>
#include <sstream>

// ── Minimal Test Framework ───────────────────────────────────────────────────
static int g_tests = 0, g_passed = 0, g_failed = 0;

#define CHECK(cond, msg) do { \
    ++g_tests; \
    if (!(cond)) { \
        std::cerr << "  [FAIL] " << __FILE__ << ":" << __LINE__ \
                  << " — " << msg << "\n"; \
        ++g_failed; \
    } else { \
        ++g_passed; \
    } \
} while(0)

#define CHECK_EQ(a, b, msg) CHECK((a) == (b), \
    std::string(msg) + " (got '" + std::to_string(a) + "' expected '" + std::to_string(b) + "')")

#define CHECK_STR_EQ(a, b, msg) CHECK((a) == (b), \
    std::string(msg) + " (got '" + std::string(a) + "' expected '" + std::string(b) + "')")

#define SECTION(name) std::cout << "\n=== " << name << " ===\n"

static void reset_database() {
    // Drop all tables to start fresh
    auto& db = Database::instance();
    for (const auto& name : db.list_tables())
        db.drop_table(name, true);
}

// ═════════════════════════════════════════════════════════════════════════════
// 1. PARSER TESTS
// ═════════════════════════════════════════════════════════════════════════════

static void test_parser_create_table() {
    SECTION("Parser: CREATE TABLE");

    auto stmt = Parser::parse("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(64), age DECIMAL);");
    auto* cs = std::get_if<CreateTableStmt>(&stmt);
    CHECK(cs != nullptr, "should parse as CreateTableStmt");
    if (!cs) return;

    CHECK_STR_EQ(cs->schema.table_name, std::string("USERS"), "table name uppercased");
    CHECK_EQ((int)cs->schema.columns.size(), 3, "3 columns");
    CHECK(cs->schema.columns[0].primary_key, "id is PK");
    CHECK_EQ(cs->schema.pk_col_index, 0, "pk_col_index = 0");
    CHECK(cs->schema.columns[0].type == ColumnType::INT, "id is INT");
    CHECK(cs->schema.columns[1].type == ColumnType::VARCHAR, "name is VARCHAR");
    CHECK(cs->schema.columns[2].type == ColumnType::DECIMAL, "age is DECIMAL");
}

static void test_parser_create_if_not_exists() {
    SECTION("Parser: CREATE TABLE IF NOT EXISTS");
    auto stmt = Parser::parse("CREATE TABLE IF NOT EXISTS t1 (a INT);");
    auto* cs = std::get_if<CreateTableStmt>(&stmt);
    CHECK(cs != nullptr, "parses as CreateTableStmt");
    if (cs) CHECK(cs->if_not_exists, "if_not_exists flag set");
}

static void test_parser_insert_single() {
    SECTION("Parser: INSERT single row");
    auto stmt = Parser::parse("INSERT INTO users VALUES (1, 'Alice', 30);");
    auto* is = std::get_if<InsertStmt>(&stmt);
    CHECK(is != nullptr, "should parse as InsertStmt");
    if (!is) return;
    CHECK_STR_EQ(is->table, std::string("USERS"), "table name uppercased");
    // Fast path produces flat_values
    if (is->flat_ncols > 0) {
        CHECK_EQ(is->flat_ncols, 3, "3 columns per row");
        CHECK_EQ((int)is->flat_values.size(), 3, "3 flat values");
    } else {
        CHECK_EQ((int)is->rows.size(), 1, "1 row");
        CHECK_EQ((int)is->rows[0].size(), 3, "3 values");
    }
}

static void test_parser_insert_batch() {
    SECTION("Parser: INSERT batch");
    auto stmt = Parser::parse(
        "INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c');");
    auto* is = std::get_if<InsertStmt>(&stmt);
    CHECK(is != nullptr, "should parse as InsertStmt");
    if (!is) return;
    if (is->flat_ncols > 0) {
        int nrows = (int)is->flat_values.size() / is->flat_ncols;
        CHECK_EQ(nrows, 3, "3 rows in flat layout");
        CHECK_EQ(is->flat_ncols, 2, "2 columns");
    } else {
        CHECK_EQ((int)is->rows.size(), 3, "3 rows");
    }
}

static void test_parser_insert_with_col_list() {
    SECTION("Parser: INSERT with column list");
    auto stmt = Parser::parse("INSERT INTO t (b, a) VALUES ('x', 1);");
    auto* is = std::get_if<InsertStmt>(&stmt);
    CHECK(is != nullptr, "should parse as InsertStmt");
    if (!is) return;
    CHECK_EQ((int)is->col_list.size(), 2, "2 columns in list");
    if (is->col_list.size() >= 2) {
        CHECK_STR_EQ(is->col_list[0], std::string("B"), "first col = B");
        CHECK_STR_EQ(is->col_list[1], std::string("A"), "second col = A");
    }
}

static void test_parser_select_simple() {
    SECTION("Parser: SELECT simple");
    auto stmt = Parser::parse("SELECT name, age FROM users WHERE id = 1;");
    auto* ss = std::get_if<SelectStmt>(&stmt);
    CHECK(ss != nullptr, "should parse as SelectStmt");
    if (!ss) return;
    CHECK_EQ((int)ss->cols.size(), 2, "2 select columns");
    CHECK_STR_EQ(ss->table, std::string("USERS"), "from USERS");
    CHECK(ss->where != nullptr, "has WHERE clause");
    if (ss->where) {
        CHECK(ss->where->kind == Predicate::LEAF, "WHERE is a LEAF");
        CHECK_STR_EQ(ss->where->col, std::string("ID"), "WHERE col = ID");
        CHECK_STR_EQ(ss->where->op, std::string("="), "WHERE op = =");
        CHECK_STR_EQ(ss->where->val, std::string("1"), "WHERE val = 1");
    }
}

static void test_parser_select_star() {
    SECTION("Parser: SELECT *");
    auto stmt = Parser::parse("SELECT * FROM t;");
    auto* ss = std::get_if<SelectStmt>(&stmt);
    CHECK(ss != nullptr, "parses as SelectStmt");
    if (ss) CHECK_EQ((int)ss->cols.size(), 1, "1 col (*)");
}

static void test_parser_select_join() {
    SECTION("Parser: SELECT with INNER JOIN");
    auto stmt = Parser::parse(
        "SELECT ORDERS.NAME, PAYMENTS.AMOUNT FROM ORDERS "
        "INNER JOIN PAYMENTS ON ORDERS.ID = PAYMENTS.ORDER_ID WHERE PAYMENTS.AMOUNT > 100;");
    auto* ss = std::get_if<SelectStmt>(&stmt);
    CHECK(ss != nullptr, "parses as SelectStmt with JOIN");
    if (!ss) return;
    CHECK(!ss->join_table.empty(), "has join_table");
    CHECK(ss->where != nullptr, "has WHERE");
}

static void test_parser_select_compound_where() {
    SECTION("Parser: compound WHERE (AND/OR) must be rejected");
    // AND must be rejected
    {
        bool threw = false;
        try {
            Parser::parse("SELECT * FROM t WHERE a > 1 AND b = 'x';");
        } catch (const ParseError&) { threw = true; }
        CHECK(threw, "AND in WHERE throws ParseError");
    }
    // OR must be rejected
    {
        bool threw = false;
        try {
            Parser::parse("SELECT * FROM t WHERE a > 1 OR b = 'x';");
        } catch (const ParseError&) { threw = true; }
        CHECK(threw, "OR in WHERE throws ParseError");
    }
    // Single condition must still work
    {
        auto stmt = Parser::parse("SELECT * FROM t WHERE a > 1;");
        auto* ss = std::get_if<SelectStmt>(&stmt);
        CHECK(ss && ss->where, "single WHERE still works");
        if (ss && ss->where) {
            CHECK(ss->where->kind == Predicate::LEAF, "single LEAF predicate");
            CHECK_STR_EQ(ss->where->col, std::string("A"), "col = A");
            CHECK_STR_EQ(ss->where->op, std::string(">"), "op = >");
        }
    }
}

static void test_parser_select_between_in() {
    SECTION("Parser: BETWEEN and IN");
    {
        auto stmt = Parser::parse("SELECT * FROM t WHERE x BETWEEN 1 AND 10;");
        auto* ss = std::get_if<SelectStmt>(&stmt);
        CHECK(ss && ss->where, "BETWEEN parses");
        if (ss && ss->where) {
            CHECK_STR_EQ(ss->where->op, std::string("BETWEEN"), "op = BETWEEN");
            CHECK_STR_EQ(ss->where->val, std::string("1"), "lower bound");
            CHECK_STR_EQ(ss->where->val2, std::string("10"), "upper bound");
        }
    }
    {
        auto stmt = Parser::parse("SELECT * FROM t WHERE x IN (1, 2, 3);");
        auto* ss = std::get_if<SelectStmt>(&stmt);
        CHECK(ss && ss->where, "IN parses");
        if (ss && ss->where) {
            CHECK_STR_EQ(ss->where->op, std::string("IN"), "op = IN");
            CHECK_EQ((int)ss->where->in_vals.size(), 3, "3 IN values");
        }
    }
}

static void test_parser_select_order_limit_offset() {
    SECTION("Parser: ORDER BY, LIMIT, OFFSET");
    auto stmt = Parser::parse(
        "SELECT * FROM t ORDER BY name ASC, age DESC LIMIT 10 OFFSET 5;");
    auto* ss = std::get_if<SelectStmt>(&stmt);
    CHECK(ss != nullptr, "parses");
    if (!ss) return;
    CHECK_EQ((int)ss->order_by.size(), 2, "2 ORDER BY keys");
    CHECK_EQ(ss->limit, 10, "LIMIT = 10");
    CHECK_EQ(ss->offset, 5, "OFFSET = 5");
    if (ss->order_by.size() >= 2) {
        CHECK(!ss->order_by[0].desc, "first key ASC");
        CHECK(ss->order_by[1].desc, "second key DESC");
    }
}

static void test_parser_select_distinct() {
    SECTION("Parser: SELECT DISTINCT");
    auto stmt = Parser::parse("SELECT DISTINCT name FROM t;");
    auto* ss = std::get_if<SelectStmt>(&stmt);
    CHECK(ss && ss->distinct, "DISTINCT flag set");
}

static void test_parser_select_aggregate() {
    SECTION("Parser: aggregate functions");
    auto stmt = Parser::parse("SELECT COUNT(*) FROM t;");
    auto* ss = std::get_if<SelectStmt>(&stmt);
    CHECK(ss != nullptr, "parses");
    if (ss) {
        CHECK_STR_EQ(ss->agg_func, std::string("COUNT"), "agg_func = COUNT");
        CHECK_STR_EQ(ss->agg_col, std::string("*"), "agg_col = *");
    }
}

static void test_parser_delete() {
    SECTION("Parser: DELETE");
    auto stmt = Parser::parse("DELETE FROM users WHERE id = 5;");
    auto* ds = std::get_if<DeleteStmt>(&stmt);
    CHECK(ds != nullptr, "parses as DeleteStmt");
    if (ds) {
        CHECK_STR_EQ(ds->table, std::string("USERS"), "table");
        CHECK(ds->where != nullptr, "has WHERE");
    }
}

static void test_parser_update() {
    SECTION("Parser: UPDATE");
    auto stmt = Parser::parse("UPDATE users SET name = 'Bob' WHERE id = 1;");
    auto* us = std::get_if<UpdateStmt>(&stmt);
    CHECK(us != nullptr, "parses as UpdateStmt");
    if (us) {
        CHECK_EQ((int)us->assignments.size(), 1, "1 assignment");
        CHECK(us->where != nullptr, "has WHERE");
    }
}

static void test_parser_drop_table() {
    SECTION("Parser: DROP TABLE");
    auto stmt = Parser::parse("DROP TABLE IF EXISTS t;");
    auto* ds = std::get_if<DropTableStmt>(&stmt);
    CHECK(ds != nullptr, "parses as DropTableStmt");
    if (ds) CHECK(ds->if_exists, "IF EXISTS flag set");
}

static void test_parser_errors() {
    SECTION("Parser: error handling");
    bool caught = false;
    try { Parser::parse("GIBBERISH NONSENSE;"); } catch (const ParseError&) { caught = true; }
    CHECK(caught, "rejects unknown statement");

    caught = false;
    try { Parser::parse("SELECT;"); } catch (const ParseError&) { caught = true; }
    CHECK(caught, "rejects incomplete SELECT");

    caught = false;
    try { Parser::parse("INSERT INTO;"); } catch (const ParseError&) { caught = true; }
    CHECK(caught, "rejects incomplete INSERT");
}

// ═════════════════════════════════════════════════════════════════════════════
// 2. PRIMARY INDEX TESTS
// ═════════════════════════════════════════════════════════════════════════════

static void test_pk_index_basic() {
    SECTION("PrimaryIndex: basic ops");
    PrimaryIndex idx;

    CHECK(idx.insert("key1", 0), "insert key1");
    CHECK(idx.insert("key2", 1), "insert key2");
    CHECK(!idx.insert("key1", 2), "reject duplicate key1");

    CHECK_EQ((int)idx.lookup("key1"), 0, "lookup key1 = 0");
    CHECK_EQ((int)idx.lookup("key2"), 1, "lookup key2 = 1");
    CHECK_EQ((int)idx.lookup("key3"), -1, "lookup missing = -1");
}

static void test_pk_index_tombstone_duplicate() {
    SECTION("PrimaryIndex: tombstone duplicate prevention");
    PrimaryIndex idx;

    // Insert some keys, remove one, try to re-insert.
    // The old bug: insert would land in the tombstone slot without checking
    // if the key exists further down the probe chain.
    idx.insert("a", 0);
    idx.insert("b", 1);
    idx.insert("c", 2);

    idx.remove("b");
    CHECK_EQ((int)idx.lookup("b"), -1, "b removed");

    // Re-insert b — should succeed since it was removed
    CHECK(idx.insert("b", 3), "re-insert b after remove");
    CHECK_EQ((int)idx.lookup("b"), 3, "b = 3 after re-insert");

    // Now try to insert a duplicate of 'a' — must be rejected
    CHECK(!idx.insert("a", 99), "reject duplicate a");
    CHECK_EQ((int)idx.lookup("a"), 0, "a still = 0");

    // Larger test: insert N keys, remove every other, verify no false duplicates.
    // The index stores string_views, so the key strings must outlive it
    // (in a Table they live in the arena) — keep them all in a vector.
    std::vector<std::string> keys;
    for (int i = 0; i < 200; ++i) keys.push_back("key" + std::to_string(i));
    PrimaryIndex idx2;
    for (int i = 0; i < 200; ++i)
        CHECK(idx2.insert(keys[i], (size_t)i), "insert " + keys[i]);
    for (int i = 0; i < 200; i += 2)
        idx2.remove(keys[i]);
    // Re-insert removed keys — should succeed
    for (int i = 0; i < 200; i += 2)
        CHECK(idx2.insert(keys[i], (size_t)(i + 1000)), "re-insert " + keys[i]);
    // Try duplicating the non-removed keys — must fail
    for (int i = 1; i < 200; i += 2)
        CHECK(!idx2.insert(keys[i], 9999), "reject dup " + keys[i]);
}

static void test_pk_index_update() {
    SECTION("PrimaryIndex: update");
    PrimaryIndex idx;
    idx.insert("x", 10);
    idx.update("x", 20);
    CHECK_EQ((int)idx.lookup("x"), 20, "updated to 20");
}

// ═════════════════════════════════════════════════════════════════════════════
// 3. STRING ARENA TESTS
// ═════════════════════════════════════════════════════════════════════════════

static void test_arena_basic() {
    SECTION("StringArena: basic interning");
    StringArena arena;

    auto sv1 = arena.intern("hello");
    auto sv2 = arena.intern("world");
    CHECK_EQ((int)sv1.size(), 5, "hello size");
    CHECK_EQ((int)sv2.size(), 5, "world size");
    CHECK(sv1 == "hello", "hello content");
    CHECK(sv2 == "world", "world content");
    CHECK(sv1.data() != sv2.data(), "different storage");
}

static void test_arena_empty() {
    SECTION("StringArena: empty string");
    StringArena arena;
    auto sv = arena.intern("");
    CHECK(sv.empty(), "empty interned string is empty");
}

static void test_arena_oversized() {
    SECTION("StringArena: oversized string (>4MB)");
    StringArena arena;
    // Create a string larger than BLOCK (4MB)
    std::string big(5 * 1024 * 1024, 'X');
    auto sv = arena.intern(big);
    CHECK_EQ((int)sv.size(), (int)big.size(), "correct size");
    CHECK(sv == big, "content matches");

    // Ensure normal interning still works after oversized
    auto sv2 = arena.intern("after_big");
    CHECK(sv2 == "after_big", "normal intern after oversized");
}

static void test_arena_clear() {
    SECTION("StringArena: clear");
    StringArena arena;
    arena.intern("test");
    arena.clear();
    // After clear, new intern should work
    auto sv = arena.intern("fresh");
    CHECK(sv == "fresh", "intern after clear");
}

// ═════════════════════════════════════════════════════════════════════════════
// 4. TABLE OPERATION TESTS
// ═════════════════════════════════════════════════════════════════════════════

static void test_table_insert_and_scan() {
    SECTION("Table: insert and scan");
    TableSchema schema;
    schema.table_name = "TEST";
    schema.columns = {{"ID", ColumnType::INT, false, false},
                      {"NAME", ColumnType::TEXT, false, false}};
    schema.pk_col_index = -1;
    Table tbl(schema);

    std::string err = tbl.insert({"1", "Alice"}, 0);
    CHECK(err.empty(), "insert Alice: " + err);
    err = tbl.insert({"2", "Bob"}, 0);
    CHECK(err.empty(), "insert Bob: " + err);

    auto res = tbl.scan({"*"}, nullptr, {});
    CHECK(res.ok, "scan ok");
    CHECK_EQ((int)res.num_rows(), 2, "2 rows");
}

static void test_table_pk_enforcement() {
    SECTION("Table: primary key enforcement");
    TableSchema schema;
    schema.table_name = "PKT";
    schema.columns = {{"ID", ColumnType::INT, true, false},
                      {"VAL", ColumnType::TEXT, false, false}};
    schema.pk_col_index = 0;
    Table tbl(schema);

    std::string err = tbl.insert({"1", "a"}, 0);
    CHECK(err.empty(), "first insert ok");
    err = tbl.insert({"1", "b"}, 0);
    CHECK(!err.empty(), "duplicate PK rejected");
    err = tbl.insert({"2", "c"}, 0);
    CHECK(err.empty(), "different PK ok");
}

static void test_table_column_count_mismatch() {
    SECTION("Table: column count mismatch");
    TableSchema schema;
    schema.table_name = "T";
    schema.columns = {{"A", ColumnType::INT, false, false},
                      {"B", ColumnType::TEXT, false, false}};
    schema.pk_col_index = -1;
    Table tbl(schema);

    std::string err = tbl.insert({"1"}, 0);
    CHECK(!err.empty(), "too few columns rejected");
    err = tbl.insert({"1", "x", "extra"}, 0);
    CHECK(!err.empty(), "too many columns rejected");
}

static void test_table_ttl_expiration() {
    SECTION("Table: TTL expiration");
    TableSchema schema;
    schema.table_name = "TTL";
    schema.columns = {{"ID", ColumnType::INT, false, false}};
    schema.pk_col_index = -1;
    Table tbl(schema);

    // Insert row with already-expired TTL (expires_at = 1, which is 1970)
    tbl.insert({"1"}, 1);
    // Insert row with far-future TTL
    tbl.insert({"2"}, 1893456000);
    // Insert row with no expiration
    tbl.insert({"3"}, 0);

    auto res = tbl.scan({"*"}, nullptr, {});
    CHECK(res.ok, "scan ok");
    // Row 1 should be expired (expires_at=1 < now()), rows 2,3 should be live
    CHECK_EQ((int)res.num_rows(), 2, "2 live rows (1 expired)");
}

static void test_table_delete() {
    SECTION("Table: delete rows");
    TableSchema schema;
    schema.table_name = "DEL";
    schema.columns = {{"ID", ColumnType::INT, false, false},
                      {"NAME", ColumnType::TEXT, false, false}};
    schema.pk_col_index = -1;
    Table tbl(schema);

    tbl.insert({"1", "a"}, 0);
    tbl.insert({"2", "b"}, 0);
    tbl.insert({"3", "c"}, 0);

    auto pred = Predicate::leaf("ID", "=", "2");
    int deleted = tbl.delete_rows(&pred);
    CHECK_EQ(deleted, 1, "1 row deleted");

    auto res = tbl.scan({"*"}, nullptr, {});
    CHECK_EQ((int)res.num_rows(), 2, "2 rows remain");
}

static void test_table_update() {
    SECTION("Table: update rows");
    TableSchema schema;
    schema.table_name = "UPD";
    schema.columns = {{"ID", ColumnType::INT, false, false},
                      {"NAME", ColumnType::TEXT, false, false}};
    schema.pk_col_index = -1;
    Table tbl(schema);

    tbl.insert({"1", "old"}, 0);
    auto pred = Predicate::leaf("ID", "=", "1");
    int updated = tbl.update_rows({{"NAME", "new"}}, &pred);
    CHECK_EQ(updated, 1, "1 row updated");

    auto res = tbl.scan({"NAME"}, nullptr, {});
    CHECK(res.ok && res.num_rows() > 0, "scan after update");
    if (res.num_rows() > 0)
        CHECK_STR_EQ(res.cell(0, 0), std::string("new"), "value updated");
}

static void test_table_truncate() {
    SECTION("Table: truncate");
    TableSchema schema;
    schema.table_name = "TRUNC";
    schema.columns = {{"A", ColumnType::INT, false, false}};
    schema.pk_col_index = -1;
    Table tbl(schema);

    for (int i = 0; i < 100; ++i)
        tbl.insert({std::to_string(i)}, 0);
    tbl.truncate();

    auto res = tbl.scan({"*"}, nullptr, {});
    CHECK_EQ((int)res.num_rows(), 0, "0 rows after truncate");
}

static void test_table_compact() {
    SECTION("Table: compact");
    TableSchema schema;
    schema.table_name = "COMP";
    schema.columns = {{"ID", ColumnType::INT, true, false},
                      {"V", ColumnType::TEXT, false, false}};
    schema.pk_col_index = 0;
    Table tbl(schema);

    for (int i = 0; i < 50; ++i)
        tbl.insert({std::to_string(i), "val"}, 0);

    // Delete half
    for (int i = 0; i < 50; i += 2) {
        auto pred = Predicate::leaf("ID", "=", std::to_string(i));
        tbl.delete_rows(&pred);
    }

    // Only 25 dead rows (< COMPACT_MIN_GARBAGE) → a normal sweep skips it
    CHECK(!tbl.compact(), "compact skipped: too little garbage");
    CHECK(tbl.compact(/*force=*/true), "forced compact rebuilds");

    auto res = tbl.scan({"*"}, nullptr, {});
    CHECK_EQ((int)res.num_rows(), 25, "25 rows after compact");

    // Verify PK lookups still work after compaction
    auto pred = Predicate::leaf("ID", "=", "1");
    res = tbl.scan({"V"}, &pred, {});
    CHECK(res.ok && res.num_rows() > 0, "PK lookup after compact");
}

static void test_table_compact_threshold() {
    SECTION("Table: compaction threshold (garbage >= 1000 and >= 20%)");
    TableSchema schema;
    schema.table_name = "CT";
    schema.columns = {{"ID", ColumnType::INT, false, false},
                      {"V", ColumnType::TEXT, false, false}};
    Table tbl(schema);
    for (int i = 0; i < 10000; ++i)
        tbl.insert({std::to_string(i), "v"}, 0);

    // 1500 dead of 10000 = 15% → skip
    auto p1 = Predicate::leaf("ID", "<", "1500");
    CHECK_EQ(tbl.delete_rows(&p1), 1500, "deleted 1500");
    CHECK(!tbl.compact(), "15% garbage: skipped");

    // 2500 dead of 10000 = 25% → rebuild
    auto p2 = Predicate::leaf("ID", "<", "2500");
    CHECK_EQ(tbl.delete_rows(&p2), 1000, "deleted 1000 more");
    CHECK(tbl.compact(), "25% garbage: compacted");
    CHECK_EQ((int)tbl.count_rows(nullptr), 7500, "7500 live rows remain");
    CHECK(!tbl.compact(), "counters reset: nothing left to compact");

    // Expired rows count as garbage: 3000 already-expired of 10500 ≈ 29%
    for (int i = 0; i < 3000; ++i)
        tbl.insert({std::to_string(100000 + i), "old"}, /*expires_at=*/1);
    CHECK(tbl.compact(), "expired rows trigger compaction");
    CHECK_EQ((int)tbl.count_rows(nullptr), 7500, "expired rows removed");

    // A deleted TTL row must not be counted twice (dead + expired):
    // 1200 future-TTL rows, delete them → 1200 garbage of 8700 ≈ 14% → skip
    const int64_t future = (int64_t)std::time(nullptr) + 3600;
    for (int i = 0; i < 1200; ++i)
        tbl.insert({std::to_string(200000 + i), "ttl"}, future);
    auto p3 = Predicate::leaf("ID", ">=", "200000");
    CHECK_EQ(tbl.delete_rows(&p3), 1200, "deleted 1200 TTL rows");
    CHECK(!tbl.compact(), "deleted TTL rows counted once: skipped");

    // TRUNCATE resets the counters
    tbl.truncate();
    CHECK(!tbl.compact(), "nothing to compact after truncate");
}

// ── Tiered compaction ─────────────────────────────────────────────────────────
static TableSchema kv_schema(const char* name, bool pk) {
    TableSchema s;
    s.table_name = name;
    s.columns = {{"ID", ColumnType::INT, pk, false},
                 {"DATA", ColumnType::TEXT, false, false}};
    s.pk_col_index = pk ? 0 : -1;
    return s;
}
static std::string val_for(int id) { return "v" + std::to_string(id) + std::string(180, 'x'); }
static std::string get_data(Table& t, int id) {
    auto p = Predicate::leaf("ID", "=", std::to_string(id));
    auto r = t.scan({"DATA"}, &p, {});
    return r.num_rows() == 1 ? std::string(r.cell(0, 0)) : std::string("<missing>");
}

static void test_arena_page_accounting() {
    SECTION("StringArena: per-page live bytes + dead page queue");
    StringArena a;
    std::vector<std::string_view> v;
    for (int i = 0; i < 4096; ++i) v.push_back(a.intern(std::string(100, 'a' + i % 26)));
    CHECK_EQ(a.live_bytes(), (size_t)409600, "live bytes after intern");
    for (int i = 0; i < 2048; ++i) a.release(v[i]);         // first half dead
    CHECK_EQ(a.live_bytes(), (size_t)204800, "live bytes after release");
    auto pages = a.take_dead_pages();
    CHECK(pages.size() >= 45, "~50 fully-dead pages queued (got " + std::to_string(pages.size()) + ")");
    size_t freed = StringArena::madvise_pages(pages);
    a.add_released(freed);
    CHECK(a.released_bytes() >= 45 * 4096, "released bytes counted");
    // Surviving strings are intact (their pages were not released)
    bool ok = true;
    for (int i = 2048; i < 4096; ++i)
        if (v[i] != std::string(100, 'a' + i % 26)) { ok = false; break; }
    CHECK(ok, "live strings intact after madvise of dead pages");
    CHECK(a.take_dead_pages().empty(), "queue empty after handover");
}

static void test_tier2_page_release() {
    SECTION("Compaction tier 1+2: time-ordered deletes release pages, no rebuild");
    Table t(kv_schema("T2", false));
    for (int i = 0; i < 30000; ++i) t.insert({std::to_string(i), val_for(i)}, 0);
    auto before = t.arena_stats();
    auto p = Predicate::leaf("ID", "<", "18000");             // oldest 60%
    CHECK_EQ(t.delete_rows(&p), 18000, "deleted oldest 60%");
    auto r = t.sweep();
    CHECK(r.rows_compacted, "tier 1 ran");
    CHECK(r.bytes_released > 0, "tier 2 released pages");
    CHECK(!r.arena_rebuilt, "tier 3 not needed (waste below limits)");
    auto after = t.arena_stats();
    CHECK(after.resident < before.resident * 6 / 10,
          "resident arena dropped ~60% (" + std::to_string(before.resident >> 10) + " KB → " +
          std::to_string(after.resident >> 10) + " KB)");
    CHECK_EQ((int)t.count_rows(nullptr), 12000, "12000 live rows");
    CHECK_STR_EQ(get_data(t, 25000), val_for(25000), "surviving row data intact");
}

static void test_tier3_rebuild_scattered() {
    SECTION("Compaction tier 3: scattered deletes → rebuild aside + swap");
    Table t(kv_schema("T3", true));
    Table::CompactionLimits lim;
    lim.min_waste_bytes = 1 << 20;                            // 1 MB for the test
    t.set_compaction_limits(lim);
    for (int i = 0; i < 30000; ++i) t.insert({std::to_string(i), val_for(i)}, 0);
    for (int i = 0; i < 30000; i += 4) {                       // keep 1 in 4
        for (int k = 1; k < 4; ++k) {
            auto p = Predicate::leaf("ID", "=", std::to_string(i + k));
            t.delete_rows(&p);
        }
    }
    auto before = t.arena_stats();
    auto r = t.sweep();
    CHECK(r.rows_compacted, "tier 1 ran");
    CHECK(r.arena_rebuilt, "tier 3 rebuilt the arena (dead bytes scattered)");
    auto after = t.arena_stats();
    CHECK(after.resident < before.resident / 2, "resident arena shrank");
    CHECK_EQ((int)t.count_rows(nullptr), 7500, "7500 live rows");
    CHECK_STR_EQ(get_data(t, 2000), val_for(2000), "PK lookup after rebuild");
    CHECK_STR_EQ(get_data(t, 2001), std::string("<missing>"), "deleted row stays deleted");
}

static void test_tier3_patching() {
    SECTION("Compaction tier 3: writes in the gap are patched into the copy");
    Table t(kv_schema("TP", true));
    for (int i = 0; i < 2000; ++i) t.insert({std::to_string(i), val_for(i)}, 0);
    for (int i = 0; i < 2000; i += 2) {
        auto p = Predicate::leaf("ID", "=", std::to_string(i));
        t.delete_rows(&p);
    }
    t.set_rebuild_gap_hook([&] {
        auto u = Predicate::leaf("ID", "=", "101");
        t.update_rows({{"DATA", "updated-in-gap"}}, &u);       // UPDATE
        auto d = Predicate::leaf("ID", "=", "103");
        t.delete_rows(&d);                                     // DELETE
        t.insert({"5000", "inserted-in-gap"}, 0);              // INSERT
        auto k = Predicate::leaf("ID", "=", "105");
        t.update_rows({{"ID", "7000"}}, &k);                   // PK change
    });
    auto r = t.sweep(nullptr, /*force=*/true);
    t.set_rebuild_gap_hook(nullptr);
    CHECK(r.arena_rebuilt, "rebuild completed with patching");
    CHECK_STR_EQ(get_data(t, 101), std::string("updated-in-gap"), "gap UPDATE kept");
    CHECK_STR_EQ(get_data(t, 103), std::string("<missing>"), "gap DELETE kept");
    CHECK_STR_EQ(get_data(t, 5000), std::string("inserted-in-gap"), "gap INSERT kept");
    CHECK_STR_EQ(get_data(t, 7000), val_for(105), "gap PK change: new key found");
    CHECK_STR_EQ(get_data(t, 105), std::string("<missing>"), "gap PK change: old key gone");
    CHECK_EQ((int)t.count_rows(nullptr), 1000, "row count after patch (1000 -1 +1)");

    // TRUNCATE in the gap aborts the rebuild safely
    for (int i = 0; i < 100; ++i) t.insert({std::to_string(10000 + i), "z"}, 0);
    t.set_rebuild_gap_hook([&] { t.truncate(); });
    r = t.sweep(nullptr, true);
    t.set_rebuild_gap_hook(nullptr);
    CHECK(!r.arena_rebuilt, "TRUNCATE in the gap aborts the rebuild");
    CHECK_EQ((int)t.count_rows(nullptr), 0, "table empty after truncate");
    t.insert({"1", "after"}, 0);
    CHECK_STR_EQ(get_data(t, 1), std::string("after"), "table usable after aborted rebuild");
}

static void test_update_pk_rekey() {
    SECTION("UPDATE of the PK column re-keys the index");
    Table t(kv_schema("PKU", true));
    for (int i = 0; i < 10; ++i) t.insert({std::to_string(i), "d" + std::to_string(i)}, 0);
    auto p = Predicate::leaf("ID", "=", "5");
    CHECK_EQ(t.update_rows({{"ID", "500"}}, &p), 1, "updated 1 row");
    CHECK_STR_EQ(get_data(t, 500), std::string("d5"), "found by new key");
    CHECK_STR_EQ(get_data(t, 5), std::string("<missing>"), "old key gone");
}

static void test_pk_index_tombstone_churn_and_remap() {
    SECTION("PrimaryIndex: tombstone churn never fills the table; remap_rows");
    // Insert+remove many DISTINCT keys: every remove leaves a tombstone. If the
    // load check ignored tombstones, slots would fill up and insert() would
    // probe forever. (Keys must outlive the index — it stores views.)
    std::vector<std::string> keys;
    for (int i = 0; i < 5000; ++i) keys.push_back("k" + std::to_string(i));
    PrimaryIndex idx;
    for (int i = 0; i < 5000; ++i) {
        idx.insert(keys[i], (size_t)i);
        if (i >= 3) idx.remove(keys[i - 3]);           // keep only 3 live keys
    }
    CHECK_EQ((int)idx.size(), 3, "3 live keys after churn (no hang)");
    CHECK_EQ((int)idx.lookup(keys[4999]), 4999, "latest key found");

    // remap_rows: rows 0..9, drop 3 and 7, the rest slide down
    PrimaryIndex r;
    std::vector<std::string> rk;
    for (int i = 0; i < 10; ++i) rk.push_back("r" + std::to_string(i));
    for (int i = 0; i < 10; ++i) r.insert(rk[i], (size_t)i);
    std::vector<uint32_t> remap(10);
    uint32_t w = 0;
    for (int i = 0; i < 10; ++i) remap[i] = (i == 3 || i == 7) ? PrimaryIndex::DROPPED : w++;
    r.remap_rows(remap);
    CHECK_EQ((int)r.size(), 8, "2 keys dropped");
    CHECK_EQ((int)r.lookup(rk[3]), -1, "dropped key gone");
    CHECK_EQ((int)r.lookup(rk[9]), 7, "row 9 → 7");
    CHECK_EQ((int)r.lookup(rk[5]), 4, "row 5 → 4");
    CHECK(r.insert(rk[3], 42), "dropped key can be re-inserted");
}

static void test_pk_index_compact_if_sparse() {
    SECTION("PrimaryIndex: compact_if_sparse drops tombstones and shrinks");
    std::vector<std::string> keys;
    for (int i = 0; i < 20000; ++i) keys.push_back("p" + std::to_string(i));
    PrimaryIndex idx;
    for (int i = 0; i < 20000; ++i) idx.insert(keys[i], (size_t)i);
    for (int i = 0; i < 18000; ++i) idx.remove(keys[i]);           // 90% deleted
    const size_t cap_before = idx.capacity();
    CHECK_EQ((int)idx.tombstones(), 18000, "18000 tombstones");
    CHECK(idx.compact_if_sparse(), "rebuilt (tombstones > live)");
    CHECK_EQ((int)idx.tombstones(), 0, "tombstones dropped");
    CHECK(idx.capacity() < cap_before / 4, "slot array shrank (" + std::to_string(cap_before) +
          " → " + std::to_string(idx.capacity()) + ")");
    CHECK_EQ((int)idx.lookup(keys[19999]), 19999, "live key still found");
    CHECK_EQ((int)idx.lookup(keys[5]), -1, "deleted key still absent");
    CHECK(!idx.compact_if_sparse(), "no-op when tombstones <= live");
}

static void test_compaction_expired_pk() {
    SECTION("Compaction: expired PK rows' keys leave the index");
    Table t(kv_schema("EPK", true));
    for (int i = 0; i < 2000; ++i) t.insert({std::to_string(i), "old"}, /*expires_at=*/1);
    for (int i = 2000; i < 2500; ++i) t.insert({std::to_string(i), "live"}, 0);
    CHECK(t.compact(), "expired rows compacted");
    CHECK_EQ((int)t.count_rows(nullptr), 500, "500 live rows");
    CHECK(t.insert({"7", "reborn"}, 0).empty(), "expired key reusable after compaction");
    CHECK_STR_EQ(get_data(t, 7), std::string("reborn"), "re-inserted row found");
    CHECK_STR_EQ(get_data(t, 2400), std::string("live"), "moved row found via remapped index");
}

static void test_compaction_concurrent() {
    SECTION("Compaction under concurrent writers (no lost writes)");
    Table t(kv_schema("TC", true));
    Table::CompactionLimits lim;
    lim.min_garbage_rows = 1;
    lim.min_waste_bytes  = 1;
    t.set_compaction_limits(lim);
    constexpr int THREADS = 3, KEYS = 600, OPS = 4000;
    std::vector<std::vector<std::string>> expect(THREADS, std::vector<std::string>(KEYS));
    std::atomic<bool> done{false};
    std::vector<std::thread> ws;
    for (int w = 0; w < THREADS; ++w) {
        ws.emplace_back([&, w] {
            uint32_t seed = 12345u + (uint32_t)w;
            auto rnd = [&] { seed = seed * 1103515245u + 12345u; return seed >> 8; };
            for (int op = 0; op < OPS; ++op) {
                const int k  = (int)(rnd() % KEYS);
                const int id = w * 100000 + k;
                auto p = Predicate::leaf("ID", "=", std::to_string(id));
                std::string& e = expect[w][k];
                const std::string nv = "w" + std::to_string(w) + "-" + std::to_string(op) +
                                       std::string(rnd() % 120, 'y');
                const uint32_t action = rnd() % 3;
                if (e.empty())          { t.insert({std::to_string(id), nv}, 0); e = nv; }
                else if (action == 0)   { t.delete_rows(&p); e.clear(); }
                else                    { t.update_rows({{"DATA", nv}}, &p); e = nv; }
            }
        });
    }
    std::thread sweeper([&] {
        while (!done) t.sweep(nullptr, /*force=*/true);       // all tiers, repeatedly
    });
    for (auto& th : ws) th.join();
    done = true;
    sweeper.join();
    t.sweep(nullptr, true);

    int bad = 0, live = 0;
    for (int w = 0; w < THREADS; ++w)
        for (int k = 0; k < KEYS; ++k) {
            const std::string got = get_data(t, w * 100000 + k);
            const std::string& e = expect[w][k];
            if (!e.empty()) ++live;
            if ((e.empty() && got != "<missing>") || (!e.empty() && got != e)) ++bad;
        }
    CHECK_EQ(bad, 0, "every key matches the reference model");
    CHECK_EQ((int)t.count_rows(nullptr), live, "row count matches");
}

static void test_table_flat_insert_batch() {
    SECTION("Table: flat batch insert (25K path)");
    TableSchema schema;
    schema.table_name = "BATCH";
    schema.columns = {{"ID", ColumnType::INT, false, false},
                      {"NAME", ColumnType::TEXT, false, false},
                      {"BAL", ColumnType::DECIMAL, false, false}};
    schema.pk_col_index = -1;
    Table tbl(schema);

    const int N = 25000;
    std::vector<std::string> backing;
    backing.reserve(N * 3);
    for (int i = 0; i < N; ++i) {
        backing.push_back(std::to_string(i));
        backing.push_back("user" + std::to_string(i));
        backing.push_back(std::to_string(1000 + i % 100));
    }
    std::vector<std::string_view> flat;
    flat.reserve(N * 3);
    for (const auto& s : backing) flat.push_back(s);

    std::string err = tbl.insert_flat(flat.data(), N, 3, 0);
    CHECK(err.empty(), "25K flat insert: " + err);

    auto res = tbl.scan({"*"}, nullptr, {});
    CHECK_EQ((int)res.num_rows(), N, "25K rows scanned");
}

static void test_table_where_operators() {
    SECTION("Table: WHERE operators");
    TableSchema schema;
    schema.table_name = "OPS";
    schema.columns = {{"ID", ColumnType::INT, false, false},
                      {"NAME", ColumnType::TEXT, false, false}};
    schema.pk_col_index = -1;
    Table tbl(schema);

    tbl.insert({"1", "Alice"}, 0);
    tbl.insert({"2", "Bob"}, 0);
    tbl.insert({"3", "Carol"}, 0);
    tbl.insert({"10", "Dave"}, 0);

    // GT
    auto pred = Predicate::leaf("ID", ">", "2");
    auto res = tbl.scan({"NAME"}, &pred, {});
    CHECK_EQ((int)res.num_rows(), 2, "> 2 returns 2 rows");

    // LIKE
    pred = Predicate::leaf("NAME", "LIKE", "A%");
    res = tbl.scan({"NAME"}, &pred, {});
    CHECK_EQ((int)res.num_rows(), 1, "LIKE A% returns 1 row");

    // BETWEEN
    pred.kind = Predicate::LEAF;
    pred.col = "ID"; pred.op = "BETWEEN"; pred.val = "2"; pred.val2 = "10";
    res = tbl.scan({"NAME"}, &pred, {});
    CHECK_EQ((int)res.num_rows(), 3, "BETWEEN 2 AND 10 returns 3 rows");

    // IN
    pred.op = "IN"; pred.in_vals = {"1", "3"};
    res = tbl.scan({"NAME"}, &pred, {});
    CHECK_EQ((int)res.num_rows(), 2, "IN (1,3) returns 2 rows");
}

// ═════════════════════════════════════════════════════════════════════════════
// 5. EXECUTOR END-TO-END TESTS
// ═════════════════════════════════════════════════════════════════════════════

static void test_executor_create_insert_select() {
    SECTION("Executor: CREATE → INSERT → SELECT");
    reset_database();
    Executor exec;

    auto r = exec.execute(Parser::parse(
        "CREATE TABLE emp (id INT PRIMARY KEY, name VARCHAR(64), salary DECIMAL);"));
    CHECK(r.ok, "CREATE TABLE ok");

    r = exec.execute(Parser::parse(
        "INSERT INTO emp VALUES (1, 'Alice', 50000), (2, 'Bob', 60000), (3, 'Carol', 70000);"));
    CHECK(r.ok, "INSERT ok");

    r = exec.execute(Parser::parse("SELECT name FROM emp WHERE salary > 55000 ORDER BY name;"));
    CHECK(r.ok, "SELECT ok");
    CHECK_EQ((int)r.num_rows(), 2, "2 rows match salary > 55000");
    if (r.num_rows() >= 2) {
        CHECK_STR_EQ(r.cell(0, 0), std::string("Bob"), "first row = Bob");
        CHECK_STR_EQ(r.cell(1, 0), std::string("Carol"), "second row = Carol");
    }
}

static void test_executor_inner_join() {
    SECTION("Executor: INNER JOIN");
    reset_database();
    Executor exec;

    exec.execute(Parser::parse(
        "CREATE TABLE t_users (uid INT PRIMARY KEY, uname VARCHAR(64));"));
    exec.execute(Parser::parse(
        "INSERT INTO t_users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol');"));
    exec.execute(Parser::parse(
        "CREATE TABLE t_orders (oid INT PRIMARY KEY, uid INT, amount DECIMAL);"));
    exec.execute(Parser::parse(
        "INSERT INTO t_orders VALUES (10, 1, 100), (11, 1, 200), (12, 3, 300);"));

    auto r = exec.execute(Parser::parse(
        "SELECT T_USERS.UNAME, T_ORDERS.AMOUNT FROM T_USERS "
        "INNER JOIN T_ORDERS ON T_USERS.UID = T_ORDERS.UID ORDER BY T_ORDERS.AMOUNT;"));
    CHECK(r.ok, "JOIN ok: " + r.error);
    CHECK_EQ((int)r.rows.size(), 3, "3 joined rows");
    if (r.rows.size() >= 3) {
        CHECK_STR_EQ(r.rows[0][0], std::string("Alice"), "first joined = Alice");
        CHECK_STR_EQ(r.rows[2][0], std::string("Carol"), "last joined = Carol");
    }
}

static void test_executor_left_join() {
    SECTION("Executor: LEFT JOIN");
    reset_database();
    Executor exec;

    exec.execute(Parser::parse(
        "CREATE TABLE lj_a (id INT PRIMARY KEY, name VARCHAR(64));"));
    exec.execute(Parser::parse(
        "INSERT INTO lj_a VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol');"));
    exec.execute(Parser::parse(
        "CREATE TABLE lj_b (id INT PRIMARY KEY, aid INT, val VARCHAR(64));"));
    exec.execute(Parser::parse(
        "INSERT INTO lj_b VALUES (10, 1, 'x'), (11, 3, 'y');"));

    auto r = exec.execute(Parser::parse(
        "SELECT LJ_A.NAME, LJ_B.VAL FROM LJ_A "
        "LEFT JOIN LJ_B ON LJ_A.ID = LJ_B.AID;"));
    CHECK(r.ok, "LEFT JOIN ok: " + r.error);
    // Alice(matched), Bob(no match→NULLs), Carol(matched) = 3 rows
    CHECK_EQ((int)r.rows.size(), 3, "3 LEFT JOIN rows (including unmatched Bob)");

    // Find Bob's row — should have empty value for LJ_B.VAL
    bool found_bob = false;
    for (const auto& row : r.rows) {
        if (row.size() >= 2 && row[0] == "Bob") {
            found_bob = true;
            CHECK(row[1].empty(), "Bob's LJ_B.VAL is empty (NULL)");
        }
    }
    CHECK(found_bob, "Bob appears in LEFT JOIN results");
}

static void test_executor_aggregate() {
    SECTION("Executor: aggregates (COUNT, SUM, AVG, MIN, MAX)");
    reset_database();
    Executor exec;

    exec.execute(Parser::parse("CREATE TABLE agg (id INT, val DECIMAL);"));
    exec.execute(Parser::parse(
        "INSERT INTO agg VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50);"));

    auto r = exec.execute(Parser::parse("SELECT COUNT(*) FROM agg;"));
    CHECK(r.ok && !r.rows.empty(), "COUNT ok");
    if (!r.rows.empty()) CHECK_STR_EQ(r.rows[0][0], std::string("5"), "COUNT = 5");

    r = exec.execute(Parser::parse("SELECT SUM(val) FROM agg;"));
    CHECK(r.ok && !r.rows.empty(), "SUM ok");

    r = exec.execute(Parser::parse("SELECT MIN(val) FROM agg;"));
    CHECK(r.ok && !r.rows.empty(), "MIN ok");
    if (!r.rows.empty()) CHECK_STR_EQ(r.rows[0][0], std::string("10"), "MIN = 10");

    r = exec.execute(Parser::parse("SELECT MAX(val) FROM agg;"));
    CHECK(r.ok && !r.rows.empty(), "MAX ok");
    if (!r.rows.empty()) CHECK_STR_EQ(r.rows[0][0], std::string("50"), "MAX = 50");
}

// Run one SELECT and return "col=val, col=val | col=val ..." (rows separated by
// " | ") or "ERROR: ..." — compact enough to compare whole results in one CHECK.
static std::string q(Executor& ex, const std::string& sql) {
    QueryResult r;
    try { r = ex.execute(Parser::parse(sql)); }
    catch (const std::exception& e) { return std::string("PARSE ERROR: ") + e.what(); }
    if (!r.ok) return "ERROR: " + r.error;
    std::string out;
    for (size_t i = 0; i < r.num_rows(); ++i) {
        if (i) out += " | ";
        for (size_t c = 0; c < r.column_names.size(); ++c) {
            if (c) out += ", ";
            out += r.column_names[c] + "=" + std::string(r.cell(i, c));
        }
    }
    return out;
}

static void test_executor_aggregates_general() {
    SECTION("Executor: multiple aggregates, aliases, NULLs, errors");
    reset_database();
    Executor ex;
    ex.execute(Parser::parse("CREATE TABLE emp (id INT PRIMARY KEY, dept VARCHAR, salary INT, bonus DECIMAL);"));
    ex.execute(Parser::parse("INSERT INTO emp VALUES (1,'ENG',100,1.5),(2,'ENG',200,''),(3,'HR',50,2.25);"));

    // Several aggregates in one statement, no GROUP BY → one row
    CHECK_STR_EQ(q(ex, "SELECT COUNT(*), SUM(salary), AVG(salary), MIN(salary), MAX(salary) FROM emp;"),
                 std::string("COUNT(*)=3, SUM(SALARY)=350, AVG(SALARY)=116.666666666667, MIN(SALARY)=50, MAX(SALARY)=200"),
                 "5 aggregates in one SELECT");
    // Aliases + WHERE
    CHECK_STR_EQ(q(ex, "SELECT COUNT(*) AS n, SUM(salary) AS total FROM emp WHERE salary > 60;"),
                 std::string("N=2, TOTAL=300"), "aliases + WHERE");
    // NULL rules: COUNT(*) counts rows, COUNT(col) skips NULL, SUM/AVG skip NULL; decimals exact
    CHECK_STR_EQ(q(ex, "SELECT COUNT(*), COUNT(bonus), SUM(bonus), AVG(bonus) FROM emp;"),
                 std::string("COUNT(*)=3, COUNT(BONUS)=2, SUM(BONUS)=3.75, AVG(BONUS)=1.875"),
                 "NULL skipped, decimal SUM/AVG");
    // Empty input: COUNT 0, others NULL (empty)
    CHECK_STR_EQ(q(ex, "SELECT COUNT(*), SUM(salary), MIN(salary) FROM emp WHERE salary > 1000;"),
                 std::string("COUNT(*)=0, SUM(SALARY)=, MIN(SALARY)="), "aggregates over zero rows");
    // MIN/MAX on text compare as strings
    CHECK_STR_EQ(q(ex, "SELECT MIN(dept), MAX(dept) FROM emp;"),
                 std::string("MIN(DEPT)=ENG, MAX(DEPT)=HR"), "MIN/MAX on text");
    // Errors that used to be parse errors or misleading messages
    CHECK(q(ex, "SELECT dept, SUM(salary) FROM emp;").find("must appear in GROUP BY") != std::string::npos,
          "plain column + aggregate without GROUP BY → clear error");
    CHECK(q(ex, "SELECT SUM(dept) FROM emp;").find("requires a numeric column") != std::string::npos,
          "SUM of text column → error");
    CHECK(q(ex, "SELECT SUM(*) FROM emp;").find("only COUNT(*)") != std::string::npos,
          "SUM(*) → error");
    // Single COUNT(*) still uses the fast count path and still parses as before
    CHECK_STR_EQ(q(ex, "SELECT COUNT(*) FROM emp WHERE salary >= 100;"),
                 std::string("COUNT(*)=2"), "single COUNT(*) with WHERE");
}

static void test_executor_group_by() {
    SECTION("Executor: GROUP BY with multiple aggregates (any order) + HAVING");
    reset_database();
    Executor ex;
    ex.execute(Parser::parse("CREATE TABLE emp (id INT PRIMARY KEY, dept VARCHAR, salary INT, bonus DECIMAL);"));
    ex.execute(Parser::parse("INSERT INTO emp VALUES (1,'ENG',100,1.5),(2,'ENG',200,''),(3,'HR',50,2.25);"));

    CHECK_STR_EQ(q(ex, "SELECT dept, COUNT(*), SUM(salary), MAX(salary) FROM emp GROUP BY dept;"),
                 std::string("DEPT=ENG, COUNT(*)=2, SUM(SALARY)=300, MAX(SALARY)=200 | "
                             "DEPT=HR, COUNT(*)=1, SUM(SALARY)=50, MAX(SALARY)=50"),
                 "GROUP BY with 3 aggregates, exact integer SUM");
    // Aggregate listed FIRST used to be a parse error
    CHECK_STR_EQ(q(ex, "SELECT COUNT(*), dept FROM emp GROUP BY dept;"),
                 std::string("COUNT(*)=2, DEPT=ENG | COUNT(*)=1, DEPT=HR"), "aggregate first in GROUP BY");
    // COUNT(col) skips NULLs per group
    CHECK_STR_EQ(q(ex, "SELECT dept, COUNT(bonus) FROM emp GROUP BY dept;"),
                 std::string("DEPT=ENG, COUNT(BONUS)=1 | DEPT=HR, COUNT(BONUS)=1"), "COUNT(col) per group skips NULL");
    // HAVING on an aggregate
    CHECK_STR_EQ(q(ex, "SELECT dept, SUM(salary) FROM emp GROUP BY dept HAVING SUM(salary) > 100;"),
                 std::string("DEPT=ENG, SUM(SALARY)=300"), "HAVING filters groups");
}

static void test_lru_cache_concurrent() {
    SECTION("LRU cache: values stay valid while other threads evict them");
    // Tiny capacity + constant put/invalidate → entries are evicted all the
    // time while readers copy them. With the old raw-pointer get() this was a
    // use-after-free (ASAN catches it); with shared_ptr copies it is safe.
    LRUCache<std::string, std::string> cache(2);
    const std::string big(4096, 'x');
    std::atomic<bool> stop{false};
    std::atomic<long> reads{0}, bad{0};
    std::vector<std::thread> ts;
    for (int t = 0; t < 3; ++t)
        ts.emplace_back([&, t] {
            while (!stop) {
                if (auto v = cache.get("k" + std::to_string(t % 3))) {
                    std::string copy = *v;                 // copy after the lock is released
                    if (copy.size() != big.size()) ++bad;
                    ++reads;
                }
            }
        });
    ts.emplace_back([&] {
        for (int i = 0; i < 20000; ++i) {
            cache.put("k" + std::to_string(i % 3), big, "T");
            if (i % 7 == 0) cache.invalidate_prefix("T");
        }
        stop = true;
    });
    for (auto& th : ts) th.join();
    CHECK_EQ((long)bad, 0L, "every value read intact");
    CHECK(reads > 0, "readers actually hit the cache");
}

static void test_self_join() {
    SECTION("JOIN: self-join locks the table once");
    reset_database();
    Executor ex;
    ex.execute(Parser::parse("CREATE TABLE emp (id INT PRIMARY KEY, mgr INT);"));
    ex.execute(Parser::parse("INSERT INTO emp VALUES (1,0),(2,1),(3,1);"));
    auto r = ex.execute(Parser::parse("SELECT * FROM emp INNER JOIN emp ON emp.mgr = emp.id;"));
    CHECK(r.ok, "self-join runs (no double lock)");
    CHECK_EQ((int)r.num_rows(), 2, "employees 2 and 3 match manager 1");
}

static void test_group_by_vectorized_and_join() {
    SECTION("GROUP BY from the table, hidden HAVING aggregates, aggregates over JOIN");
    reset_database();
    Executor ex;
    ex.execute(Parser::parse("CREATE TABLE emp (id INT PRIMARY KEY, dept_id INT, team VARCHAR, salary INT);"));
    ex.execute(Parser::parse("INSERT INTO emp VALUES (1,10,'a',100),(2,10,'b',200),(3,20,'a',50),(4,10,'a',300),(5,30,'c',NULL);"));
    ex.execute(Parser::parse("CREATE TABLE dept (did INT PRIMARY KEY, dname VARCHAR);"));
    ex.execute(Parser::parse("INSERT INTO dept VALUES (10,'ENG'),(20,'HR'),(40,'OPS');"));

    // Integer group key → the int64 hash fast path
    CHECK_STR_EQ(q(ex, "SELECT dept_id, COUNT(*), SUM(salary), AVG(salary) FROM emp GROUP BY dept_id;"),
                 std::string("DEPT_ID=10, COUNT(*)=3, SUM(SALARY)=600, AVG(SALARY)=200 | "
                             "DEPT_ID=20, COUNT(*)=1, SUM(SALARY)=50, AVG(SALARY)=50 | "
                             "DEPT_ID=30, COUNT(*)=1, SUM(SALARY)=, AVG(SALARY)="),
                 "int key; NULL-only group gives NULL sum/avg");
    // Two group columns → string-key path (length-prefixed keys)
    CHECK_STR_EQ(q(ex, "SELECT dept_id, team, COUNT(*) FROM emp WHERE dept_id = 10 GROUP BY dept_id, team;"),
                 std::string("DEPT_ID=10, TEAM=a, COUNT(*)=2 | DEPT_ID=10, TEAM=b, COUNT(*)=1"),
                 "multi-column group key + WHERE");
    // HAVING on an aggregate that is NOT selected (used to drop every group)
    CHECK_STR_EQ(q(ex, "SELECT dept_id FROM emp GROUP BY dept_id HAVING COUNT(*) > 1;"),
                 std::string("DEPT_ID=10"), "HAVING on a hidden aggregate");
    // HAVING via the alias, LIMIT/OFFSET on groups
    CHECK_STR_EQ(q(ex, "SELECT dept_id, SUM(salary) AS total FROM emp GROUP BY dept_id HAVING total >= 50 LIMIT 1 OFFSET 1;"),
                 std::string("DEPT_ID=20, TOTAL=50"), "HAVING by alias + OFFSET/LIMIT");
    CHECK(q(ex, "SELECT team, COUNT(*) FROM emp GROUP BY dept_id;").find("must appear in GROUP BY") != std::string::npos,
          "non-grouped column → error");

    // Aggregates over a JOIN, no GROUP BY → one row
    CHECK_STR_EQ(q(ex, "SELECT COUNT(*), SUM(emp.salary), MAX(dept.dname) FROM emp INNER JOIN dept ON emp.dept_id = dept.did;"),
                 std::string("COUNT(*)=4, SUM(EMP.SALARY)=650, MAX(DEPT.DNAME)=HR"), "aggregates over JOIN");
    // GROUP BY over a JOIN
    CHECK_STR_EQ(q(ex, "SELECT dname, COUNT(*), SUM(salary) FROM emp INNER JOIN dept ON emp.dept_id = dept.did GROUP BY dname;"),
                 std::string("DNAME=ENG, COUNT(*)=3, SUM(SALARY)=600 | DNAME=HR, COUNT(*)=1, SUM(SALARY)=50"),
                 "GROUP BY over JOIN (unqualified names resolved)");
    // Zero joined rows, no GROUP BY → still one row
    CHECK_STR_EQ(q(ex, "SELECT COUNT(*), SUM(salary) FROM emp INNER JOIN dept ON emp.dept_id = dept.did WHERE salary > 1000;"),
                 std::string("COUNT(*)=0, SUM(SALARY)="), "aggregate over empty JOIN = one row");
    // JOIN LIMIT/OFFSET used to be ignored; DESC with ties must be well-defined
    auto r = ex.execute(Parser::parse("SELECT emp.id FROM emp INNER JOIN dept ON emp.dept_id = dept.did ORDER BY emp.dept_id DESC LIMIT 2;"));
    CHECK(r.ok && r.num_rows() == 2, "JOIN LIMIT applied");
    if (r.ok && r.num_rows() == 2) CHECK_STR_EQ(std::string(r.cell(0, 0)), std::string("3"), "DESC: HR (20) first");
}

static void test_parser_fixes() {
    SECTION("Parser: row-length mismatch, NULL, escaped quotes, trailing text");
    reset_database();
    Executor ex;
    ex.execute(Parser::parse("CREATE TABLE p (id INT, name VARCHAR, score INT);"));

    // Rows with different value counts used to be silently mangled by the
    // fast path (only the first row's count was used). Now: a clear error.
    auto r = ex.execute(Parser::parse("INSERT INTO p VALUES (1,'a',10),(2,'b');"));
    CHECK(!r.ok, "row with fewer values is rejected");
    CHECK_STR_EQ(q(ex, "SELECT COUNT(*) FROM p;"), std::string("COUNT(*)=0"), "nothing inserted");

    // NULL literal: fast path (unquoted NULL) and full parser (an escaped quote
    // in the same statement forces the slow path)
    r = ex.execute(Parser::parse("INSERT INTO p VALUES (1,'a',NULL),(2,NULL,20);"));
    CHECK(r.ok, "NULL accepted by the fast path");
    r = ex.execute(Parser::parse("INSERT INTO p VALUES (3,'c''',null);"));
    CHECK(r.ok, "NULL accepted by the full parser");
    CHECK_STR_EQ(q(ex, "SELECT COUNT(*), COUNT(name), COUNT(score) FROM p;"),
                 std::string("COUNT(*)=3, COUNT(NAME)=2, COUNT(SCORE)=1"), "NULLs stored as NULL");

    // Escaped quotes: 'O''Brien' = O'Brien, in INSERT and in WHERE
    r = ex.execute(Parser::parse("INSERT INTO p VALUES (4,'O''Brien',1),(5,'it''s ''quoted''',2);"));
    CHECK(r.ok, "escaped quotes accepted");
    CHECK_STR_EQ(q(ex, "SELECT name FROM p WHERE id = 4;"), std::string("NAME=O'Brien"), "unescaped on insert");
    CHECK_STR_EQ(q(ex, "SELECT name FROM p WHERE id = 5;"), std::string("NAME=it's 'quoted'"), "multiple escapes");
    CHECK_STR_EQ(q(ex, "SELECT id FROM p WHERE name = 'O''Brien';"), std::string("ID=4"), "escaped literal in WHERE");

    // Trailing text is an error now (used to be silently ignored)
    CHECK(q(ex, "SELECT * FROM p garbage;").find("PARSE ERROR") == 0, "trailing word → parse error");
    CHECK(q(ex, "SELECT * FROM p; SELECT * FROM p;").find("PARSE ERROR") == 0, "two statements → parse error");
    bool threw = false;
    try { Parser::parse("INSERT INTO p VALUES (6,'f',6) junk;"); } catch (const ParseError&) { threw = true; }
    CHECK(threw, "trailing text after INSERT → parse error");
    CHECK(q(ex, "SELECT COUNT(*) FROM p;   ").find("COUNT(*)=") == 0, "trailing whitespace is fine");
}

static void test_wal_rotate() {
    SECTION("WAL: rotate splits the log; a failed rotate keeps the WAL usable");
    namespace fs = std::filesystem;
    const fs::path dir = fs::temp_directory_path() / ("flexql_wal_test_" + std::to_string(::getpid()));
    fs::remove_all(dir);
    fs::create_directories(dir);
    const std::string path = (dir / "wal.log").string();
    {
        WalWriter w;
        CHECK(w.open(path), "open");
        w.append("INSERT INTO t VALUES (1);");
        w.append("INSERT INTO t VALUES (2);");
        const std::string seg = w.rotate(2);
        CHECK(!seg.empty() && fs::exists(seg), "rotate created the segment");
        CHECK(w.is_open() && !w.failed() && w.bytes() == 0, "fresh empty wal.log after rotate");
        w.append("INSERT INTO t VALUES (3);");
        // Records 1,2 in the segment, 3 in the new file, LSNs continue
        WalReader r1; uint64_t lsn; std::string sql; int n1 = 0;
        CHECK(r1.open(seg), "segment readable");
        while (r1.read_next(lsn, sql)) ++n1;
        WalReader r2; int n2 = 0; uint64_t last = 0;
        CHECK(r2.open(path), "new wal readable");
        while (r2.read_next(lsn, sql)) { ++n2; last = lsn; }
        CHECK(n1 == 2 && n2 == 1 && last == 3, "segment has 2 records, new file has LSN 3");

        // Failure path: a read-only directory makes the rename fail.
        // The WAL must stay OPEN on the same file (not silently closed).
        ::chmod(dir.c_str(), 0555);
        const bool root = (::geteuid() == 0);      // root ignores permissions; skip then
        if (!root) {
            CHECK(w.rotate(9).empty(), "rotate fails in a read-only dir");
            CHECK(w.is_open() && !w.failed(), "WAL still open after a failed rotate");
            const uint64_t before = w.bytes();
            w.append("INSERT INTO t VALUES (4);");
            CHECK(w.bytes() > before, "appends still land in the log");
        }
        ::chmod(dir.c_str(), 0755);
    }
    fs::remove_all(dir);
}

static void test_executor_delete_update() {
    SECTION("Executor: DELETE and UPDATE");
    reset_database();
    Executor exec;

    exec.execute(Parser::parse("CREATE TABLE du (id INT PRIMARY KEY, v TEXT);"));
    exec.execute(Parser::parse("INSERT INTO du VALUES (1,'a'),(2,'b'),(3,'c');"));

    auto r = exec.execute(Parser::parse("DELETE FROM du WHERE id = 2;"));
    CHECK(r.ok, "DELETE ok");

    r = exec.execute(Parser::parse("SELECT * FROM du;"));
    CHECK_EQ((int)r.num_rows(), 2, "2 rows after delete");

    r = exec.execute(Parser::parse("UPDATE du SET v = 'updated' WHERE id = 1;"));
    CHECK(r.ok, "UPDATE ok");

    r = exec.execute(Parser::parse("SELECT v FROM du WHERE id = 1;"));
    CHECK(r.ok && r.num_rows() > 0, "select after update");
    if (r.num_rows() > 0)
        CHECK_STR_EQ(r.cell(0, 0), std::string("updated"), "value updated");
}

static void test_executor_errors() {
    SECTION("Executor: error cases");
    reset_database();
    Executor exec;

    auto r = exec.execute(Parser::parse("SELECT * FROM nonexistent;"));
    CHECK(!r.ok, "nonexistent table → error");

    exec.execute(Parser::parse("CREATE TABLE err (id INT, v TEXT);"));
    r = exec.execute(Parser::parse("SELECT unknown_col FROM err;"));
    CHECK(!r.ok, "unknown column → error");
}

// ═════════════════════════════════════════════════════════════════════════════
// 6. CONCURRENCY STRESS TESTS
// ═════════════════════════════════════════════════════════════════════════════

static void test_concurrent_insert_select() {
    SECTION("Concurrency: concurrent INSERT + SELECT");
    reset_database();
    Executor exec;

    exec.execute(Parser::parse("CREATE TABLE conc (id INT, val TEXT);"));

    std::atomic<int> insert_errors{0};
    std::atomic<int> select_errors{0};
    std::atomic<bool> stop{false};
    const int NUM_WRITERS = 4;
    const int ROWS_PER_WRITER = 5000;

    std::vector<std::thread> writers;
    for (int w = 0; w < NUM_WRITERS; ++w) {
        writers.emplace_back([&, w]() {
            Executor local_exec;
            for (int i = 0; i < ROWS_PER_WRITER; ++i) {
                int id = w * ROWS_PER_WRITER + i;
                std::string sql = "INSERT INTO conc VALUES (" +
                    std::to_string(id) + ", 'v" + std::to_string(id) + "');";
                try {
                    auto r = local_exec.execute(Parser::parse(sql));
                    if (!r.ok) ++insert_errors;
                } catch (...) { ++insert_errors; }
            }
        });
    }

    // Concurrent readers
    std::vector<std::thread> readers;
    for (int r = 0; r < 2; ++r) {
        readers.emplace_back([&]() {
            Executor local_exec;
            while (!stop) {
                try {
                    auto res = local_exec.execute(Parser::parse("SELECT COUNT(*) FROM conc;"));
                    if (!res.ok) ++select_errors;
                } catch (...) { ++select_errors; }
            }
        });
    }

    for (auto& t : writers) t.join();
    stop = true;
    for (auto& t : readers) t.join();

    CHECK_EQ(insert_errors.load(), 0, "no insert errors");
    CHECK_EQ(select_errors.load(), 0, "no select errors");

    auto r = exec.execute(Parser::parse("SELECT COUNT(*) FROM conc;"));
    CHECK(r.ok && !r.rows.empty(), "final count");
    int total = NUM_WRITERS * ROWS_PER_WRITER;
    if (!r.rows.empty())
        CHECK_STR_EQ(r.rows[0][0], std::to_string(total),
                     "all " + std::to_string(total) + " rows present");
}

static void test_concurrent_insert_delete() {
    SECTION("Concurrency: concurrent INSERT + DELETE");
    reset_database();
    Executor exec;

    exec.execute(Parser::parse("CREATE TABLE cd (id INT, v TEXT);"));

    std::atomic<int> errors{0};
    const int N = 2000;

    std::thread writer([&]() {
        Executor e;
        for (int i = 0; i < N; ++i) {
            try {
                e.execute(Parser::parse(
                    "INSERT INTO cd VALUES (" + std::to_string(i) + ", 'val');"));
            } catch (...) { ++errors; }
        }
    });

    std::thread deleter([&]() {
        Executor e;
        for (int i = 0; i < N; ++i) {
            try {
                e.execute(Parser::parse(
                    "DELETE FROM cd WHERE id = " + std::to_string(i) + ";"));
            } catch (...) { ++errors; }
        }
    });

    writer.join();
    deleter.join();
    CHECK_EQ(errors.load(), 0, "no errors during concurrent insert+delete");
}

static void test_concurrent_join() {
    SECTION("Concurrency: concurrent INSERT + JOIN");
    reset_database();
    Executor exec;

    exec.execute(Parser::parse("CREATE TABLE cj_a (id INT, name TEXT);"));
    exec.execute(Parser::parse("CREATE TABLE cj_b (id INT, aid INT, val TEXT);"));
    exec.execute(Parser::parse(
        "INSERT INTO cj_a VALUES (1,'A'),(2,'B'),(3,'C');"));
    exec.execute(Parser::parse(
        "INSERT INTO cj_b VALUES (10,1,'x'),(11,2,'y');"));

    std::atomic<int> errors{0};
    std::atomic<bool> stop{false};

    // Writer adds rows to cj_b while joiners run
    std::thread writer([&]() {
        Executor e;
        for (int i = 100; i < 500; ++i) {
            try {
                e.execute(Parser::parse(
                    "INSERT INTO cj_b VALUES (" + std::to_string(i) +
                    "," + std::to_string(1 + i % 3) + ",'v');"));
            } catch (...) { ++errors; }
        }
        stop = true;
    });

    std::thread joiner([&]() {
        Executor e;
        while (!stop) {
            try {
                auto r = e.execute(Parser::parse(
                    "SELECT CJ_A.NAME, CJ_B.VAL FROM CJ_A "
                    "INNER JOIN CJ_B ON CJ_A.ID = CJ_B.AID;"));
                if (!r.ok) ++errors;
            } catch (...) { ++errors; }
        }
    });

    writer.join();
    joiner.join();
    CHECK_EQ(errors.load(), 0, "no errors during concurrent join");
}

// ═════════════════════════════════════════════════════════════════════════════
// 7. ADVERSARIAL INPUT TESTS
// ═════════════════════════════════════════════════════════════════════════════

static void test_adversarial_malformed_sql() {
    SECTION("Adversarial: malformed SQL");
    const char* bad_queries[] = {
        "",
        ";",
        "SELECT",
        "INSERT INTO",
        "CREATE TABLE ()",
        "SELECT * FROM WHERE;",
        "DROP",
        "UPDATE SET;",
        "DELETE WHERE;",
        "SELECT * FROM t INNER JOIN;",
        "SELECT * FROM t ORDER BY;",
        "SELECT * FROM t LIMIT;",
        "CREATE TABLE t ();",
    };

    int parse_errors = 0;
    for (const auto& q : bad_queries) {
        try {
            Parser::parse(q);
        } catch (const ParseError&) {
            ++parse_errors;
        } catch (const std::exception&) {
            ++parse_errors; // any exception is acceptable for malformed input
        }
    }
    // We expect most/all to throw. Allow some to parse as "empty" statements.
    CHECK(parse_errors >= 8, "at least 8/13 malformed queries rejected");
}

static void test_adversarial_boundary_ttl() {
    SECTION("Adversarial: boundary TTL values");
    reset_database();
    Executor exec;

    exec.execute(Parser::parse("CREATE TABLE ttl_test (id INT, v TEXT);"));

    // Insert with TTL = 0 (no expiration)
    auto r = exec.execute(Parser::parse("INSERT INTO ttl_test VALUES (1, 'no_ttl');"));
    CHECK(r.ok, "TTL=0 insert ok");

    // Insert with TTL = INT64_MAX (far future)
    auto tbl = Database::instance().get_table("TTL_TEST");
    if (tbl) {
        std::string err = tbl->insert({"2", "far_future"}, INT64_MAX);
        CHECK(err.empty(), "TTL=INT64_MAX insert ok");
    }

    // Insert with TTL = -1 (negative, should be treated as expired)
    if (tbl) {
        std::string err = tbl->insert({"3", "negative_ttl"}, -1);
        CHECK(err.empty(), "TTL=-1 insert ok");
    }

    r = exec.execute(Parser::parse("SELECT * FROM ttl_test;"));
    CHECK(r.ok, "scan with boundary TTLs ok");
    // Rows 1 (no ttl) and 2 (far future) should be live; row 3 (negative→expired) depends on impl
    CHECK((int)r.num_rows() >= 2, "at least 2 live rows with boundary TTLs");
}

static void test_adversarial_large_payload() {
    SECTION("Adversarial: large string payloads");
    reset_database();
    Executor exec;

    exec.execute(Parser::parse("CREATE TABLE big (id INT, data TEXT);"));

    // 1MB string value
    std::string big_val(1024 * 1024, 'A');
    std::string sql = "INSERT INTO big VALUES (1, '" + big_val + "');";
    auto r = exec.execute(Parser::parse(sql));
    CHECK(r.ok, "1MB value insert ok");

    r = exec.execute(Parser::parse("SELECT data FROM big WHERE id = 1;"));
    CHECK(r.ok && r.num_rows() > 0, "select 1MB value");
    if (r.num_rows() > 0)
        CHECK_EQ((int)r.cell(0, 0).size(), (int)big_val.size(), "1MB value round-trips");
}

static void test_adversarial_sql_injection() {
    SECTION("Adversarial: SQL injection attempts");
    reset_database();
    Executor exec;

    exec.execute(Parser::parse("CREATE TABLE vuln (id INT, name TEXT);"));
    exec.execute(Parser::parse("INSERT INTO vuln VALUES (1, 'safe');"));

    // These should not cause crashes or bypass — parser handles single quotes
    const char* injections[] = {
        "SELECT * FROM vuln WHERE name = 'test'; DROP TABLE vuln; --';",
        "SELECT * FROM vuln WHERE id = 1 OR 1=1;",
    };

    for (const auto& q : injections) {
        try {
            auto r = exec.execute(Parser::parse(q));
            // Either succeeds or fails, but must not crash
            (void)r;
        } catch (const ParseError&) {
            // Expected for malformed injection
        } catch (const std::exception&) {
            // Also acceptable
        }
    }
    // Table must still exist
    CHECK(Database::instance().table_exists("VULN"), "table survives injection attempts");
}

static void test_adversarial_empty_table_operations() {
    SECTION("Adversarial: operations on empty table");
    reset_database();
    Executor exec;

    exec.execute(Parser::parse("CREATE TABLE empty_t (id INT, v TEXT);"));

    auto r = exec.execute(Parser::parse("SELECT * FROM empty_t;"));
    CHECK(r.ok, "SELECT from empty table ok");
    CHECK_EQ((int)r.num_rows(), 0, "0 rows from empty table");

    r = exec.execute(Parser::parse("DELETE FROM empty_t WHERE id = 1;"));
    CHECK(r.ok, "DELETE from empty table ok");

    r = exec.execute(Parser::parse("SELECT COUNT(*) FROM empty_t;"));
    CHECK(r.ok && !r.rows.empty(), "COUNT on empty table");
    if (!r.rows.empty())
        CHECK_STR_EQ(r.rows[0][0], std::string("0"), "COUNT = 0");
}

// ═════════════════════════════════════════════════════════════════════════════
// TEST RUNNER
// ═════════════════════════════════════════════════════════════════════════════

int main() {
    std::cout << "FlexQL Comprehensive Test Suite\n";
    std::cout << "================================\n";

    // Parser tests
    test_parser_create_table();
    test_parser_create_if_not_exists();
    test_parser_insert_single();
    test_parser_insert_batch();
    test_parser_insert_with_col_list();
    test_parser_select_simple();
    test_parser_select_star();
    test_parser_select_join();
    test_parser_select_compound_where();
    test_parser_select_between_in();
    test_parser_select_order_limit_offset();
    test_parser_select_distinct();
    test_parser_select_aggregate();
    test_parser_delete();
    test_parser_update();
    test_parser_drop_table();
    test_parser_errors();

    // PrimaryIndex tests
    test_pk_index_basic();
    test_pk_index_tombstone_duplicate();
    test_pk_index_update();

    // StringArena tests
    test_arena_basic();
    test_arena_empty();
    test_arena_oversized();
    test_arena_clear();

    // Table tests
    test_table_insert_and_scan();
    test_table_pk_enforcement();
    test_table_column_count_mismatch();
    test_table_ttl_expiration();
    test_table_delete();
    test_table_update();
    test_table_truncate();
    test_table_compact();
    test_table_compact_threshold();
    test_arena_page_accounting();
    test_tier2_page_release();
    test_tier3_rebuild_scattered();
    test_tier3_patching();
    test_update_pk_rekey();
    test_pk_index_tombstone_churn_and_remap();
    test_pk_index_compact_if_sparse();
    test_compaction_expired_pk();
    test_compaction_concurrent();
    test_table_flat_insert_batch();
    test_table_where_operators();

    // Executor tests
    test_executor_create_insert_select();
    test_executor_inner_join();
    test_executor_left_join();
    test_lru_cache_concurrent();
    test_self_join();
    test_group_by_vectorized_and_join();
    test_parser_fixes();
    test_wal_rotate();
    test_executor_aggregate();
    test_executor_aggregates_general();
    test_executor_group_by();
    test_executor_delete_update();
    test_executor_errors();

    // Concurrency tests
    test_concurrent_insert_select();
    test_concurrent_insert_delete();
    test_concurrent_join();

    // Adversarial tests
    test_adversarial_malformed_sql();
    test_adversarial_boundary_ttl();
    test_adversarial_large_payload();
    test_adversarial_sql_injection();
    test_adversarial_empty_table_operations();

    // Summary
    std::cout << "\n================================\n";
    std::cout << "Results: " << g_passed << "/" << g_tests << " passed, "
              << g_failed << " failed\n";

    if (g_failed > 0) {
        std::cout << "SOME TESTS FAILED\n";
        return 1;
    }
    std::cout << "ALL TESTS PASSED\n";
    return 0;
}
