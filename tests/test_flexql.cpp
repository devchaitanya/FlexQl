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
    std::string(msg) + " (got '" + (a) + "' expected '" + (b) + "')")

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

    // Larger test: insert N keys, remove every other, verify no false duplicates
    PrimaryIndex idx2;
    for (int i = 0; i < 200; ++i) {
        std::string key = "key" + std::to_string(i);
        CHECK(idx2.insert(key, (size_t)i), "insert " + key);
    }
    for (int i = 0; i < 200; i += 2) {
        std::string key = "key" + std::to_string(i);
        idx2.remove(key);
    }
    // Re-insert removed keys — should succeed
    for (int i = 0; i < 200; i += 2) {
        std::string key = "key" + std::to_string(i);
        CHECK(idx2.insert(key, (size_t)(i + 1000)), "re-insert " + key);
    }
    // Try duplicating the non-removed keys — must fail
    for (int i = 1; i < 200; i += 2) {
        std::string key = "key" + std::to_string(i);
        CHECK(!idx2.insert(key, 9999), "reject dup " + key);
    }
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
    CHECK_EQ((int)res.rows.size(), 2, "2 rows");
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
    CHECK_EQ((int)res.rows.size(), 2, "2 live rows (1 expired)");
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
    CHECK_EQ((int)res.rows.size(), 2, "2 rows remain");
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
    CHECK(res.ok && !res.rows.empty(), "scan after update");
    if (!res.rows.empty())
        CHECK_STR_EQ(res.rows[0][0], std::string("new"), "value updated");
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
    CHECK_EQ((int)res.rows.size(), 0, "0 rows after truncate");
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

    tbl.compact();

    auto res = tbl.scan({"*"}, nullptr, {});
    CHECK_EQ((int)res.rows.size(), 25, "25 rows after compact");

    // Verify PK lookups still work after compaction
    auto pred = Predicate::leaf("ID", "=", "1");
    res = tbl.scan({"V"}, &pred, {});
    CHECK(res.ok && !res.rows.empty(), "PK lookup after compact");
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
    CHECK_EQ((int)res.rows.size(), N, "25K rows scanned");
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
    CHECK_EQ((int)res.rows.size(), 2, "> 2 returns 2 rows");

    // LIKE
    pred = Predicate::leaf("NAME", "LIKE", "A%");
    res = tbl.scan({"NAME"}, &pred, {});
    CHECK_EQ((int)res.rows.size(), 1, "LIKE A% returns 1 row");

    // BETWEEN
    pred.kind = Predicate::LEAF;
    pred.col = "ID"; pred.op = "BETWEEN"; pred.val = "2"; pred.val2 = "10";
    res = tbl.scan({"NAME"}, &pred, {});
    CHECK_EQ((int)res.rows.size(), 3, "BETWEEN 2 AND 10 returns 3 rows");

    // IN
    pred.op = "IN"; pred.in_vals = {"1", "3"};
    res = tbl.scan({"NAME"}, &pred, {});
    CHECK_EQ((int)res.rows.size(), 2, "IN (1,3) returns 2 rows");
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
    CHECK_EQ((int)r.rows.size(), 2, "2 rows match salary > 55000");
    if (r.rows.size() >= 2) {
        CHECK_STR_EQ(r.rows[0][0], std::string("Bob"), "first row = Bob");
        CHECK_STR_EQ(r.rows[1][0], std::string("Carol"), "second row = Carol");
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

static void test_executor_delete_update() {
    SECTION("Executor: DELETE and UPDATE");
    reset_database();
    Executor exec;

    exec.execute(Parser::parse("CREATE TABLE du (id INT PRIMARY KEY, v TEXT);"));
    exec.execute(Parser::parse("INSERT INTO du VALUES (1,'a'),(2,'b'),(3,'c');"));

    auto r = exec.execute(Parser::parse("DELETE FROM du WHERE id = 2;"));
    CHECK(r.ok, "DELETE ok");

    r = exec.execute(Parser::parse("SELECT * FROM du;"));
    CHECK_EQ((int)r.rows.size(), 2, "2 rows after delete");

    r = exec.execute(Parser::parse("UPDATE du SET v = 'updated' WHERE id = 1;"));
    CHECK(r.ok, "UPDATE ok");

    r = exec.execute(Parser::parse("SELECT v FROM du WHERE id = 1;"));
    CHECK(r.ok && !r.rows.empty(), "select after update");
    if (!r.rows.empty())
        CHECK_STR_EQ(r.rows[0][0], std::string("updated"), "value updated");
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
    CHECK((int)r.rows.size() >= 2, "at least 2 live rows with boundary TTLs");
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
    CHECK(r.ok && !r.rows.empty(), "select 1MB value");
    if (!r.rows.empty())
        CHECK_EQ((int)r.rows[0][0].size(), (int)big_val.size(), "1MB value round-trips");
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
    CHECK_EQ((int)r.rows.size(), 0, "0 rows from empty table");

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
    test_table_flat_insert_batch();
    test_table_where_operators();

    // Executor tests
    test_executor_create_insert_select();
    test_executor_inner_join();
    test_executor_left_join();
    test_executor_aggregate();
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
