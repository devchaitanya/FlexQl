#pragma once
#include "common/types.h"
#include "storage/table.h"   // WhereClause, OrderByClause (kept for legacy)
#include <string>
#include <string_view>
#include <vector>
#include <variant>
#include <memory>

// ── Compound predicate tree ────────────────────────────────────────────────────
// Replaces the single-condition WhereClause for WHERE clauses, enabling
// AND / OR / NOT / BETWEEN / IN / IS NULL.

struct Predicate {
    enum Kind { LEAF, AND_NODE, OR_NODE, NOT_NODE } kind = LEAF;

    // LEAF fields (kind == LEAF)
    std::string col;
    std::string op;      // "=","!=",">",">=","<","<=","LIKE",
                         // "IS NULL","IS NOT NULL","BETWEEN","IN"
    std::string val;     // primary operand
    std::string val2;    // BETWEEN upper bound
    std::vector<std::string> in_vals;   // IN(...) list

    // AND / OR / NOT children
    std::vector<std::unique_ptr<Predicate>> children;

    Predicate() = default;
    Predicate(const Predicate&)            = delete;
    Predicate& operator=(const Predicate&) = delete;
    Predicate(Predicate&&)                 = default;
    Predicate& operator=(Predicate&&)      = default;

    // Convenience factories
    static Predicate leaf(std::string c, std::string o, std::string v,
                          std::string v2 = {},
                          std::vector<std::string> ivs = {}) {
        Predicate p;
        p.kind    = LEAF;
        p.col     = std::move(c);
        p.op      = std::move(o);
        p.val     = std::move(v);
        p.val2    = std::move(v2);
        p.in_vals = std::move(ivs);
        return p;
    }
    // Build from legacy WhereClause
    static Predicate from_where(const WhereClause& w) {
        return leaf(w.col, w.op, w.val);
    }
};

// ── Statement AST nodes ───────────────────────────────────────────────────────

struct CreateTableStmt {
    TableSchema schema;
    bool        if_not_exists = false;
};

struct InsertStmt {
    std::string                                        table;
    std::vector<std::string>                           col_list;  // optional (col1,col2) after table name
    std::vector<std::vector<std::string_view>>         rows;

    // ── Fast-path flat layout (populated by fast_parse_insert) ────────────────
    std::vector<std::string_view>                      flat_values;
    int                                                flat_ncols = 0;
};

struct SelectStmt {
    std::vector<std::string>            cols;
    std::vector<std::string>            col_aliases; // parallel to cols; empty string = no alias
    std::string                         table;
    std::string                         join_table;
    std::string                         join_left;
    std::string                         join_right;
    bool                                left_join = false; // LEFT JOIN vs INNER JOIN
    std::unique_ptr<Predicate>          where;
    std::vector<OrderByClause>          order_by;  // multi-column ORDER BY
    bool                                distinct  = false;
    int                                 limit     = -1;
    int                                 offset    = 0;
    std::string                         agg_func;
    std::string                         agg_col;
    // GROUP BY
    std::vector<std::string>            group_by;
    std::unique_ptr<Predicate>          having;
};

struct DeleteStmt {
    std::string                table;
    std::unique_ptr<Predicate> where;
};

struct DropTableStmt {
    std::string table;
    bool        if_exists = false;
};

struct ShowTablesStmt    {};
struct ShowDatabasesStmt {};

struct DescribeStmt {
    std::string table;
};

struct UpdateStmt {
    std::string table;
    std::vector<std::pair<std::string,std::string>> assignments;
    std::unique_ptr<Predicate> where;
};

struct TruncateStmt {
    std::string table;
};

struct UseDatabaseStmt {
    std::string db_name; // informational only — single-db server
};

// ALTER TABLE tbl ADD COLUMN col_def | MODIFY COLUMN col_def | DROP COLUMN name
struct AlterTableStmt {
    std::string  table;
    enum Action { ADD_COLUMN, DROP_COLUMN, MODIFY_COLUMN } action = ADD_COLUMN;
    ColumnDef    col_def;     // for ADD/MODIFY
    std::string  drop_col;    // for DROP
};

using Statement = std::variant<
    CreateTableStmt, InsertStmt, SelectStmt, DeleteStmt,
    DropTableStmt, ShowTablesStmt, ShowDatabasesStmt, DescribeStmt,
    UpdateStmt, TruncateStmt, UseDatabaseStmt, AlterTableStmt
>;
