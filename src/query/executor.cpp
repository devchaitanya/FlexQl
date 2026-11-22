#include "query/executor.h"
#include "parser/ast.h"
#include "storage/database.h"
#include "cache/lru_cache.h"
#include "utils/string_utils.h"
#include <algorithm>
#include <cfloat>
#include <functional>
#include <set>
#include <shared_mutex>
#include <unordered_map>

// ── Global LRU query cache ────────────────────────────────────────────────────
static LRUCache<std::string, QueryResult> g_cache(1024);

// ── Dispatch ──────────────────────────────────────────────────────────────────
QueryResult Executor::execute(const Statement& stmt) {
    return std::visit([this](const auto& s) -> QueryResult {
        using T = std::decay_t<decltype(s)>;
        if constexpr (std::is_same_v<T, CreateTableStmt>)   return exec_create(s);
        if constexpr (std::is_same_v<T, InsertStmt>)        return exec_insert(s);
        if constexpr (std::is_same_v<T, SelectStmt>)        return exec_select(s);
        if constexpr (std::is_same_v<T, DeleteStmt>)        return exec_delete(s);
        if constexpr (std::is_same_v<T, UpdateStmt>)        return exec_update(s);
        if constexpr (std::is_same_v<T, DropTableStmt>)     return exec_drop(s);
        if constexpr (std::is_same_v<T, TruncateStmt>)      return exec_truncate(s);
        if constexpr (std::is_same_v<T, ShowTablesStmt>)    return exec_show_tables();
        if constexpr (std::is_same_v<T, ShowDatabasesStmt>) return exec_show_databases();
        if constexpr (std::is_same_v<T, DescribeStmt>)      return exec_describe(s);
        if constexpr (std::is_same_v<T, UseDatabaseStmt>)   return exec_use_database(s);
        if constexpr (std::is_same_v<T, AlterTableStmt>)    return exec_alter(s);
        return QueryResult::err("unknown statement type");
    }, stmt);
}

QueryResult Executor::execute(Statement&& stmt) {
    if (auto* ins = std::get_if<InsertStmt>(&stmt))
        return exec_insert(std::move(*ins));
    return execute(static_cast<const Statement&>(stmt));
}

// ── CREATE TABLE ──────────────────────────────────────────────────────────────
QueryResult Executor::exec_create(const CreateTableStmt& s) {
    std::string err = Database::instance().create_table(s.schema, s.if_not_exists);
    if (!err.empty()) return QueryResult::err(err);
    return QueryResult::success();
}

// ── INSERT ────────────────────────────────────────────────────────────────────
QueryResult Executor::exec_insert(const InsertStmt& s) {
    auto tbl = Database::instance().get_table(s.table);
    if (!tbl) return QueryResult::err("no such table: " + s.table);
    int num_cols = (int)tbl->schema().columns.size();

    for (const auto& row_vals : s.rows) {
        std::vector<std::string> values;
        values.reserve(row_vals.size());
        for (auto sv : row_vals) values.emplace_back(sv);

        int64_t expires_at = 0;
        if ((int)values.size() == num_cols + 1) {
            try { expires_at = (int64_t)std::stoll(values.back()); } catch (...) {}
            values.pop_back();
        }
        std::string err = tbl->insert(std::move(values), expires_at);
        if (!err.empty()) return QueryResult::err(err);
    }

    g_cache.invalidate_prefix(strutil::to_upper(s.table));
    return QueryResult::success();
}

QueryResult Executor::exec_insert(InsertStmt&& s) {
    auto tbl = Database::instance().get_table(s.table);
    if (!tbl) return QueryResult::err("no such table: " + s.table);
    const int num_cols = (int)tbl->schema().columns.size();

    // ── Column-list reordering ─────────────────────────────────────────────────
    // INSERT INTO t (col2, col1) VALUES (v2, v1) → reorder to schema order,
    // filling missing columns with empty string (NULL equivalent).
    if (!s.col_list.empty()) {
        // Build mapping: col_list[i] → schema index
        std::vector<int> col_map;
        col_map.reserve(s.col_list.size());
        for (const auto& cname : s.col_list) {
            int idx = tbl->schema().col_index(cname);
            if (idx < 0) return QueryResult::err("unknown column: " + cname);
            col_map.push_back(idx);
        }
        // Rewrite flat_values / rows to full schema order
        if (s.flat_ncols > 0) {
            const size_t nrows = s.flat_values.size() / (size_t)s.flat_ncols;
            std::vector<std::string_view> reordered(nrows * (size_t)num_cols,
                                                     std::string_view("", 0));
            for (size_t r = 0; r < nrows; ++r)
                for (int ci = 0; ci < (int)col_map.size(); ++ci)
                    reordered[r * (size_t)num_cols + (size_t)col_map[ci]] =
                        s.flat_values[r * (size_t)s.flat_ncols + (size_t)ci];
            s.flat_values = std::move(reordered);
            s.flat_ncols  = num_cols;
        } else {
            for (auto& row : s.rows) {
                std::vector<std::string_view> full(num_cols, std::string_view("", 0));
                for (int ci = 0; ci < (int)col_map.size() && ci < (int)row.size(); ++ci)
                    full[(size_t)col_map[ci]] = row[(size_t)ci];
                row = std::move(full);
            }
        }
    }

    if (s.flat_ncols > 0) {
        const size_t n = s.flat_values.size() / (size_t)s.flat_ncols;
        int64_t expires_at = 0;
        if (s.flat_ncols == num_cols + 1) {
            if (n > 0)
                expires_at = (int64_t)std::strtoll(
                    s.flat_values[num_cols].data(), nullptr, 10);
            const int new_ncols = num_cols;
            std::vector<std::string_view> trimmed;
            trimmed.reserve(n * (size_t)new_ncols);
            for (size_t r = 0; r < n; ++r)
                for (int c = 0; c < new_ncols; ++c)
                    trimmed.push_back(s.flat_values[r * (size_t)s.flat_ncols + c]);
            std::string err = tbl->insert_flat(trimmed.data(), n, new_ncols, expires_at);
            if (!err.empty()) return QueryResult::err(err);
        } else {
            std::string err = tbl->insert_flat(s.flat_values.data(), n, s.flat_ncols, 0);
            if (!err.empty()) return QueryResult::err(err);
        }
        g_cache.invalidate_prefix(s.table);
        return QueryResult::success();
    }

    const size_t n = s.rows.size();
    std::vector<int64_t> expires_vec(n, 0);
    for (size_t r = 0; r < n; ++r) {
        auto& row = s.rows[r];
        if ((int)row.size() == num_cols + 1) {
            expires_vec[r] = (int64_t)std::strtoll(row.back().data(), nullptr, 10);
            row.pop_back();
        }
    }
    std::string err = tbl->insert_batch(std::move(s.rows), std::move(expires_vec));
    if (!err.empty()) return QueryResult::err(err);
    g_cache.invalidate_prefix(strutil::to_upper(s.table));
    return QueryResult::success();
}

// ── SELECT ────────────────────────────────────────────────────────────────────
// ── Aggregate helpers ─────────────────────────────────────────────────────────
// "SUM(T.SALARY)" → func="SUM", arg="SALARY" (table prefix dropped);
// "COUNT(*)" → arg="*". Returns false if expr is not an aggregate call.
static bool parse_aggregate(const std::string& expr, std::string& func, std::string& arg,
                            std::string* raw_arg = nullptr) {
    const size_t lp = expr.find('(');
    if (lp == std::string::npos || expr.empty() || expr.back() != ')') return false;
    func = strutil::to_upper(expr.substr(0, lp));
    if (func != "COUNT" && func != "SUM" && func != "AVG" && func != "MIN" && func != "MAX")
        return false;
    arg = expr.substr(lp + 1, expr.size() - lp - 2);
    if (raw_arg) *raw_arg = arg;                 // keeps "T.COL" (needed for JOINs)
    const size_t dot = arg.find('.');
    if (dot != std::string::npos) arg = arg.substr(dot + 1);
    return true;
}

static bool has_aggregate(const std::vector<std::string>& cols) {
    std::string f, a;
    for (const auto& c : cols) if (parse_aggregate(c, f, a)) return true;
    return false;
}

static bool is_numeric_type(ColumnType t) {
    return t == ColumnType::INT || t == ColumnType::DECIMAL;
}

// ── Aggregates without GROUP BY ───────────────────────────────────────────────
//   SELECT COUNT(*), SUM(SALARY) AS total, MAX(AGE) FROM t [WHERE ...]
// The whole (filtered) table is ONE group, so the result is exactly one row.
// Plain columns are rejected: with no GROUP BY there is no single value to show
// for them (which row's DEPT would it be?).
static QueryResult exec_aggregate(const Table& tbl, const SelectStmt& s) {
    struct Item { std::string func, arg; int state = -1; };   // state: index into states/cols
    std::vector<Item> items;
    std::vector<int>  cols;        // DISTINCT columns to aggregate: SUM(x), MAX(x) share one pass slot

    for (const auto& expr : s.cols) {
        Item it;
        if (!parse_aggregate(expr, it.func, it.arg))
            return QueryResult::err("column " + expr + " must appear in GROUP BY "
                                    "or be used in an aggregate function");
        if (it.arg == "*") {
            if (it.func != "COUNT")
                return QueryResult::err(it.func + "(*) is not valid (only COUNT(*))");
        } else {
            const int c = tbl.schema().col_index(it.arg);
            if (c < 0) return QueryResult::err("unknown column: " + it.arg);
            if ((it.func == "SUM" || it.func == "AVG") &&
                !is_numeric_type(tbl.schema().columns[c].type))
                return QueryResult::err(it.func + " requires a numeric column: " + it.arg);
            for (size_t k = 0; k < cols.size(); ++k)
                if (cols[k] == c) { it.state = (int)k; break; }
            if (it.state < 0) { it.state = (int)cols.size(); cols.push_back(c); }
        }
        items.push_back(std::move(it));
    }

    // Only COUNT(*)s → the SIMD count path (no per-column work at all).
    // Otherwise ONE pass computes every aggregate under one lock.
    int64_t rows;
    std::vector<AggState> states;
    if (cols.empty()) rows = (int64_t)tbl.count_rows(s.where.get());
    else              rows = tbl.aggregate(s.where.get(), cols, states);

    QueryResult res;
    res.ok = true;
    std::vector<std::string> row;
    for (size_t i = 0; i < items.size(); ++i) {
        const Item& it = items[i];
        const bool aliased = i < s.col_aliases.size() && !s.col_aliases[i].empty();
        res.column_names.push_back(aliased ? s.col_aliases[i] : s.cols[i]);
        row.push_back(it.state < 0 ? std::to_string(rows)                    // COUNT(*)
                                   : states[(size_t)it.state].result(it.func, false, rows));
    }
    if (s.offset == 0 && s.limit != 0) res.rows.push_back(std::move(row));   // one row, honours LIMIT 0
    return res;
}

QueryResult Executor::exec_select(const SelectStmt& s) {
    // Routing is decided by the SELECT list itself (not by a single agg_func
    // field), so any number of aggregates works:
    //   JOIN (+ aggregates/GROUP BY → exec_join_grouped) → exec_join
    //   GROUP BY                  → exec_group_by   (aggregates per group)
    //   aggregates, no GROUP BY   → exec_aggregate  (whole table = one group, one row)
    //   otherwise                 → plain scan (+ result cache)
    const bool any_agg = has_aggregate(s.cols);

    if (!s.join_table.empty()) {
        if (any_agg || !s.group_by.empty()) return exec_join_grouped(s);
        return exec_join(s);
    }

    auto tbl = Database::instance().get_table(s.table);
    if (!tbl) return QueryResult::err("no such table: " + s.table);

    if (!s.group_by.empty()) return exec_group_by(s);
    if (any_agg)             return exec_aggregate(*tbl, s);

    // Validate requested columns
    if (s.cols.size() != 1 || s.cols[0] != "*") {
        for (const auto& c : s.cols) {
            std::string col = c;
            size_t dot = col.find('.');
            if (dot != std::string::npos) col = col.substr(dot + 1);
            if (tbl->schema().col_index(col) < 0)
                return QueryResult::err("unknown column: " + c);
        }
    }

    // Cache key
    std::string cache_key = strutil::to_upper(s.table) + "|";
    cache_key += (s.cols.size()==1 && s.cols[0]=="*") ? "*"
        : [&]{ std::string k; for(auto& c:s.cols) k+=c+","; return k; }();
    // Predicate is not cached for compound predicates — only simple LEAF ones
    // Key must include EVERY operand: BETWEEN's upper bound and the IN list too,
    // otherwise "BETWEEN 1 AND 2" and "BETWEEN 1 AND 4" share a cache entry.
    if (s.where && s.where->kind == Predicate::LEAF) {
        cache_key += "|W:" + s.where->col + '\x1f' + s.where->op + '\x1f' +
                     s.where->val + '\x1f' + s.where->val2;
        for (const auto& v : s.where->in_vals) { cache_key += '\x1f'; cache_key += v; }
    }
    else if (s.where)
        cache_key.clear(); // skip cache for compound predicates
    if (!cache_key.empty() && !s.order_by.empty())
        for (const auto& ob : s.order_by)
            cache_key += "|O:" + ob.col + (ob.desc ? "D" : "A");
    if (!cache_key.empty())
        for (const auto& a : s.col_aliases) { cache_key += "|A:"; cache_key += a; }
    if (!cache_key.empty() && s.distinct)  cache_key += "|D";
    if (!cache_key.empty() && s.limit >= 0) cache_key += "|L" + std::to_string(s.limit);
    if (!cache_key.empty() && s.offset > 0) cache_key += "|F" + std::to_string(s.offset);

    if (!cache_key.empty())
        if (auto cached = g_cache.get(cache_key)) return *cached;   // copy: safe even if evicted now

    // Push LIMIT/OFFSET into scan() unless DISTINCT needs all rows first
    int scan_limit  = s.distinct ? -1 : s.limit;
    int scan_offset = s.distinct ?  0 : s.offset;
    QueryResult res = tbl->scan(s.cols, s.where.get(), s.order_by,
                                scan_limit, scan_offset);
    if (!res.ok) return res;

    // Apply AS aliases to output column names
    if (!s.col_aliases.empty()) {
        for (size_t i = 0; i < res.column_names.size() && i < s.col_aliases.size(); ++i)
            if (!s.col_aliases[i].empty())
                res.column_names[i] = s.col_aliases[i];
    }

    if (s.distinct) {
        // Convert compact flat storage to rows for dedup, then clear flat storage
        const size_t ncols = res.column_names.size();
        const size_t nrows = res.num_rows();
        if (!res.flat_offsets.empty()) {
            res.rows.reserve(nrows);
            for (size_t r = 0; r < nrows; ++r) {
                std::vector<std::string> row(ncols);
                for (size_t c = 0; c < ncols; ++c)
                    row[c] = std::string(res.cell(r, c));
                res.rows.push_back(std::move(row));
            }
            res.flat_offsets.clear();
            res.flat_data.clear();
        }
        std::set<std::vector<std::string>> seen;
        std::vector<std::vector<std::string>> unique_rows;
        for (auto& row : res.rows)
            if (seen.insert(row).second)
                unique_rows.push_back(std::move(row));
        res.rows = std::move(unique_rows);

        // Apply OFFSET/LIMIT after dedup (not pushed into scan for DISTINCT)
        if (s.offset > 0 && s.offset < (int)res.rows.size())
            res.rows.erase(res.rows.begin(), res.rows.begin() + s.offset);
        else if (s.offset >= (int)res.rows.size())
            res.rows.clear();
        if (s.limit >= 0 && (int)res.rows.size() > s.limit)
            res.rows.resize(s.limit);
    }

    if (!cache_key.empty())
        g_cache.put(cache_key, res, strutil::to_upper(s.table));
    return res;
}

// ── GROUP BY (single table, and over JOIN) ────────────────────────────────────
// Both paths end in the same shape — per group: key values, row count, and one
// AggState per aggregated column (Table::GroupResult) — so planning the output
// columns, HAVING and LIMIT are shared.
//
// What each output column is:
//   GROUP → value of GROUP BY column #idx
//   AGG   → func over aggregate slot #idx (a distinct aggregated column)
//   STAR  → COUNT(*) = rows in the group
namespace {
struct OutItem {
    enum Kind { GROUP, AGG, STAR } kind = GROUP;
    int         idx = -1;
    std::string func;          // AGG only
    std::string name;          // output column name (alias or expression)
    std::string expr;          // expression text, for HAVING matching
    bool        hidden = false;// needed only by HAVING, not returned
};

constexpr int AMBIGUOUS = -2;  // resolve() result: name matches several columns

// Plan the output columns.
//   resolve(name) → source column id (-1 unknown, AMBIGUOUS)
//   numeric(id)   → column is INT/DECIMAL
// Fills group_ids (per GROUP BY entry), agg_ids (distinct aggregated columns)
// and items. Returns "" or an error message.
std::string plan_group_output(const SelectStmt& s,
                              const std::function<int(const std::string&)>& resolve,
                              const std::function<bool(int)>& numeric,
                              std::vector<int>& group_ids, std::vector<int>& agg_ids,
                              std::vector<OutItem>& items) {
    auto bad_name = [](int id, const std::string& n) {
        return id == AMBIGUOUS ? "ambiguous column: " + n + " (qualify it with the table name)"
                               : "unknown column: " + n;
    };
    for (const auto& g : s.group_by) {
        const int id = resolve(g);
        if (id < 0) return "GROUP BY: " + bad_name(id, g);
        group_ids.push_back(id);
    }

    auto add_item = [&](const std::string& expr, const std::string& name,
                        bool hidden) -> std::string {
        OutItem it;
        it.expr = expr; it.name = name; it.hidden = hidden;
        std::string func, arg, raw;
        if (parse_aggregate(expr, func, arg, &raw)) {
            if (raw == "*") {
                if (func != "COUNT") return func + "(*) is not valid (only COUNT(*))";
                it.kind = OutItem::STAR;
            } else {
                const int id = resolve(raw);
                if (id < 0) return bad_name(id, raw);
                if ((func == "SUM" || func == "AVG") && !numeric(id))
                    return func + " requires a numeric column: " + raw;
                int slot = -1;
                for (size_t k = 0; k < agg_ids.size(); ++k) if (agg_ids[k] == id) slot = (int)k;
                if (slot < 0) { slot = (int)agg_ids.size(); agg_ids.push_back(id); }
                it.kind = OutItem::AGG; it.idx = slot; it.func = func;
            }
        } else {
            if (expr == "*") return "SELECT * cannot be combined with GROUP BY or aggregates";
            const int id = resolve(expr);
            int pos = -1;
            for (size_t g = 0; g < group_ids.size(); ++g) if (id >= 0 && group_ids[g] == id) pos = (int)g;
            if (pos < 0)
                return "column " + expr + " must appear in GROUP BY or be used in an aggregate function";
            it.kind = OutItem::GROUP; it.idx = pos;
        }
        items.push_back(std::move(it));
        return "";
    };

    for (size_t i = 0; i < s.cols.size(); ++i) {
        const bool aliased = i < s.col_aliases.size() && !s.col_aliases[i].empty();
        std::string e = add_item(s.cols[i], aliased ? s.col_aliases[i] : s.cols[i], false);
        if (!e.empty()) return e;
    }

    // HAVING may use aggregates/columns that are NOT in the SELECT list
    // ("... GROUP BY dept HAVING COUNT(*) > 1"). Compute them as hidden
    // columns. (Before, such a HAVING could never match → every group dropped.)
    if (s.having) {
        std::function<std::string(const Predicate&)> walk = [&](const Predicate& p) -> std::string {
            if (p.kind != Predicate::LEAF) {
                for (const auto& c : p.children) { std::string e = walk(*c); if (!e.empty()) return e; }
                return "";
            }
            for (const auto& it : items)
                if (strutil::iequals(it.expr, p.col) || strutil::iequals(it.name, p.col)) return "";
            return add_item(p.col, p.col, true);
        };
        std::string e = walk(*s.having);
        if (!e.empty()) return "HAVING: " + e;
    }
    return "";
}

// HAVING is evaluated on a group's computed values (vals[j] ↔ items[j]).
bool having_ok(const Predicate& p, const std::vector<OutItem>& items,
               const std::vector<std::string>& vals) {
    if (p.kind == Predicate::AND_NODE) {
        for (const auto& c : p.children) if (!having_ok(*c, items, vals)) return false;
        return true;
    }
    if (p.kind == Predicate::OR_NODE) {
        for (const auto& c : p.children) if (having_ok(*c, items, vals)) return true;
        return false;
    }
    if (p.kind == Predicate::NOT_NODE)
        return !p.children.empty() && !having_ok(*p.children[0], items, vals);
    const std::string* v = nullptr;
    for (size_t j = 0; j < items.size(); ++j)
        if (strutil::iequals(items[j].expr, p.col) || strutil::iequals(items[j].name, p.col)) { v = &vals[j]; break; }
    if (!v) return false;
    if (p.op == "IS NULL")     return v->empty();
    if (p.op == "IS NOT NULL") return !v->empty();
    if (v->empty()) return false;                       // NULL compares as unknown → false
    char* e1 = nullptr; char* e2 = nullptr;
    const double a = std::strtod(v->c_str(), &e1), b = std::strtod(p.val.c_str(), &e2);
    const bool num = e1 && *e1 == '\0' && e2 && *e2 == '\0' && !p.val.empty();
    auto cmp = [&](int c) {                              // c = sign(value - literal)
        if (p.op == "=")  return c == 0;
        if (p.op == "!=") return c != 0;
        if (p.op == ">")  return c >  0;
        if (p.op == ">=") return c >= 0;
        if (p.op == "<")  return c <  0;
        if (p.op == "<=") return c <= 0;
        return false;
    };
    if (num) return cmp(a < b ? -1 : a > b ? 1 : 0);
    const int c = v->compare(p.val);
    return cmp(c < 0 ? -1 : c > 0 ? 1 : 0);
}

// Turn grouped data into the result: one row per group, HAVING, OFFSET/LIMIT.
QueryResult build_grouped_result(const SelectStmt& s, Table::GroupResult& gr,
                                 const std::vector<OutItem>& items,
                                 const std::vector<char>& agg_numeric) {
    // Aggregates WITHOUT GROUP BY (only reachable here for JOINs): the whole
    // input is one group, and SQL returns that one row even for zero rows
    // (COUNT = 0, the others NULL).
    if (s.group_by.empty() && gr.rows.empty()) {
        gr.keys.emplace_back();
        gr.rows.push_back(0);
        gr.states.emplace_back();
        for (char n : agg_numeric) gr.states.back().emplace_back(n != 0);
    }
    QueryResult res;
    res.ok = true;
    for (const auto& it : items) if (!it.hidden) res.column_names.push_back(it.name);

    std::vector<std::string> vals(items.size());
    for (size_t g = 0; g < gr.rows.size(); ++g) {
        for (size_t j = 0; j < items.size(); ++j) {
            const OutItem& it = items[j];
            if      (it.kind == OutItem::GROUP) vals[j] = gr.keys[g][(size_t)it.idx];
            else if (it.kind == OutItem::STAR)  vals[j] = std::to_string(gr.rows[g]);
            else vals[j] = gr.states[g][(size_t)it.idx].result(it.func, false, gr.rows[g]);
        }
        if (s.having && !having_ok(*s.having, items, vals)) continue;
        std::vector<std::string> row;
        for (size_t j = 0; j < items.size(); ++j) if (!items[j].hidden) row.push_back(vals[j]);
        res.rows.push_back(std::move(row));
    }
    if (s.offset > 0) {
        const size_t off = std::min(res.rows.size(), (size_t)s.offset);
        res.rows.erase(res.rows.begin(), res.rows.begin() + (std::ptrdiff_t)off);
    }
    if (s.limit >= 0 && res.rows.size() > (size_t)s.limit) res.rows.resize((size_t)s.limit);
    return res;
}
} // namespace

// Single table: Table::group_aggregate reads the table directly (one pass,
// no materialized rows, no stod). Before, every matching row was copied into
// strings first and each value re-parsed with stod (~83 ms on 1M rows).
QueryResult Executor::exec_group_by(const SelectStmt& s) {
    auto tbl = Database::instance().get_table(s.table);
    if (!tbl) return QueryResult::err("no such table: " + s.table);
    const TableSchema& sc = tbl->schema();

    auto resolve = [&](const std::string& name) -> int {
        std::string n = name;
        const size_t dot = n.find('.');
        if (dot != std::string::npos) n = n.substr(dot + 1);
        for (int i = 0; i < (int)sc.columns.size(); ++i)
            if (strutil::iequals(sc.columns[i].name, n)) return i;
        return -1;
    };
    auto numeric = [&](int id) { return is_numeric_type(sc.columns[id].type); };

    std::vector<int> group_ids, agg_ids;
    std::vector<OutItem> items;
    const std::string err = plan_group_output(s, resolve, numeric, group_ids, agg_ids, items);
    if (!err.empty()) return QueryResult::err(err);

    Table::GroupResult gr = tbl->group_aggregate(s.where.get(), group_ids, agg_ids);
    std::vector<char> agg_numeric;
    for (int id : agg_ids) agg_numeric.push_back(numeric(id));
    return build_grouped_result(s, gr, items, agg_numeric);
}

// JOIN + aggregates and/or GROUP BY: run the join for ALL columns (no ORDER BY
// or LIMIT yet), then group the joined rows with the same AggState rules.
// Columns are named "TABLE.COL"; an unqualified name must be unique.
QueryResult Executor::exec_join_grouped(const SelectStmt& s) {
    QueryResult base = exec_join(s, /*all_columns=*/true);
    if (!base.ok) return base;

    auto resolve = [&](const std::string& name) -> int {
        if (name.find('.') != std::string::npos) {
            for (int i = 0; i < (int)base.column_names.size(); ++i)
                if (strutil::iequals(base.column_names[i], name)) return i;
            return -1;
        }
        int found = -1;
        for (int i = 0; i < (int)base.column_names.size(); ++i) {
            const std::string& cn = base.column_names[i];
            const size_t d = cn.find('.');
            if (strutil::iequals(d == std::string::npos ? cn : cn.substr(d + 1), name)) {
                if (found >= 0) return AMBIGUOUS;
                found = i;
            }
        }
        return found;
    };
    auto numeric = [&](int id) {
        const std::string& cn = base.column_names[(size_t)id];
        const size_t d = cn.find('.');
        if (d == std::string::npos) return false;
        auto t = Database::instance().get_table(cn.substr(0, d));
        if (!t) return false;
        const int c = t->schema().col_index(cn.substr(d + 1));
        return c >= 0 && is_numeric_type(t->schema().columns[c].type);
    };

    std::vector<int> group_ids, agg_ids;
    std::vector<OutItem> items;
    const std::string err = plan_group_output(s, resolve, numeric, group_ids, agg_ids, items);
    if (!err.empty()) return QueryResult::err(err);

    std::vector<char> agg_numeric;
    for (int id : agg_ids) agg_numeric.push_back(numeric(id));

    // Hash grouping over the joined rows (length-prefixed keys, as in Table).
    Table::GroupResult gr;
    std::unordered_map<std::string, uint32_t> map;
    std::string key;
    const size_t nrows = base.num_rows();
    for (size_t r = 0; r < nrows; ++r) {
        key.clear();
        for (int g : group_ids) {
            const std::string_view v = base.cell(r, (size_t)g);
            const uint32_t len = (uint32_t)v.size();
            key.append(reinterpret_cast<const char*>(&len), sizeof len);
            key.append(v.data(), v.size());
        }
        auto it = map.find(key);
        if (it == map.end()) {
            it = map.emplace(key, (uint32_t)gr.rows.size()).first;
            gr.keys.emplace_back();
            for (int g : group_ids) gr.keys.back().emplace_back(base.cell(r, (size_t)g));
            gr.rows.push_back(0);
            gr.states.emplace_back();
            for (char n : agg_numeric) gr.states.back().emplace_back(n != 0);
        }
        const uint32_t g = it->second;
        ++gr.rows[g];
        for (size_t k = 0; k < agg_ids.size(); ++k)
            gr.states[g][k].add_text(base.cell(r, (size_t)agg_ids[k]));
    }
    return build_grouped_result(s, gr, items, agg_numeric);
}

// ── JOIN (INNER + LEFT) ───────────────────────────────────────────────────────
QueryResult Executor::exec_join(const SelectStmt& s, bool all_columns) {
    auto tbl_a = Database::instance().get_table(s.table);
    auto tbl_b = Database::instance().get_table(s.join_table);
    if (!tbl_a) return QueryResult::err("no such table: " + s.table);
    if (!tbl_b) return QueryResult::err("no such table: " + s.join_table);

    // Two tables → lock both (shared) in a global order (by name), so
    // "A JOIN B" and "B JOIN A" can never deadlock each other.
    // Self-join (T JOIN T): both sides are the SAME Table object — lock its
    // mutex once. Locking a std::shared_mutex twice from one thread is
    // undefined behaviour, and deadlocks if a writer queues between the two.
    const bool self_join = (tbl_a.get() == tbl_b.get());
    std::shared_lock<std::shared_mutex> lock_first(
        strutil::to_upper(s.table) < strutil::to_upper(s.join_table)
            ? tbl_a->mtx : tbl_b->mtx, std::defer_lock);
    std::shared_lock<std::shared_mutex> lock_second(
        strutil::to_upper(s.table) < strutil::to_upper(s.join_table)
            ? tbl_b->mtx : tbl_a->mtx, std::defer_lock);
    lock_first.lock();
    if (!self_join) lock_second.lock();

    const TableSchema& sa = tbl_a->schema();
    const TableSchema& sb = tbl_b->schema();

    auto strip_prefix = [](const std::string& col) {
        size_t dot = col.find('.');
        return dot != std::string::npos ? col.substr(dot + 1) : col;
    };

    std::string left_col  = strip_prefix(s.join_left);
    std::string right_col = strip_prefix(s.join_right);

    int col_a = sa.col_index(left_col);
    int col_b = sb.col_index(right_col);
    if (col_a < 0) { col_a = sa.col_index(right_col); col_b = sb.col_index(left_col); }
    if (col_a < 0 || col_b < 0)
        return QueryResult::err("join column not found");

    auto rows_a = tbl_a->live_rows();
    auto rows_b = tbl_b->live_rows();

    // For INNER JOIN, build on smaller table.  For LEFT JOIN, build stays on left (A).
    bool swapped = !s.left_join && (rows_b.size() < rows_a.size());
    if (swapped) {
        std::swap(rows_a, rows_b);
        std::swap(col_a, col_b);
    }

    std::unordered_multimap<std::string, size_t> hash_map;
    hash_map.reserve(rows_a.size());
    for (size_t i = 0; i < rows_a.size(); ++i) {
        const auto& vals = rows_a[i].first;
        if (col_a < (int)vals.size())
            hash_map.emplace(vals[col_a], i);
    }

    const TableSchema& schema_a = swapped ? sb : sa;
    const TableSchema& schema_b = swapped ? sa : sb;

    QueryResult res;
    for (const auto& cd : schema_a.columns)
        res.column_names.push_back(schema_a.table_name + "." + cd.name);
    for (const auto& cd : schema_b.columns)
        res.column_names.push_back(schema_b.table_name + "." + cd.name);

    // all_columns: called by exec_join_grouped, which needs every column of
    // both tables (and applies its own HAVING/LIMIT), whatever the SELECT list.
    bool select_all = all_columns || (s.cols.size() == 1 && s.cols[0] == "*");
    std::vector<int> out_a_idx, out_b_idx;
    std::vector<std::string> out_names;

    if (select_all) {
        for (int i = 0; i < (int)schema_a.columns.size(); ++i) { out_a_idx.push_back(i); out_b_idx.push_back(-1); }
        for (int i = 0; i < (int)schema_b.columns.size(); ++i) { out_a_idx.push_back(-1); out_b_idx.push_back(i); }
        out_names = res.column_names;
    } else {
        for (const auto& c : s.cols) {
            std::string tbl_part, col_part;
            size_t dot = c.find('.');
            if (dot != std::string::npos) { tbl_part = c.substr(0, dot); col_part = c.substr(dot+1); }
            else col_part = c;
            int ia = schema_a.col_index(col_part), ib = schema_b.col_index(col_part);
            if (!tbl_part.empty()) {
                if (strutil::iequals(tbl_part, schema_a.table_name) && ia >= 0) { out_a_idx.push_back(ia); out_b_idx.push_back(-1); }
                else if (strutil::iequals(tbl_part, schema_b.table_name) && ib >= 0) { out_a_idx.push_back(-1); out_b_idx.push_back(ib); }
                else return QueryResult::err("unknown column: " + c);
            } else {
                if      (ia >= 0) { out_a_idx.push_back(ia); out_b_idx.push_back(-1); }
                else if (ib >= 0) { out_a_idx.push_back(-1); out_b_idx.push_back(ib); }
                else return QueryResult::err("unknown column: " + c);
            }
            out_names.push_back(c);
        }
    }

    // ORDER BY may name a column that is not in the SELECT list
    // ("SELECT emp.id ... ORDER BY emp.dept_id"). Rows are projected while
    // probing, so such a column would be gone by sort time (it used to be
    // silently ignored). Then: build FULL rows, sort, and project afterwards.
    std::vector<int> req_a, req_b;              // requested projection (if deferred)
    std::vector<std::string> req_names;
    bool project_later = false;
    if (!select_all && !all_columns) {
        for (const auto& ob : s.order_by) {
            const std::string oc = strip_prefix(ob.col);
            bool present = false;
            for (const auto& n : out_names) if (strutil::iequals(strip_prefix(n), oc)) present = true;
            if (!present) project_later = true;
        }
    }
    if (project_later) {
        req_a = out_a_idx; req_b = out_b_idx; req_names = out_names;
        out_a_idx.clear(); out_b_idx.clear(); out_names.clear();
        for (int i = 0; i < (int)schema_a.columns.size(); ++i) {
            out_a_idx.push_back(i); out_b_idx.push_back(-1);
            out_names.push_back(schema_a.table_name + "." + schema_a.columns[i].name);
        }
        for (int i = 0; i < (int)schema_b.columns.size(); ++i) {
            out_a_idx.push_back(-1); out_b_idx.push_back(i);
            out_names.push_back(schema_b.table_name + "." + schema_b.columns[i].name);
        }
    }
    res.column_names = out_names;

    // WHERE predicate evaluation helper for combined rows
    auto eval_where = [&](const std::vector<std::string>& vals_a,
                          const std::vector<std::string>& vals_b) -> bool {
        if (!s.where) return true;
        std::function<bool(const Predicate&)> eval = [&](const Predicate& pred) -> bool {
            if (pred.kind == Predicate::AND_NODE) {
                for (auto& c : pred.children) { if (!eval(*c)) return false; }
                return true;
            }
            if (pred.kind == Predicate::OR_NODE) {
                for (auto& c : pred.children) { if (eval(*c)) return true; }
                return false;
            }
            if (pred.kind == Predicate::NOT_NODE)
                return !pred.children.empty() && !eval(*pred.children[0]);
            // LEAF
            std::string col_name = strip_prefix(pred.col);
            std::string row_val;
            ColumnType col_type = ColumnType::TEXT;
            int ia2 = schema_a.col_index(col_name), ib2 = schema_b.col_index(col_name);
            if (ia2 >= 0 && ia2 < (int)vals_a.size()) { row_val = vals_a[ia2]; col_type = schema_a.columns[ia2].type; }
            else if (ib2 >= 0 && ib2 < (int)vals_b.size()) { row_val = vals_b[ib2]; col_type = schema_b.columns[ib2].type; }
            bool numeric = (col_type == ColumnType::INT || col_type == ColumnType::DECIMAL);
            try {
                if (numeric) {
                    double a = std::stod(row_val), b = std::stod(pred.val);
                    if (pred.op=="=")  return a==b;
                    if (pred.op=="!=") return a!=b;
                    if (pred.op==">")  return a>b;
                    if (pred.op==">=") return a>=b;
                    if (pred.op=="<")  return a<b;
                    if (pred.op=="<=") return a<=b;
                } else {
                    if (pred.op=="=")  return row_val==pred.val;
                    if (pred.op=="!=") return row_val!=pred.val;
                    if (pred.op==">")  return row_val>pred.val;
                    if (pred.op==">=") return row_val>=pred.val;
                    if (pred.op=="<")  return row_val<pred.val;
                    if (pred.op=="<=") return row_val<=pred.val;
                }
            } catch (...) {}
            return false;
        };
        return eval(*s.where);
    };

    // Probe phase
    std::vector<bool> a_matched(rows_a.size(), false); // track matched left-table rows for LEFT JOIN
    for (size_t bi = 0; bi < rows_b.size(); ++bi) {
        const auto& vals_b = rows_b[bi].first;
        if (col_b >= (int)vals_b.size()) continue;
        const std::string& key = vals_b[col_b];
        auto range = hash_map.equal_range(key);
        for (auto it = range.first; it != range.second; ++it) {
            const auto& vals_a = rows_a[it->second].first;
            if (!eval_where(vals_a, vals_b)) continue;
            a_matched[it->second] = true;
            std::vector<std::string> out_row;
            out_row.reserve(out_a_idx.size());
            for (size_t k = 0; k < out_a_idx.size(); ++k) {
                if      (out_a_idx[k] >= 0 && out_a_idx[k] < (int)vals_a.size()) out_row.push_back(vals_a[out_a_idx[k]]);
                else if (out_b_idx[k] >= 0 && out_b_idx[k] < (int)vals_b.size()) out_row.push_back(vals_b[out_b_idx[k]]);
                else out_row.push_back("");
            }
            res.rows.push_back(std::move(out_row));
        }
    }

    // LEFT JOIN: emit unmatched left-side (rows_a) rows with NULLs for right columns
    if (s.left_join && !swapped) {
        for (size_t ai = 0; ai < rows_a.size(); ++ai) {
            if (a_matched[ai]) continue;
            const auto& vals_a = rows_a[ai].first;
            std::vector<std::string> out_row(out_a_idx.size(), "");
            for (size_t k = 0; k < out_a_idx.size(); ++k)
                if (out_a_idx[k] >= 0 && out_a_idx[k] < (int)vals_a.size())
                    out_row[k] = vals_a[out_a_idx[k]];
            res.rows.push_back(std::move(out_row));
        }
    }

    if (all_columns) return res;   // grouping comes next; it handles LIMIT itself

    // ORDER BY — multi-column for JOIN results (applied right-to-left)
    for (int ki = (int)s.order_by.size() - 1; ki >= 0; --ki) {
        std::string order_col = strip_prefix(s.order_by[ki].col);
        int sort_idx = -1;
        for (int i = 0; i < (int)res.column_names.size(); ++i)
            if (strutil::iequals(strip_prefix(res.column_names[i]), order_col)) { sort_idx = i; break; }
        if (sort_idx >= 0) {
            bool desc = s.order_by[ki].desc;
            // A sort comparator must be a strict weak ordering: cmp(x, x) is
            // false. The old DESC version returned !(a < b), which is TRUE for
            // equal values in both directions → undefined behaviour in
            // std::stable_sort. DESC must be "b < a", not "not (a < b)".
            auto less = [&](const std::string& x, const std::string& y) {
                try { return std::stod(x) < std::stod(y); }
                catch (...) { return x < y; }
            };
            std::stable_sort(res.rows.begin(), res.rows.end(),
                [&](const auto& a, const auto& b) {
                    if (sort_idx >= (int)a.size()) return false;
                    return desc ? less(b[sort_idx], a[sort_idx])
                                : less(a[sort_idx], b[sort_idx]);
                });
        }
    }

    if (project_later) {                        // sorted on full rows → now project
        const size_t na = schema_a.columns.size();
        for (auto& row : res.rows) {
            std::vector<std::string> out(req_a.size());
            for (size_t k = 0; k < req_a.size(); ++k)
                out[k] = req_a[k] >= 0 ? std::move(row[(size_t)req_a[k]])
                                       : std::move(row[na + (size_t)req_b[k]]);
            row = std::move(out);
        }
        res.column_names = req_names;
    }

    // OFFSET / LIMIT (previously ignored for JOINs: every row was returned)
    if (s.offset > 0) {
        const size_t off = std::min(res.rows.size(), (size_t)s.offset);
        res.rows.erase(res.rows.begin(), res.rows.begin() + (std::ptrdiff_t)off);
    }
    if (s.limit >= 0 && res.rows.size() > (size_t)s.limit) res.rows.resize((size_t)s.limit);
    return res;
}

// ── DELETE ────────────────────────────────────────────────────────────────────
QueryResult Executor::exec_delete(const DeleteStmt& s) {
    auto tbl = Database::instance().get_table(s.table);
    if (!tbl) return QueryResult::err("no such table: " + s.table);
    tbl->delete_rows(s.where.get());
    g_cache.invalidate_prefix(strutil::to_upper(s.table));
    return QueryResult::success();
}

// ── DROP TABLE ────────────────────────────────────────────────────────────────
QueryResult Executor::exec_drop(const DropTableStmt& s) {
    g_cache.invalidate_prefix(s.table);
    std::string err = Database::instance().drop_table(s.table, s.if_exists);
    if (!err.empty()) return QueryResult::err(err);
    return QueryResult::success();
}

// ── SHOW TABLES ───────────────────────────────────────────────────────────────
QueryResult Executor::exec_show_tables() {
    QueryResult res;
    res.column_names = {"Tables"};
    for (const auto& name : Database::instance().list_tables())
        res.rows.push_back({name});
    return res;
}

// ── SHOW DATABASES ────────────────────────────────────────────────────────────
QueryResult Executor::exec_show_databases() {
    QueryResult res;
    res.column_names = {"Database"};
    res.rows.push_back({"flexql"});
    return res;
}

// ── UPDATE ────────────────────────────────────────────────────────────────────
QueryResult Executor::exec_update(const UpdateStmt& s) {
    auto tbl = Database::instance().get_table(s.table);
    if (!tbl) return QueryResult::err("no such table: " + s.table);

    for (const auto& [col, val] : s.assignments) {
        int idx = tbl->schema().col_index(col);
        if (idx < 0)
            return QueryResult::err("unknown column: " + col);
        // Enforce schema on the new value (NOT NULL + type)
        std::string err = tbl->validate_value(idx, val);
        if (!err.empty()) return QueryResult::err(err);
    }

    int n = tbl->update_rows(s.assignments, s.where.get());
    g_cache.invalidate_prefix(strutil::to_upper(s.table));

    QueryResult res;
    res.ok = true;
    res.column_names = {"rows_updated"};
    res.rows = {{std::to_string(n)}};
    return res;
}

// ── TRUNCATE TABLE ────────────────────────────────────────────────────────────
QueryResult Executor::exec_truncate(const TruncateStmt& s) {
    auto tbl = Database::instance().get_table(s.table);
    if (!tbl) return QueryResult::err("no such table: " + s.table);
    tbl->truncate();
    g_cache.invalidate_prefix(strutil::to_upper(s.table));
    return QueryResult::success();
}

// ── DESCRIBE ──────────────────────────────────────────────────────────────────
QueryResult Executor::exec_describe(const DescribeStmt& s) {
    auto tbl = Database::instance().get_table(s.table);
    if (!tbl) return QueryResult::err("no such table: " + s.table);

    QueryResult res;
    res.column_names = {"Field", "Type", "Key", "Null"};

    auto type_str = [](ColumnType t) -> std::string {
        switch (t) {
            case ColumnType::INT:      return "INT";
            case ColumnType::DECIMAL:  return "DECIMAL";
            case ColumnType::VARCHAR:  return "VARCHAR";
            case ColumnType::TEXT:     return "TEXT";
            case ColumnType::DATETIME: return "DATETIME";
            default:                   return "TEXT";
        }
    };

    std::shared_lock lock(tbl->mtx);
    for (const auto& col : tbl->schema().columns) {
        res.rows.push_back({col.name, type_str(col.type),
                            col.primary_key ? "PRI" : "-",
                            col.not_null    ? "NO"  : "YES"});
    }
    return res;
}

// ── USE DATABASE ──────────────────────────────────────────────────────────────
QueryResult Executor::exec_use_database(const UseDatabaseStmt& s) {
    // Single-database server: just acknowledge the USE command.
    (void)s;
    return QueryResult::success();
}

// ── ALTER TABLE ───────────────────────────────────────────────────────────────
QueryResult Executor::exec_alter(const AlterTableStmt& s) {
    auto tbl = Database::instance().get_table(s.table);
    if (!tbl) return QueryResult::err("no such table: " + s.table);

    if (s.action == AlterTableStmt::DROP_COLUMN) {
        // DROP COLUMN: not supported in-place without full table rebuild — return error
        return QueryResult::err("ALTER TABLE DROP COLUMN not supported (schema is immutable after creation)");
    }

    if (s.action == AlterTableStmt::ADD_COLUMN) {
        // ADD COLUMN: not yet supported — would require rewriting all rows
        return QueryResult::err("ALTER TABLE ADD COLUMN not yet supported");
    }

    if (s.action == AlterTableStmt::MODIFY_COLUMN) {
        return QueryResult::err("ALTER TABLE MODIFY COLUMN not yet supported");
    }

    return QueryResult::err("unknown ALTER TABLE action");
}
