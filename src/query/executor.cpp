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
QueryResult Executor::exec_select(const SelectStmt& s) {
    if (!s.join_table.empty()) return exec_join(s);

    auto tbl = Database::instance().get_table(s.table);
    if (!tbl) return QueryResult::err("no such table: " + s.table);

    // ── GROUP BY ──────────────────────────────────────────────────────────────
    if (!s.group_by.empty()) return exec_group_by(s);

    // ── Aggregate ─────────────────────────────────────────────────────────────
    if (!s.agg_func.empty()) {
        // COUNT(*) — O(1) memory, no row materialisation.
        if (s.agg_func == "COUNT") {
            size_t cnt = tbl->count_rows(s.where.get());
            QueryResult res;
            res.ok = true;
            res.column_names = {"COUNT(" + s.agg_col + ")"};
            res.rows = {{std::to_string(cnt)}};
            return res;
        }

        // SUM/AVG/MIN/MAX — single-pass, O(1) memory.
        int col_idx = -1;
        for (int i = 0; i < (int)tbl->schema().columns.size(); ++i)
            if (strutil::iequals(tbl->schema().columns[i].name, s.agg_col)) { col_idx = i; break; }
        if (col_idx < 0) return QueryResult::err("unknown column: " + s.agg_col);

        auto agg = tbl->compute_aggregate(col_idx, s.where.get());

        std::string result;
        if      (s.agg_func == "SUM") result = (agg.count > 0) ? std::to_string(agg.sum) : "0";
        else if (s.agg_func == "AVG") result = (agg.count > 0) ? std::to_string(agg.sum / agg.count) : "0";
        else if (s.agg_func == "MIN") result = agg.min_str;
        else if (s.agg_func == "MAX") result = agg.max_str;

        QueryResult res;
        res.ok = true;
        res.column_names = {s.agg_func + "(" + s.agg_col + ")"};
        res.rows = {{result}};
        return res;
    }

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
    if (s.where && s.where->kind == Predicate::LEAF)
        cache_key += "|W:" + s.where->col + s.where->op + s.where->val;
    else if (s.where)
        cache_key.clear(); // skip cache for compound predicates
    if (!cache_key.empty() && !s.order_by.empty())
        for (const auto& ob : s.order_by)
            cache_key += "|O:" + ob.col + (ob.desc ? "D" : "A");
    if (!cache_key.empty() && s.distinct)  cache_key += "|D";
    if (!cache_key.empty() && s.limit >= 0) cache_key += "|L" + std::to_string(s.limit);
    if (!cache_key.empty() && s.offset > 0) cache_key += "|F" + std::to_string(s.offset);

    if (!cache_key.empty())
        if (auto* cached = g_cache.get(cache_key)) return *cached;

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

// ── GROUP BY + HAVING ─────────────────────────────────────────────────────────
QueryResult Executor::exec_group_by(const SelectStmt& s) {
    auto tbl = Database::instance().get_table(s.table);
    if (!tbl) return QueryResult::err("no such table: " + s.table);

    // Collect only the columns actually needed (group-by + aggregate targets)
    std::vector<std::string> needed;
    for (const auto& g : s.group_by) {
        std::string gn = g;
        size_t dot = gn.find('.');
        if (dot != std::string::npos) gn = gn.substr(dot + 1);
        bool dup = false;
        for (auto& n : needed) if (strutil::iequals(n, gn)) { dup = true; break; }
        if (!dup) needed.push_back(gn);
    }
    for (const auto& c : s.cols) {
        size_t lp = c.find('(');
        if (lp != std::string::npos) {
            std::string arg = c.substr(lp + 1, c.size() - lp - 2);
            if (arg != "*" && !arg.empty()) {
                std::string an = arg;
                size_t dot = an.find('.');
                if (dot != std::string::npos) an = an.substr(dot + 1);
                bool dup = false;
                for (auto& n : needed) if (strutil::iequals(n, an)) { dup = true; break; }
                if (!dup) needed.push_back(an);
            }
        }
    }

    QueryResult base = tbl->scan(needed.empty() ? std::vector<std::string>{"*"} : needed,
                                 s.where.get(), {});
    if (!base.ok) return base;

    // Index group-by columns in result
    std::vector<int> grp_idx;
    for (const auto& g : s.group_by) {
        std::string gname = g;
        size_t dot = gname.find('.');
        if (dot != std::string::npos) gname = gname.substr(dot + 1);
        int idx = -1;
        for (int i = 0; i < (int)base.column_names.size(); ++i)
            if (strutil::iequals(base.column_names[i], gname)) { idx = i; break; }
        if (idx < 0) return QueryResult::err("GROUP BY: unknown column: " + g);
        grp_idx.push_back(idx);
    }

    // Build group key → rows map (preserves insertion order with vector)
    std::vector<std::string>                            group_keys_ordered;
    std::unordered_map<std::string, std::vector<size_t>> groups;
    const size_t base_nrows = base.num_rows();
    for (size_t r = 0; r < base_nrows; ++r) {
        std::string key;
        for (int gi : grp_idx) { key += base.cell(r, (size_t)gi); key += '\0'; }
        auto it = groups.find(key);
        if (it == groups.end()) {
            group_keys_ordered.push_back(key);
            groups[key] = {r};
        } else {
            it->second.push_back(r);
        }
    }

    // Resolve output columns: group-by cols + aggregates
    // Detect aggregates in s.cols: e.g. "COUNT(*)", "SUM(COL)"
    struct AggSpec { std::string func, col; int out_col_idx = -1; };
    std::vector<AggSpec> aggs;
    std::vector<std::string> out_names;

    for (const auto& c : s.cols) {
        // Check for FUNC(col) pattern
        size_t lp = c.find('(');
        if (lp != std::string::npos) {
            std::string fn  = strutil::to_upper(c.substr(0, lp));
            std::string arg = c.substr(lp + 1, c.size() - lp - 2); // strip ()
            aggs.push_back({fn, arg, (int)out_names.size()});
            out_names.push_back(c);
        } else {
            // Regular column — must be a group-by column
            int cidx = -1;
            for (int i = 0; i < (int)base.column_names.size(); ++i)
                if (strutil::iequals(base.column_names[i], c)) { cidx = i; break; }
            if (cidx < 0) return QueryResult::err("SELECT column not in GROUP BY: " + c);
            out_names.push_back(c);
            aggs.push_back({"COL", std::to_string(cidx), (int)out_names.size()-1});
        }
    }

    QueryResult res;
    res.column_names = out_names;

    for (const auto& key : group_keys_ordered) {
        const auto& row_idxs = groups.at(key);
        std::vector<std::string> out_row(out_names.size());

        for (auto& ag : aggs) {
            if (ag.func == "COL") {
                int cidx = std::stoi(ag.col);
                out_row[ag.out_col_idx] = base.cell(row_idxs[0], (size_t)cidx);
                continue;
            }
            if (ag.func == "COUNT") {
                out_row[ag.out_col_idx] = std::to_string(row_idxs.size());
                continue;
            }
            // Resolve column index
            int cidx = -1;
            for (int i = 0; i < (int)base.column_names.size(); ++i)
                if (strutil::iequals(base.column_names[i], ag.col)) { cidx = i; break; }
            if (cidx < 0) { out_row[ag.out_col_idx] = ""; continue; }

            double sumv = 0, minv = DBL_MAX, maxv = -DBL_MAX;
            std::string minstr, maxstr;
            int cnt = 0;
            for (size_t ri : row_idxs) {
                auto v = base.cell(ri, (size_t)cidx);
                try {
                    double d = std::stod(std::string(v));
                    sumv += d;
                    if (d < minv) { minv = d; minstr = std::string(v); }
                    if (d > maxv) { maxv = d; maxstr = std::string(v); }
                    ++cnt;
                } catch (...) {
                    if (minstr.empty() || v < minstr) minstr = std::string(v);
                    if (maxstr.empty() || v > maxstr) maxstr = std::string(v);
                    ++cnt;
                }
            }
            if      (ag.func == "SUM") out_row[ag.out_col_idx] = cnt>0?std::to_string(sumv):"0";
            else if (ag.func == "AVG") out_row[ag.out_col_idx] = cnt>0?std::to_string(sumv/cnt):"0";
            else if (ag.func == "MIN") out_row[ag.out_col_idx] = minstr;
            else if (ag.func == "MAX") out_row[ag.out_col_idx] = maxstr;
        }

        // HAVING filter — apply against a fake "row" with aggregate results
        // For simplicity, evaluate HAVING as a string/numeric comparison on the output row.
        if (s.having) {
            // Evaluate HAVING predicate: match column names in out_names
            std::function<bool(const Predicate&)> eval_having =
                [&](const Predicate& pred) -> bool {
                if (pred.kind == Predicate::AND_NODE) {
                    for (auto& c2 : pred.children) if (!eval_having(*c2)) return false;
                    return true;
                }
                if (pred.kind == Predicate::OR_NODE) {
                    for (auto& c2 : pred.children) if (eval_having(*c2)) return true;
                    return false;
                }
                if (pred.kind == Predicate::NOT_NODE)
                    return !pred.children.empty() && !eval_having(*pred.children[0]);
                // LEAF
                int cidx = -1;
                for (int i = 0; i < (int)out_names.size(); ++i)
                    if (strutil::iequals(out_names[i], pred.col)) { cidx = i; break; }
                if (cidx < 0) return false;
                const std::string& v = out_row[cidx];
                try {
                    double a = std::stod(v), b = std::stod(pred.val);
                    if (pred.op == "=")  return a == b;
                    if (pred.op == "!=") return a != b;
                    if (pred.op == ">")  return a >  b;
                    if (pred.op == ">=") return a >= b;
                    if (pred.op == "<")  return a <  b;
                    if (pred.op == "<=") return a <= b;
                } catch (...) {
                    if (pred.op == "=")  return v == pred.val;
                    if (pred.op == "!=") return v != pred.val;
                    if (pred.op == ">")  return v >  pred.val;
                    if (pred.op == ">=") return v >= pred.val;
                    if (pred.op == "<")  return v <  pred.val;
                    if (pred.op == "<=") return v <= pred.val;
                }
                return false;
            };
            if (!eval_having(*s.having)) continue;
        }
        res.rows.push_back(std::move(out_row));
    }

    if (s.limit >= 0 && (int)res.rows.size() > s.limit)
        res.rows.resize(s.limit);

    return res;
}

// ── JOIN (INNER + LEFT) ───────────────────────────────────────────────────────
QueryResult Executor::exec_join(const SelectStmt& s) {
    auto tbl_a = Database::instance().get_table(s.table);
    auto tbl_b = Database::instance().get_table(s.join_table);
    if (!tbl_a) return QueryResult::err("no such table: " + s.table);
    if (!tbl_b) return QueryResult::err("no such table: " + s.join_table);

    std::shared_lock<std::shared_mutex> lock_first(
        strutil::to_upper(s.table) < strutil::to_upper(s.join_table)
            ? tbl_a->mtx : tbl_b->mtx, std::defer_lock);
    std::shared_lock<std::shared_mutex> lock_second(
        strutil::to_upper(s.table) < strutil::to_upper(s.join_table)
            ? tbl_b->mtx : tbl_a->mtx, std::defer_lock);
    lock_first.lock();
    lock_second.lock();

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

    bool select_all = (s.cols.size() == 1 && s.cols[0] == "*");
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

    // ORDER BY — multi-column for JOIN results (applied right-to-left)
    for (int ki = (int)s.order_by.size() - 1; ki >= 0; --ki) {
        std::string order_col = strip_prefix(s.order_by[ki].col);
        int sort_idx = -1;
        for (int i = 0; i < (int)res.column_names.size(); ++i)
            if (strutil::iequals(strip_prefix(res.column_names[i]), order_col)) { sort_idx = i; break; }
        if (sort_idx >= 0) {
            bool desc = s.order_by[ki].desc;
            std::stable_sort(res.rows.begin(), res.rows.end(),
                [&](const auto& a, const auto& b) {
                    if (sort_idx >= (int)a.size()) return false;
                    bool less_than;
                    try { less_than = std::stod(a[sort_idx]) < std::stod(b[sort_idx]); }
                    catch (...) { less_than = a[sort_idx] < b[sort_idx]; }
                    return desc ? !less_than : less_than;
                });
        }
    }

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

    for (const auto& [col, _] : s.assignments)
        if (tbl->schema().col_index(col) < 0)
            return QueryResult::err("unknown column: " + col);

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
