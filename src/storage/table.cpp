#include "storage/table.h"
#include "parser/ast.h"
#include "utils/string_utils.h"
#include <algorithm>
#include <mutex>
#include <stdexcept>
#include <ctime>
#include <immintrin.h>   // AVX2 / SSE intrinsics

// ── Construction ──────────────────────────────────────────────────────────────
Table::Table(TableSchema schema)
    : schema_(std::move(schema)),
      num_cols_((int)schema_.columns.size()),
      int_cols_((size_t)num_cols_) {}

// ── Helpers ───────────────────────────────────────────────────────────────────
bool Table::is_expired(const RowMeta& m) const {
    if (m.expires_at == 0) return false;
    return m.expires_at <= (int64_t)std::time(nullptr);
}

// Simple wildcard match: % = any sequence, _ = any single char.
static bool like_match(std::string_view str, std::string_view pat) {
    size_t si = 0, pi = 0;
    size_t star_pi = std::string_view::npos, star_si = 0;
    while (si < str.size()) {
        if (pi < pat.size() && (pat[pi] == '_' || pat[pi] == str[si])) {
            ++si; ++pi;
        } else if (pi < pat.size() && pat[pi] == '%') {
            star_pi = pi++;
            star_si = si;
        } else if (star_pi != std::string_view::npos) {
            pi = star_pi + 1;
            si = ++star_si;
        } else {
            return false;
        }
    }
    while (pi < pat.size() && pat[pi] == '%') ++pi;
    return pi == pat.size();
}

bool Table::compare_values(std::string_view row_val,
                            const std::string& op,
                            const std::string& target,
                            ColumnType type) const {
    if (op == "LIKE") return like_match(row_val, target);

    // Numeric compare for INT / DECIMAL
    if (type == ColumnType::INT || type == ColumnType::DECIMAL) {
        try {
            double a = std::stod(std::string(row_val));
            double b = std::stod(target);
            if (op == "=")  return a == b;
            if (op == "!=") return a != b;
            if (op == ">")  return a > b;
            if (op == ">=") return a >= b;
            if (op == "<")  return a < b;
            if (op == "<=") return a <= b;
        } catch (...) { return false; }
    }
    // String compare
    if (op == "=")  return row_val == target;
    if (op == "!=") return row_val != target;
    if (op == ">")  return row_val > target;
    if (op == ">=") return row_val >= target;
    if (op == "<")  return row_val < target;
    if (op == "<=") return row_val <= target;
    return false;
}


bool Table::matches_pred(size_t row_idx, const Predicate& pred) const {
    switch (pred.kind) {
        case Predicate::AND_NODE:
            for (const auto& c : pred.children)
                if (!matches_pred(row_idx, *c)) return false;
            return true;

        case Predicate::OR_NODE:
            for (const auto& c : pred.children)
                if (matches_pred(row_idx, *c)) return true;
            return false;

        case Predicate::NOT_NODE:
            return !pred.children.empty() && !matches_pred(row_idx, *pred.children[0]);

        case Predicate::LEAF: {
            std::string col_name = pred.col;
            size_t dot = col_name.find('.');
            if (dot != std::string::npos) col_name = col_name.substr(dot + 1);
            int idx = schema_.col_index(col_name);
            if (idx < 0 || idx >= num_cols_) return false;
            std::string_view row_val = val_at(row_idx, idx);

            if (pred.op == "IS NULL")     return row_val.empty();
            if (pred.op == "IS NOT NULL") return !row_val.empty();

            // Fast path: use native int64 for numeric columns (no stod overhead)
            if (is_numeric_col(idx) && idx < (int)int_cols_.size() &&
                !int_cols_[idx].empty()) {
                int64_t rv = int_cols_[idx][row_idx];

                if (pred.op == "BETWEEN" || pred.op == "NOT BETWEEN") {
                    int64_t lo = parse_int64(pred.val);
                    int64_t hi = parse_int64(pred.val2);
                    bool in_range = rv >= lo && rv <= hi;
                    return pred.op == "BETWEEN" ? in_range : !in_range;
                }
                if (pred.op == "IN" || pred.op == "NOT IN") {
                    bool found = false;
                    for (const auto& v : pred.in_vals)
                        if (rv == parse_int64(v)) { found = true; break; }
                    return pred.op == "IN" ? found : !found;
                }
                if (pred.op == "LIKE") return like_match(row_val, pred.val);

                int64_t tv = parse_int64(pred.val);
                if (pred.op == "=")  return rv == tv;
                if (pred.op == "!=") return rv != tv;
                if (pred.op == ">")  return rv > tv;
                if (pred.op == ">=") return rv >= tv;
                if (pred.op == "<")  return rv < tv;
                if (pred.op == "<=") return rv <= tv;
                return false;
            }

            // String / non-numeric fallback
            ColumnType ct = schema_.columns[idx].type;

            if (pred.op == "BETWEEN" || pred.op == "NOT BETWEEN") {
                bool in_range = !compare_values(row_val, "<", pred.val, ct) &&
                                !compare_values(row_val, ">", pred.val2, ct);
                return pred.op == "BETWEEN" ? in_range : !in_range;
            }

            if (pred.op == "IN" || pred.op == "NOT IN") {
                bool found = false;
                for (const auto& v : pred.in_vals)
                    if (compare_values(row_val, "=", v, ct)) { found = true; break; }
                return pred.op == "IN" ? found : !found;
            }

            return compare_values(row_val, pred.op, pred.val, ct);
        }
    }
    return false;
}

std::vector<std::string> Table::project(size_t row_idx,
                                         const std::vector<int>& col_indices) const {
    std::vector<std::string> out;
    out.reserve(col_indices.size());
    for (int c : col_indices) {
        if (c >= 0 && c < num_cols_)
            out.emplace_back(val_at(row_idx, c));
        else
            out.emplace_back();
    }
    return out;
}

// ── Single-row insert (legacy / non-batch path) ───────────────────────────────
std::string Table::insert(std::vector<std::string> values, int64_t expires_at) {
    std::unique_lock lock(mtx);

    if ((int)values.size() != num_cols_)
        return "column count mismatch: expected " + std::to_string(num_cols_) +
               " got " + std::to_string(values.size());

    if (schema_.pk_col_index >= 0) {
        if (pk_index_.lookup(values[schema_.pk_col_index]) >= 0)
            return "duplicate primary key: " + values[schema_.pk_col_index];
    }

    size_t row_idx = rows_meta_.size();
    for (const auto& v : values)
        rows_vals_.push_back(arena_.intern(v));
    rows_meta_.push_back({expires_at, false});

    // Populate native int64 columns
    for (int c = 0; c < num_cols_; ++c)
        if (is_numeric_col(c))
            int_cols_[c].push_back(parse_int64(values[c]));

    if (schema_.pk_col_index >= 0)
        pk_index_.insert(val_at(row_idx, schema_.pk_col_index), row_idx);

    return "";
}

// ── Flat batch insert — hot path (no inner-vector heap allocations) ───────────
std::string Table::insert_flat(const std::string_view* flat, size_t n_rows,
                               int ncols, int64_t expires_at) {
    if (ncols != num_cols_)
        return "column count mismatch: expected " + std::to_string(num_cols_) +
               " got " + std::to_string(ncols);

    std::unique_lock lock(mtx);

    // One-time pre-reserve: multiplier * batch_size estimates total workload.
    // With batch=25K: 25K * 800 = 20M → covers both 10M and 20M without reallocation.
    // With batch=5K:  5K * 800  = 4M  → covers 1M–4M; graceful doubling beyond that.
    if (rows_meta_.empty() && n_rows >= 1000) {
        size_t est = std::max(n_rows * 200UL, 1000000UL);
        rows_meta_.reserve(est);
        rows_vals_.reserve(est * (size_t)num_cols_);
        for (int c = 0; c < num_cols_; ++c)
            if (is_numeric_col(c)) int_cols_[c].reserve(est);
    } else if (rows_meta_.capacity() < rows_meta_.size() + n_rows) {
        size_t new_cap = std::max(rows_meta_.size() * 2, rows_meta_.size() + n_rows);
        rows_meta_.reserve(new_cap);
        rows_vals_.reserve(new_cap * (size_t)num_cols_);
        for (int c = 0; c < num_cols_; ++c)
            if (is_numeric_col(c)) int_cols_[c].reserve(new_cap);
    }

    const bool has_pk = schema_.pk_col_index >= 0;

    // Fast path: no primary key (e.g. BIG_USERS benchmark table).
    // Use resize() instead of push_back() to avoid per-element size checks,
    // and intern all values in a single indexed pass for better auto-vectorization.
    if (!has_pk) {
        const size_t base_row = rows_meta_.size();
        const size_t base_val = rows_vals_.size();
        rows_meta_.resize(base_row + n_rows, {expires_at, false});
        rows_vals_.resize(base_val + n_rows * (size_t)num_cols_);
        size_t vi = base_val;
        for (size_t r = 0; r < n_rows; ++r) {
            const std::string_view* row = flat + r * (size_t)num_cols_;
            for (int c = 0; c < num_cols_; ++c)
                rows_vals_[vi++] = arena_.intern(row[c]);
        }
        // Populate native int64 columns
        for (int c = 0; c < num_cols_; ++c) {
            if (!is_numeric_col(c)) continue;
            int_cols_[c].resize(base_row + n_rows);
            for (size_t r = 0; r < n_rows; ++r)
                int_cols_[c][base_row + r] = parse_int64(flat[r * (size_t)num_cols_ + c]);
        }
        return "";
    }

    // General path: primary key table — validate then insert one row at a time.
    for (size_t r = 0; r < n_rows; ++r) {
        const std::string_view* row = flat + r * (size_t)num_cols_;
        if (pk_index_.lookup(row[schema_.pk_col_index]) >= 0)
            return "duplicate primary key: " + std::string(row[schema_.pk_col_index]);
        size_t row_idx = rows_meta_.size();
        for (int c = 0; c < num_cols_; ++c)
            rows_vals_.push_back(arena_.intern(row[c]));
        rows_meta_.push_back({expires_at, false});
        // Populate native int64 columns
        for (int c = 0; c < num_cols_; ++c)
            if (is_numeric_col(c))
                int_cols_[c].push_back(parse_int64(row[c]));
        pk_index_.insert(val_at(row_idx, schema_.pk_col_index), row_idx);
    }
    return "";
}

// ── Batch insert ──────────────────────────────────────────────────────────────
std::string Table::insert_batch(std::vector<std::vector<std::string_view>> batch,
                                std::vector<int64_t> expires_vec) {
    const size_t n = batch.size();
    std::unique_lock lock(mtx);

    // One-time pre-reserve on first large batch — eliminates all reallocation spikes.
    if (rows_meta_.empty() && n >= 1000) {
        size_t est = std::max(n * 200UL, 1000000UL);
        rows_meta_.reserve(est);
        rows_vals_.reserve(est * (size_t)num_cols_);
        for (int c = 0; c < num_cols_; ++c)
            if (is_numeric_col(c)) int_cols_[c].reserve(est);
    } else {
        // Amortised growth: only reallocate when necessary
        if (rows_meta_.capacity() < rows_meta_.size() + n) {
            size_t new_cap = std::max(rows_meta_.size() * 2, rows_meta_.size() + n);
            rows_meta_.reserve(new_cap);
            rows_vals_.reserve(new_cap * (size_t)num_cols_);
            for (int c = 0; c < num_cols_; ++c)
                if (is_numeric_col(c)) int_cols_[c].reserve(new_cap);
        }
    }

    // Pre-validate all rows
    for (size_t r = 0; r < n; ++r) {
        if ((int)batch[r].size() != num_cols_)
            return "column count mismatch at row " + std::to_string(r) +
                   ": expected " + std::to_string(num_cols_) +
                   " got " + std::to_string(batch[r].size());
        if (schema_.pk_col_index >= 0) {
            if (pk_index_.lookup(batch[r][schema_.pk_col_index]) >= 0)
                return "duplicate primary key: " +
                       std::string(batch[r][schema_.pk_col_index]);
        }
    }

    // Intern + store
    for (size_t r = 0; r < n; ++r) {
        size_t row_idx = rows_meta_.size();
        for (auto sv : batch[r])
            rows_vals_.push_back(arena_.intern(sv));
        rows_meta_.push_back({expires_vec[r], false});
        // Populate native int64 columns
        for (int c = 0; c < num_cols_; ++c)
            if (is_numeric_col(c))
                int_cols_[c].push_back(parse_int64(batch[r][c]));
        if (schema_.pk_col_index >= 0)
            pk_index_.insert(val_at(row_idx, schema_.pk_col_index), row_idx);
    }
    return "";
}

// ── Delete ────────────────────────────────────────────────────────────────────
int Table::delete_rows(const Predicate* where) {
    std::unique_lock lock(mtx);

    // PK fast path: equality (WHERE pk = X) → O(1) hash lookup
    if (where && where->kind == Predicate::LEAF && where->op == "=" &&
        schema_.pk_col_index >= 0) {
        std::string col_name = where->col;
        size_t dot = col_name.find('.');
        if (dot != std::string::npos) col_name = col_name.substr(dot + 1);
        if (col_name == schema_.columns[schema_.pk_col_index].name) {
            long long pos = pk_index_.lookup(where->val);
            if (pos >= 0) {
                auto& m = rows_meta_[(size_t)pos];
                if (!m.deleted && !is_expired(m)) {
                    m.deleted = true;
                    pk_index_.remove(where->val);
                    return 1;
                }
            }
            return 0;
        }
    }

    // PK fast path: IN (WHERE pk IN (...)) → O(k) hash lookups
    if (where && where->kind == Predicate::LEAF && where->op == "IN" &&
        schema_.pk_col_index >= 0) {
        std::string col_name = where->col;
        size_t dot = col_name.find('.');
        if (dot != std::string::npos) col_name = col_name.substr(dot + 1);
        if (col_name == schema_.columns[schema_.pk_col_index].name) {
            int count = 0;
            for (const auto& v : where->in_vals) {
                long long pos = pk_index_.lookup(v);
                if (pos >= 0) {
                    auto& m = rows_meta_[(size_t)pos];
                    if (!m.deleted && !is_expired(m)) {
                        m.deleted = true;
                        pk_index_.remove(v);
                        ++count;
                    }
                }
            }
            return count;
        }
    }

    // General scan
    int count = 0;
    for (size_t i = 0; i < rows_meta_.size(); ++i) {
        auto& m = rows_meta_[i];
        if (m.deleted || is_expired(m)) continue;
        if (!where || matches_pred(i, *where)) {
            m.deleted = true;
            if (schema_.pk_col_index >= 0)
                pk_index_.remove(val_at(i, schema_.pk_col_index));
            ++count;
        }
    }
    return count;
}

// ── AVX2 SIMD helpers for numeric column scanning ─────────────────────────────
namespace {

// Check whether row is alive (not deleted, not expired)
struct AliveChecker {
    const RowMeta* meta;
    int64_t now;
    bool operator()(size_t i) const {
        return !meta[i].deleted && (meta[i].expires_at == 0 || meta[i].expires_at > now);
    }
};

// AVX2 comparison scan: 4 × int64 per cycle.
inline void avx2_filter_cmp(const int64_t* col, size_t n,
                             const AliveChecker& alive,
                             const std::string& op, int64_t target,
                             std::vector<size_t>& out) {
    const __m256i tgt = _mm256_set1_epi64x(target);
    const __m256i ones = _mm256_set1_epi64x(-1LL);

    size_t i = 0;
    for (; i + 3 < n; i += 4) {
        __m256i vals = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(col + i));
        __m256i mask;
        if      (op[0] == '=' && op.size() == 1)
            mask = _mm256_cmpeq_epi64(vals, tgt);
        else if (op == "!=")
            mask = _mm256_andnot_si256(_mm256_cmpeq_epi64(vals, tgt), ones);
        else if (op == ">")
            mask = _mm256_cmpgt_epi64(vals, tgt);
        else if (op == ">=")
            mask = _mm256_or_si256(_mm256_cmpgt_epi64(vals, tgt),
                                   _mm256_cmpeq_epi64(vals, tgt));
        else if (op[0] == '<' && op.size() == 1)
            mask = _mm256_cmpgt_epi64(tgt, vals);
        else if (op == "<=")
            mask = _mm256_or_si256(_mm256_cmpgt_epi64(tgt, vals),
                                   _mm256_cmpeq_epi64(vals, tgt));
        else return;

        int m = _mm256_movemask_pd(_mm256_castsi256_pd(mask));
        if (m & 1) if (alive(i))   out.push_back(i);
        if (m & 2) if (alive(i+1)) out.push_back(i+1);
        if (m & 4) if (alive(i+2)) out.push_back(i+2);
        if (m & 8) if (alive(i+3)) out.push_back(i+3);
    }
    // Scalar tail
    for (; i < n; ++i) {
        if (!alive(i)) continue;
        int64_t v = col[i];
        bool hit = false;
        if      (op[0] == '=' && op.size() == 1) hit = (v == target);
        else if (op == "!=") hit = (v != target);
        else if (op == ">")  hit = (v > target);
        else if (op == ">=") hit = (v >= target);
        else if (op[0] == '<' && op.size() == 1) hit = (v < target);
        else if (op == "<=") hit = (v <= target);
        if (hit) out.push_back(i);
    }
}

// AVX2 BETWEEN scan
inline void avx2_filter_between(const int64_t* col, size_t n,
                                 const AliveChecker& alive,
                                 int64_t lo, int64_t hi, bool negate,
                                 std::vector<size_t>& out) {
    const __m256i lo_v = _mm256_set1_epi64x(lo);
    const __m256i hi_v = _mm256_set1_epi64x(hi);

    size_t i = 0;
    for (; i + 3 < n; i += 4) {
        __m256i vals = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(col + i));
        __m256i ge_lo = _mm256_or_si256(_mm256_cmpgt_epi64(vals, lo_v),
                                        _mm256_cmpeq_epi64(vals, lo_v));
        __m256i le_hi = _mm256_or_si256(_mm256_cmpgt_epi64(hi_v, vals),
                                        _mm256_cmpeq_epi64(vals, hi_v));
        __m256i mask = _mm256_and_si256(ge_lo, le_hi);
        if (negate) {
            __m256i ones = _mm256_set1_epi64x(-1LL);
            mask = _mm256_andnot_si256(mask, ones);
        }
        int m = _mm256_movemask_pd(_mm256_castsi256_pd(mask));
        if (m & 1) if (alive(i))   out.push_back(i);
        if (m & 2) if (alive(i+1)) out.push_back(i+1);
        if (m & 4) if (alive(i+2)) out.push_back(i+2);
        if (m & 8) if (alive(i+3)) out.push_back(i+3);
    }
    for (; i < n; ++i) {
        if (!alive(i)) continue;
        int64_t v = col[i];
        bool in_range = v >= lo && v <= hi;
        if (negate ? !in_range : in_range) out.push_back(i);
    }
}

// AVX2 IN scan
inline void avx2_filter_in(const int64_t* col, size_t n,
                             const AliveChecker& alive,
                             const std::vector<int64_t>& targets, bool negate,
                             std::vector<size_t>& out) {
    size_t i = 0;
    for (; i + 3 < n; i += 4) {
        __m256i vals = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(col + i));
        __m256i mask = _mm256_setzero_si256();
        for (int64_t t : targets) {
            __m256i tv = _mm256_set1_epi64x(t);
            mask = _mm256_or_si256(mask, _mm256_cmpeq_epi64(vals, tv));
        }
        if (negate) {
            __m256i ones = _mm256_set1_epi64x(-1LL);
            mask = _mm256_andnot_si256(mask, ones);
        }
        int m = _mm256_movemask_pd(_mm256_castsi256_pd(mask));
        if (m & 1) if (alive(i))   out.push_back(i);
        if (m & 2) if (alive(i+1)) out.push_back(i+1);
        if (m & 4) if (alive(i+2)) out.push_back(i+2);
        if (m & 8) if (alive(i+3)) out.push_back(i+3);
    }
    for (; i < n; ++i) {
        if (!alive(i)) continue;
        int64_t v = col[i];
        bool found = false;
        for (int64_t t : targets)
            if (v == t) { found = true; break; }
        if (negate ? !found : found) out.push_back(i);
    }
}

} // anonymous namespace

// ── Scan ──────────────────────────────────────────────────────────────────────
QueryResult Table::scan(const std::vector<std::string>& select_cols,
                         const Predicate* where,
                         const std::vector<OrderByClause>& order_by) const {
    std::shared_lock lock(mtx);

    // Resolve column indices
    bool select_all = (select_cols.size() == 1 && select_cols[0] == "*");
    std::vector<int> col_indices;
    std::vector<std::string> out_names;

    if (select_all) {
        for (int i = 0; i < num_cols_; ++i) {
            col_indices.push_back(i);
            out_names.push_back(schema_.columns[i].name);
        }
    } else {
        for (const auto& c : select_cols) {
            std::string col_name = c;
            size_t dot = col_name.find('.');
            if (dot != std::string::npos) col_name = col_name.substr(dot + 1);
            int idx = schema_.col_index(col_name);
            if (idx < 0) return QueryResult::err("unknown column: " + c);
            col_indices.push_back(idx);
            out_names.push_back(schema_.columns[idx].name);
        }
    }

    // Fast path: simple PK equality lookup (LEAF predicate, op="=", pk column)
    if (where && where->kind == Predicate::LEAF && where->op == "=" &&
        schema_.pk_col_index >= 0) {
        std::string col_name = where->col;
        size_t dot = col_name.find('.');
        if (dot != std::string::npos) col_name = col_name.substr(dot + 1);
        if (col_name == schema_.columns[schema_.pk_col_index].name) {
            long long pos = pk_index_.lookup(where->val);
            QueryResult res;
            res.column_names = out_names;
            if (pos >= 0) {
                size_t idx = (size_t)pos;
                if (!rows_meta_[idx].deleted && !is_expired(rows_meta_[idx]))
                    res.rows.push_back(project(idx, col_indices));
            }
            return res;
        }
    }

    // Fast path: PK IN lookup (WHERE pk IN (...)) → O(k) hash lookups
    if (where && where->kind == Predicate::LEAF &&
        (where->op == "IN" || where->op == "NOT IN") &&
        schema_.pk_col_index >= 0) {
        std::string col_name = where->col;
        size_t dot = col_name.find('.');
        if (dot != std::string::npos) col_name = col_name.substr(dot + 1);
        if (col_name == schema_.columns[schema_.pk_col_index].name &&
            where->op == "IN") {
            QueryResult res;
            res.column_names = out_names;
            for (const auto& v : where->in_vals) {
                long long pos = pk_index_.lookup(v);
                if (pos >= 0) {
                    size_t idx = (size_t)pos;
                    if (!rows_meta_[idx].deleted && !is_expired(rows_meta_[idx]))
                        res.rows.push_back(project(idx, col_indices));
                }
            }
            return res;
        }
    }

    // Collect matching row indices
    std::vector<size_t> row_idxs;
    row_idxs.reserve(rows_meta_.size());

    // SIMD fast path: simple numeric LEAF predicate with int_cols_ available
    bool used_simd = false;
    if (where && where->kind == Predicate::LEAF &&
        where->op != "IS NULL" && where->op != "IS NOT NULL" &&
        where->op != "LIKE") {
        std::string col_name = where->col;
        size_t dot = col_name.find('.');
        if (dot != std::string::npos) col_name = col_name.substr(dot + 1);
        int pred_col = schema_.col_index(col_name);
        if (pred_col >= 0 && is_numeric_col(pred_col) &&
            pred_col < (int)int_cols_.size() && !int_cols_[pred_col].empty()) {
            const int64_t* col_data = int_cols_[pred_col].data();
            const size_t n = rows_meta_.size();
            AliveChecker alive{rows_meta_.data(), (int64_t)std::time(nullptr)};

            if (where->op == "BETWEEN" || where->op == "NOT BETWEEN") {
                int64_t lo = parse_int64(where->val);
                int64_t hi = parse_int64(where->val2);
                avx2_filter_between(col_data, n, alive, lo, hi,
                                    where->op == "NOT BETWEEN", row_idxs);
                used_simd = true;
            } else if (where->op == "IN" || where->op == "NOT IN") {
                std::vector<int64_t> targets;
                targets.reserve(where->in_vals.size());
                for (const auto& v : where->in_vals)
                    targets.push_back(parse_int64(v));
                avx2_filter_in(col_data, n, alive, targets,
                               where->op == "NOT IN", row_idxs);
                used_simd = true;
            } else {
                int64_t target = parse_int64(where->val);
                avx2_filter_cmp(col_data, n, alive, where->op, target, row_idxs);
                used_simd = true;
            }
        }
    }

    // General scan fallback
    if (!used_simd) {
        for (size_t i = 0; i < rows_meta_.size(); ++i) {
            if (rows_meta_[i].deleted || is_expired(rows_meta_[i])) continue;
            if (where && !matches_pred(i, *where)) continue;
            row_idxs.push_back(i);
        }
    }

    // ORDER BY — multi-column stable sort (rightmost key applied first)
    if (!order_by.empty()) {
        // Resolve each ORDER BY key to a schema column index
        struct SortKey { int col_idx; bool numeric; bool desc; };
        std::vector<SortKey> keys;
        for (const auto& ob : order_by) {
            std::string col_name = ob.col;
            size_t dot = col_name.find('.');
            if (dot != std::string::npos) col_name = col_name.substr(dot + 1);
            int schema_col = schema_.col_index(col_name);
            if (schema_col < 0) continue;
            bool numeric = (schema_.columns[schema_col].type == ColumnType::INT ||
                            schema_.columns[schema_col].type == ColumnType::DECIMAL);
            keys.push_back({schema_col, numeric, ob.desc});
        }
        // Apply keys right-to-left so leftmost key dominates
        for (int ki = (int)keys.size() - 1; ki >= 0; --ki) {
            const auto& k = keys[ki];
            // Use int_cols_ for numeric sort (avoids stod per comparison)
            const bool use_int = k.numeric && k.col_idx < (int)int_cols_.size() &&
                                 !int_cols_[k.col_idx].empty();
            std::stable_sort(row_idxs.begin(), row_idxs.end(),
                [&](size_t a, size_t b) {
                    bool less_than;
                    if (use_int) {
                        int64_t va = int_cols_[k.col_idx][a];
                        int64_t vb = int_cols_[k.col_idx][b];
                        less_than = va < vb;
                    } else if (k.numeric) {
                        std::string_view sva = val_at(a, k.col_idx);
                        std::string_view svb = val_at(b, k.col_idx);
                        try {
                            less_than = std::stod(std::string(sva)) < std::stod(std::string(svb));
                        } catch (...) {
                            less_than = sva < svb;
                        }
                    } else {
                        less_than = val_at(a, k.col_idx) < val_at(b, k.col_idx);
                    }
                    return k.desc ? !less_than : less_than;
                });
        }
    }

    // Project in sorted order
    QueryResult res;
    res.column_names = out_names;
    res.rows.reserve(row_idxs.size());
    for (size_t i : row_idxs)
        res.rows.push_back(project(i, col_indices));

    return res;
}

// ── Live rows (for JOIN) ──────────────────────────────────────────────────────
std::vector<std::pair<std::vector<std::string>, size_t>>
Table::live_rows() const {
    // Caller holds shared_lock on mtx externally.
    std::vector<std::pair<std::vector<std::string>, size_t>> out;
    out.reserve(rows_meta_.size());
    for (size_t i = 0; i < rows_meta_.size(); ++i) {
        if (!rows_meta_[i].deleted && !is_expired(rows_meta_[i])) {
            std::vector<std::string> vals;
            vals.reserve((size_t)num_cols_);
            for (int c = 0; c < num_cols_; ++c)
                vals.emplace_back(val_at(i, c));
            out.emplace_back(std::move(vals), i);
        }
    }
    return out;
}

// ── Update ────────────────────────────────────────────────────────────────────
int Table::update_rows(const std::vector<std::pair<std::string,std::string>>& assignments,
                       const Predicate* where) {
    std::unique_lock lock(mtx);

    // Helper: apply assignments to a single row
    auto apply = [&](size_t i) {
        for (const auto& [col, val] : assignments) {
            int idx = schema_.col_index(col);
            if (idx >= 0 && idx < num_cols_) {
                val_ref(i, idx) = arena_.intern(val);
                // Update native int64 column
                if (is_numeric_col(idx) && idx < (int)int_cols_.size() &&
                    !int_cols_[idx].empty())
                    int_cols_[idx][i] = parse_int64(val);
            }
        }
    };

    // PK fast path: equality (WHERE pk = X) → O(1) hash lookup
    if (where && where->kind == Predicate::LEAF && where->op == "=" &&
        schema_.pk_col_index >= 0) {
        std::string col_name = where->col;
        size_t dot = col_name.find('.');
        if (dot != std::string::npos) col_name = col_name.substr(dot + 1);
        if (col_name == schema_.columns[schema_.pk_col_index].name) {
            long long pos = pk_index_.lookup(where->val);
            if (pos >= 0 && !rows_meta_[(size_t)pos].deleted &&
                !is_expired(rows_meta_[(size_t)pos])) {
                apply((size_t)pos);
                return 1;
            }
            return 0;
        }
    }

    // PK fast path: IN (WHERE pk IN (...)) → O(k) hash lookups
    if (where && where->kind == Predicate::LEAF && where->op == "IN" &&
        schema_.pk_col_index >= 0) {
        std::string col_name = where->col;
        size_t dot = col_name.find('.');
        if (dot != std::string::npos) col_name = col_name.substr(dot + 1);
        if (col_name == schema_.columns[schema_.pk_col_index].name) {
            int count = 0;
            for (const auto& v : where->in_vals) {
                long long pos = pk_index_.lookup(v);
                if (pos >= 0 && !rows_meta_[(size_t)pos].deleted &&
                    !is_expired(rows_meta_[(size_t)pos])) {
                    apply((size_t)pos);
                    ++count;
                }
            }
            return count;
        }
    }

    // General scan
    int count = 0;
    for (size_t i = 0; i < rows_meta_.size(); ++i) {
        if (rows_meta_[i].deleted || is_expired(rows_meta_[i])) continue;
        if (where && !matches_pred(i, *where)) continue;
        apply(i);
        ++count;
    }
    return count;
}

// ── Truncate ──────────────────────────────────────────────────────────────────
void Table::truncate() {
    std::unique_lock lock(mtx);
    rows_meta_.clear();
    rows_vals_.clear();
    arena_.clear();
    pk_index_.clear();
    for (auto& ic : int_cols_) ic.clear();
}

// ── Snapshot rows (for persistence) ──────────────────────────────────────────
std::vector<std::pair<std::vector<std::string>, int64_t>>
Table::snapshot_rows() const {
    std::shared_lock lock(mtx);
    std::vector<std::pair<std::vector<std::string>, int64_t>> out;
    out.reserve(rows_meta_.size());
    for (size_t i = 0; i < rows_meta_.size(); ++i) {
        if (rows_meta_[i].deleted || is_expired(rows_meta_[i])) continue;
        std::vector<std::string> vals;
        vals.reserve((size_t)num_cols_);
        for (int c = 0; c < num_cols_; ++c)
            vals.emplace_back(val_at(i, c));
        out.emplace_back(std::move(vals), rows_meta_[i].expires_at);
    }
    return out;
}

// ── Compact ───────────────────────────────────────────────────────────────────
void Table::compact() {
    std::unique_lock lock(mtx);

    // Rebuild a fresh arena and both vectors, keeping only live rows.
    StringArena new_arena;
    std::vector<RowMeta>          new_meta;
    std::vector<std::string_view> new_vals;
    std::vector<std::vector<int64_t>> new_int_cols((size_t)num_cols_);
    new_meta.reserve(rows_meta_.size());
    new_vals.reserve(rows_vals_.size());
    for (int c = 0; c < num_cols_; ++c)
        if (is_numeric_col(c)) new_int_cols[c].reserve(rows_meta_.size());
    pk_index_.clear();

    for (size_t i = 0; i < rows_meta_.size(); ++i) {
        if (rows_meta_[i].deleted || is_expired(rows_meta_[i])) continue;
        size_t new_idx = new_meta.size();
        for (int c = 0; c < num_cols_; ++c)
            new_vals.push_back(new_arena.intern(val_at(i, c)));
        new_meta.push_back(rows_meta_[i]);
        // Rebuild int_cols_
        for (int c = 0; c < num_cols_; ++c)
            if (is_numeric_col(c) && !int_cols_[c].empty())
                new_int_cols[c].push_back(int_cols_[c][i]);
        if (schema_.pk_col_index >= 0)
            pk_index_.insert(new_vals[new_idx * (size_t)num_cols_ +
                                      (size_t)schema_.pk_col_index],
                             new_idx);
    }

    arena_     = std::move(new_arena);
    rows_meta_ = std::move(new_meta);
    rows_vals_ = std::move(new_vals);
    int_cols_  = std::move(new_int_cols);
}
