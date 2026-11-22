#include "storage/table.h"
#include "parser/ast.h"
#include "utils/string_utils.h"
#include <algorithm>
#include <mutex>
#include <unordered_map>
#include <stdexcept>
#include <ctime>
#include <immintrin.h>   // AVX2 / SSE intrinsics
#include <ostream>

// ── Construction ──────────────────────────────────────────────────────────────
Table::Table(TableSchema schema)
    : schema_(std::move(schema)),
      num_cols_((int)schema_.columns.size()),
      int_cols_((size_t)num_cols_),
      int_exact_((size_t)num_cols_, 1) {}

// True if every literal of a predicate leaf is an exact integer, so the int64
// fast path compares exactly what the slow (stod) path would.
static bool pred_literals_exact(const Predicate& p) {
    auto exact = [](const std::string& s) {
        if (s.empty()) return false;
        size_t start = (s[0] == '-' || s[0] == '+') ? 1 : 0;
        if (start >= s.size() || s.size() - start > 18) return false;
        for (size_t i = start; i < s.size(); ++i)
            if (s[i] < '0' || s[i] > '9') return false;
        return true;
    };
    if (p.op == "BETWEEN" || p.op == "NOT BETWEEN")
        return exact(p.val) && exact(p.val2);
    if (p.op == "IN" || p.op == "NOT IN") {
        for (const auto& v : p.in_vals) if (!exact(v)) return false;
        return true;
    }
    return exact(p.val);
}

// ── Helpers ───────────────────────────────────────────────────────────────────
bool Table::is_expired(const RowMeta& m) const {
    if (m.expires_at == 0) return false;
    return m.expires_at <= (int64_t)std::time(nullptr);
}

// ── Type validation helper ────────────────────────────────────────────────────
std::string Table::validate_type(std::string_view val, ColumnType type,
                                  const std::string& col_name) {
    if (val.empty()) return "";  // allow empty / NULL-ish values

    switch (type) {
        case ColumnType::INT: {
            size_t start = (val[0] == '-' || val[0] == '+') ? 1 : 0;
            if (start >= val.size()) return "invalid INT value for column " + col_name + ": " + std::string(val);
            for (size_t i = start; i < val.size(); ++i)
                if (val[i] < '0' || val[i] > '9')
                    return "invalid INT value for column " + col_name + ": " + std::string(val);
            return "";
        }
        case ColumnType::DECIMAL: {
            size_t start = (val[0] == '-' || val[0] == '+') ? 1 : 0;
            if (start >= val.size()) return "invalid DECIMAL value for column " + col_name + ": " + std::string(val);
            bool dot_seen = false;
            for (size_t i = start; i < val.size(); ++i) {
                if (val[i] == '.' && !dot_seen) { dot_seen = true; continue; }
                if (val[i] < '0' || val[i] > '9')
                    return "invalid DECIMAL value for column " + col_name + ": " + std::string(val);
            }
            return "";
        }
        case ColumnType::VARCHAR:
        case ColumnType::TEXT:
            return "";  // any string is valid
        case ColumnType::DATETIME:
            // Accept numeric timestamps and common date formats
            return "";
    }
    return "";
}

std::string Table::validate_value(int col, std::string_view v) const {
    const ColumnDef& cd = schema_.columns[col];
    if (v.empty()) {
        if (cd.not_null || col == schema_.pk_col_index)
            return "NULL value not allowed in NOT NULL column " + cd.name;
        return "";
    }
    return validate_type(v, cd.type, cd.name);
}

// Hot path for batch INSERT: a yes/no check per value with no std::string
// construction; the error message is built only when a value is invalid.
static inline bool numeric_ok(std::string_view v, bool allow_dot) {
    size_t i = (v[0] == '-' || v[0] == '+') ? 1 : 0;
    if (i >= v.size()) return false;
    bool dot = false;
    for (; i < v.size(); ++i) {
        const char ch = v[i];
        if (ch >= '0' && ch <= '9') continue;
        if (allow_dot && ch == '.' && !dot) { dot = true; continue; }
        return false;
    }
    return true;
}

std::string Table::validate_row(const std::string_view* row) const {
    for (int c = 0; c < num_cols_; ++c) {
        const std::string_view v = row[c];
        const ColumnDef& cd = schema_.columns[c];
        bool ok;
        if (v.empty())                         ok = !(cd.not_null || c == schema_.pk_col_index);
        else if (cd.type == ColumnType::INT)     ok = numeric_ok(v, false);
        else if (cd.type == ColumnType::DECIMAL) ok = numeric_ok(v, true);
        else                                     ok = true;
        if (__builtin_expect(!ok, 0)) return validate_value(c, v);   // builds the message
    }
    return "";
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


Table::BoundPred Table::bind(const Predicate& pred) const {
    BoundPred b;
    b.kind = (int)pred.kind;
    b.src  = &pred;
    if (pred.kind != Predicate::LEAF) {
        b.children.reserve(pred.children.size());
        for (const auto& c : pred.children) b.children.push_back(bind(*c));
        return b;
    }
    // Column: strip an optional "table." prefix, resolve the index once.
    std::string_view col_name = pred.col;
    const size_t dot = col_name.find('.');
    if (dot != std::string_view::npos) col_name = col_name.substr(dot + 1);
    b.col = schema_.col_index(std::string(col_name));
    if (b.col < 0 || b.col >= num_cols_) { b.col = -1; return b; }
    b.ct = schema_.columns[b.col].type;

    const std::string& o = pred.op;
    b.op = o == "="  ? BoundPred::EQ : o == "!=" ? BoundPred::NE :
           o == ">"  ? BoundPred::GT : o == ">=" ? BoundPred::GE :
           o == "<"  ? BoundPred::LT : o == "<=" ? BoundPred::LE :
           o == "LIKE" ? BoundPred::LIKE :
           o == "IS NULL" ? BoundPred::IS_NULL : o == "IS NOT NULL" ? BoundPred::NOT_NULL :
           o == "BETWEEN" ? BoundPred::BETWEEN : o == "NOT BETWEEN" ? BoundPred::NOT_BETWEEN :
           o == "IN" ? BoundPred::IN : o == "NOT IN" ? BoundPred::NOT_IN : BoundPred::OTHER;

    // int64 fast path only if the column holds exact ints AND every literal is
    // an exact int (checked ONCE here — previously re-checked for every row).
    b.use_int = int_fast(b.col) && b.op != BoundPred::LIKE && b.op != BoundPred::OTHER &&
                b.op != BoundPred::IS_NULL && b.op != BoundPred::NOT_NULL &&
                pred_literals_exact(pred);
    if (b.use_int) {
        b.iv  = parse_int64(pred.val);
        b.iv2 = parse_int64(pred.val2);
        for (const auto& v : pred.in_vals) b.ivals.push_back(parse_int64(v));
    }
    return b;
}

bool Table::eval(size_t row_idx, const BoundPred& b) const {
    switch (b.kind) {
        case Predicate::AND_NODE:
            for (const auto& c : b.children) if (!eval(row_idx, c)) return false;
            return true;
        case Predicate::OR_NODE:
            for (const auto& c : b.children) if (eval(row_idx, c)) return true;
            return false;
        case Predicate::NOT_NODE:
            return !b.children.empty() && !eval(row_idx, b.children[0]);
        default: break;   // LEAF
    }
    if (b.col < 0) return false;
    const std::string_view row_val = val_at(row_idx, b.col);
    if (b.op == BoundPred::IS_NULL)  return row_val.empty();
    if (b.op == BoundPred::NOT_NULL) return !row_val.empty();

    if (b.use_int) {
        const int64_t rv = int_cols_[b.col][row_idx];
        switch (b.op) {
            case BoundPred::EQ: return rv == b.iv;
            case BoundPred::NE: return rv != b.iv;
            case BoundPred::GT: return rv >  b.iv;
            case BoundPred::GE: return rv >= b.iv;
            case BoundPred::LT: return rv <  b.iv;
            case BoundPred::LE: return rv <= b.iv;
            case BoundPred::BETWEEN:     return rv >= b.iv && rv <= b.iv2;
            case BoundPred::NOT_BETWEEN: return !(rv >= b.iv && rv <= b.iv2);
            case BoundPred::IN:
            case BoundPred::NOT_IN: {
                bool found = false;
                for (int64_t v : b.ivals) if (rv == v) { found = true; break; }
                return b.op == BoundPred::IN ? found : !found;
            }
            default: return false;
        }
    }

    // String / non-exact numeric fallback (same semantics as before).
    const Predicate& p = *b.src;
    if (b.op == BoundPred::BETWEEN || b.op == BoundPred::NOT_BETWEEN) {
        const bool in_range = !compare_values(row_val, "<", p.val, b.ct) &&
                              !compare_values(row_val, ">", p.val2, b.ct);
        return b.op == BoundPred::BETWEEN ? in_range : !in_range;
    }
    if (b.op == BoundPred::IN || b.op == BoundPred::NOT_IN) {
        bool found = false;
        for (const auto& v : p.in_vals)
            if (compare_values(row_val, "=", v, b.ct)) { found = true; break; }
        return b.op == BoundPred::IN ? found : !found;
    }
    return compare_values(row_val, p.op, p.val, b.ct);
}

bool Table::matches_pred(size_t row_idx, const Predicate& pred) const {
    return eval(row_idx, bind(pred));
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

    // Enforce schema (NOT NULL + types)
    for (int c = 0; c < num_cols_; ++c) {
        auto err = validate_value(c, values[c]);
        if (!err.empty()) return err;
    }

    if (schema_.pk_col_index >= 0) {
        if (pk_index_.lookup(values[schema_.pk_col_index]) >= 0)
            return "duplicate primary key: " + values[schema_.pk_col_index];
    }

    size_t row_idx = rows_meta_.size();
    for (const auto& v : values)
        rows_vals_.push_back(arena_.intern(v));
    rows_meta_.push_back({expires_at, false});
    note_expiry(expires_at, 1);

    // Populate native int64 columns
    for (int c = 0; c < num_cols_; ++c)
        if (is_numeric_col(c))
            push_int(c, values[c]);

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

    // Enforce schema on EVERY row before storing anything (outside the lock —
    // the schema is immutable), so a bad row rejects the whole batch.
    for (size_t r = 0; r < n_rows; ++r) {
        auto err = validate_row(flat + r * (size_t)num_cols_);
        if (!err.empty()) return "row " + std::to_string(r + 1) + ": " + err;
    }

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
        note_expiry(expires_at, n_rows);
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
                store_int(c, base_row + r, flat[r * (size_t)num_cols_ + c]);
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
        note_expiry(expires_at, 1);
        // Populate native int64 columns
        for (int c = 0; c < num_cols_; ++c)
            if (is_numeric_col(c))
                push_int(c, row[c]);
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
        // Enforce schema (NOT NULL + types) on every row
        auto err = validate_row(batch[r].data());
        if (!err.empty()) return "row " + std::to_string(r + 1) + ": " + err;
    }

    // Intern + store
    for (size_t r = 0; r < n; ++r) {
        size_t row_idx = rows_meta_.size();
        for (auto sv : batch[r])
            rows_vals_.push_back(arena_.intern(sv));
        rows_meta_.push_back({expires_vec[r], false});
        note_expiry(expires_vec[r], 1);
        // Populate native int64 columns
        for (int c = 0; c < num_cols_; ++c)
            if (is_numeric_col(c))
                push_int(c, batch[r][c]);
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
                    tombstone((size_t)pos);
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
                        tombstone((size_t)pos);
                        pk_index_.remove(v);
                        ++count;
                    }
                }
            }
            return count;
        }
    }

    // General scan (WHERE bound once, not per row)
    int count = 0;
    BoundPred bw;
    if (where) bw = bind(*where);
    for (size_t i = 0; i < rows_meta_.size(); ++i) {
        auto& m = rows_meta_[i];
        if (m.deleted || is_expired(m)) continue;
        if (!where || eval(i, bw)) {
            tombstone(i);
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

// ── O(1)-memory COUNT ─────────────────────────────────────────────────────────
size_t Table::count_rows(const Predicate* where) const {
    std::shared_lock lock(mtx);

    // SIMD fast path for numeric LEAF predicates
    if (where && where->kind == Predicate::LEAF &&
        where->op != "IS NULL" && where->op != "IS NOT NULL" &&
        where->op != "LIKE") {
        std::string col_name = where->col;
        size_t dot = col_name.find('.');
        if (dot != std::string::npos) col_name = col_name.substr(dot + 1);
        int pred_col = schema_.col_index(col_name);
        if (int_fast(pred_col) && pred_literals_exact(*where)) {
            std::vector<size_t> matched;
            matched.reserve(rows_meta_.size());
            const int64_t* col_data = int_cols_[pred_col].data();
            const size_t n = rows_meta_.size();
            AliveChecker alive{rows_meta_.data(), (int64_t)std::time(nullptr)};

            if (where->op == "BETWEEN" || where->op == "NOT BETWEEN") {
                avx2_filter_between(col_data, n, alive, parse_int64(where->val),
                                    parse_int64(where->val2),
                                    where->op == "NOT BETWEEN", matched);
            } else if (where->op == "IN" || where->op == "NOT IN") {
                std::vector<int64_t> targets;
                targets.reserve(where->in_vals.size());
                for (const auto& v : where->in_vals)
                    targets.push_back(parse_int64(v));
                avx2_filter_in(col_data, n, alive, targets,
                               where->op == "NOT IN", matched);
            } else {
                avx2_filter_cmp(col_data, n, alive, where->op,
                                parse_int64(where->val), matched);
            }
            return matched.size();
        }
    }

    // General fallback (WHERE bound once, not per row)
    size_t count = 0;
    BoundPred bw;
    if (where) bw = bind(*where);
    for (size_t i = 0; i < rows_meta_.size(); ++i) {
        if (rows_meta_[i].deleted || is_expired(rows_meta_[i])) continue;
        if (where && !eval(i, bw)) continue;
        ++count;
    }
    return count;
}

// ── O(1)-memory aggregate (SUM/MIN/MAX) ───────────────────────────────────────
int64_t Table::aggregate(const Predicate* where, const std::vector<int>& cols,
                         std::vector<AggState>& out) const {
    std::shared_lock lock(mtx);

    // One AggState per requested column; numeric-ness comes from the schema.
    out.clear();
    std::vector<char> use_int(cols.size());
    for (size_t k = 0; k < cols.size(); ++k) {
        out.emplace_back(is_numeric_col(cols[k]));
        use_int[k] = int_fast(cols[k]);     // exact int64 copy available → no parsing
    }
    BoundPred bw;
    if (where) bw = bind(*where);
    const int64_t now = (int64_t)std::time(nullptr);

    // Vectorized (chunked) execution:
    //   1. for a chunk of 1024 rows, collect the matching row numbers into a
    //      small "selection vector" (liveness + WHERE evaluated ONCE per row);
    //   2. then, per column, run a tight loop over just those rows with a LOCAL
    //      accumulator. A local the compiler can see is not aliased by the
    //      column pointer, so isum/imin/imax/count stay in CPU registers.
    //      (Updating out[k] directly per row forced a store to memory per row:
    //      ~40% slower.) Memory stays O(1): one 1024-entry buffer.
    constexpr size_t CHUNK = 1024;
    size_t  sel[CHUNK];
    int64_t rows = 0;
    const size_t n = rows_meta_.size();
    for (size_t base = 0; base < n; base += CHUNK) {
        const size_t end = std::min(n, base + CHUNK);
        size_t m = 0;
        for (size_t i = base; i < end; ++i) {
            const RowMeta& meta = rows_meta_[i];
            if (meta.deleted || (meta.expires_at != 0 && meta.expires_at <= now)) continue;
            if (where && !eval(i, bw)) continue;
            sel[m++] = i;
        }
        rows += (int64_t)m;
        for (size_t k = 0; k < cols.size(); ++k) {
            AggState acc = std::move(out[k]);          // local copy → registers
            const int c = cols[k];
            if (use_int[k]) {                          // int_fast ⇒ no NULLs in column
                const int64_t* col = int_cols_[c].data();
                for (size_t j = 0; j < m; ++j) acc.add_int(col[sel[j]]);
            } else {
                for (size_t j = 0; j < m; ++j) acc.add_text(val_at(sel[j], c));
            }
            out[k] = std::move(acc);
        }
    }
    return rows;
}

Table::GroupResult Table::group_aggregate(const Predicate* where,
                                          const std::vector<int>& group_cols,
                                          const std::vector<int>& agg_cols) const {
    std::shared_lock lock(mtx);

    std::vector<char> numeric(agg_cols.size()), use_int(agg_cols.size());
    for (size_t k = 0; k < agg_cols.size(); ++k) {
        numeric[k] = is_numeric_col(agg_cols[k]);
        use_int[k] = int_fast(agg_cols[k]);
    }
    BoundPred bw;
    if (where) bw = bind(*where);
    const int64_t now = (int64_t)std::time(nullptr);

    GroupResult gr;
    std::vector<size_t> first_row;                 // per group: where its key text comes from

    auto new_group = [&](size_t row) -> uint32_t {
        first_row.push_back(row);
        gr.rows.push_back(0);
        gr.states.emplace_back();
        for (size_t k = 0; k < agg_cols.size(); ++k) gr.states.back().emplace_back(numeric[k] != 0);
        return (uint32_t)(gr.rows.size() - 1);
    };
    auto add_row = [&](uint32_t g, size_t i) {
        ++gr.rows[g];
        std::vector<AggState>& st = gr.states[g];
        for (size_t k = 0; k < agg_cols.size(); ++k) {
            const int c = agg_cols[k];
            if (use_int[k]) st[k].add_int(int_cols_[c][i]);
            else            st[k].add_text(val_at(i, c));
        }
    };
    auto alive_match = [&](size_t i) {
        const RowMeta& m = rows_meta_[i];
        if (m.deleted || (m.expires_at != 0 && m.expires_at <= now)) return false;
        return !where || eval(i, bw);
    };

    if (group_cols.size() == 1 && int_fast(group_cols[0])) {
        // Fast path: ONE group column holding exact integers → the hash key is
        // the int64 itself. No string is built per row at all.
        const int64_t* gcol = int_cols_[group_cols[0]].data();
        std::unordered_map<int64_t, uint32_t> map;
        for (size_t i = 0; i < rows_meta_.size(); ++i) {
            if (!alive_match(i)) continue;
            auto [it, inserted] = map.try_emplace(gcol[i], 0);
            if (inserted) it->second = new_group(i);
            add_row(it->second, i);
        }
    } else {
        // General path: key = for each group column [4-byte length][bytes].
        // Length-prefixing makes keys unambiguous: ("ab","c") ≠ ("a","bc"),
        // whatever bytes the values contain. One reused buffer → no
        // allocation per row except when a NEW group is inserted.
        std::unordered_map<std::string, uint32_t> map;
        std::string key;
        for (size_t i = 0; i < rows_meta_.size(); ++i) {
            if (!alive_match(i)) continue;
            key.clear();
            for (int g : group_cols) {
                const std::string_view v = val_at(i, g);
                const uint32_t len = (uint32_t)v.size();
                key.append(reinterpret_cast<const char*>(&len), sizeof len);
                key.append(v.data(), v.size());
            }
            auto it = map.find(key);
            if (it == map.end()) it = map.emplace(key, new_group(i)).first;
            add_row(it->second, i);
        }
    }

    gr.keys.resize(first_row.size());
    for (size_t g = 0; g < first_row.size(); ++g)
        for (int c : group_cols) gr.keys[g].emplace_back(val_at(first_row[g], c));
    return gr;
}

// ── Scan ──────────────────────────────────────────────────────────────────────
QueryResult Table::scan(const std::vector<std::string>& select_cols,
                         const Predicate* where,
                         const std::vector<OrderByClause>& order_by,
                         int limit, int offset) const {
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
                if (!rows_meta_[idx].deleted && !is_expired(rows_meta_[idx])) {
                    size_t ncols = col_indices.size();
                    res.flat_offsets.reserve(ncols + 1);
                    for (int c : col_indices) {
                        res.flat_offsets.push_back((uint32_t)res.flat_data.size());
                        auto sv = val_at(idx, c);
                        res.flat_data.append(sv.data(), sv.size());
                    }
                    res.flat_offsets.push_back((uint32_t)res.flat_data.size());
                }
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
            res.flat_offsets.reserve(where->in_vals.size() * col_indices.size() + 1);
            for (const auto& v : where->in_vals) {
                long long pos = pk_index_.lookup(v);
                if (pos >= 0) {
                    size_t idx = (size_t)pos;
                    if (!rows_meta_[idx].deleted && !is_expired(rows_meta_[idx]))
                        for (int c : col_indices) {
                            res.flat_offsets.push_back((uint32_t)res.flat_data.size());
                            auto sv = val_at(idx, c);
                            res.flat_data.append(sv.data(), sv.size());
                        }
                }
            }
            res.flat_offsets.push_back((uint32_t)res.flat_data.size());
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
        if (int_fast(pred_col) && pred_literals_exact(*where)) {
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
        // Early-out when no ORDER BY and we have a LIMIT: stop after enough rows
        const bool can_early_out = order_by.empty() && limit >= 0;
        const size_t need = can_early_out ? (size_t)(offset + limit) : SIZE_MAX;
        BoundPred bw;
        if (where) bw = bind(*where);
        for (size_t i = 0; i < rows_meta_.size(); ++i) {
            if (rows_meta_[i].deleted || is_expired(rows_meta_[i])) continue;
            if (where && !eval(i, bw)) continue;
            row_idxs.push_back(i);
            if (row_idxs.size() >= need) break;
        }
    }

    // ORDER BY — use partial_sort when LIMIT is set (O(n log k) vs O(n log n))
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

        // Build a single combined comparator for all keys (left-to-right priority)
        auto cmp = [&](size_t a, size_t b) -> bool {
            for (const auto& k : keys) {
                const bool use_int = k.numeric && int_fast(k.col_idx);
                int r;
                if (use_int) {
                    int64_t va = int_cols_[k.col_idx][a];
                    int64_t vb = int_cols_[k.col_idx][b];
                    r = (va < vb) ? -1 : (va > vb) ? 1 : 0;
                } else if (k.numeric) {
                    std::string_view sva = val_at(a, k.col_idx);
                    std::string_view svb = val_at(b, k.col_idx);
                    try {
                        double da = std::stod(std::string(sva));
                        double db = std::stod(std::string(svb));
                        r = (da < db) ? -1 : (da > db) ? 1 : 0;
                    } catch (...) {
                        r = sva.compare(svb);
                    }
                } else {
                    r = val_at(a, k.col_idx).compare(val_at(b, k.col_idx));
                }
                if (r != 0)
                    return k.desc ? (r > 0) : (r < 0);
            }
            return false; // equal
        };

        // partial_sort when we know the output size; full stable_sort otherwise
        size_t k = row_idxs.size();
        if (limit >= 0) {
            k = std::min(k, (size_t)(offset > 0 ? offset : 0) + (size_t)limit);
            std::partial_sort(row_idxs.begin(),
                              row_idxs.begin() + (ptrdiff_t)k,
                              row_idxs.end(), cmp);
        } else {
            std::stable_sort(row_idxs.begin(), row_idxs.end(), cmp);
        }
    }

    // Apply OFFSET + LIMIT, then project only the needed rows
    size_t start = 0, end = row_idxs.size();
    if (offset > 0) start = std::min((size_t)offset, end);
    if (limit >= 0)  end   = std::min(end, start + (size_t)limit);

    const size_t ncols = col_indices.size();
    const size_t nrows = end - start;

    QueryResult res;
    res.column_names = out_names;
    // Estimate average cell size (~12 bytes) to pre-reserve the data buffer
    res.flat_data.reserve(nrows * ncols * 12);
    res.flat_offsets.resize(nrows * ncols + 1);
    size_t cell_idx = 0;
    for (size_t i = start; i < end; ++i) {
        size_t row_idx = row_idxs[i];
        for (size_t ci = 0; ci < ncols; ++ci) {
            int c = col_indices[ci];
            res.flat_offsets[cell_idx] = (uint32_t)res.flat_data.size();
            if (c >= 0 && c < num_cols_) {
                auto sv = val_at(row_idx, c);
                res.flat_data.append(sv.data(), sv.size());
            }
            ++cell_idx;
        }
    }
    res.flat_offsets[cell_idx] = (uint32_t)res.flat_data.size();

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
        note_dirty(i);
        for (const auto& [col, val] : assignments) {
            int idx = schema_.col_index(col);
            if (idx >= 0 && idx < num_cols_) {
                const std::string_view old_v = val_ref(i, idx);
                const std::string_view new_v = arena_.intern(val);
                if (idx == schema_.pk_col_index && old_v != new_v) {
                    // The index key is a view into the arena: re-key it BEFORE
                    // the old bytes are released (they may be madvise()d).
                    pk_index_.remove(old_v);
                    pk_index_.insert(new_v, i);
                }
                val_ref(i, idx) = new_v;
                arena_.release(old_v);   // old value's bytes are now dead
                // Update native int64 column
                if (is_numeric_col(idx) && idx < (int)int_cols_.size() &&
                    !int_cols_[idx].empty())
                    store_int(idx, i, val);
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

    // General scan (WHERE bound once, not per row)
    int count = 0;
    BoundPred bw;
    if (where) bw = bind(*where);
    for (size_t i = 0; i < rows_meta_.size(); ++i) {
        if (rows_meta_[i].deleted || is_expired(rows_meta_[i])) continue;
        if (where && !eval(i, bw)) continue;
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
    {
        // A sweep may be madvise()ing pages of this arena right now (it holds
        // release_mtx_, not mtx): wait for it, then unmap and bump the
        // generation so any page list taken from the old arena is discarded.
        std::lock_guard rg(release_mtx_);
        arena_.clear();
        ++arena_gen_;
    }
    if (compacting_) abort_compaction_ = true;   // patching can't express "all gone"
    pk_index_.clear();
    for (auto& ic : int_cols_) ic.clear();
    std::fill(int_exact_.begin(), int_exact_.end(), 1);
    dead_rows_ = 0;
    expiry_buckets_.clear();
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

// ── Streaming snapshot writer ─────────────────────────────────────────────────
bool Table::write_rows(std::ostream& out) const {
    std::shared_lock lock(mtx);
    auto put32 = [&](uint32_t v) {
        char b[4] = {char(v), char(v >> 8), char(v >> 16), char(v >> 24)};
        out.write(b, 4);
    };
    auto put64 = [&](uint64_t v) {
        char b[8];
        for (int i = 0; i < 8; ++i) b[i] = char(v >> (i * 8));
        out.write(b, 8);
    };

    uint64_t live = 0;
    for (size_t i = 0; i < rows_meta_.size(); ++i)
        if (!rows_meta_[i].deleted && !is_expired(rows_meta_[i])) ++live;
    put64(live);

    for (size_t i = 0; i < rows_meta_.size(); ++i) {
        if (rows_meta_[i].deleted || is_expired(rows_meta_[i])) continue;
        put64((uint64_t)rows_meta_[i].expires_at);
        for (int c = 0; c < num_cols_; ++c) {
            std::string_view v = val_at(i, c);
            put32((uint32_t)v.size());
            if (!v.empty()) out.write(v.data(), (std::streamsize)v.size());
        }
    }
    return !!out;
}

// ── Tiered compaction ─────────────────────────────────────────────────────────
// See the phase table in table.h. Lock order (no cycles):
//   sweep_mtx_ → [write gate (shared)] → mtx → release_mtx_
// release_mtx_ is also taken ALONE by phase 2, which never takes mtx while
// holding it.

Table::ArenaStats Table::arena_stats() const {
    std::shared_lock lock(mtx);
    return {arena_.live_bytes(), arena_.reserved_bytes(),
            arena_.released_bytes(), arena_.resident_bytes()};
}

bool Table::rebuild_due() const {
    const size_t resident = arena_.resident_bytes();
    const size_t live     = arena_.live_bytes();
    const size_t waste    = resident > live ? resident - live : 0;
    return waste >= limits_.min_waste_bytes &&
           waste * limits_.waste_ratio >= resident;
}

Table::SweepResult Table::sweep(std::shared_mutex* gate, bool force) {
    std::lock_guard one_sweep(sweep_mtx_);       // one sweep per table at a time
    SweepResult res;

    // Phase 0 — peek: cheap check under a SHARED lock (readers unaffected).
    bool rows_due;
    {
        std::shared_lock peek(mtx);
        rows_due = force || compaction_due();
    }

    // Phase 1 — tier 1: drop dead rows from the arrays (exclusive, short).
    if (rows_due) {
        std::shared_lock<std::shared_mutex> g;
        if (gate) g = std::shared_lock<std::shared_mutex>(*gate);
        std::unique_lock lock(mtx);
        // Re-check: the table may have changed between the two locks.
        if (force || compaction_due()) {
            compact_rows_locked();
            res.rows_compacted = true;
        }
    }

    // Phase 2 — tier 2: give fully-dead pages back to the OS (no table lock).
    res.bytes_released = release_dead_pages();

    // Phase 3 — tier 3: only if dead bytes are still scattered across pages.
    res.arena_rebuilt = rebuild_arena(gate, force);
    return res;
}

// Tier 1 — two-pointer pass: live rows slide down (16-byte view copies, no
// string copies); dead/expired rows' bytes are released to the arena, whose
// page counters queue any page that becomes fully dead.
void Table::compact_rows_locked() {
    const int64_t now    = (int64_t)std::time(nullptr);
    const size_t  nc     = (size_t)num_cols_;
    const bool    has_pk = schema_.pk_col_index >= 0;
    const size_t  pkc    = has_pk ? (size_t)schema_.pk_col_index : 0;

    std::map<int64_t, size_t> buckets;      // expiry buckets of the survivors

    // Row numbers change, but no string moves: record old → new row numbers
    // and fix the PK index IN PLACE afterwards (one pass over its slots),
    // instead of re-hashing and re-inserting every surviving key.
    const bool remap_pk = has_pk && rows_meta_.size() < PrimaryIndex::DROPPED;
    std::vector<uint32_t> remap;
    if (remap_pk)    remap.assign(rows_meta_.size(), PrimaryIndex::DROPPED);
    else if (has_pk) pk_index_.reset(rows_meta_.size() - std::min(dead_rows_, rows_meta_.size()));

    size_t w = 0;                           // write position
    for (size_t r = 0; r < rows_meta_.size(); ++r) {      // r = read position
        const RowMeta m = rows_meta_[r];
        if (m.deleted || (m.expires_at != 0 && m.expires_at <= now)) {
            for (size_t c = 0; c < nc; ++c) arena_.release(rows_vals_[r * nc + c]);
            continue;
        }
        if (w != r) {
            rows_meta_[w] = m;
            std::copy_n(rows_vals_.begin() + (std::ptrdiff_t)(r * nc), nc,
                        rows_vals_.begin() + (std::ptrdiff_t)(w * nc));
            for (int c = 0; c < num_cols_; ++c)
                if (is_numeric_col(c) && !int_cols_[c].empty())
                    int_cols_[c][w] = int_cols_[c][r];
        }
        if (m.expires_at != 0) ++buckets[m.expires_at / 60];
        if (remap_pk)    remap[r] = (uint32_t)w;
        else if (has_pk) pk_index_.insert(rows_vals_[w * nc + pkc], w);  // >4G rows fallback
        ++w;
    }
    if (remap_pk) pk_index_.remap_rows(remap);
    // Deleted/dropped keys left tombstones; if they now outnumber the live
    // keys, rebuild the index smaller (drops tombstones, shortens chains).
    if (has_pk) pk_index_.compact_if_sparse();

    rows_meta_.resize(w);
    rows_vals_.resize(w * nc);
    for (auto& ic : int_cols_) if (!ic.empty()) ic.resize(w);
    // The arrays' memory is only returned if we shrink their capacity; do it
    // when they are mostly empty (one realloc + 16-byte copies).
    if (rows_meta_.capacity() > 2 * w + 1024) {
        rows_meta_.shrink_to_fit();
        rows_vals_.shrink_to_fit();
        for (auto& ic : int_cols_) ic.shrink_to_fit();
    }

    dead_rows_      = 0;
    expiry_buckets_ = std::move(buckets);
}

// Tier 2 — madvise() the queued dead pages WITHOUT holding the table lock.
// Safe: a queued page has 0 live bytes (no string_view points into it) and
// lies behind the bump pointer (nothing will ever be written there again).
// Only TRUNCATE / an arena swap could unmap it underneath us; both bump
// arena_gen_ under release_mtx_, which we hold while calling madvise().
size_t Table::release_dead_pages() {
    std::vector<char*> pages;
    uint64_t gen;
    {
        std::unique_lock lock(mtx);             // writers push to the list
        pages = arena_.take_dead_pages();       // O(1) handover (std::exchange)
        gen   = arena_gen_;
    }
    if (pages.empty()) return 0;

    std::lock_guard rg(release_mtx_);
    if (gen != arena_gen_) return 0;            // arena replaced: pages are gone
    const size_t n = StringArena::madvise_pages(pages);   // batched ranges
    arena_.add_released(n);
    return n;
}

// Tier 3 — rebuild the arena aside under a SHARED lock (readers keep running;
// writers wait), then patch the writes made in the gap before the exclusive
// lock and swap. Freeing the old arena happens after the lock is released.
bool Table::rebuild_arena(std::shared_mutex* gate, bool force) {
    const size_t nc     = (size_t)num_cols_;
    const bool   has_pk = schema_.pk_col_index >= 0;
    const size_t pkc    = has_pk ? (size_t)schema_.pk_col_index : 0;

    // The copy. Declared out here: after the swap these hold the OLD data,
    // which is then freed when the function returns — outside the lock.
    StringArena                       n_arena;
    std::vector<RowMeta>              n_meta;
    std::vector<std::string_view>     n_vals;
    std::vector<std::vector<int64_t>> n_ints((size_t)num_cols_);
    PrimaryIndex                      n_pk;
    std::vector<long long>            remap;      // old row → new row, -1 = dropped
    size_t                            old_count = 0;

    // Append live row r (current numbering) to the copy; returns its new index.
    auto copy_row = [&](size_t r) -> long long {
        const size_t n = n_meta.size();
        n_meta.push_back(rows_meta_[r]);
        for (size_t c = 0; c < nc; ++c)
            n_vals.push_back(n_arena.intern(rows_vals_[r * nc + c]));
        for (int c = 0; c < num_cols_; ++c)
            if (is_numeric_col(c) && !int_cols_[c].empty())
                n_ints[c].push_back(int_cols_[c][r]);
        if (has_pk) n_pk.insert(n_vals[n * nc + pkc], n);
        return (long long)n;
    };

    // ── A. Build under a SHARED lock ─────────────────────────────────────────
    {
        std::shared_lock lock(mtx);
        if (rows_meta_.empty()) return false;
        if (!force && !rebuild_due()) return false;
        // Writers are blocked while we hold the shared lock, so only this
        // thread touches these fields now; writers read them under exclusive.
        compacting_       = true;
        abort_compaction_ = false;
        dirty_rows_.clear();
        old_count = rows_meta_.size();

        const int64_t now = (int64_t)std::time(nullptr);
        n_meta.reserve(old_count);
        n_vals.reserve(old_count * nc);
        remap.assign(old_count, -1);
        if (has_pk) n_pk.reset(old_count - std::min(dead_rows_, old_count));
        for (size_t r = 0; r < old_count; ++r) {
            const RowMeta& m = rows_meta_[r];
            if (m.deleted || (m.expires_at != 0 && m.expires_at <= now)) continue;
            remap[r] = copy_row(r);
        }
    }

    // ── The gap: no table lock held — writers may run here ───────────────────
    if (rebuild_gap_hook_) rebuild_gap_hook_();

    // ── B. Patch + swap under an EXCLUSIVE lock (microseconds normally) ──────
    std::shared_lock<std::shared_mutex> g;
    if (gate) g = std::shared_lock<std::shared_mutex>(*gate);
    std::unique_lock lock(mtx);
    compacting_ = false;
    std::vector<size_t> dirty = std::exchange(dirty_rows_, {});
    if (abort_compaction_ || dirty.size() > old_count / 2 + 1024) {
        // TRUNCATE in the gap, or so many changes that patching ≈ rebuilding:
        // discard the copy and try again at the next sweep.
        abort_compaction_ = false;
        return false;
    }

    // Order matters for the PK index of the copy: first REMOVE every key that
    // went away in the gap (deleted rows, changed PKs), and only then INSERT
    // the new keys. Otherwise "DELETE k; INSERT k" in the gap would try to add
    // k while the dead row still holds it, leaving the new row unindexed.

    // (a) Rows UPDATEd / DELETEd in the gap (old numbering, via remap):
    //     apply the changes, remove stale keys, remember keys to re-insert.
    size_t dead = 0;
    std::vector<size_t> rekey;                        // copy rows needing a PK insert
    for (size_t r : dirty) {
        if (r >= old_count) continue;                 // appended: copied in (c)
        const long long n = remap[r];
        if (n < 0) continue;                          // was already dead at build
        RowMeta& nm = n_meta[(size_t)n];
        if (nm.deleted) continue;                     // patched already (dup entry)
        if (rows_meta_[r].deleted) {                  // DELETE in the gap
            nm.deleted = true;
            ++dead;
            if (has_pk) n_pk.remove(n_vals[(size_t)n * nc + pkc]);
            continue;
        }
        for (size_t c = 0; c < nc; ++c) {             // UPDATE in the gap
            std::string_view& nv  = n_vals[(size_t)n * nc + c];
            const std::string_view cur = rows_vals_[r * nc + c];
            if (nv == cur) continue;
            const bool is_pk = has_pk && c == pkc;
            if (is_pk) { n_pk.remove(nv); rekey.push_back((size_t)n); }  // before its bytes go
            n_arena.release(nv);
            nv = n_arena.intern(cur);
        }
        for (int c = 0; c < num_cols_; ++c)
            if (is_numeric_col(c) && !int_cols_[c].empty())
                n_ints[c][(size_t)n] = int_cols_[c][r];
    }
    // (b) Insert the changed PKs (all stale keys are gone now).
    for (size_t n : rekey)
        if (!n_meta[n].deleted) n_pk.insert(n_vals[n * nc + pkc], n);

    // (c) Rows INSERTed in the gap were appended: copy them as they are now.
    for (size_t r = old_count; r < rows_meta_.size(); ++r)
        if (!rows_meta_[r].deleted) copy_row(r);

    // (c) Swap. The old structures move into the n_* locals.
    {
        std::lock_guard rg(release_mtx_);             // vs a concurrent madvise
        std::swap(arena_, n_arena);
        ++arena_gen_;                                 // old page lists are void
    }
    std::swap(rows_meta_, n_meta);
    std::swap(rows_vals_, n_vals);
    std::swap(int_cols_,  n_ints);
    std::swap(pk_index_,  n_pk);
    dead_rows_ = dead;
    expiry_buckets_.clear();
    for (const auto& m : rows_meta_)
        if (!m.deleted && m.expires_at != 0) ++expiry_buckets_[m.expires_at / 60];
    lock.unlock();
    g = {};
    return true;   // old arena + arrays (now in n_*) freed here, unlocked
}
