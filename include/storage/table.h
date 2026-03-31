#pragma once
#include "common/types.h"
#include "index/pk_index.h"
#include "storage/arena.h"
#include <shared_mutex>
#include <vector>
#include <string>
#include <string_view>
#include <memory>
#include <ctime>
#include <cstdint>

// ── Legacy single-condition WHERE (kept for internal use inside Table) ─────────
struct WhereClause {
    std::string col;
    std::string op;    // "=", "!=", ">", ">=", "<", "<=", "LIKE"
    std::string val;
};

struct OrderByClause {
    std::string col;
    bool        desc = false;
};

// ── Forward-declare Predicate so Table methods can accept it ──────────────────
// Full definition is in parser/ast.h (included by callers after this header).
struct Predicate;

// Per-row metadata (replaces the old Row struct in storage).
struct RowMeta {
    int64_t expires_at = 0;   // Unix epoch seconds; 0 = never expires
    bool    deleted    = false;
};

class Table {
public:
    explicit Table(TableSchema schema);

    // ── Write operations ──────────────────────────────────────────────────────
    // Single-row insert (legacy path, used by non-batch executor fallback).
    std::string insert(std::vector<std::string> values, int64_t expires_at);

    // Batch insert: string_view rows pointing into the caller's SQL buffer.
    // The SQL buffer must outlive this call (guaranteed by handle_client scope).
    std::string insert_batch(std::vector<std::vector<std::string_view>> batch,
                             std::vector<int64_t> expires_vec);

    // Flat batch insert: all values in a single vector (n_rows × ncols layout).
    // Zero heap allocations compared to insert_batch — hot path for INSERT.
    std::string insert_flat(const std::string_view* flat, size_t n_rows, int ncols,
                            int64_t expires_at);

    // Returns number of rows deleted.  where=nullptr → delete all.
    int delete_rows(const Predicate* where);

    // Update matching rows; returns number of rows modified.
    int update_rows(const std::vector<std::pair<std::string,std::string>>& assignments,
                    const Predicate* where);

    // Remove all rows (fast path — no tombstoning).
    void truncate();

    // ── Read operations ───────────────────────────────────────────────────────
    QueryResult scan(const std::vector<std::string>& select_cols,
                     const Predicate* where,
                     const std::vector<OrderByClause>& order_by,
                     int limit = -1, int offset = 0) const;

    // O(1)-memory aggregate: counts matching rows without materializing them.
    size_t count_rows(const Predicate* where) const;

    // O(1)-memory aggregate: computes SUM/MIN/MAX in a single pass.
    struct AggResult {
        double      sum     = 0;
        double      min_val = 0;
        double      max_val = 0;
        std::string min_str;
        std::string max_str;
        int64_t     count   = 0;
    };
    AggResult compute_aggregate(int col_idx, const Predicate* where) const;

    // Raw access for JOIN (caller holds shared_lock externally).
    std::vector<std::pair<std::vector<std::string>, size_t>> live_rows() const;

    // Snapshot: returns all live rows as {values, expires_at} pairs.
    // Used by Snapshot::write() to serialise table state.
    std::vector<std::pair<std::vector<std::string>, int64_t>> snapshot_rows() const;

    const TableSchema& schema() const { return schema_; }

    // Background TTL sweeper — physically removes deleted/expired rows.
    void compact();

    mutable std::shared_mutex mtx; // public so Executor can lock for JOIN

private:
    TableSchema  schema_;
    int          num_cols_;   // schema_.columns.size(), cached for hot-path

    // Columnar row storage — eliminates per-row heap allocation.
    StringArena                   arena_;        // owns all string data
    std::vector<RowMeta>          rows_meta_;    // one entry per logical row
    std::vector<std::string_view> rows_vals_;    // rows_meta_.size() × num_cols_ entries
    PrimaryIndex                  pk_index_;     // key → row index (key lives in arena_)

    // Native int64 storage per column — eliminates stod() during scans.
    // int_cols_[col_idx] is populated only for INT/DECIMAL columns; empty for others.
    std::vector<std::vector<int64_t>> int_cols_;

    // ── Private helpers ───────────────────────────────────────────────────────
    bool is_expired(const RowMeta& m) const;

    // True if col_idx is a numeric type (INT or DECIMAL).
    bool is_numeric_col(int col_idx) const {
        return schema_.columns[col_idx].type == ColumnType::INT ||
               schema_.columns[col_idx].type == ColumnType::DECIMAL;
    }

    // Parse a string_view to int64; allocation-free. Returns 0 on failure.
    static int64_t parse_int64(std::string_view sv) {
        if (sv.empty()) return 0;
        bool neg = (sv[0] == '-');
        size_t start = (neg || sv[0] == '+') ? 1 : 0;
        int64_t val = 0;
        for (size_t i = start; i < sv.size(); ++i) {
            char c = sv[i];
            if (c < '0' || c > '9') break; // stop at '.' for decimals
            val = val * 10 + (c - '0');
        }
        return neg ? -val : val;
    }

    // Access helpers
    std::string_view val_at(size_t row_idx, int col_idx) const {
        return rows_vals_[row_idx * (size_t)num_cols_ + (size_t)col_idx];
    }
    std::string_view& val_ref(size_t row_idx, int col_idx) {
        return rows_vals_[row_idx * (size_t)num_cols_ + (size_t)col_idx];
    }

    bool matches_pred(size_t row_idx, const Predicate& pred) const;
    bool compare_values(std::string_view row_val,
                        const std::string& op,
                        const std::string& target,
                        ColumnType type) const;

    std::vector<std::string> project(size_t row_idx,
                                     const std::vector<int>& col_indices) const;
};
