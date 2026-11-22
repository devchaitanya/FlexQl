#pragma once
#include "common/types.h"
#include "common/aggregate.h"
#include "index/pk_index.h"
#include "storage/arena.h"
#include <shared_mutex>
#include <vector>
#include <map>
#include <mutex>
#include <functional>
#include <string>
#include <string_view>
#include <memory>
#include <ctime>
#include <cstdint>
#include <iosfwd>

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

    // Whole-table aggregates in ONE pass: every row matching `where` feeds the
    // value of each column in `cols` into its own AggState (out[k] ↔ cols[k]).
    // One shared lock for the whole pass → all aggregates of a query see the
    // same snapshot (COUNT and SUM can't disagree). O(#cols) memory: no result
    // rows are built. INT columns use the int64 copy (no parsing).
    // Returns the number of matching rows, which is COUNT(*).
    int64_t aggregate(const Predicate* where, const std::vector<int>& cols,
                      std::vector<AggState>& out) const;

    // GROUP BY straight from the table, in ONE pass, without materializing
    // rows: hash(group values) → that group's row count + one AggState per
    // aggregated column. Groups are returned in first-seen order.
    //   keys[g]      text of the group columns (from the group's first row)
    //   rows[g]      rows in the group            (= COUNT(*))
    //   states[g][k] aggregate state for agg_cols[k]
    struct GroupResult {
        std::vector<std::vector<std::string>> keys;
        std::vector<int64_t>                  rows;
        std::vector<std::vector<AggState>>    states;
    };
    GroupResult group_aggregate(const Predicate* where, const std::vector<int>& group_cols,
                                const std::vector<int>& agg_cols) const;

    // Raw access for JOIN (caller holds shared_lock externally).
    std::vector<std::pair<std::vector<std::string>, size_t>> live_rows() const;

    // Snapshot: returns all live rows as {values, expires_at} pairs.
    // Used by Snapshot::write() to serialise table state.
    std::vector<std::pair<std::vector<std::string>, int64_t>> snapshot_rows() const;

    // Snapshot: stream [live_row_count:8] then each live row
    // ([expires_at:8] + per column [len:4][bytes]) straight from table memory
    // to `out` — no intermediate copy of the table. Returns false on I/O error.
    bool write_rows(std::ostream& out) const;

    const TableSchema& schema() const { return schema_; }

    // Schema enforcement for one value of column `col`: NOT NULL (an empty
    // value is NULL; the PK column is implicitly NOT NULL) and type checks.
    // Returns "" if valid, else an error message. Needs no lock (schema is
    // immutable after CREATE TABLE).
    std::string validate_value(int col, std::string_view v) const;

    // ── Tiered compaction (run by the background sweeper) ─────────────────────
    //   Phase 0  peek     shared lock      read counters; most sweeps stop here
    //   Phase 1  tier 1   EXCLUSIVE, short drop dead/expired rows from the row
    //                                      arrays (16-byte view moves, no string
    //                                      copies); their bytes are released
    //   Phase 2  tier 2   NO table lock    madvise() arena pages whose live-byte
    //                                      count hit 0 — RAM back to the OS,
    //                                      nothing moved
    //   Phase 3  tier 3   shared → EXCL µs only if dead bytes are still scattered
    //                                      (waste >= limits): rebuild the arena
    //                                      aside under a SHARED lock, patch the
    //                                      writes made in the gap, swap pointers
    // `gate` (the Database write gate) is held shared only around the exclusive
    // phases, so a periodic snapshot never forks mid-mutation.
    struct SweepResult {
        bool   rows_compacted = false;   // tier 1 ran
        size_t bytes_released = 0;       // tier 2
        bool   arena_rebuilt  = false;   // tier 3 ran
    };
    SweepResult sweep(std::shared_mutex* gate = nullptr, bool force = false);

    // Compatibility wrapper: runs a sweep, returns whether rows were compacted.
    bool compact(bool force = false) { return sweep(nullptr, force).rows_compacted; }

    struct CompactionLimits {
        size_t min_garbage_rows = 1000;              // tier 1: at least this many…
        size_t row_ratio        = 5;                 // …and garbage*5 >= rows (20%)
        size_t min_waste_bytes  = 64ull << 20;       // tier 3: >= 64 MB wasted…
        size_t waste_ratio      = 2;                 // …and waste*2 >= resident (50%)
    };
    void set_compaction_limits(const CompactionLimits& l) {
        std::unique_lock lock(mtx);
        limits_ = l;
    }

    struct ArenaStats { size_t live = 0, reserved = 0, released = 0, resident = 0; };
    ArenaStats arena_stats() const;

    // Test seam: called by tier 3 between the shared-lock build and the
    // exclusive swap (no table lock held) to inject writes into "the gap".
    void set_rebuild_gap_hook(std::function<void()> f) { rebuild_gap_hook_ = std::move(f); }

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

    // int_exact_[c] stays true while EVERY value stored in numeric column c is an
    // exact integer literal (no fraction, not empty/NULL). Only then may the int64
    // fast paths (SIMD filters, int sort, int aggregates) be used; otherwise they
    // would truncate decimals (10.7 → 10) or treat NULL as 0.
    std::vector<char> int_exact_;

    // ── Compaction bookkeeping (guarded by mtx) ───────────────────────────────
    // Lets compact() decide in O(#buckets) whether a rebuild is worthwhile,
    // instead of rebuilding the whole table on every sweep.
    //   dead_rows_       — rows tombstoned by DELETE since the last compaction.
    //   expiry_buckets_  — minute (expires_at / 60) → number of non-deleted
    //                      rows expiring in that minute. Expiry is not an event
    //                      (no code runs when a row expires), so we count rows
    //                      per minute at insert time and sum the past buckets.
    size_t                    dead_rows_ = 0;
    std::map<int64_t, size_t> expiry_buckets_;

    void note_expiry(int64_t expires_at, size_t n) {
        if (expires_at != 0) expiry_buckets_[expires_at / 60] += n;
    }
    void forget_expiry(int64_t expires_at) {
        if (expires_at == 0) return;
        auto it = expiry_buckets_.find(expires_at / 60);
        if (it != expiry_buckets_.end() && --it->second == 0) expiry_buckets_.erase(it);
    }
    // Mark a live row deleted and keep the counters in step. A row with a TTL
    // leaves its expiry bucket so it is not counted twice (dead + expired).
    // The row's bytes stay live until tier 1 removes the row from the arrays.
    void tombstone(size_t row) {
        RowMeta& m = rows_meta_[row];
        m.deleted = true;
        ++dead_rows_;
        forget_expiry(m.expires_at);
        note_dirty(row);
    }

    // ── Tier-3 rebuild state (guarded by mtx) ─────────────────────────────────
    // While a rebuild is building its copy, writers record the OLD row numbers
    // they touch; the swap re-applies those rows to the copy ("patching").
    CompactionLimits    limits_;
    bool                compacting_         = false;
    bool                abort_compaction_   = false; // set by TRUNCATE
    std::vector<size_t> dirty_rows_;
    std::function<void()> rebuild_gap_hook_;
    void note_dirty(size_t row) { if (compacting_) dirty_rows_.push_back(row); }

    // Serialises sweeps; and guards madvise() against TRUNCATE unmapping the
    // arena underneath it (arena_gen_ changes whenever the arena is replaced).
    std::mutex sweep_mtx_;
    std::mutex release_mtx_;
    uint64_t   arena_gen_ = 0;

    void   compact_rows_locked();              // tier 1 body (exclusive lock held)
    size_t release_dead_pages();               // tier 2
    bool   rebuild_arena(std::shared_mutex* gate, bool force);   // tier 3
    bool   rebuild_due() const;                // tier 3 trigger (lock held)
    // Rows in buckets whose whole minute is already past (a slight
    // undercount: rows expiring earlier in the current minute are ignored).
    size_t expired_estimate(int64_t now) const {
        size_t n = 0;
        for (const auto& [minute, cnt] : expiry_buckets_) {
            if (minute * 60 + 59 > now) break;   // map is sorted by minute
            n += cnt;
        }
        return n;
    }
    bool compaction_due() const {
        const size_t garbage = dead_rows_ + expired_estimate((int64_t)std::time(nullptr));
        return garbage >= limits_.min_garbage_rows &&
               garbage * limits_.row_ratio >= rows_meta_.size();
    }

    // ── Private helpers ───────────────────────────────────────────────────────
    // Parse an exact integer literal; false for empty, fractional, non-numeric,
    // or too-long (overflow-prone) input.
    static bool parse_int64_exact(std::string_view sv, int64_t& out) {
        if (sv.empty()) return false;
        bool neg = (sv[0] == '-');
        size_t start = (neg || sv[0] == '+') ? 1 : 0;
        if (start >= sv.size() || sv.size() - start > 18) return false;
        int64_t val = 0;
        for (size_t i = start; i < sv.size(); ++i) {
            char c = sv[i];
            if (c < '0' || c > '9') return false;
            val = val * 10 + (c - '0');
        }
        out = neg ? -val : val;
        return true;
    }

    // Store a numeric value into int_cols_[c][row] and clear int_exact_[c] if
    // the value is not an exact integer.
    void store_int(int c, size_t row, std::string_view sv) {
        int64_t v = 0;
        if (!parse_int64_exact(sv, v)) { int_exact_[c] = 0; v = parse_int64(sv); }
        int_cols_[c][row] = v;
    }
    void push_int(int c, std::string_view sv) {
        int64_t v = 0;
        if (!parse_int64_exact(sv, v)) { int_exact_[c] = 0; v = parse_int64(sv); }
        int_cols_[c].push_back(v);
    }

    // True if the int64 fast path is valid for column c.
    bool int_fast(int c) const {
        return c >= 0 && c < num_cols_ && is_numeric_col(c) &&
               !int_cols_[c].empty() && int_exact_[c];
    }
    bool is_expired(const RowMeta& m) const;

    // Validate a single value against its column type.  Returns "" on success,
    // or an error message on type mismatch.
    static std::string validate_type(std::string_view val, ColumnType type, const std::string& col_name);

    // Validate a full row (num_cols_ values) with validate_value.
    std::string validate_row(const std::string_view* row) const;

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

    // ── WHERE evaluation ──────────────────────────────────────────────────────
    // A Predicate holds strings (column name, operator, literals). Resolving
    // them per row (copy + split the column name, look it up by name, compare
    // the operator as text, re-parse the literals) costs ~50 ns/row. bind()
    // does all of that ONCE per query; eval() is then a column index, an
    // operator enum and pre-parsed int64 literals. Bind under the table lock
    // (it reads int_exact_).
    struct BoundPred {
        enum Op : uint8_t { EQ, NE, GT, GE, LT, LE, LIKE, IS_NULL, NOT_NULL,
                            BETWEEN, NOT_BETWEEN, IN, NOT_IN, OTHER };
        int                    kind    = 0;        // Predicate::Kind
        int                    col     = -1;       // -1 → never matches
        Op                     op      = OTHER;
        bool                   use_int = false;    // int64 fast path is exact
        int64_t                iv = 0, iv2 = 0;    // parsed literals
        std::vector<int64_t>   ivals;              // parsed IN list
        ColumnType             ct      = ColumnType::TEXT;
        const Predicate*       src     = nullptr;  // strings for the slow path
        std::vector<BoundPred> children;
    };
    BoundPred bind(const Predicate& pred) const;
    bool      eval(size_t row_idx, const BoundPred& b) const;
    bool      matches_pred(size_t row_idx, const Predicate& pred) const;   // bind+eval (one-off)
    bool compare_values(std::string_view row_val,
                        const std::string& op,
                        const std::string& target,
                        ColumnType type) const;

    std::vector<std::string> project(size_t row_idx,
                                     const std::vector<int>& col_indices) const;
};
