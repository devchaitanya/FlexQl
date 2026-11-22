#pragma once
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <string_view>

// ── AggState ──────────────────────────────────────────────────────────────────
// Running state of ONE aggregate argument (a column) over a stream of values.
// Feed values one by one with add_int()/add_text(), then ask result("SUM") etc.
//
// One implementation is shared by both aggregate paths, so they follow the
// same SQL rules:
//   - whole-table aggregates  SELECT COUNT(*), SUM(x) FROM t   (Table::aggregate)
//   - per-group aggregates    ... GROUP BY dept                (exec_group_by)
//
// SQL rules implemented here:
//   1. NULL (an empty value) is ignored by every aggregate except COUNT(*).
//      COUNT(col) counts only non-NULL values.
//   2. SUM of integers is exact (int64). It switches to double on the first
//      decimal value, or if the int64 sum would overflow.
//   3. MIN/MAX compare numerically for INT/DECIMAL columns, as text otherwise.
//   4. Over zero values, SUM/AVG/MIN/MAX are NULL (returned as ""), COUNT is 0.
struct AggState {
    bool        numeric = true;   // column type is INT/DECIMAL (decided by the schema)
    int64_t     count   = 0;      // number of non-NULL values seen

    // SUM: exact int64 while every value is an int and nothing overflowed.
    // dsum is only built at the moment that stops being true (first decimal or
    // overflow) — so the common all-int case never touches floating point.
    bool        all_int = true;
    int64_t     isum    = 0;
    double      dsum    = 0;

    // MIN/MAX kept per kind and merged in result():
    //   integers  → imin/imax compared as int64 (exact, no conversion per row)
    //   decimals  → dmin/dmax as double + their original text (e.g. "10.50")
    //   text cols → tmin/tmax compared as strings
    bool        has_int = false, has_dec = false;
    int64_t     imin = 0, imax = 0;
    double      dmin = 0, dmax = 0;
    std::string dmin_s, dmax_s, tmin, tmax;

    explicit AggState(bool is_numeric = true) : numeric(is_numeric) {}

    // Fast path: a value already parsed as int64 (from Table::int_cols_).
    // Hot loop for SUM/MIN/MAX over INT columns: one add + overflow flag, two compares.
    void add_int(int64_t v) {
        if (all_int) {
            // __builtin_add_overflow returns true if isum + v does not fit in int64
            int64_t next;
            if (__builtin_add_overflow(isum, v, &next)) to_double_sum((double)v);
            else                                        isum = next;
        } else {
            dsum += (double)v;
        }
        if (!has_int || v < imin) imin = v;
        if (!has_int || v > imax) imax = v;
        has_int = true;
        ++count;
    }

    // General path: a value as stored text ("42", "10.5", "alice", "" = NULL).
    void add_text(std::string_view v) {
        if (v.empty()) return;                          // rule 1: NULL is ignored
        if (!numeric) {                                 // rule 3: text column
            if (count == 0 || v < tmin) tmin.assign(v);
            if (count == 0 || v > tmax) tmax.assign(v);
            ++count;
            return;
        }
        int64_t iv;
        if (exact_int(v, iv)) { add_int(iv); return; }  // "42" → exact int path
        const double d = std::strtod(std::string(v).c_str(), nullptr);
        if (all_int) to_double_sum(d);                  // rule 2: a decimal appeared
        else         dsum += d;
        if (!has_dec || d < dmin) { dmin = d; dmin_s.assign(v); }
        if (!has_dec || d > dmax) { dmax = d; dmax_s.assign(v); }
        has_dec = true;
        ++count;
    }

    // Final value of `func` over everything added. `rows` = number of matching
    // rows, which is what COUNT(*) returns (NULLs included).
    std::string result(const std::string& func, bool star, int64_t rows) const {
        if (func == "COUNT") return std::to_string(star ? rows : count);
        if (count == 0)      return "";                 // rule 4: NULL
        if (func == "MIN")   return !numeric ? tmin : pick(true);
        if (func == "MAX")   return !numeric ? tmax : pick(false);
        if (func == "SUM")   return all_int ? std::to_string(isum) : fmt(dsum);
        if (func == "AVG")   return fmt(all_int ? (double)isum / (double)count
                                                : dsum / (double)count);
        return "";
    }

private:
    // Leave exact-int mode: from now on SUM is a double that starts from the
    // integer total so far, plus the value that forced the switch.
    void to_double_sum(double first) {
        dsum    = (double)isum + first;
        all_int = false;
    }

    // MIN/MAX of a numeric column: merge the integer and decimal candidates.
    std::string pick(bool want_min) const {
        if (!has_dec) return std::to_string(want_min ? imin : imax);
        if (!has_int) return want_min ? dmin_s : dmax_s;
        const double i = (double)(want_min ? imin : imax);
        const double d = want_min ? dmin : dmax;
        const bool int_wins = want_min ? (i <= d) : (i >= d);
        return int_wins ? std::to_string(want_min ? imin : imax) : (want_min ? dmin_s : dmax_s);
    }

public:
    // 15 significant digits, no trailing zeros: 30 → "30", 3.75 → "3.75"
    // (std::to_string(double) would give "30.000000").
    static std::string fmt(double d) {
        char buf[32];
        std::snprintf(buf, sizeof buf, "%.15g", d);
        return buf;
    }

    // Exact integer literal (optional sign, ≤ 18 digits so it cannot overflow).
    static bool exact_int(std::string_view s, int64_t& out) {
        if (s.empty()) return false;
        const bool neg = s[0] == '-';
        size_t i = (neg || s[0] == '+') ? 1 : 0;
        if (i >= s.size() || s.size() - i > 18) return false;
        int64_t v = 0;
        for (; i < s.size(); ++i) {
            if (s[i] < '0' || s[i] > '9') return false;
            v = v * 10 + (s[i] - '0');
        }
        out = neg ? -v : v;
        return true;
    }
};
