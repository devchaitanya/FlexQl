#pragma once
#include <string>
#include <string_view>
#include <vector>
#include <cstdint>

// ── Column types ────────────────────────────────────────────────────────────
enum class ColumnType { INT, DECIMAL, VARCHAR, TEXT, DATETIME };

// ── Column definition (from CREATE TABLE) ───────────────────────────────────
struct ColumnDef {
    std::string name;
    ColumnType  type        = ColumnType::TEXT;
    bool        primary_key = false;
    bool        not_null    = false;
};

// ── Table schema ────────────────────────────────────────────────────────────
struct TableSchema {
    std::string             table_name;
    std::vector<ColumnDef>  columns;
    int                     pk_col_index = -1; // -1 = no primary key

    int col_index(const std::string& name) const {
        for (int i = 0; i < (int)columns.size(); ++i)
            if (columns[i].name == name) return i;
        return -1;
    }
};

// ── Query result ─────────────────────────────────────────────────────────────
struct QueryResult {
    std::vector<std::string>              column_names;
    std::vector<std::vector<std::string>> rows;
    // Compact flat storage: all cell values concatenated in flat_data,
    // with boundary offsets in flat_offsets (num_cells + 1 entries).
    std::string              flat_data;
    std::vector<uint32_t>    flat_offsets;   // size = num_rows*ncols + 1
    std::string                           error;
    bool                                  ok = true;
    int64_t                               elapsed_us = 0; // set by server after execution

    // Number of logical rows (works for both compact and nested storage)
    size_t num_rows() const {
        if (!flat_offsets.empty()) {
            size_t nc = column_names.size();
            return nc > 0 ? (flat_offsets.size() - 1) / nc : 0;
        }
        return rows.size();
    }

    // Access cell (row, col). Works for both layouts.
    std::string_view cell(size_t r, size_t c) const {
        if (!flat_offsets.empty()) {
            size_t idx = r * column_names.size() + c;
            uint32_t off = flat_offsets[idx];
            uint32_t len = flat_offsets[idx + 1] - off;
            return std::string_view(flat_data.data() + off, len);
        }
        return rows[r][c];
    }

    static QueryResult err(std::string msg) {
        QueryResult r;
        r.ok    = false;
        r.error = std::move(msg);
        return r;
    }
    static QueryResult success() {
        QueryResult r;
        r.ok = true;
        return r;
    }
};
