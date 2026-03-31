#pragma once
#include <string>
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
    std::string                           error;
    bool                                  ok = true;
    int64_t                               elapsed_us = 0; // set by server after execution

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
