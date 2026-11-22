#include "storage/database.h"
#include "utils/string_utils.h"
#include <mutex>

std::string Database::create_table(TableSchema schema, bool if_not_exists) {
    std::unique_lock lock(registry_mtx_);
    std::string name = strutil::to_upper(schema.table_name);
    schema.table_name = name;

    if (tables_.count(name)) {
        if (if_not_exists) return ""; // silently succeed
        return "table already exists: " + name;
    }
    tables_[name] = std::make_shared<Table>(std::move(schema));
    return "";
}

std::shared_ptr<Table> Database::get_table(const std::string& name) const {
    std::shared_lock lock(registry_mtx_);
    auto it = tables_.find(strutil::to_upper(name));
    return (it == tables_.end()) ? nullptr : it->second;
}

bool Database::table_exists(const std::string& name) const {
    std::shared_lock lock(registry_mtx_);
    return tables_.count(strutil::to_upper(name)) > 0;
}

std::vector<std::string> Database::list_tables() const {
    std::shared_lock lock(registry_mtx_);
    std::vector<std::string> names;
    names.reserve(tables_.size());
    for (const auto& [name, _] : tables_)
        names.push_back(name);
    std::sort(names.begin(), names.end());
    return names;
}

std::string Database::drop_table(const std::string& name, bool if_exists) {
    std::unique_lock lock(registry_mtx_);
    std::string upper = strutil::to_upper(name);
    auto it = tables_.find(upper);
    if (it == tables_.end()) {
        if (if_exists) return "";
        return "no such table: " + upper;
    }
    tables_.erase(it);
    return "";
}

size_t Database::compact_all() {
    // Take the table list, then release the registry lock so CREATE/DROP are
    // not blocked for the whole sweep (shared_ptr keeps a dropped table alive
    // until we are done with it).
    std::vector<std::shared_ptr<Table>> tbls;
    {
        std::shared_lock lock(registry_mtx_);
        tbls.reserve(tables_.size());
        for (auto& [name, tbl] : tables_) tbls.push_back(tbl);
    }
    size_t touched = 0;
    for (auto& tbl : tbls) {
        // The write gate is held (shared) only around each table's exclusive
        // phases, so a periodic snapshot never waits for a whole sweep.
        Table::SweepResult r = tbl->sweep(&write_gate_);
        if (r.rows_compacted || r.bytes_released || r.arena_rebuilt) ++touched;
    }
    return touched;
}
