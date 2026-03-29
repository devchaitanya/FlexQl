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

void Database::compact_all() {
    std::shared_lock lock(registry_mtx_);
    for (auto& [name, tbl] : tables_)
        tbl->compact();
}
