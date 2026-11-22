#pragma once
#include "storage/table.h"
#include <unordered_map>
#include <memory>
#include <shared_mutex>
#include <string>

// Singleton holding all tables.
class Database {
public:
    static Database& instance() {
        static Database db;
        return db;
    }

    // Returns "" on success or error message.
    std::string create_table(TableSchema schema, bool if_not_exists);

    // Returns nullptr if not found.
    std::shared_ptr<Table> get_table(const std::string& name) const;

    bool table_exists(const std::string& name) const;

    // Returns sorted list of all table names.
    std::vector<std::string> list_tables() const;

    // Returns "" on success or error string.
    std::string drop_table(const std::string& name, bool if_exists);

    // Called by TtlManager: tiered sweep of every table (see Table::sweep).
    // Takes the write gate itself, only around exclusive phases. Returns the
    // number of tables where any tier did work.
    size_t compact_all();

    // Global "write gate". Every mutation holds it in SHARED mode for the whole
    // "WAL append + apply" step (writers don't block each other — each table's
    // own mtx still serialises writes to the same table). A periodic snapshot
    // takes it EXCLUSIVELY for the instant of fork(), guaranteeing no write is
    // half-applied and that the snapshot's LSN matches its contents exactly.
    std::shared_mutex& write_gate() { return write_gate_; }

    Database(const Database&)            = delete;
    Database& operator=(const Database&) = delete;

private:
    Database() = default;

    mutable std::shared_mutex              registry_mtx_;
    std::shared_mutex                      write_gate_;
    std::unordered_map<std::string, std::shared_ptr<Table>> tables_;
};
