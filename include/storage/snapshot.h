#pragma once
#include <string>
#include <cstdint>

// ── Snapshot (checkpoint) for FlexQL ─────────────────────────────────────────
// A snapshot is a complete binary serialisation of all tables at a given LSN.
// It lets the server restart without replaying the full WAL from the beginning.
//
// Binary format (all little-endian):
//   [magic:4="FQSS"][version:4=1][lsn:8][table_count:4]
//   For each table:
//     [name_len:4][name:name_len]
//     [pk_col_index:4]
//     [col_count:4]
//     For each column:
//       [name_len:4][name:name_len][type:1][flags:1]
//     [live_row_count:8]
//     For each live row:
//       [expires_at:8]
//       For each column: [val_len:4][val:val_len]

struct SnapshotInfo {
    uint64_t lsn        = 0;    // WAL LSN at which the snapshot was taken
    std::string path;           // absolute path of the snapshot file
};

class Database;

class Snapshot {
public:
    static constexpr uint32_t MAGIC   = 0x53535146; // "FQSS" LE
    static constexpr uint32_t VERSION = 1;

    // Write a snapshot of the current database state.
    // path: file to write (created/overwritten).
    // lsn:  the WAL LSN at the time of the snapshot.
    // Returns true on success.
    static bool write(const std::string& path, uint64_t lsn);

    // Load a snapshot into the database (replaces all current tables).
    // Returns the LSN stored in the snapshot (callers replay WAL from lsn+1).
    // Returns 0 on failure or if path does not exist.
    static uint64_t load(const std::string& path);

    // Find the most recent snapshot in dir (returns empty SnapshotInfo if none).
    static SnapshotInfo find_latest(const std::string& dir = "data/snapshots");
};
