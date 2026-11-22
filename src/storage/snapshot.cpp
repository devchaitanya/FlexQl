#include "storage/snapshot.h"
#include "storage/database.h"
#include "storage/table.h"
#include <filesystem>
#include <fstream>
#include <vector>
#include <string>
#include <cstdint>
#include <regex>
#include <iostream>
#include <algorithm>
#include <cstdio>      // std::rename
#include <fcntl.h>     // ::open
#include <unistd.h>    // ::fsync, ::close

// fsync a file or directory by path, so its contents (or, for a directory,
// its entries — e.g. a rename) survive power loss. Returns false on failure.
static bool fsync_path(const std::string& p, bool is_dir) {
    int fd = ::open(p.c_str(), is_dir ? (O_RDONLY | O_DIRECTORY) : O_RDONLY);
    if (fd < 0) return false;
    bool ok = (::fsync(fd) == 0);
    ::close(fd);
    return ok;
}

// ── Low-level binary helpers ──────────────────────────────────────────────────

static bool write_u8(std::ofstream& f, uint8_t v) {
    return !!f.write(reinterpret_cast<const char*>(&v), 1);
}
static bool write_u32(std::ofstream& f, uint32_t v) {
    uint8_t b[4] = {uint8_t(v), uint8_t(v>>8), uint8_t(v>>16), uint8_t(v>>24)};
    return !!f.write(reinterpret_cast<const char*>(b), 4);
}
static bool write_u64(std::ofstream& f, uint64_t v) {
    uint8_t b[8];
    for (int i = 0; i < 8; ++i) b[i] = uint8_t(v >> (i*8));
    return !!f.write(reinterpret_cast<const char*>(b), 8);
}
static bool write_str(std::ofstream& f, const std::string& s) {
    if (!write_u32(f, (uint32_t)s.size())) return false;
    if (!s.empty()) f.write(s.data(), (std::streamsize)s.size());
    return !!f;
}

static bool read_u8(std::ifstream& f, uint8_t& v) {
    return !!f.read(reinterpret_cast<char*>(&v), 1);
}
static bool read_u32(std::ifstream& f, uint32_t& v) {
    uint8_t b[4];
    if (!f.read(reinterpret_cast<char*>(b), 4)) return false;
    v = uint32_t(b[0]) | (uint32_t(b[1])<<8) | (uint32_t(b[2])<<16) | (uint32_t(b[3])<<24);
    return true;
}
static bool read_u64(std::ifstream& f, uint64_t& v) {
    uint8_t b[8];
    if (!f.read(reinterpret_cast<char*>(b), 8)) return false;
    v = 0;
    for (int i = 0; i < 8; ++i) v |= (uint64_t(b[i]) << (i*8));
    return true;
}
static bool read_str(std::ifstream& f, std::string& s) {
    uint32_t len;
    if (!read_u32(f, len)) return false;
    s.resize(len);
    if (len > 0 && !f.read(s.data(), len)) return false;
    return true;
}

// Column type encoding (1 byte)
static uint8_t encode_type(ColumnType t) {
    switch (t) {
        case ColumnType::INT:      return 0;
        case ColumnType::DECIMAL:  return 1;
        case ColumnType::VARCHAR:  return 2;
        case ColumnType::TEXT:     return 3;
        case ColumnType::DATETIME: return 4;
        default:                   return 3;
    }
}
static ColumnType decode_type(uint8_t v) {
    switch (v) {
        case 0: return ColumnType::INT;
        case 1: return ColumnType::DECIMAL;
        case 2: return ColumnType::VARCHAR;
        case 4: return ColumnType::DATETIME;
        default: return ColumnType::TEXT;
    }
}

// ── Snapshot::write ───────────────────────────────────────────────────────────
bool Snapshot::write(const std::string& path, uint64_t lsn) {
    try {
        std::filesystem::create_directories(
            std::filesystem::path(path).parent_path());
    } catch (...) {}

    // Crash-safe protocol: write to <path>.tmp, fsync it, atomically rename it
    // over <path>, then fsync the directory. At every instant either the old
    // complete snapshot or the new complete snapshot exists — never a torn one.
    const std::string tmp_path = path + ".tmp";
    // 1 MB stream buffer → large write() syscalls instead of many small ones.
    // (Must be installed before open; must outlive the stream.)
    std::vector<char> iobuf(1 << 20);
    std::ofstream f;
    f.rdbuf()->pubsetbuf(iobuf.data(), (std::streamsize)iobuf.size());
    f.open(tmp_path, std::ios::binary | std::ios::trunc);
    if (!f) return false;

    Database& db = Database::instance();
    std::vector<std::string> table_names = db.list_tables();

    // Header
    if (!write_u32(f, MAGIC))              return false;
    if (!write_u32(f, VERSION))            return false;
    if (!write_u64(f, lsn))               return false;
    if (!write_u32(f, (uint32_t)table_names.size())) return false;

    for (const auto& tname : table_names) {
        auto tbl = db.get_table(tname);
        if (!tbl) continue;

        // Lock table for reading during serialisation
        std::shared_lock<std::shared_mutex> lock(tbl->mtx);
        const TableSchema& schema = tbl->schema();

        if (!write_str(f, schema.table_name)) return false;
        if (!write_u32(f, (uint32_t)(int32_t)schema.pk_col_index)) return false;
        if (!write_u32(f, (uint32_t)schema.columns.size())) return false;

        for (const auto& col : schema.columns) {
            if (!write_str(f, col.name)) return false;
            uint8_t type_byte = encode_type(col.type);
            uint8_t flags = (col.primary_key ? 1 : 0) | (col.not_null ? 2 : 0);
            if (!write_u8(f, type_byte)) return false;
            if (!write_u8(f, flags))     return false;
        }

        lock.unlock(); // write_rows() takes its own shared lock

        // Stream rows directly from table memory (no full-table copy). Same
        // on-disk format as before: [count:8] then [expires:8][len:4][bytes]...
        if (!tbl->write_rows(f)) return false;
    }

    f.flush();
    if (!f) return false;
    f.close();                                   // hand all bytes to the kernel
    if (f.fail()) return false;

    if (!fsync_path(tmp_path, /*is_dir=*/false)) return false;   // bytes on disk
    if (std::rename(tmp_path.c_str(), path.c_str()) != 0) return false; // atomic swap

    std::string dir = std::filesystem::path(path).parent_path().string();
    if (dir.empty()) dir = ".";
    return fsync_path(dir, /*is_dir=*/true);     // make the rename itself durable
}

// ── Snapshot::load ────────────────────────────────────────────────────────────
uint64_t Snapshot::load(const std::string& path) {
    std::ifstream f(path, std::ios::binary);
    if (!f) return 0;

    uint32_t magic, version;
    uint64_t lsn;
    uint32_t table_count;

    if (!read_u32(f, magic))   return 0;
    if (magic != MAGIC)        return 0;
    if (!read_u32(f, version)) return 0;
    if (version != VERSION)    return 0;
    if (!read_u64(f, lsn))     return 0;
    if (!read_u32(f, table_count)) return 0;

    Database& db = Database::instance();

    for (uint32_t t = 0; t < table_count; ++t) {
        std::string tname;
        uint32_t pk_col_u32, col_count;
        if (!read_str(f, tname)) return 0;
        if (!read_u32(f, pk_col_u32)) return 0;
        if (!read_u32(f, col_count))  return 0;

        TableSchema schema;
        schema.table_name  = tname;
        schema.pk_col_index = (int)(int32_t)pk_col_u32;

        for (uint32_t c = 0; c < col_count; ++c) {
            ColumnDef col;
            uint8_t type_byte, flags;
            if (!read_str(f, col.name))   return 0;
            if (!read_u8(f, type_byte))   return 0;
            if (!read_u8(f, flags))       return 0;
            col.type        = decode_type(type_byte);
            col.primary_key = (flags & 1) != 0;
            col.not_null    = (flags & 2) != 0;
            schema.columns.push_back(std::move(col));
        }

        uint64_t row_count;
        if (!read_u64(f, row_count)) return 0;

        // Create table (ignore if already exists — recovery may re-create)
        db.drop_table(tname, /*if_exists=*/true);
        db.create_table(schema, /*if_not_exists=*/false);
        auto tbl = db.get_table(tname);
        if (!tbl) return 0;

        for (uint64_t r = 0; r < row_count; ++r) {
            uint64_t expires_raw;
            if (!read_u64(f, expires_raw)) return 0;
            int64_t expires_at = (int64_t)expires_raw;

            std::vector<std::string> vals;
            vals.resize(col_count);
            for (uint32_t c = 0; c < col_count; ++c) {
                if (!read_str(f, vals[c])) return 0;
            }
            tbl->insert(std::move(vals), expires_at);
        }
    }

    return lsn;
}

// ── Snapshot::find_latest ─────────────────────────────────────────────────────
std::vector<SnapshotInfo> Snapshot::list_all(const std::string& dir) {
    std::vector<SnapshotInfo> out;
    try {
        if (!std::filesystem::exists(dir)) return out;
        std::regex re("snap_(\\d+)\\.bin");      // ".bin.tmp" files don't match
        for (const auto& entry : std::filesystem::directory_iterator(dir)) {
            std::string fname = entry.path().filename().string();
            std::smatch m;
            if (std::regex_match(fname, m, re))
                out.push_back({std::stoull(m[1].str()), entry.path().string()});
        }
    } catch (...) {}
    std::sort(out.begin(), out.end(),
              [](const SnapshotInfo& a, const SnapshotInfo& b) { return a.lsn > b.lsn; });
    return out;
}

SnapshotInfo Snapshot::find_latest(const std::string& dir) {
    auto all = list_all(dir);
    return all.empty() ? SnapshotInfo{} : all.front();
}

void Snapshot::prune(const std::string& dir, size_t keep) {
    std::error_code ec;
    auto all = list_all(dir);
    for (size_t i = keep; i < all.size(); ++i)
        std::filesystem::remove(all[i].path, ec);

    // Leftovers from writes interrupted by a crash
    try {
        for (const auto& entry : std::filesystem::directory_iterator(dir)) {
            const std::string fname = entry.path().filename().string();
            if (fname.size() > 4 && fname.compare(fname.size() - 4, 4, ".tmp") == 0)
                std::filesystem::remove(entry.path(), ec);
        }
    } catch (...) {}
}
