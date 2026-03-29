#pragma once
#include <cstdint>
#include <string>
#include <string_view>
#include <mutex>
#include <fstream>

// ── Write-Ahead Log ───────────────────────────────────────────────────────────
// Record format (all little-endian):
//   [magic:4][lsn:8][payload_len:4][crc32:4][sql:payload_len]
//
// Zero-copy write design:
//   Each append() builds a 20-byte header on the stack and calls writev()
//   with header + SQL payload — no intermediate copy into a buffer.
//   This keeps L3 cache clean for the arena/columnar storage hot path.
//   fdatasync is called only in sync() (shutdown/checkpoint).
//
// Thread-safety: WalWriter is fully thread-safe (internal mutex).
// WalReader is single-threaded (used only during startup recovery).

class WalWriter {
public:
    static constexpr uint32_t MAGIC = 0x464C5741; // "ALWF" LE

    WalWriter() = default;
    ~WalWriter();

    // Open (or create) the WAL file.  Returns true on success.
    bool open(const std::string& path = "data/wal/wal.log");

    // Append a SQL statement to the WAL.
    // Returns the LSN assigned to this record (monotonically increasing).
    // Thread-safe; uses group-commit buffering.
    uint64_t append(std::string_view sql);

    // Force all buffered records to disk (fdatasync).
    void sync();

    uint64_t next_lsn() const { return next_lsn_; }
    uint64_t durable_lsn() const { return durable_lsn_; }

    bool is_open() const { return fd_ >= 0; }

private:
    int      fd_          = -1;
    uint64_t next_lsn_    = 1;
    uint64_t durable_lsn_ = 0;

    mutable std::mutex mtx_;

    static uint32_t crc32c(const void* data, size_t len) noexcept;
};

// ── WAL Reader (used only at startup for recovery) ────────────────────────────
class WalReader {
public:
    WalReader() = default;

    bool open(const std::string& path = "data/wal/wal.log");

    // Read the next record.  Returns false at EOF or on unrecoverable corruption.
    // On soft corruption (bad CRC), skips record and continues.
    bool read_next(uint64_t& lsn_out, std::string& sql_out);

    bool is_open() const { return file_.is_open(); }

private:
    std::ifstream file_;
    static uint32_t crc32c(const void* data, size_t len) noexcept;
};
