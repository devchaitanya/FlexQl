#pragma once
#include <cstdint>
#include <string>
#include <string_view>
#include <mutex>
#include <fstream>
#include <atomic>
#include <vector>

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
    // Thread-safe; each call makes one writev() syscall.
    uint64_t append(std::string_view sql);

    // Force all buffered records to disk (fdatasync).
    void sync();

    // Truncate WAL file to 0 bytes (called after a snapshot is written).
    void truncate();

    uint64_t next_lsn() const { return next_lsn_; }

    // Ensure future LSNs are > min_lsn (call after loading a snapshot, whose
    // LSN may exceed anything left in a truncated WAL).
    void ensure_lsn_after(uint64_t min_lsn) {
        std::lock_guard<std::mutex> guard(mtx_);
        if (next_lsn_ <= min_lsn) next_lsn_ = min_lsn + 1;
    }
    uint64_t durable_lsn() const { return durable_lsn_; }

    bool is_open() const { return fd_ >= 0; }

    // True if the WAL was open but was LOST at runtime (rotate() could not
    // reopen any file). The server must then reject writes: accepting them
    // would acknowledge changes that are never logged.
    bool failed() const { return failed_.load(std::memory_order_relaxed); }

    // Bytes currently in the active WAL file (drives periodic snapshots).
    uint64_t bytes() const { return bytes_.load(std::memory_order_relaxed); }

    const std::string& path() const { return path_; }

    // Close the active WAL file, rename it to "<path>.<tag>.old" (a "segment"),
    // and start a fresh empty WAL at <path>. Records written afterwards go to
    // the new file. Returns the segment's path, or "" on failure.
    // Caller must ensure no append() is in flight (hold the write gate).
    std::string rotate(uint64_t tag);

    // Old segments "<path>.<n>.old", sorted by n ascending (oldest first).
    static std::vector<std::string> list_segments(const std::string& path);

private:
    int         fd_          = -1;
    uint64_t    next_lsn_    = 1;
    uint64_t    durable_lsn_ = 0;
    std::string path_;
    std::atomic<uint64_t> bytes_{0};
    std::atomic<bool>     failed_{false};

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
