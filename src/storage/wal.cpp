#include "storage/wal.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/uio.h>    // writev
#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <filesystem>
#include <algorithm>
#include <cstdio>
#include <iostream>
#include <nmmintrin.h>  // SSE4.2 _mm_crc32_u64/_mm_crc32_u8

// ── CRC-32C (Castagnoli) — hardware SSE4.2 implementation ────────────────────
// Uses the crc32 instruction (available on all x86-64 CPUs since ~2009).
// Processes 8 bytes per iteration for ~10 GB/s throughput.
// Falls back to 1-byte processing for the trailing <8 bytes.

static uint32_t compute_crc32c(const void* data, size_t len) noexcept {
    const uint8_t* p   = static_cast<const uint8_t*>(data);
    uint64_t       crc = 0xFFFFFFFFu;

    const uint8_t* end8 = p + (len & ~(size_t)7);
    while (p < end8) {
        uint64_t v;
        __builtin_memcpy(&v, p, 8);
        crc = _mm_crc32_u64(crc, v);
        p += 8;
    }
    size_t tail = len & 7;
    while (tail--) crc = _mm_crc32_u8((uint32_t)crc, *p++);

    return static_cast<uint32_t>(crc ^ 0xFFFFFFFFu);
}

uint32_t WalWriter::crc32c(const void* data, size_t len) noexcept {
    return compute_crc32c(data, len);
}

uint32_t WalReader::crc32c(const void* data, size_t len) noexcept {
    return compute_crc32c(data, len);
}

// ── WalWriter ─────────────────────────────────────────────────────────────────

WalWriter::~WalWriter() {
    if (fd_ >= 0) {
        sync();
        ::close(fd_);
    }
}

bool WalWriter::open(const std::string& path) {
    try {
        std::filesystem::create_directories(
            std::filesystem::path(path).parent_path());
    } catch (...) {}

    fd_ = ::open(path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (fd_ < 0) return false;
    path_ = path;
    off_t sz = ::lseek(fd_, 0, SEEK_END);
    bytes_ = sz > 0 ? (uint64_t)sz : 0;

    // Derive next_lsn_ by scanning existing records so we don't reuse LSNs
    // after a restart.
    WalReader rd;
    if (rd.open(path)) {
        uint64_t lsn; std::string sql;
        while (rd.read_next(lsn, sql))
            if (lsn >= next_lsn_) next_lsn_ = lsn + 1;
    }

    return true;
}

uint64_t WalWriter::append(std::string_view sql) {
    // Header layout: magic(4) + lsn(8) + payload_len(4) + crc32c(4) = 20 bytes
    const uint32_t payload_len = static_cast<uint32_t>(sql.size());
    const uint32_t checksum    = crc32c(sql.data(), sql.size());

    std::lock_guard<std::mutex> guard(mtx_);
    const uint64_t lsn = next_lsn_++;

    // Build 20-byte header on stack (no heap allocation)
    uint8_t hdr[20];
    auto le32 = [](uint8_t* p, uint32_t v) {
        p[0]=uint8_t(v); p[1]=uint8_t(v>>8); p[2]=uint8_t(v>>16); p[3]=uint8_t(v>>24);
    };
    auto le64 = [](uint8_t* p, uint64_t v) {
        for (int i=0; i<8; ++i) p[i]=uint8_t(v>>(i*8));
    };
    le32(hdr,    MAGIC);
    le64(hdr+4,  lsn);
    le32(hdr+12, payload_len);
    le32(hdr+16, checksum);

    // Zero-copy: write header + SQL payload in one atomic writev() call.
    // O_APPEND guarantees atomic positioning; mutex ensures single writer.
    if (fd_ >= 0) {
        struct iovec iov[2];
        iov[0].iov_base = hdr;
        iov[0].iov_len  = 20;
        iov[1].iov_base = const_cast<char*>(sql.data());
        iov[1].iov_len  = sql.size();
        ssize_t total = ::writev(fd_, iov, 2);
        if (total == static_cast<ssize_t>(20 + sql.size()))
            durable_lsn_ = lsn;
        if (total > 0) bytes_.fetch_add((uint64_t)total, std::memory_order_relaxed);
    }

    return lsn;
}

void WalWriter::sync() {
    std::lock_guard<std::mutex> guard(mtx_);
    if (fd_ >= 0) ::fdatasync(fd_);
}

void WalWriter::truncate() {
    std::lock_guard<std::mutex> guard(mtx_);
    if (fd_ >= 0) {
        ::fdatasync(fd_);
        int _r = ::ftruncate(fd_, 0); (void)_r;
        ::lseek(fd_, 0, SEEK_SET);
        bytes_ = 0;
        // Do NOT reset next_lsn_: LSNs must stay monotonic across truncation,
        // otherwise new records (LSN 1, 2, ...) would be <= the snapshot LSN
        // and silently skipped during recovery.
    }
}

std::string WalWriter::rotate(uint64_t tag) {
    std::lock_guard<std::mutex> guard(mtx_);
    if (fd_ < 0 || path_.empty()) return "";
    const std::string seg = path_ + "." + std::to_string(tag) + ".old";

    ::close(fd_);
    fd_ = -1;
    auto reopen = [&] { fd_ = ::open(path_.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644); };

    if (std::rename(path_.c_str(), seg.c_str()) != 0) {
        reopen();                         // rename failed: keep appending to the same file
    } else {
        reopen();                         // normal case: a fresh, empty wal.log
        if (fd_ >= 0) { bytes_ = 0; return seg; }
        // The new file could not be created (EMFILE, ENOSPC, permissions...).
        // Undo the rename and keep appending to the old file: nothing rotated.
        // (Previously the WAL was left CLOSED here and every later write was
        // silently not logged.)
        if (std::rename(seg.c_str(), path_.c_str()) == 0) reopen();
    }
    if (fd_ < 0) {
        failed_ = true;
        std::cerr << "[wal] CRITICAL: WAL could not be reopened after rotate; "
                     "writes will be rejected\n";
    }
    return "";
}

std::vector<std::string> WalWriter::list_segments(const std::string& path) {
    namespace fs = std::filesystem;
    std::vector<std::pair<uint64_t, std::string>> found;
    try {
        fs::path p(path);
        fs::path dir = p.parent_path().empty() ? fs::path(".") : p.parent_path();
        const std::string prefix = p.filename().string() + ".";   // "wal.log."
        const std::string suffix = ".old";
        if (!fs::exists(dir)) return {};
        for (const auto& e : fs::directory_iterator(dir)) {
            std::string name = e.path().filename().string();
            if (name.size() <= prefix.size() + suffix.size()) continue;
            if (name.compare(0, prefix.size(), prefix) != 0) continue;
            if (name.compare(name.size() - suffix.size(), suffix.size(), suffix) != 0) continue;
            std::string num = name.substr(prefix.size(),
                                          name.size() - prefix.size() - suffix.size());
            if (num.empty() || num.find_first_not_of("0123456789") != std::string::npos)
                continue;
            found.emplace_back(std::stoull(num), e.path().string());
        }
    } catch (...) {}
    std::sort(found.begin(), found.end());
    std::vector<std::string> out;
    for (auto& f : found) out.push_back(std::move(f.second));
    return out;
}

// ── WalReader ─────────────────────────────────────────────────────────────────

bool WalReader::open(const std::string& path) {
    file_.open(path, std::ios::binary);
    return file_.is_open();
}

bool WalReader::read_next(uint64_t& lsn_out, std::string& sql_out) {
    if (!file_) return false;

    auto read_u32 = [&](uint32_t& v) -> bool {
        uint8_t b[4];
        if (!file_.read(reinterpret_cast<char*>(b), 4)) return false;
        v = uint32_t(b[0]) | (uint32_t(b[1])<<8) | (uint32_t(b[2])<<16) | (uint32_t(b[3])<<24);
        return true;
    };
    auto read_u64 = [&](uint64_t& v) -> bool {
        uint8_t b[8];
        if (!file_.read(reinterpret_cast<char*>(b), 8)) return false;
        v = 0;
        for (int i = 0; i < 8; ++i) v |= (uint64_t(b[i]) << (i * 8));
        return true;
    };

    uint32_t magic;
    if (!read_u32(magic)) return false; // clean EOF

    if (magic != WalWriter::MAGIC) {
        // Not a valid record start — file may be truncated; stop.
        return false;
    }

    uint64_t lsn;
    uint32_t payload_len, checksum;
    if (!read_u64(lsn))        return false;
    if (!read_u32(payload_len)) return false;
    if (!read_u32(checksum))   return false;

    std::string sql(payload_len, '\0');
    if (!file_.read(sql.data(), payload_len)) return false;

    if (crc32c(sql.data(), payload_len) != checksum) {
        // CRC mismatch: record is corrupt — stop recovery here.
        return false;
    }

    lsn_out = lsn;
    sql_out = std::move(sql);
    return true;
}
