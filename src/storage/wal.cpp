#include "storage/wal.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/uio.h>    // writev
#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <filesystem>
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
        next_lsn_    = 1;
        durable_lsn_ = 0;
    }
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
