#include "network/protocol.h"
#include <sys/socket.h>
#include <arpa/inet.h>
#include <cstring>

namespace protocol {

// Fast unsigned integer to string (returns length written)
static inline int fast_u64(size_t val, char* buf) {
    if (val == 0) { buf[0] = '0'; return 1; }
    char tmp[20];
    int pos = 0;
    while (val) { tmp[pos++] = '0' + (char)(val % 10); val /= 10; }
    for (int i = 0; i < pos; ++i) buf[i] = tmp[pos - 1 - i];
    return pos;
}

std::string encode_response(const QueryResult& res) {
    if (!res.ok)
        return "ERROR: " + res.error + "\nEND\n";

    if (res.column_names.empty())
        return "OK\nEND\n";

    const int ncols = (int)res.column_names.size();
    const size_t nrows = res.num_rows();
    std::string out;
    out.reserve(nrows * ncols * 24 + 32);

    // COLS header
    out += "COLS";
    for (const auto& col : res.column_names) { out += ' '; out += col; }
    out += '\n';

    // Pre-compute static row prefix and column name parts
    char row_prefix[32];
    int rp_len = 4;
    std::memcpy(row_prefix, "ROW ", 4);
    rp_len += fast_u64((size_t)ncols, row_prefix + rp_len);
    row_prefix[rp_len++] = ' ';

    std::vector<std::string> col_hdr(ncols);
    for (int i = 0; i < ncols; ++i) {
        char lbuf[16];
        int ln = fast_u64(res.column_names[i].size(), lbuf);
        col_hdr[i].append(lbuf, ln);
        col_hdr[i] += ':';
        col_hdr[i] += res.column_names[i];
    }

    char lbuf[16];

    if (!res.flat_offsets.empty()) {
        const char* data = res.flat_data.data();
        const uint32_t* offs = res.flat_offsets.data();
        size_t cell_idx = 0;
        for (size_t r = 0; r < nrows; ++r) {
            out.append(row_prefix, rp_len);
            for (int i = 0; i < ncols; ++i) {
                out += col_hdr[i];
                uint32_t off = offs[cell_idx];
                uint32_t len = offs[cell_idx + 1] - off;
                int ln = fast_u64(len, lbuf);
                out.append(lbuf, ln);
                out += ':';
                out.append(data + off, len);
                ++cell_idx;
            }
            out += '\n';
        }
    } else {
        for (size_t r = 0; r < nrows; ++r) {
            out.append(row_prefix, rp_len);
            for (int i = 0; i < ncols; ++i) {
                out += col_hdr[i];
                const std::string& val = res.rows[r][i];
                int ln = fast_u64(val.size(), lbuf);
                out.append(lbuf, ln);
                out += ':';
                out += val;
            }
            out += '\n';
        }
    }

    out += "END\n";
    return out;
}

bool send_all(int fd, const std::string& msg) {
    size_t total = 0;
    while (total < msg.size()) {
        ssize_t n = ::send(fd, msg.data() + total, msg.size() - total, MSG_NOSIGNAL);
        if (n <= 0) return false;
        total += n;
    }
    return true;
}

std::string recv_sql(int fd) {
    // Thread-local recv buffer: avoids 2MB heap alloc/dealloc per request.
    // Safe: each worker thread has its own instance.
    static thread_local std::string sql;
    sql.clear();
    if (sql.capacity() < 2 * 1024 * 1024)
        sql.reserve(2 * 1024 * 1024);

    // Maximum SQL message size (16 MB) to prevent OOM from malicious clients
    static constexpr size_t MAX_SQL_SIZE = 16 * 1024 * 1024;

    // 256 KB recv buffer — cuts syscall count from ~22 to ~6 per 1.4MB batch.
    static thread_local char buf[256 * 1024];
    while (true) {
        ssize_t n = ::recv(fd, buf, sizeof(buf), 0);
        if (n <= 0) return ""; // disconnected or error
        sql.append(buf, n);
        if (sql.size() > MAX_SQL_SIZE) return ""; // reject oversized messages
        if (sql.back() == ';')
            return sql;
    }
}

bool send_response(int fd, const std::string& resp) {
    return send_all(fd, resp);
}

bool stream_response(int fd, const QueryResult& res) {
    if (!res.ok)
        return send_all(fd, "ERROR: " + res.error + "\nEND\n");

    if (res.column_names.empty())
        return send_all(fd, "OK\nEND\n");

    const int ncols = (int)res.column_names.size();
    const size_t nrows = res.num_rows();
    std::string buf;
    buf.reserve(256 * 1024);

    // COLS header
    buf = "COLS";
    for (const auto& col : res.column_names) { buf += ' '; buf += col; }
    buf += '\n';

    // Pre-compute static row prefix: "ROW <ncols> "
    char row_prefix[32];
    int rp_len = 4; // "ROW "
    std::memcpy(row_prefix, "ROW ", 4);
    rp_len += fast_u64((size_t)ncols, row_prefix + rp_len);
    row_prefix[rp_len++] = ' ';

    // Pre-compute per-column name part: "<name_len>:<name>"
    std::vector<std::string> col_hdr(ncols);
    for (int i = 0; i < ncols; ++i) {
        char lbuf[16];
        int ln = fast_u64(res.column_names[i].size(), lbuf);
        col_hdr[i].append(lbuf, ln);
        col_hdr[i] += ':';
        col_hdr[i] += res.column_names[i];
    }

    static constexpr size_t FLUSH_THRESHOLD = 200 * 1024;
    char lbuf[16];

    // Use compact flat storage path when available
    if (!res.flat_offsets.empty()) {
        const char* data = res.flat_data.data();
        const uint32_t* offs = res.flat_offsets.data();
        size_t cell_idx = 0;
        for (size_t r = 0; r < nrows; ++r) {
            buf.append(row_prefix, rp_len);
            for (int i = 0; i < ncols; ++i) {
                buf += col_hdr[i];
                uint32_t off = offs[cell_idx];
                uint32_t len = offs[cell_idx + 1] - off;
                int ln = fast_u64(len, lbuf);
                buf.append(lbuf, ln);
                buf += ':';
                buf.append(data + off, len);
                ++cell_idx;
            }
            buf += '\n';
            if (buf.size() >= FLUSH_THRESHOLD) {
                if (!send_all(fd, buf)) return false;
                buf.clear();
            }
        }
    } else {
        // Fallback for nested rows storage (JOIN, aggregates, etc.)
        for (size_t r = 0; r < nrows; ++r) {
            buf.append(row_prefix, rp_len);
            for (int i = 0; i < ncols; ++i) {
                buf += col_hdr[i];
                const std::string& val = res.rows[r][i];
                int ln = fast_u64(val.size(), lbuf);
                buf.append(lbuf, ln);
                buf += ':';
                buf += val;
            }
            buf += '\n';
            if (buf.size() >= FLUSH_THRESHOLD) {
                if (!send_all(fd, buf)) return false;
                buf.clear();
            }
        }
    }

    buf += "END\n";
    return send_all(fd, buf);
}

std::string recv_response(int fd) {
    // Buffered 4KB reader — eliminates per-char syscall overhead.
    char rbuf[4096];
    int  rpos = 0, rlen = 0;

    auto gc = [&](char& c) -> bool {
        if (rpos >= rlen) {
            rlen = (int)::recv(fd, rbuf, sizeof(rbuf), 0);
            rpos = 0;
            if (rlen <= 0) return false;
        }
        c = rbuf[rpos++];
        return true;
    };

    std::string result;
    while (true) {
        std::string line;
        char c;
        while (gc(c)) {
            if (c == '\n') break;
            line += c;
        }
        result += line + "\n";
        if (line == "END" || line == "OK" ||
            (line.size() >= 6 && line.substr(0, 6) == "ERROR:"))
            break;
    }
    return result;
}

} // namespace protocol
