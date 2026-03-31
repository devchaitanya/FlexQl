#include "network/protocol.h"
#include <sys/socket.h>
#include <arpa/inet.h>
#include <cstring>

namespace protocol {

std::string encode_response(const QueryResult& res) {
    if (!res.ok)
        return "ERROR: " + res.error + "\nEND\n";

    if (res.column_names.empty())
        return "OK\nEND\n";

    const int ncols = (int)res.column_names.size();
    std::string out;
    out.reserve(res.rows.size() * ncols * 24 + 32);

    // COLS header: kept for our REPL client (used when result set is empty);
    // the official benchmark client ignores unrecognised lines.
    out += "COLS";
    for (const auto& col : res.column_names) { out += ' '; out += col; }
    out += '\n';

    // ROW lines: official length-prefixed format
    //   ROW <N> <len>:<col_name><len>:<col_val>...\n
    // This is what the official benchmark flexql.cpp parses via parse_row_payload().
    char numbuf[24];
    for (const auto& row : res.rows) {
        out += "ROW ";
        int n = snprintf(numbuf, sizeof(numbuf), "%d", ncols);
        out.append(numbuf, n);
        out += ' ';
        for (int i = 0; i < ncols; ++i) {
            const std::string& name = res.column_names[i];
            const std::string& val  = (i < (int)row.size()) ? row[i] : "";
            n = snprintf(numbuf, sizeof(numbuf), "%zu", name.size());
            out.append(numbuf, n); out += ':'; out += name;
            n = snprintf(numbuf, sizeof(numbuf), "%zu", val.size());
            out.append(numbuf, n); out += ':'; out += val;
        }
        out += '\n';
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
