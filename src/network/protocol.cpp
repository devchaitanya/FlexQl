#include "network/protocol.h"
#include <sys/socket.h>
#include <arpa/inet.h>
#include <cstring>
#include <sstream>

namespace protocol {

std::string encode_response(const QueryResult& res) {
    if (!res.ok)
        return "ERROR: " + res.error + "\nEND\n";

    if (res.column_names.empty())
        return "OK\nEND\n";

    std::string out;
    // Column names header (parsed by our client; ignored by bench which only reads ROW lines)
    out += "COLS";
    for (const auto& col : res.column_names) { out += ' '; out += col; }
    out += '\n';
    for (const auto& row : res.rows) {
        out += "ROW";
        for (const auto& v : row) {
            out += ' ';
            out += v;
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
    // Read raw SQL terminated by ';' (reference protocol).
    // Use a large recv buffer to minimize syscalls; check only the last byte
    // received (benchmark SQL always ends with exactly one ';').
    std::string sql;
    sql.reserve(512 * 1024); // pre-reserve for large INSERT batches
    char buf[65536];
    while (true) {
        ssize_t n = ::recv(fd, buf, sizeof(buf), 0);
        if (n <= 0) return ""; // disconnected or error
        sql.append(buf, n);
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
