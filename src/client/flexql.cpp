#include "flexql.h"
#include "network/protocol.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <unistd.h>
#include <cstring>
#include <cstdlib>
#include <string>
#include <vector>
#include <sstream>

// ── Internal struct ───────────────────────────────────────────────────────────
struct FlexQL {
    int                      sockfd         = -1;
    std::vector<std::string> last_col_names;       // column names from last COLS line
    bool                     last_was_query = false; // true if last response had a COLS header
};

// ── Helpers ───────────────────────────────────────────────────────────────────
static char* make_errmsg(const std::string& msg) {
    char* buf = (char*)std::malloc(msg.size() + 1);
    if (buf) std::memcpy(buf, msg.c_str(), msg.size() + 1);
    return buf;
}

// Send SQL string (raw — no length prefix, reference protocol).
static bool send_sql(int fd, const char* sql) {
    return protocol::send_all(fd, std::string(sql));
}

// Parse server response. Handles COLS/ROW/OK/END/ERROR: lines.
// Populates out_col_names and out_was_query from the COLS header.
// Returns "" on success, error string on ERROR:.
static std::string parse_and_dispatch(
    const std::string& response,
    int (*callback)(void*, int, char**, char**),
    void* arg,
    std::vector<std::string>& out_col_names,
    bool& out_was_query)
{
    out_was_query = false;
    out_col_names.clear();

    std::istringstream ss(response);
    std::string line;
    std::vector<std::string> col_names; // from COLS line

    while (std::getline(ss, line)) {
        if (line.empty()) continue;

        if (line.size() >= 5 && line.substr(0, 5) == "COLS ") {
            out_was_query = true;
            std::istringstream cs(line.substr(5));
            std::string col;
            while (cs >> col) col_names.push_back(col);
            out_col_names = col_names;
            continue;
        }

        if (line.size() >= 4 && line.substr(0, 4) == "ROW ") {
            if (callback == nullptr) continue;
            std::string rest = line.substr(4);
            std::vector<std::string> vals;
            std::istringstream vs(rest);
            std::string v;
            while (vs >> v) vals.push_back(v);

            int n = (int)vals.size();
            std::vector<char*> cvals(n), cnames(n);
            std::vector<std::string> fallback(n);
            // Use real column names if count matches, else fallback col0/col1/...
            bool have_names = ((int)col_names.size() == n);
            for (int i = 0; i < n; ++i) {
                cvals[i] = const_cast<char*>(vals[i].c_str());
                if (have_names) {
                    cnames[i] = const_cast<char*>(col_names[i].c_str());
                } else {
                    fallback[i] = "col" + std::to_string(i);
                    cnames[i]   = const_cast<char*>(fallback[i].c_str());
                }
            }
            int ret = callback(arg, n, cvals.data(), cnames.data());
            if (ret != 0) break;

        } else if (line.size() >= 7 && line.substr(0, 7) == "ERROR: ") {
            return line.substr(7);

        } else if (line == "OK" || line == "END") {
            break;
        }
    }
    return "";
}

// ── API implementation ────────────────────────────────────────────────────────
int flexql_open(const char* host, int port, FlexQL** db) {
    if (!host || !db) return FLEXQL_ERROR;

    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return FLEXQL_ERROR;

    // Resolve host
    struct addrinfo hints{}, *res = nullptr;
    hints.ai_family   = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    std::string port_str = std::to_string(port);
    if (::getaddrinfo(host, port_str.c_str(), &hints, &res) != 0) {
        ::close(fd); return FLEXQL_ERROR;
    }

    int rc = ::connect(fd, res->ai_addr, res->ai_addrlen);
    ::freeaddrinfo(res);
    if (rc < 0) { ::close(fd); return FLEXQL_ERROR; }

    *db = new FlexQL();
    (*db)->sockfd = fd;
    return FLEXQL_OK;
}

int flexql_close(FlexQL* db) {
    if (!db) return FLEXQL_ERROR;
    if (db->sockfd >= 0) ::close(db->sockfd);
    delete db;
    return FLEXQL_OK;
}

int flexql_exec(FlexQL* db,
                const char* sql,
                int (*callback)(void*, int, char**, char**),
                void* arg,
                char** errmsg)
{
    if (errmsg) *errmsg = nullptr;
    if (!db || db->sockfd < 0 || !sql) {
        if (errmsg) *errmsg = make_errmsg("invalid database handle");
        return FLEXQL_ERROR;
    }

    // Send SQL to server
    if (!send_sql(db->sockfd, sql)) {
        if (errmsg) *errmsg = make_errmsg("failed to send query to server");
        return FLEXQL_ERROR;
    }

    // Receive response
    std::string response = protocol::recv_response(db->sockfd);
    if (response.empty()) {
        if (errmsg) *errmsg = make_errmsg("no response from server");
        return FLEXQL_ERROR;
    }

    // Parse and dispatch callbacks
    std::string err = parse_and_dispatch(response, callback, arg,
                                         db->last_col_names, db->last_was_query);
    if (!err.empty()) {
        if (errmsg) *errmsg = make_errmsg(err);
        return FLEXQL_ERROR;
    }
    return FLEXQL_OK;
}

// ── Query metadata accessors ─────────────────────────────────────────────────
int flexql_last_was_query(FlexQL* db) {
    return (db && db->last_was_query) ? 1 : 0;
}

int flexql_last_col_count(FlexQL* db) {
    return db ? (int)db->last_col_names.size() : 0;
}

const char* flexql_last_col_name(FlexQL* db, int i) {
    if (!db || i < 0 || i >= (int)db->last_col_names.size()) return nullptr;
    return db->last_col_names[i].c_str();
}

void flexql_free(void* ptr) {
    std::free(ptr);
}
