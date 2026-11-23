// Integration tests for the event-driven (epoll) TCP server.
// Runs a real TcpServer in-process on a free port and talks to it over sockets.
//
//   make test-server        (also: test-server-asan, test-server-tsan)

#include "network/server.h"
#include "network/protocol.h"

#include <arpa/inet.h>
#include <dirent.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

static int g_pass = 0, g_fail = 0;

#define CHECK(cond, msg)                                                         \
    do {                                                                         \
        if (cond) { ++g_pass; }                                                  \
        else { ++g_fail; std::cerr << "  FAIL: " << msg << "  (" #cond ")\n"; }  \
    } while (0)

static void section(const char* name) { std::cout << "── " << name << "\n"; }

using Clock = std::chrono::steady_clock;
static double ms_since(Clock::time_point t0) {
    return std::chrono::duration<double, std::milli>(Clock::now() - t0).count();
}

// ── Test client ───────────────────────────────────────────────────────────────
// Keeps unread bytes between responses, so two replies that arrive in one
// recv() are split correctly (needed for the ordering test).
struct Client {
    int         fd = -1;
    std::string pending;

    bool connect_to(int port, int timeout_ms = 3000) {
        fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) return false;
        sockaddr_in a{};
        a.sin_family = AF_INET;
        a.sin_port   = htons((uint16_t)port);
        ::inet_pton(AF_INET, "127.0.0.1", &a.sin_addr);
        if (::connect(fd, (sockaddr*)&a, sizeof a) < 0) { ::close(fd); fd = -1; return false; }
        timeval tv{timeout_ms / 1000, (timeout_ms % 1000) * 1000};   // a hang fails, not blocks
        ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
        int one = 1;
        ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
        return true;
    }
    bool send(const std::string& s) { return protocol::send_all(fd, s); }

    // One full response (through "END\n"); "" on timeout/disconnect.
    std::string read_response() {
        char buf[65536];
        while (true) {
            const size_t e = pending.find("END\n");
            if (e != std::string::npos &&
                (e == 0 || pending[e - 1] == '\n')) {          // "END" on its own line
                std::string r = pending.substr(0, e + 4);
                pending.erase(0, e + 4);
                return r;
            }
            const ssize_t n = ::recv(fd, buf, sizeof buf, 0);
            if (n <= 0) return "";
            pending.append(buf, (size_t)n);
        }
    }
    std::string query(const std::string& sql) { return send(sql) ? read_response() : ""; }
    void close() { if (fd >= 0) ::close(fd); fd = -1; }
    ~Client() { close(); }
};

static size_t count_rows(const std::string& resp) {
    size_t n = 0, pos = 0;
    while ((pos = resp.find("ROW ", pos)) != std::string::npos) {
        if (pos == 0 || resp[pos - 1] == '\n') ++n;
        pos += 4;
    }
    return n;
}

// Value of the single cell in a 1x1 result ("ROW 1 <len>:<name><len>:<value>").
static std::string single_value(const std::string& resp) {
    const size_t row = resp.find("\nROW ");
    if (row == std::string::npos) return "";
    const size_t eol = resp.find('\n', row + 1);
    const std::string line = resp.substr(row + 1, eol - row - 1);
    const size_t colon = line.rfind(':');
    return colon == std::string::npos ? "" : line.substr(colon + 1);
}

static int free_port() {
    int s = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in a{};
    a.sin_family = AF_INET;
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    a.sin_port = 0;
    ::bind(s, (sockaddr*)&a, sizeof a);
    socklen_t len = sizeof a;
    ::getsockname(s, (sockaddr*)&a, &len);
    ::close(s);
    return ntohs(a.sin_port);
}

static size_t open_fds() {
    size_t n = 0;
    if (DIR* d = ::opendir("/proc/self/fd")) {
        while (::readdir(d)) ++n;
        ::closedir(d);
    }
    return n;
}

// A server running in a background thread.
struct TestServer {
    int         port;
    TcpServer   srv;
    std::thread th;
    explicit TestServer(int workers) : port(free_port()), srv(port, workers) {
        th = std::thread([this] { srv.run(); });
        for (int i = 0; i < 200; ++i) {                 // wait until it accepts
            Client c;
            if (c.connect_to(port)) return;
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        std::cerr << "server did not start\n";
    }
    void stop() { srv.stop(); if (th.joinable()) th.join(); }
    ~TestServer() { stop(); }
};

// ── Tests ─────────────────────────────────────────────────────────────────────

// The reason for the epoll design: idle connections must not hold workers.
// The old thread-per-connection server hung here as soon as idle >= workers.
static void test_idle_clients_do_not_block(TestServer& s) {
    section("idle clients (100) don't block a 2-worker server");
    std::vector<Client> idle(100);
    size_t connected = 0;
    for (auto& c : idle) connected += c.connect_to(s.port);
    CHECK(connected == idle.size(), "all idle clients connected");
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    Client q;
    CHECK(q.connect_to(s.port), "query client connects");
    auto t0 = Clock::now();
    const std::string r = q.query("SHOW TABLES;");
    CHECK(r.find("END\n") != std::string::npos, "query answered despite 100 idle clients");
    CHECK(ms_since(t0) < 1000, "answered quickly");

    // An idle client can still be served later, by whichever worker is free.
    CHECK(idle[37].query("SHOW TABLES;").find("END\n") != std::string::npos,
          "a previously idle client is served");
}

// epoll reports a socket readable as soon as ANY bytes arrive; the server must
// buffer until the statement is complete.
static void test_split_statement(TestServer& s) {
    section("statement split across many sends");
    Client c;
    CHECK(c.connect_to(s.port), "connect");
    const std::string sql = "CREATE TABLE SPLIT_T (ID INT PRIMARY KEY, NAME VARCHAR);";
    for (size_t i = 0; i < sql.size(); i += 3) {
        c.send(sql.substr(i, 3));
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    CHECK(c.read_response() == "OK\nEND\n", "split CREATE succeeds");
    CHECK(c.query("INSERT INTO SPLIT_T VALUES (1,'a'),(2,'b');") == "OK\nEND\n", "insert");
    CHECK(single_value(c.query("SELECT COUNT(*) FROM SPLIT_T;")) == "2", "count after split create");
}

static void test_big_insert_and_select(TestServer& s) {
    section("large INSERT (20k rows in one statement) and large SELECT");
    Client c;
    CHECK(c.connect_to(s.port, 10000), "connect");
    CHECK(c.query("CREATE TABLE BIGT (ID INT PRIMARY KEY, NAME VARCHAR, SCORE INT);") == "OK\nEND\n",
          "create");
    std::string sql = "INSERT INTO BIGT VALUES ";
    for (int i = 1; i <= 20000; ++i) {
        if (i > 1) sql += ',';
        sql += "(" + std::to_string(i) + ",'user" + std::to_string(i) + "'," +
               std::to_string(i % 100) + ")";
    }
    sql += ';';
    CHECK(sql.size() > 400000, "statement is large (arrives in many reads)");
    CHECK(c.query(sql) == "OK\nEND\n", "big insert accepted");
    CHECK(single_value(c.query("SELECT COUNT(*) FROM BIGT;")) == "20000", "count = 20000");
    const std::string all = c.query("SELECT * FROM BIGT;");
    CHECK(count_rows(all) == 20000, "SELECT * returns all 20000 rows");
}

// Two statements from one client must run one at a time and be answered in
// order: the socket is disarmed (EPOLLONESHOT) while the first one runs.
static void test_ordering_per_client(TestServer& s) {
    section("one client's statements are answered in order");
    Client c;
    CHECK(c.connect_to(s.port, 10000), "connect");
    c.send("SELECT * FROM BIGT;");                     // slow-ish (20k rows)
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    c.send("SELECT COUNT(*) FROM BIGT;");              // arrives while the first runs
    const std::string r1 = c.read_response();
    const std::string r2 = c.read_response();
    CHECK(count_rows(r1) == 20000, "first reply is the big SELECT");
    CHECK(single_value(r2) == "20000", "second reply is the COUNT");
}

static void test_errors_keep_connection(TestServer& s) {
    section("errors don't close the connection");
    Client c;
    CHECK(c.connect_to(s.port), "connect");
    CHECK(c.query("SELEC nonsense;").rfind("ERROR:", 0) == 0, "parse error reported");
    CHECK(c.query("SELECT * FROM NO_SUCH_TABLE;").rfind("ERROR:", 0) == 0, "exec error reported");
    CHECK(c.query("SHOW TABLES;").find("END\n") != std::string::npos, "connection still usable");
}

static void test_many_concurrent_clients(TestServer& s) {
    section("16 concurrent clients x 30 rounds (writes + reads)");
    constexpr int CLIENTS = 16, ROUNDS = 30;
    std::atomic<int> bad{0};
    std::vector<std::thread> ts;
    auto t0 = Clock::now();
    for (int t = 0; t < CLIENTS; ++t) {
        ts.emplace_back([&, t] {
            Client c;
            if (!c.connect_to(s.port, 10000)) { ++bad; return; }
            const std::string tbl = "CONC_" + std::to_string(t);
            if (c.query("CREATE TABLE " + tbl + " (ID INT PRIMARY KEY, V INT);") != "OK\nEND\n") ++bad;
            for (int r = 0; r < ROUNDS; ++r) {
                std::string ins = "INSERT INTO " + tbl + " VALUES ";
                for (int k = 0; k < 10; ++k) {
                    if (k) ins += ',';
                    const int id = r * 10 + k;
                    ins += "(" + std::to_string(id) + "," + std::to_string(id * 2) + ")";
                }
                ins += ';';
                if (c.query(ins) != "OK\nEND\n") ++bad;
                if (single_value(c.query("SELECT COUNT(*) FROM " + tbl + ";")) !=
                    std::to_string((r + 1) * 10)) ++bad;
            }
        });
    }
    for (auto& th : ts) th.join();
    CHECK(bad == 0, "every reply correct (" << bad << " bad)");
    std::cout << "   " << CLIENTS * ROUNDS * 2 << " queries in " << (int)ms_since(t0) << " ms\n";
}

// Disconnects (idle, mid-statement) must free the connection: no fd leak.
static void test_disconnects_free_resources(TestServer& s) {
    section("disconnects (idle and mid-statement) free their sockets");
    const size_t before = open_fds();
    for (int i = 0; i < 300; ++i) {
        Client c;
        c.connect_to(s.port);
        if (i % 2) c.send("SELECT * FR");               // half a statement, then gone
    }
    size_t after = 0;
    for (int i = 0; i < 100; ++i) {                     // server closes them asynchronously
        after = open_fds();
        if (after <= before + 2) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    CHECK(after <= before + 2, "fd count back to baseline (" << before << " → " << after << ")");
    Client c;
    CHECK(c.connect_to(s.port) && c.query("SHOW TABLES;").find("END\n") != std::string::npos,
          "server healthy afterwards");
}

static void test_oversize_statement_closes(TestServer& s) {
    section("statement larger than 16 MB closes the connection");
    Client c;
    CHECK(c.connect_to(s.port, 5000), "connect");
    std::string junk(protocol::MAX_SQL_SIZE + 1024 * 1024, 'x');   // no ';'
    c.send(junk);                                     // may fail midway: server closes
    char b;
    const ssize_t n = ::recv(c.fd, &b, 1, 0);
    CHECK(n == 0 || (n < 0 && errno == ECONNRESET), "server closed the connection");
    Client d;
    CHECK(d.connect_to(s.port) && d.query("SHOW TABLES;").find("END\n") != std::string::npos,
          "other clients unaffected");
}

// Shutdown must not wait for idle or half-sent clients.
static void test_shutdown_with_idle_clients() {
    section("stop() returns promptly with idle and mid-statement clients");
    TestServer s(2);
    std::vector<Client> idle(20);
    for (auto& c : idle) c.connect_to(s.port);
    idle[0].send("SELECT * FROM BI");                   // incomplete statement pending
    CHECK(idle[1].query("SHOW TABLES;").find("END\n") != std::string::npos, "served before stop");
    auto t0 = Clock::now();
    s.stop();
    CHECK(ms_since(t0) < 2000, "stop() took " << (int)ms_since(t0) << " ms");
    char b;
    CHECK(::recv(idle[5].fd, &b, 1, 0) == 0, "idle clients see the connection closed");
}

int main() {
    std::cout << "FlexQL server (epoll) integration tests\n";
    {
        TestServer s(2);                                // 2 workers on purpose
        test_idle_clients_do_not_block(s);
        test_split_statement(s);
        test_big_insert_and_select(s);
        test_ordering_per_client(s);
        test_errors_keep_connection(s);
        test_many_concurrent_clients(s);
        test_disconnects_free_resources(s);
        test_oversize_statement_closes(s);
    }
    test_shutdown_with_idle_clients();

    std::cout << "\nResults: " << g_pass << "/" << (g_pass + g_fail) << " passed, "
              << g_fail << " failed\n";
    return g_fail == 0 ? 0 : 1;
}
