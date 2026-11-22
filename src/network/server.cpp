#include "network/server.h"
#include "network/protocol.h"
#include "parser/parser.h"
#include "parser/ast.h"
#include "query/executor.h"
#include "storage/wal.h"
#include "storage/snapshot.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <unistd.h>
#include "storage/database.h"
#include <sys/wait.h>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <shared_mutex>

// ── Global WAL writer (shared across all client threads) ─────────────────────
// Opened once at server startup; closed at shutdown.
WalWriter g_wal;

static constexpr const char* SNAP_DIR = "data/snapshots";

// Time of the last mutation (ms on the steady clock) — for the idle trigger.
static std::atomic<int64_t> g_last_write_ms{0};

static int64_t now_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}

// Read a numeric env override (for testing), else the default.
static uint64_t env_u64(const char* name, uint64_t def) {
    const char* v = std::getenv(name);
    return v ? std::strtoull(v, nullptr, 10) : def;
}

static std::string snap_path_for(uint64_t lsn) {
    return std::string(SNAP_DIR) + "/snap_" + std::to_string(lsn) + ".bin";
}

static void delete_wal_segments() {
    std::error_code ec;
    for (const auto& seg : WalWriter::list_segments(g_wal.path()))
        std::filesystem::remove(seg, ec);
}

TcpServer::TcpServer(int port, int num_threads)
    : port_(port), pool_(num_threads) {}

TcpServer::~TcpServer() { stop(); }

void TcpServer::run() {
    const int sfd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (sfd < 0) { perror("socket"); return; }
    server_fd_ = sfd;

    int opt = 1;
    ::setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons(port_);

    if (::bind(sfd, (sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("bind"); ::close(server_fd_.exchange(-1)); return;
    }
    if (::listen(sfd, 128) < 0) {
        perror("listen"); ::close(server_fd_.exchange(-1)); return;
    }

    running_ = true;
    std::cout << "[FlexQL server] listening on port " << port_ << "\n";

    if (g_wal.is_open()) {
        snap_running_ = true;
        snap_thread_  = std::thread(&TcpServer::snapshot_loop, this);
    }

    while (running_) {
        sockaddr_in client_addr{};
        socklen_t   len = sizeof(client_addr);
        int cfd = ::accept(sfd, (sockaddr*)&client_addr, &len);
        if (cfd < 0) {
            if (!running_) break;                       // stop requested: normal exit
            const int e = errno;
            // Temporary conditions must NOT shut the server down:
            //   EINTR        a signal interrupted accept      → retry
            //   ECONNABORTED client gave up before we accepted → retry
            //   EMFILE/ENFILE out of file descriptors         → back off; existing
            //   ENOBUFS/ENOMEM  clients finish and free fds, then we accept again
            if (e == EINTR || e == ECONNABORTED || e == EPROTO) continue;
            if (e == EMFILE || e == ENFILE || e == ENOBUFS || e == ENOMEM) {
                perror("accept (backing off)");
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                continue;
            }
            perror("accept");                           // unexpected → stop serving
            break;
        }
        // Disable Nagle's algorithm for low-latency response delivery
        int nodelay = 1;
        ::setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));
        pool_.submit([this, cfd]{ handle_client(cfd); });
    }
}

void TcpServer::request_stop() noexcept {
    running_.store(false);                       // lock-free atomic: signal-safe
    const int fd = server_fd_.load();
    if (fd >= 0) ::shutdown(fd, SHUT_RDWR);      // wakes the blocked accept() (Linux: EINVAL)
}

void TcpServer::stop() {
    if (stopped_.exchange(true)) return;         // second call (destructor): nothing to do
    request_stop();
    const int fd = server_fd_.exchange(-1);
    if (fd >= 0) ::close(fd);
    // Stop the periodic snapshotter first (waits for an in-progress child),
    // so it never races with the final shutdown snapshot below.
    snap_running_ = false;
    if (snap_thread_.joinable()) snap_thread_.join();
    {
        // Wake workers blocked in recv() on idle clients: shutdown() makes
        // their recv() return 0 ("client disconnected"), so handle_client
        // exits and pool_.shutdown() can join. Without this, one idle client
        // (e.g. an open REPL) made shutdown hang forever.
        std::lock_guard lk(clients_mtx_);
        for (int c : clients_) ::shutdown(c, SHUT_RDWR);
    }
    pool_.shutdown();
    if (g_wal.is_open()) {
        // Write a snapshot so the next startup loads instantly instead of
        // replaying the entire WAL from scratch.
        uint64_t lsn = g_wal.next_lsn() > 1 ? g_wal.next_lsn() - 1 : 0;
        if (Snapshot::write(snap_path_for(lsn), lsn)) {
            std::cout << "[FlexQL server] snapshot written (lsn=" << lsn << ")\n";
            g_wal.truncate();
            delete_wal_segments();            // all covered by this snapshot
            std::cout << "[FlexQL server] WAL reset\n";
            // New snapshot is durable → safe to drop old ones (keep 1 previous
            // as a fallback in case the newest is ever found corrupt).
            Snapshot::prune(SNAP_DIR, 2);
        } else {
            std::cerr << "[FlexQL server] WARNING: snapshot write failed, keeping WAL\n";
            g_wal.sync();
        }
    }
}

// Determines whether a statement type must be durably logged to the WAL.
// Read-only statements (SELECT, SHOW, DESCRIBE) are not logged.
static bool needs_wal(const Statement& stmt) {
    return std::visit([](const auto& s) -> bool {
        using T = std::decay_t<decltype(s)>;
        if constexpr (std::is_same_v<T, SelectStmt>)        return false;
        if constexpr (std::is_same_v<T, ShowTablesStmt>)    return false;
        if constexpr (std::is_same_v<T, ShowDatabasesStmt>) return false;
        if constexpr (std::is_same_v<T, DescribeStmt>)      return false;
        return true; // INSERT, UPDATE, DELETE, CREATE, DROP, TRUNCATE → log
    }, stmt);
}

void TcpServer::handle_client(int client_fd) {
    {
        // Register for stop(). Checked under the same mutex stop() uses, so a
        // connection that was still queued when stop() ran is closed here
        // instead of blocking in recv() after stop() already woke everyone.
        std::lock_guard lk(clients_mtx_);
        if (!running_) { ::close(client_fd); return; }
        clients_.insert(client_fd);
    }
    Executor exec;
    while (true) {
        std::string sql = protocol::recv_sql(client_fd);
        if (sql.empty()) break; // client disconnected

        std::string response;
        QueryResult result;
        bool        have_result = false;
        try {
            auto        t0   = std::chrono::high_resolution_clock::now();
            Statement   stmt = Parser::parse(sql);

            if (needs_wal(stmt) && g_wal.failed()) {
                // The log was lost at runtime: refuse the write rather than
                // acknowledge a change that would vanish on the next crash.
                result = QueryResult::err("write rejected: write-ahead log unavailable");
            } else if (needs_wal(stmt)) {
                // Hold the write gate (shared) across "log + apply" so a
                // periodic snapshot can never fork between the two steps.
                std::shared_lock gate(Database::instance().write_gate());
                if (g_wal.is_open()) g_wal.append(sql);
                result = exec.execute(std::move(stmt));
                g_last_write_ms.store(now_ms(), std::memory_order_relaxed);
            } else {
                result = exec.execute(std::move(stmt));
            }
            auto        t1   = std::chrono::high_resolution_clock::now();
            result.elapsed_us = std::chrono::duration_cast<
                                   std::chrono::microseconds>(t1 - t0).count();
            have_result      = true;
        } catch (const ParseError& e) {
            response = protocol::encode_response(
                QueryResult::err(std::string("parse error: ") + e.what()));
        } catch (const std::exception& e) {
            response = protocol::encode_response(
                QueryResult::err(std::string("internal error: ") + e.what()));
        }

        bool ok;
        if (have_result)
            ok = protocol::stream_response(client_fd, result);
        else
            ok = protocol::send_response(client_fd, response);
        if (!ok) break;
    }
    {
        std::lock_guard lk(clients_mtx_);        // unregister BEFORE close: the fd
        clients_.erase(client_fd);               // number can't be reused while
    }                                            // stop() might still shutdown() it
    ::close(client_fd);
}

// ── Periodic snapshots ────────────────────────────────────────────────────────
void TcpServer::snapshot_loop() {
    // Thresholds (env overrides exist only to make testing practical).
    const uint64_t hard_bytes = env_u64("FLEXQL_SNAP_HARD_BYTES", 256ull << 20); // 256 MB
    const uint64_t soft_bytes = env_u64("FLEXQL_SNAP_SOFT_BYTES",  32ull << 20); //  32 MB
    const int64_t  idle_ms    = (int64_t)env_u64("FLEXQL_SNAP_IDLE_SEC", 5) * 1000;

    while (snap_running_) {
        // Check once per second, waking every 100 ms so stop() is prompt.
        for (int i = 0; i < 10 && snap_running_; ++i)
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        if (!snap_running_) break;

        const uint64_t wal  = g_wal.bytes();
        const bool     idle = now_ms() - g_last_write_ms.load() >= idle_ms;

        if (wal >= hard_bytes || (wal >= soft_bytes && idle))
            periodic_snapshot();
    }
}

bool TcpServer::periodic_snapshot() {
    const auto t0 = std::chrono::steady_clock::now();
    uint64_t lsn = 0;
    pid_t    pid = -1;
    {
        // 1. Quiesce writers: waits for in-flight writes to finish, then blocks
        //    new ones. Readers (SELECT) are never blocked by this gate.
        std::unique_lock gate(Database::instance().write_gate());

        // 2. Everything up to next_lsn-1 is now fully applied in memory.
        lsn = g_wal.next_lsn() - 1;

        // 3. Rotate the WAL: records > lsn will go to a fresh wal.log, while
        //    the old file (records <= lsn) becomes a segment kept until the
        //    snapshot is safely on disk.
        if (g_wal.rotate(lsn).empty()) {
            std::cerr << "[snapshot] WAL rotate failed, skipping\n";
            return false;
        }

        // 4. fork(): the child gets a frozen copy-on-write view of memory.
        pid = ::fork();
        if (pid == 0) {
            // Child: only this thread exists here. Write the snapshot and exit
            // with _exit (skip destructors/atexit — they belong to the parent).
            bool ok = Snapshot::write(snap_path_for(lsn), lsn);
            ::_exit(ok ? 0 : 1);
        }
    }   // 5. gate released → writers resume immediately
    const auto pause_us = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - t0).count();

    if (pid < 0) {
        std::cerr << "[snapshot] fork failed (segment kept; recovery will replay it)\n";
        return false;
    }

    // Wait for the child (this is the background thread; clients keep running).
    int status = 0;
    ::waitpid(pid, &status, 0);
    const bool ok = WIFEXITED(status) && WEXITSTATUS(status) == 0;
    if (!ok) {
        std::cerr << "[snapshot] child failed (segment kept; recovery will replay it)\n";
        return false;
    }

    // Snapshot is durable → every old segment (all records <= lsn) is obsolete.
    delete_wal_segments();
    Snapshot::prune(SNAP_DIR, 2);
    std::cout << "[snapshot] periodic snapshot lsn=" << lsn
              << " written (writer pause " << pause_us << " us)\n";
    return true;
}
