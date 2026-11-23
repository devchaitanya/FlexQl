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
#include <sys/epoll.h>
#include <sys/eventfd.h>
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
    : port_(port), pool_(num_threads), rbuf_(256 * 1024) {
    // Created up front so request_stop() can always wake the event loop.
    wake_fd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
}

TcpServer::~TcpServer() {
    stop();
    const int wfd = wake_fd_.exchange(-1);
    if (wfd >= 0) ::close(wfd);
}

// Client sockets are armed one-shot: after one readiness event the kernel stops
// reporting the socket until it is re-armed. That is how a client's socket stays
// quiet while its query runs on a worker.
static constexpr uint32_t CLIENT_EVENTS = EPOLLIN | EPOLLRDHUP | EPOLLONESHOT;

void TcpServer::run() {
    std::lock_guard run_guard(run_mtx_);          // stop() waits for this
    if (stop_req_) return;

    const int sfd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
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
    if (::listen(sfd, 1024) < 0) {
        perror("listen"); ::close(server_fd_.exchange(-1)); return;
    }

    epfd_ = ::epoll_create1(EPOLL_CLOEXEC);
    if (epfd_ < 0) { perror("epoll_create1"); ::close(server_fd_.exchange(-1)); return; }
    const int wfd = wake_fd_.load();

    epoll_event ev{};
    ev.events = EPOLLIN; ev.data.fd = sfd;          // level-triggered: accept until EAGAIN
    ::epoll_ctl(epfd_, EPOLL_CTL_ADD, sfd, &ev);
    ev.events = EPOLLIN; ev.data.fd = wfd;
    ::epoll_ctl(epfd_, EPOLL_CTL_ADD, wfd, &ev);

    std::cout << "[FlexQL server] listening on port " << port_ << " (epoll)\n";

    if (g_wal.is_open()) {
        snap_running_ = true;
        snap_thread_  = std::thread(&TcpServer::snapshot_loop, this);
    }

    // request_stop() sets stop_req_ and then writes to the eventfd, so a stop
    // that lands before the loop starts is seen here, and one that lands later
    // wakes epoll_wait.
    std::vector<epoll_event> events(256);
    while (!stop_req_) {
        const int n = ::epoll_wait(epfd_, events.data(), (int)events.size(), -1);
        if (n < 0) {
            if (errno == EINTR) continue;
            perror("epoll_wait");
            break;
        }
        for (int i = 0; i < n; ++i) {
            const int fd = events[i].data.fd;
            if (fd == wfd) {
                // Woken by request_stop() and/or by workers that finished a
                // query: re-arm those clients' sockets (only this thread ever
                // changes epoll registrations or closes client sockets).
                uint64_t drain;
                while (::read(wfd, &drain, sizeof drain) > 0) {}
                std::vector<int> done;
                {
                    std::lock_guard lk(done_mtx_);
                    done.swap(done_fds_);
                }
                for (int dfd : done)
                    if (conns_.count(dfd)) rearm(dfd);
                continue;                       // loop condition sees stop_req_
            }
            if (fd == sfd) { accept_clients(sfd); continue; }
            auto it = conns_.find(fd);
            if (it == conns_.end()) continue;
            std::shared_ptr<Conn> c = it->second;   // copy: close_conn may erase it
            handle_readable(c);
        }
    }

    // ── Network shutdown ──────────────────────────────────────────────────────
    // 1. shutdown() every client socket: a worker blocked sending a large reply
    //    to a client that stopped reading fails fast instead of hanging.
    // 2. Let the pool finish queued/running queries (complete statements that
    //    were already received). The fds stay open until the pool is done, so
    //    no worker ever writes to a closed (or reused) descriptor.
    // 3. Close everything.
    for (auto& [fd, c] : conns_) ::shutdown(fd, SHUT_RDWR);
    pool_.shutdown();
    for (auto& [fd, c] : conns_) ::close(fd);
    conns_.clear();
    ::close(epfd_);
    epfd_ = -1;
    const int lfd = server_fd_.exchange(-1);
    if (lfd >= 0) ::close(lfd);
}

void TcpServer::request_stop() noexcept {
    stop_req_.store(true);                       // lock-free atomic: signal-safe
    const int fd = wake_fd_.load();
    if (fd >= 0) {                               // write() is async-signal-safe
        const uint64_t one = 1;
        ssize_t r = ::write(fd, &one, sizeof one);
        (void)r;
    }
}

void TcpServer::stop() {
    if (stopped_.exchange(true)) return;         // second call (destructor): nothing to do
    request_stop();
    // Wait for run() to leave the event loop. Its exit path already finished
    // the queued queries and closed every client connection.
    std::lock_guard wait_run(run_mtx_);
    // Stop the periodic snapshotter (waits for an in-progress child), so it
    // never races with the final shutdown snapshot below.
    snap_running_ = false;
    if (snap_thread_.joinable()) snap_thread_.join();
    pool_.shutdown();                            // no-op if run() already did it
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

void TcpServer::accept_clients(int listen_fd) {
    while (true) {
        int cfd = ::accept4(listen_fd, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (cfd < 0) {
            const int e = errno;
            if (e == EAGAIN || e == EWOULDBLOCK) return;     // no more pending
            // Temporary conditions must NOT shut the server down:
            //   EINTR / ECONNABORTED / EPROTO  → retry
            //   EMFILE/ENFILE/ENOBUFS/ENOMEM   → back off briefly; the listen
            //   socket is level-triggered, so the pending client is retried on
            //   a later loop iteration once descriptors are freed.
            if (e == EINTR || e == ECONNABORTED || e == EPROTO) continue;
            if (e == EMFILE || e == ENFILE || e == ENOBUFS || e == ENOMEM) {
                perror("accept (backing off)");
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                return;
            }
            perror("accept");
            return;
        }
        // Disable Nagle's algorithm for low-latency response delivery
        int nodelay = 1;
        ::setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));

        epoll_event ev{};
        ev.events = CLIENT_EVENTS; ev.data.fd = cfd;
        if (::epoll_ctl(epfd_, EPOLL_CTL_ADD, cfd, &ev) < 0) {
            perror("epoll_ctl add"); ::close(cfd); continue;
        }
        conns_[cfd] = std::make_shared<Conn>(cfd);
    }
}

// Event thread: the client's socket is readable (data, EOF or error). Read
// everything available without blocking; if the buffer now holds a complete
// statement, hand it to a worker (the socket stays disarmed until the worker
// replies); otherwise re-arm and wait for more bytes.
void TcpServer::handle_readable(const std::shared_ptr<Conn>& c) {
    bool closed = false, oversize = false;
    while (true) {
        const ssize_t n = ::recv(c->fd, rbuf_.data(), rbuf_.size(), 0);
        if (n > 0) {
            c->in.append(rbuf_.data(), (size_t)n);
            if (c->in.size() > protocol::MAX_SQL_SIZE) { oversize = true; break; }
            // Statement complete: dispatch now instead of one more recv() just
            // to hear EAGAIN. Anything sent after it stays in the kernel buffer;
            // the socket is level-triggered, so it fires again once re-armed.
            if (c->in.back() == ';') break;
            continue;
        }
        if (n == 0) { closed = true; break; }                 // peer closed
        if (errno == EINTR) continue;
        if (errno == EAGAIN || errno == EWOULDBLOCK) break;   // drained
        closed = true; break;                                 // ECONNRESET, ...
    }
    if (oversize) { close_conn(c->fd); return; }

    // Framing is unchanged from the blocking server: a statement is complete
    // when the received bytes end with ';'.
    if (!c->in.empty() && c->in.back() == ';') {
        std::string sql = std::move(c->in);
        c->in.clear();
        pool_.submit([this, c, sql = std::move(sql)]() mutable {
            serve_query(c, std::move(sql));
        });
        return;   // if the peer also closed, the re-armed socket reports EOF again
    }
    if (closed) { close_conn(c->fd); return; }
    rearm(c->fd);
}

void TcpServer::close_conn(int fd) {
    ::epoll_ctl(epfd_, EPOLL_CTL_DEL, fd, nullptr);
    ::close(fd);
    conns_.erase(fd);
}

void TcpServer::rearm(int fd) {
    epoll_event ev{};
    ev.events = CLIENT_EVENTS; ev.data.fd = fd;
    ::epoll_ctl(epfd_, EPOLL_CTL_MOD, fd, &ev);
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

// Worker: run ONE complete statement for a client, send the reply, re-arm the
// client's socket, and return to the pool. The Executor holds no per-session
// state, so any worker can serve any client's next statement.
void TcpServer::serve_query(const std::shared_ptr<Conn>& c, std::string sql) {
    Executor    exec;
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

    const bool ok = have_result ? protocol::stream_response(c->fd, result)
                                : protocol::send_response(c->fd, response);
    // A failed send means the client is gone: shutting the socket down makes
    // the re-armed socket report EOF, and the event thread closes it.
    if (!ok) ::shutdown(c->fd, SHUT_RDWR);

    // Hand the socket back to the event thread, which re-arms it. The mutex
    // orders this worker's last use of the fd before anything the event
    // thread does next with it (re-arm, close); re-arming from here instead
    // would leave epoll_ctl racing with a close() on the event thread.
    {
        std::lock_guard lk(done_mtx_);
        done_fds_.push_back(c->fd);
    }
    const uint64_t one = 1;
    ssize_t r = ::write(wake_fd_.load(), &one, sizeof one);
    (void)r;
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
