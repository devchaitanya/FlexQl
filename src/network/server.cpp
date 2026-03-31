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
#include <chrono>
#include <cstring>
#include <iostream>

// ── Global WAL writer (shared across all client threads) ─────────────────────
// Opened once at server startup; closed at shutdown.
WalWriter g_wal;

TcpServer::TcpServer(int port, int num_threads)
    : port_(port), pool_(num_threads) {}

TcpServer::~TcpServer() { stop(); }

void TcpServer::run() {
    server_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd_ < 0) { perror("socket"); return; }

    int opt = 1;
    ::setsockopt(server_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons(port_);

    if (::bind(server_fd_, (sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("bind"); ::close(server_fd_); return;
    }
    if (::listen(server_fd_, 128) < 0) {
        perror("listen"); ::close(server_fd_); return;
    }

    running_ = true;
    std::cout << "[FlexQL server] listening on port " << port_ << "\n";

    while (running_) {
        sockaddr_in client_addr{};
        socklen_t   len = sizeof(client_addr);
        int cfd = ::accept(server_fd_, (sockaddr*)&client_addr, &len);
        if (cfd < 0) {
            if (running_) perror("accept");
            break;
        }
        // Disable Nagle's algorithm for low-latency response delivery
        int nodelay = 1;
        ::setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));
        pool_.submit([this, cfd]{ handle_client(cfd); });
    }
}

void TcpServer::stop() {
    running_ = false;
    if (server_fd_ >= 0) { ::close(server_fd_); server_fd_ = -1; }
    pool_.shutdown();
    if (g_wal.is_open()) {
        // Write a snapshot so the next startup loads instantly instead of
        // replaying the entire WAL from scratch.
        uint64_t lsn = g_wal.next_lsn() > 1 ? g_wal.next_lsn() - 1 : 0;
        std::string snap_path = "data/snapshots/snap_" + std::to_string(lsn) + ".bin";
        if (Snapshot::write(snap_path, lsn)) {
            std::cout << "[FlexQL server] snapshot written (lsn=" << lsn << ")\n";
            g_wal.truncate();
            std::cout << "[FlexQL server] WAL reset\n";
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

            if (g_wal.is_open() && needs_wal(stmt))
                g_wal.append(sql);

            result           = exec.execute(std::move(stmt));
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
    ::close(client_fd);
}
