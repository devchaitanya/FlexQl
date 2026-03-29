#include "network/server.h"
#include "storage/wal.h"
#include "storage/snapshot.h"
#include "storage/database.h"
#include "parser/parser.h"
#include "query/executor.h"
#include "expiration/ttl_manager.h"
#include <iostream>
#include <csignal>
#include <cstdlib>
#include <thread>
#include <filesystem>

static TcpServer* g_server = nullptr;

static void signal_handler(int) {
    std::cout << "\n[FlexQL server] shutting down...\n";
    if (g_server) g_server->stop();
}

// ── Startup recovery ──────────────────────────────────────────────────────────
// 1. Load the most recent snapshot (if any) — fast baseline.
// 2. Replay any WAL records with LSN > snapshot LSN.
// This guarantees all committed mutations survive a crash.
static void recover() {
    const char* WAL_PATH_ENV = std::getenv("WAL_PATH");
    const std::string WAL_PATH  = WAL_PATH_ENV ? WAL_PATH_ENV : "data/wal/wal.log";
    constexpr const char* SNAP_DIR  = "data/snapshots";

    // Create required directories
    std::filesystem::create_directories(std::filesystem::path(WAL_PATH).parent_path());
    std::filesystem::create_directories(SNAP_DIR);

    uint64_t replay_from_lsn = 0;

    // Step 1: load most recent snapshot
    SnapshotInfo snap = Snapshot::find_latest(SNAP_DIR);
    if (!snap.path.empty()) {
        std::cout << "[recovery] loading snapshot lsn=" << snap.lsn
                  << " from " << snap.path << "\n";
        uint64_t loaded_lsn = Snapshot::load(snap.path);
        if (loaded_lsn > 0) {
            replay_from_lsn = loaded_lsn;
            std::cout << "[recovery] snapshot loaded (" << loaded_lsn << " LSN)\n";
        } else {
            std::cerr << "[recovery] snapshot load failed, replaying full WAL\n";
        }
    }

    // Step 2: replay WAL records after snapshot LSN
    WalReader reader;
    if (reader.open(WAL_PATH)) {
        Executor exec;
        uint64_t    lsn;
        std::string sql;
        size_t      replayed = 0;

        while (reader.read_next(lsn, sql)) {
            if (lsn <= replay_from_lsn) continue; // already in snapshot
            try {
                Statement stmt = Parser::parse(sql);
                exec.execute(std::move(stmt));
                ++replayed;
            } catch (...) {
                // Skip unparseable records (e.g. partial writes before crash)
            }
        }

        if (replayed > 0)
            std::cout << "[recovery] replayed " << replayed
                      << " WAL records (LSN > " << replay_from_lsn << ")\n";
    }

    // Step 3: open WAL for appending (continued from where we left off)
    if (!g_wal.open(WAL_PATH)) {
        std::cerr << "[recovery] WARNING: could not open WAL for writing at "
                  << WAL_PATH << " — data will not be persisted!\n";
    } else {
        std::cout << "[recovery] WAL open, next LSN=" << g_wal.next_lsn() << "\n";
    }
}

int main(int argc, char* argv[]) {
    int port        = 9000;
    int num_threads = (int)std::thread::hardware_concurrency();
    if (num_threads < 2) num_threads = 2;

    if (argc > 1) port        = std::atoi(argv[1]);
    if (argc > 2) num_threads = std::atoi(argv[2]);

    std::cout << "[FlexQL server] threads=" << num_threads << "\n";

    // Crash recovery: load snapshot + replay WAL
    recover();

    // Start TTL background sweeper (every 60 seconds)
    TtlManager ttl(60);
    ttl.start();

    // Start TCP server
    TcpServer server(port, num_threads);
    g_server = &server;

    std::signal(SIGINT,  signal_handler);
    std::signal(SIGTERM, signal_handler);

    server.run(); // blocks

    ttl.stop();
    std::cout << "[FlexQL server] stopped.\n";
    return 0;
}
