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
#include <algorithm>
#include <unistd.h>

static TcpServer* g_server = nullptr;

// A signal can interrupt a thread at ANY instruction — e.g. while it holds
// malloc's lock or is halfway through a cout write. So a handler may only call
// async-signal-safe functions. The old handler printed with cout and ran the
// whole stop() (joins, snapshot I/O) → could deadlock. Now it only:
//   write(2) a message, and request_stop() (atomic store + shutdown(2)).
// main() does the real shutdown after run() returns, in normal context.
static void signal_handler(int) {
    static const char msg[] = "\n[FlexQL server] shutting down...\n";
    ssize_t r = ::write(STDOUT_FILENO, msg, sizeof msg - 1);
    (void)r;
    if (g_server) g_server->request_stop();
}

// ── Startup recovery ──────────────────────────────────────────────────────────
// 1. Load the most recent snapshot (if any) — fast baseline.
// 2. Replay any WAL records with LSN > snapshot LSN.
// This guarantees all committed mutations survive a crash.
static void recover() {
    // first check if WAL path is there in environment variables if there load it else set the path to "data/wal/wal.log"
    const char* WAL_PATH_ENV = std::getenv("WAL_PATH");
    const std::string WAL_PATH  = WAL_PATH_ENV ? WAL_PATH_ENV : "data/wal/wal.log";
    // similarly set the snap directory to load the snaps (which is a full image of the database)
    constexpr const char* SNAP_DIR  = "data/snapshots";

    // Create required directories if not already there 
    // .parent_path() is there so that .log wont be created and only the data/wal folders will be created if not done
    std::filesystem::create_directories(std::filesystem::path(WAL_PATH).parent_path());
    std::filesystem::create_directories(SNAP_DIR);

    uint64_t replay_from_lsn = 0;

    // Step 1: load the newest snapshot that loads cleanly.
    // Normally that is the newest one; if it is corrupt, fall back to the
    // previous one (prune() keeps one spare).
    std::vector<SnapshotInfo> snaps = Snapshot::list_all(SNAP_DIR);
    const uint64_t max_snap_lsn = snaps.empty() ? 0 : snaps.front().lsn;
    for (size_t i = 0; i < snaps.size(); ++i) {
        const SnapshotInfo& snap = snaps[i];
        std::cout << "[recovery] loading snapshot lsn=" << snap.lsn
                  << " from " << snap.path << "\n";
        uint64_t loaded_lsn = Snapshot::load(snap.path);
        if (loaded_lsn > 0) {
            replay_from_lsn = loaded_lsn;
            std::cout << "[recovery] snapshot loaded (" << loaded_lsn << " LSN)\n";
            if (i > 0)
                std::cerr << "[recovery] WARNING: newest snapshot (lsn=" << max_snap_lsn
                          << ") was unreadable; fell back to lsn=" << loaded_lsn
                          << ". Changes between them may be lost.\n";
            break;
        }
        std::cerr << "[recovery] snapshot " << snap.path << " failed to load\n";
        // A failed load may have created some tables already — start clean
        // before trying the next snapshot.
        for (const auto& t : Database::instance().list_tables())
            Database::instance().drop_table(t, /*if_exists=*/true);
    }

    // Step 2: replay WAL records after snapshot LSN.
    // Old WAL segments (wal.log.<n>.old, left if a periodic snapshot did not
    // finish) come first, oldest to newest, then the active wal.log.
    std::vector<std::string> wal_files = WalWriter::list_segments(WAL_PATH);
    wal_files.push_back(WAL_PATH);
    uint64_t max_lsn_seen = 0;
    {
        Executor exec;
        uint64_t    lsn;
        std::string sql;
        size_t      replayed = 0;

        for (const auto& file : wal_files) {
            WalReader reader;
            if (!reader.open(file)) continue;
            while (reader.read_next(lsn, sql)) {
                max_lsn_seen = std::max(max_lsn_seen, lsn);
                if (lsn <= replay_from_lsn) continue; // already in snapshot
                try {
                    Statement stmt = Parser::parse(sql);
                    exec.execute(std::move(stmt));
                    ++replayed;
                } catch (...) {
                    // Statement failed originally too (WAL logs before executing)
                }
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
        // Continue numbering after the snapshot so post-restart records are
        // never mistaken for records already contained in the snapshot.
        g_wal.ensure_lsn_after(std::max({replay_from_lsn, max_snap_lsn, max_lsn_seen}));
        std::cout << "[recovery] WAL open, next LSN=" << g_wal.next_lsn() << "\n";
    }
}

int main(int argc, char* argv[]) {
    // Flush log output after every write: when stdout is a file, it is
    // otherwise block-buffered and a crash (kill -9) would lose the last logs.
    std::cout.setf(std::ios::unitbuf);

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

    server.run(); // blocks until a signal calls request_stop()

    ttl.stop();       // no compaction while the final snapshot is written
    server.stop();    // join threads, wake idle clients, final snapshot
    std::cout << "[FlexQL server] stopped.\n";
    return 0;
}
