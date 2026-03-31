# FlexQL Benchmark Unit Tests

This repository is for demo purposes and shows how the benchmark script can be integrated with a FlexQL project to run unit tests and performance checks in a repeatable workflow. You have to copy `benchmark_flexql.cpp` code in your project.

This project contains:
- `server`: a TCP SQL server backed by SQLite (`flexql_server.cpp`)
- `benchmark`: a benchmark and SQL subset/unit test client (`benchmark_flexql.cpp` + `flexql.cpp`)

## Prerequisites

Install the SQLite3 development library.

On Debian/Ubuntu:

```bash
sudo apt update
sudo apt install sqlite3 libsqlite3-dev
```

## Build

From the project root:

```bash
sh compile.sh
```

This produces:
- `./server`
- `./benchmark`

## Run

You must run the server first, then the benchmark in another terminal.

1. Terminal 1: start server

```bash
./server
```

You should see:

```text
FlexQL Server running on port 9000
```

2. Terminal 2: run unit tests only

```bash
./benchmark --unit-test
```

3. Terminal 2: run benchmark + unit tests (default: 1,000,000 rows)

```bash
./benchmark
```

4. Terminal 2: run benchmark + unit tests with custom row count

```bash
./benchmark 200000
```

## Typical Workflow

```bash
sh compile.sh
# Terminal 1
./server
# Terminal 2
./benchmark --unit-test
./benchmark 200000
```

## Notes

- If `./benchmark` exits with `Cannot open FlexQL`, make sure `./server` is running.
- `flexql_server.cpp` recreates `flexql.db` on each server start.
- The server listens on `127.0.0.1:9000`.
