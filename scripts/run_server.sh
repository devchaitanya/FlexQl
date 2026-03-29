#!/usr/bin/env bash
PORT=${1:-9000}
THREADS=${2:-$(nproc)}
cd "$(dirname "$0")/.."
exec ./bin/flexql-server "$PORT" "$THREADS"
