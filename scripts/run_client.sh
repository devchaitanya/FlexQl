#!/usr/bin/env bash
HOST=${1:-127.0.0.1}
PORT=${2:-9000}
cd "$(dirname "$0")/.."
exec ./bin/flexql-client "$HOST" "$PORT"
