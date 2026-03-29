#!/usr/bin/env bash
set -e
cd "$(dirname "$0")/.."
make all -j"$(nproc)"
echo "Build complete: bin/flexql-server  bin/flexql-client"
