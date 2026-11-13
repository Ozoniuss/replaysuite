#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"
REPLAY_SUITE_TEMPORAL_SERVER_HOSTPORT=localhost:7233 go test -count=1 ./...
