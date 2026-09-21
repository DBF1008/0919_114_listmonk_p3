#!/usr/bin/env bash
# Runs the unit tests for the internal/manager fixes:
#   1. worker no longer touches a pipe that cleanup() has removed (pipe.go / manager.go)
#   2. pipes are requeued (not dropped) when the nextPipes queue is full (manager.go)
#   3. sliding window counter is synchronized across goroutines (pipe.go)
#
# Usage: ./test.sh
set -euo pipefail
cd "$(dirname "$0")"

go test -race -v -count=1 ./internal/manager/...
