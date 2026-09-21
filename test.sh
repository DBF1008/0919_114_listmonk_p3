#!/usr/bin/env bash
# Runs the unit tests for the manager concurrency fixes:
#   1. worker updates pipe state before wg.Done() (cleanup race)
#   2. nextPipes full requeue instead of dropping pipes (message loss)
#   3. sliding window counter mutex (rate limit accuracy)
#
# Usage:
#   ./test.sh            # run all manager unit tests with -race
#   ./test.sh <TestName> # run a single test, e.g. ./test.sh TestSlidingWindowConcurrent
set -euo pipefail

cd "$(dirname "$0")"

PKG=./internal/manager/

if [ $# -gt 0 ]; then
	go test -race -v -count=1 -run "^$1\$" "$PKG"
	exit $?
fi

go test -race -v -count=1 \
	-run 'TestWorkerUpdatesPipeBeforeCleanup|TestRequeuePipeWhenQueueFull|TestRequeuePipeGivesUp|TestSlidingWindowConcurrent' \
	"$PKG"
