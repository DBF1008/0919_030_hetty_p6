#!/bin/sh
# Runs all unit tests for the repository.
#
# Usage:
#   ./test.sh           # run all unit tests
#   ./test.sh -race     # run all unit tests with the race detector
#
# To run tests for a single package, e.g. the sender package:
#   go test -v -count=1 ./pkg/sender/...
set -eu

cd "$(dirname "$0")"

echo "==> go vet"
go vet ./...

echo "==> go test"
go test -count=1 "$@" ./...
