#!/bin/sh
# Runs linters and all unit tests. Requires Go (see go.mod for the version)
# and a populated module cache or network access to download modules.
set -e

cd "$(dirname "$0")"

echo "==> go vet"
go vet ./...

echo "==> go build"
go build ./...

echo "==> unit tests (all packages)"
go test -count=1 ./...

echo "==> unit tests (sender & reqlog, verbose)"
go test -count=1 -v ./pkg/sender/... ./pkg/reqlog/...

echo "==> all checks passed"
