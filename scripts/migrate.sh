#!/usr/bin/env bash
set -euo pipefail

go run ./cmd/migrate "${1:-up}"
