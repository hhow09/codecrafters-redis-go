#!/bin/sh
set -e
EXEC_FROM="${EXEC_FROM:-.}"
exec "$EXEC_FROM/redis-server" "$@"
