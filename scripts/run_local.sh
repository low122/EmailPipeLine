#!/usr/bin/env bash
# Run all pipeline services locally (no Docker). Requires Redis running (e.g. redis-server).
# Run from repo root: ./scripts/run_local.sh

set -e
cd "$(dirname "$0")/.."

# Don't source .env here — values with spaces (e.g. app passwords) break bash.
# Python services load .env via load_dotenv(). Redis check uses defaults or env.
REDIS_HOST="${REDIS_HOST:-localhost}"
REDIS_PORT="${REDIS_PORT:-6379}"
if ! command -v redis-cli &>/dev/null; then
  echo "Warning: redis-cli not found. Ensure Redis is running on $REDIS_HOST:$REDIS_PORT"
else
  if ! redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" ping &>/dev/null; then
    echo "Redis not responding at $REDIS_HOST:$REDIS_PORT. Start Redis first (e.g. redis-server)."
    exit 1
  fi
fi

PIDFILE=".run_local.pids"
echo -n "" > "$PIDFILE"

start() {
  name="$1"
  shift
  python "$@" &
  echo $! >> "$PIDFILE"
  echo "Started $name (PID $!)"
}

start "imap_poller"    services/imap_poller/main.py
start "normalizer"     workers/normalizer/main.py
start "classifier"     workers/classifier/main.py
start "persister"      workers/persister/main.py
start "watcher"        workers/watcher/watcher_semantic.py

echo ""
echo "All services started. PIDs saved to $PIDFILE"
echo "To stop: kill \$(cat $PIDFILE)"
echo "Or run: xargs kill < $PIDFILE"
