#!/bin/bash
# Stonks Tracker daemon control — start/stop/restart/status.
# `start` is idempotent, so cron can call it on a timer as a keep-alive.
#
#   ./service.sh start      # background it on $PORT (default 8000)
#   ./service.sh status
#   ./service.sh restart
#   ./service.sh stop

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PORT="${PORT:-8000}"
HOST="${HOST:-0.0.0.0}"          # 0.0.0.0 so the port mapping reaches it
PID_FILE="$DIR/daemon.pid"
LOG_FILE="$DIR/daemon.log"

alive() {                         # is the recorded pid still our uvicorn?
    [ -f "$PID_FILE" ] || return 1
    local pid
    pid=$(cat "$PID_FILE" 2>/dev/null)
    [ -n "$pid" ] && ps -p "$pid" -o args= 2>/dev/null | grep -q "uvicorn app:app"
}

status() {
    if alive; then
        echo "Stonks Tracker is running (PID: $(cat "$PID_FILE"), port $PORT)"
        return 0
    fi
    # A stale file outlives a crash; clear it so start doesn't trust it.
    [ -f "$PID_FILE" ] && rm -f "$PID_FILE" && echo "Stale PID file cleared."
    echo "Stonks Tracker is not running."
    return 1
}

stop() {
    if alive; then
        local pid
        pid=$(cat "$PID_FILE")
        echo "Stopping Stonks Tracker (PID: $pid)..."
        kill "$pid"
        for _ in $(seq 20); do alive || break; sleep 0.5; done
        alive && { echo "Still up after 10s — SIGKILL."; kill -9 "$pid"; }
        echo "Stopped."
    else
        echo "Not running."
    fi
    rm -f "$PID_FILE"
}

start() {
    if status > /dev/null; then
        echo "Already running (PID: $(cat "$PID_FILE"))."
        exit 0
    fi
    # Someone else on the port means a stray instance or another app; uvicorn
    # would exit immediately and cron would retry forever, so say why.
    if ss -ltn 2>/dev/null | grep -q ":$PORT "; then
        echo "Port $PORT is already in use by something else — not starting." >&2
        exit 1
    fi

    echo "Starting Stonks Tracker on $HOST:$PORT..."
    cd "$DIR" || exit 1
    # run.sh execs uvicorn, so $! is the server itself, not a wrapper shell.
    HOST="$HOST" PORT="$PORT" nohup ./run.sh >> "$LOG_FILE" 2>&1 &
    local pid=$!
    echo "$pid" > "$PID_FILE"
    sleep 3
    if alive; then
        echo "Started with PID: $pid"
        echo "Logs: $LOG_FILE"
    else
        echo "Failed to start — last lines of $LOG_FILE:" >&2
        tail -n 15 "$LOG_FILE" >&2
        rm -f "$PID_FILE"
        exit 1
    fi
}

case "$1" in
    start)   start ;;
    stop)    stop ;;
    restart) stop; sleep 1; start ;;
    status)  status ;;
    *)       echo "Usage: $0 {start|stop|restart|status}"; exit 1 ;;
esac
