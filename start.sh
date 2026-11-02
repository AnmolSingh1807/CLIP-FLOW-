#!/bin/sh
# ClipFlow launcher for Linux / macOS.   Usage: ./start.sh [port]
cd "$(dirname "$0")" || exit 1
PORT="${1:-8080}"

echo
echo " ClipFlow - starting..."

open_browser() {
  ( sleep 3
    if command -v xdg-open >/dev/null 2>&1; then xdg-open "http://localhost:$PORT" >/dev/null 2>&1
    elif command -v open >/dev/null 2>&1; then open "http://localhost:$PORT" >/dev/null 2>&1
    fi ) &
}

if command -v javac >/dev/null 2>&1; then
  mkdir -p build
  javac -encoding UTF-8 -d build ClipFlowHub.java || { echo " Compilation failed."; exit 1; }
  open_browser
  exec java -cp build ClipFlowHub "$PORT"
elif command -v java >/dev/null 2>&1 && [ -f ClipFlow.jar ]; then
  echo " No JDK found - using the prebuilt ClipFlow.jar."
  open_browser
  exec java -jar ClipFlow.jar "$PORT"
else
  echo " Java 11 or newer is required. Install a free JDK from https://adoptium.net"
  exit 1
fi
