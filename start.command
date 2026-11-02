#!/bin/sh
# macOS: double-click this file in Finder (first time: right-click > Open).
cd "$(dirname "$0")" || exit 1
./start.sh "$@"
echo
echo "ClipFlow stopped. Press Return to close this window."
read -r _
