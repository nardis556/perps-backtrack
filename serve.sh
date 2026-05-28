#!/bin/bash
DIR="$(cd "$(dirname "$0")" && pwd)"
PORT=${1:-8082}
echo "Serving perps-backtrack at http://$(hostname -I | awk '{print $1}'):$PORT"
cd "$DIR" && python3 -m http.server "$PORT"
