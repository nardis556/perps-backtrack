#!/bin/bash
PORT=${1:-8082}
echo "Serving perps-backtrack at http://$(hostname -I | awk '{print $1}'):$PORT"
python3 -m http.server "$PORT"
