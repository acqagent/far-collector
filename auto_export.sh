#!/bin/bash
# Detached watcher: blocks until the deviation run PID exits, then runs export.
# Started via nohup so it survives shell/session close.
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"
source .venv/bin/activate
PID=${1:?usage: auto_export.sh <deviation-run-pid>}
echo "[$(date)] auto_export waiting for PID $PID" >> logs/auto_export.log
while kill -0 "$PID" 2>/dev/null; do sleep 30; done
echo "[$(date)] PID $PID exited — running export" >> logs/auto_export.log
python export_far.py all >> logs/auto_export.log 2>&1
echo "[$(date)] export done" >> logs/auto_export.log
