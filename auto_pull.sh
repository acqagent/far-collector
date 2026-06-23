#!/usr/bin/env bash
# Cron-friendly: discover + download new acquisition.gov class deviation PDFs,
# extract them into the DuckDB, and (re)normalize effective-date columns.
#
# Safe to run unattended. All output appended to logs/auto_pull.log with a
# timestamped header. Exit code is best-effort (non-zero on real failures).
#
# Suggested cron (daily 06:30 local):
#   30 6 * * *  /home/dgxgape/collector/auto_pull.sh
#
# Optional flags forwarded to incremental_pull.py:
#   AUTO_PULL_FLAGS="--max-downloads 50"  /home/dgxgape/collector/auto_pull.sh

set -u
set -o pipefail

ROOT="/home/dgxgape/collector"
PY="$ROOT/.venv/bin/python"
LOG="$ROOT/logs/auto_pull.log"
mkdir -p "$ROOT/logs"

stamp() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }
log()   { echo "[$(stamp)] $*" | tee -a "$LOG" >&2; }

log "==== auto_pull start ===="
cd "$ROOT" || { log "FATAL cd $ROOT failed"; exit 2; }

# 1) Scrape + download new PDFs (also updates the DuckDB manifest table)
log "step 1/3: incremental_pull (scrape + download + DB manifest update)"
"$PY" incremental_pull.py --update-db ${AUTO_PULL_FLAGS:-} 2>&1 | tee -a "$LOG"
SCRAPE_RC=${PIPESTATUS[0]}
log "incremental_pull rc=$SCRAPE_RC"

# 2) Extract metadata for any newly-downloaded PDFs
LATEST="$ROOT/logs/new_pdfs_latest.json"
if [[ -f "$LATEST" ]]; then
    NEW_COUNT=$("$PY" -c "import json; print(json.load(open('$LATEST'))['downloaded_count'])" 2>/dev/null || echo 0)
    log "step 2/3: incremental_extract on $NEW_COUNT newly-downloaded PDFs"
    if [[ "$NEW_COUNT" -gt 0 ]]; then
        "$PY" incremental_extract.py 2>&1 | tee -a "$LOG"
        log "incremental_extract rc=${PIPESTATUS[0]}"
    else
        log "  no new PDFs to extract — skipping"
    fi
else
    log "step 2/3: no manifest at $LATEST — skipping extraction"
fi

# 3) Re-run the date normalizer so effective_date_iso is fresh
if [[ -f "$ROOT/normalize_dates.py" ]]; then
    log "step 3/3: normalize_dates"
    "$PY" normalize_dates.py 2>&1 | tee -a "$LOG"
    log "normalize_dates rc=${PIPESTATUS[0]}"
fi

log "==== auto_pull done ===="
exit $SCRAPE_RC
