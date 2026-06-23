"""
Incremental scrape: discover and download newly-added FAR class deviation PDFs
from acquisition.gov/far-overhaul/far-part-deviation-guide.

Compared to far_seed.py + far_collector.py (which fully rebuild the manifest
and re-run extraction over everything), this script:

  1. Fetches the live deviation-guide HTML.
  2. Extracts every PDF URL it links to.
  3. Diffs against PDFs already on disk in data/pdfs/ AND
     /home/dgxgape/far-deviations/corpus/pdfs/.
  4. Downloads only the new ones into BOTH locations (deterministic
     `<sha256[:16]>_<safe-filename>` naming, matching the rest of the corpus).
  5. Writes a JSON manifest of new PDFs to logs/new_pdfs_<UTC>.json so
     downstream extraction (and the HHS P&C update flow) can pick them up.
  6. Optionally appends the new rows to the DuckDB manifest tables so they
     show up in subsequent far_collector.py runs.

Designed to be safe to run on a cron without supervision: idempotent,
no LLM calls, exit code != 0 only on hard failures.

Usage:
    python incremental_pull.py                  # dry-run-ish: lists new PDFs, downloads them
    python incremental_pull.py --no-download    # only print what's new
    python incremental_pull.py --update-db      # also INSERT new rows into DuckDB manifest
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

# Re-use logic from the existing seeder so naming stays in sync
sys.path.insert(0, str(Path(__file__).parent))
from far_seed import (  # noqa: E402
    GUIDE_URL, BASE, PDF_LINK_RE, agency_from_filename, parts_from_filename,
)

COLLECTOR_PDF_DIR = Path("/home/dgxgape/collector/data/pdfs")
CORPUS_PDF_DIR = Path("/home/dgxgape/far-deviations/corpus/pdfs")
LOG_DIR = Path("/home/dgxgape/collector/logs")
HEADERS = {"User-Agent": "SparkCollector/1.0 incremental-pull"}


def safe_filename(url: str) -> str:
    h = hashlib.sha256(url.encode()).hexdigest()[:16]
    fname = url.rsplit("/", 1)[-1]
    safe = re.sub(r"[^A-Za-z0-9._-]", "_", fname)
    return f"{h}_{safe}"


def already_have(url: str) -> bool:
    fn = safe_filename(url)
    p1 = COLLECTOR_PDF_DIR / fn
    p2 = CORPUS_PDF_DIR / fn
    return (p1.exists() and p1.stat().st_size > 1024) or \
           (p2.exists() and p2.stat().st_size > 1024)


def fetch_guide_html(client: httpx.Client) -> str:
    r = client.get(GUIDE_URL, timeout=60)
    r.raise_for_status()
    return r.text


def discover_pdf_urls(html: str) -> list[str]:
    soup = BeautifulSoup(html, "lxml")
    urls = []
    seen = set()
    for a in soup.find_all("a", href=True):
        href = a["href"].split("?")[0]
        if not PDF_LINK_RE.search(href):
            continue
        if href in seen:
            continue
        seen.add(href)
        urls.append(urljoin(BASE, href))
    return urls


def download_one(url: str, client: httpx.Client) -> tuple[Path | None, str | None]:
    """Returns (path-on-disk, error). Writes to BOTH collector and corpus dirs."""
    try:
        r = client.get(url, timeout=120, follow_redirects=True)
        if r.status_code != 200:
            return None, f"status {r.status_code}"
        if len(r.content) < 1024:
            return None, f"too small ({len(r.content)} bytes)"
        fn = safe_filename(url)
        for d in (COLLECTOR_PDF_DIR, CORPUS_PDF_DIR):
            d.mkdir(parents=True, exist_ok=True)
            (d / fn).write_bytes(r.content)
        return COLLECTOR_PDF_DIR / fn, None
    except Exception as e:
        return None, f"{type(e).__name__}: {e}"


def update_db(new_records: list[dict]) -> int:
    """INSERT OR REPLACE new PDF entries into far_part_pdfs. Skips on import error."""
    try:
        import db  # type: ignore
    except Exception as e:
        print(f"[warn] cannot import db.py — skipping DB update: {e}")
        return 0
    inserted = 0
    db.init()
    con = db.get()
    for rec in new_records:
        targets = rec["part_numbers"] or [-1]
        for n in targets:
            con.execute(
                "INSERT OR REPLACE INTO far_part_pdfs VALUES (?, ?, ?, ?, ?)",
                [rec["pdf_url"], rec["agency"], rec["filename"], n, rec["is_dod"]],
            )
            inserted += 1
    con.close()
    return inserted


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--no-download", action="store_true",
                    help="discover only; do not download new PDFs")
    ap.add_argument("--update-db", action="store_true",
                    help="INSERT new rows into the DuckDB manifest")
    ap.add_argument("--max-downloads", type=int, default=None,
                    help="cap the number of new PDFs to fetch this run")
    ap.add_argument("--sleep", type=float, default=0.5,
                    help="seconds to sleep between downloads (be polite)")
    args = ap.parse_args()

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    started = datetime.now(timezone.utc)
    print(f"[{started.isoformat(timespec='seconds')}] fetching guide page")

    with httpx.Client(headers=HEADERS, follow_redirects=True) as client:
        html = fetch_guide_html(client)
        all_urls = discover_pdf_urls(html)
        print(f"  {len(all_urls)} PDF URLs on guide page")

        new_urls = [u for u in all_urls if not already_have(u)]
        print(f"  {len(new_urls)} are new (not yet on disk)")

        records = []
        for u in new_urls:
            fname = u.rsplit("/", 1)[-1]
            agency, is_dod = agency_from_filename(fname)
            records.append({
                "pdf_url": u,
                "filename": fname,
                "agency": agency,
                "is_dod": is_dod,
                "part_numbers": parts_from_filename(fname),
                "saved_path": None,
                "error": None,
            })

        if not args.no_download and records:
            cap = args.max_downloads if args.max_downloads is not None else len(records)
            for i, rec in enumerate(records[:cap], 1):
                # skip DoD by convention (DFARS lives elsewhere)
                if rec["is_dod"]:
                    rec["error"] = "skipped: DoD"
                    print(f"  [{i}/{cap}] SKIP (DoD): {rec['filename']}")
                    continue
                path, err = download_one(rec["pdf_url"], client)
                rec["saved_path"] = str(path) if path else None
                rec["error"] = err
                tag = "OK" if path else f"FAIL: {err}"
                print(f"  [{i}/{cap}] {tag}: {rec['filename']}")
                time.sleep(args.sleep)

    if args.update_db and records:
        downloaded = [r for r in records if r["saved_path"]]
        n = update_db(downloaded)
        print(f"  DB: inserted/replaced {n} far_part_pdfs rows")

    finished = datetime.now(timezone.utc)
    out = {
        "started_utc": started.isoformat(timespec="seconds"),
        "finished_utc": finished.isoformat(timespec="seconds"),
        "duration_sec": round((finished - started).total_seconds(), 1),
        "guide_url": GUIDE_URL,
        "total_pdfs_on_guide": len(all_urls),
        "new_pdfs_count": len(records),
        "downloaded_count": sum(1 for r in records if r["saved_path"]),
        "failed_count": sum(1 for r in records if r["error"] and not r["saved_path"]),
        "new_pdfs": records,
    }
    stamp = started.strftime("%Y%m%dT%H%M%SZ")
    log_path = LOG_DIR / f"new_pdfs_{stamp}.json"
    log_path.write_text(json.dumps(out, indent=2))
    print(f"  manifest: {log_path}")
    if records and not args.no_download:
        # also write a stable "latest" pointer for downstream consumers
        (LOG_DIR / "new_pdfs_latest.json").write_text(json.dumps(out, indent=2))

    # Treat as success if every error is "expected": skipped DoD, or 404 on the
    # source page (acquisition.gov occasionally lists PDFs whose URLs are dead —
    # not a script bug). Real failures (network errors, 5xx, parse errors) → rc=1.
    def expected(err: str) -> bool:
        return err.startswith("skipped") or err.startswith("status 404")
    real_fails = [r for r in records if r["error"] and not expected(r["error"])]
    if real_fails:
        print(f"  [warn] {len(real_fails)} real failures (excluding DoD-skipped + source 404s)")
    return 0 if not real_fails else 1


if __name__ == "__main__":
    sys.exit(main())
