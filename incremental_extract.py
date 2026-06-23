"""
Incremental extraction over PDFs newly captured by incremental_pull.py.

Reads `logs/new_pdfs_latest.json` (written by incremental_pull.py),
extracts text from each new PDF, runs the deterministic regex pass for
effective date + deviation number, and INSERT-OR-REPLACEs a row into the
DuckDB `far_class_deviations` table.

The LLM second pass (title + scope refinement) is OPTIONAL: it runs only
if the configured vLLM endpoint answers /v1/models. If it isn't running,
this script still records a usable row using the filename as title.

Usage:
    python incremental_extract.py                        # process latest manifest
    python incremental_extract.py --manifest path.json   # specific manifest
    python incremental_extract.py --no-llm               # skip LLM enrichment
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import datetime
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).parent))
import db                       # type: ignore  # noqa: E402
import pdf_extract as pe        # type: ignore  # noqa: E402

LOG_DIR = Path("/home/dgxgape/collector/logs")
DEFAULT_MANIFEST = LOG_DIR / "new_pdfs_latest.json"
LLM_BASE = "http://localhost:8000/v1"   # collector convention; see models.py


def llm_alive() -> bool:
    try:
        r = httpx.get(f"{LLM_BASE}/models", timeout=2)
        return r.status_code == 200
    except Exception:
        return False


async def llm_enrich(pdf_url: str, text: str) -> tuple[str | None, str | None]:
    """Returns (title, scope) from the worker model, or (None, None) on failure."""
    try:
        import extract as ex          # type: ignore
        page = await ex.extract_class_deviations(pdf_url, text[:50000])
        if page and page.deviations:
            d = page.deviations[0]
            return d.title, d.scope
    except Exception as e:
        print(f"  [warn] llm_enrich failed: {e}")
    return None, None


def process(manifest_path: Path, use_llm: bool) -> int:
    if not manifest_path.exists():
        print(f"[error] manifest not found: {manifest_path}")
        return 2
    data = json.loads(manifest_path.read_text())
    new = [r for r in data["new_pdfs"] if r.get("saved_path") and not r.get("is_dod")]
    print(f"manifest: {manifest_path.name}  candidates: {len(new)}")
    if not new:
        return 0

    do_llm = use_llm and llm_alive()
    if use_llm and not do_llm:
        print("  [info] LLM endpoint not reachable — falling back to regex-only extraction.")

    db.init()
    con = db.get()
    inserted = 0

    for rec in new:
        path = Path(rec["saved_path"])
        if not path.exists():
            print(f"  SKIP missing: {path.name}")
            continue
        text = pe.extract_text(path)
        if not text or len(text) < 200:
            print(f"  SKIP unparseable: {path.name}")
            continue

        eff_raw = pe.find_effective_date(text)
        dev_num = pe.find_deviation_number(text, fallback_filename=rec["filename"])
        title, scope = None, None

        if do_llm:
            title, scope = asyncio.run(llm_enrich(rec["pdf_url"], text))
        if not title:
            title = rec["filename"].replace(".pdf", "").replace("_", " ")

        agency = rec["agency"]
        rid = f"{agency}|{dev_num or rec['filename']}"
        con.execute(
            """INSERT OR REPLACE INTO far_class_deviations
               (id, agency, deviation_number, title, effective_date, scope, link, scraped_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            [rid, agency, dev_num, title, eff_raw, scope, rec["pdf_url"], datetime.now()],
        )
        inserted += 1
        print(f"  OK {agency:8} {dev_num or '(no num)':20} eff_raw={eff_raw}  {path.name}")

    con.close()
    print(f"DONE: {inserted} rows inserted/replaced in far_class_deviations")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    ap.add_argument("--no-llm", action="store_true")
    args = ap.parse_args()
    return process(args.manifest, use_llm=not args.no_llm)


if __name__ == "__main__":
    sys.exit(main())
