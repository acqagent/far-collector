"""Retry the 15 non-DoD manifest PDFs that didn't land on disk in the Apr 27 run.

Selects manifest rows whose filename has no matching on-disk PDF, redownloads,
extracts text + LLM second pass, and INSERTs/REPLACEs into far_class_deviations.
"""
from __future__ import annotations

import asyncio
import os
import re
import sys
from datetime import datetime
from pathlib import Path

from rich.console import Console

import db
import extract as ex
import pdf_extract as pe

console = Console()
PDF_DIR = Path(__file__).parent / "data" / "pdfs"


def on_disk_originals() -> set[str]:
    out = set()
    for d in os.listdir(PDF_DIR):
        m = re.match(r"^[0-9a-f]{16}_(.+)$", d)
        if m:
            out.add(m.group(1))
    return out


async def main() -> int:
    con = db.get()
    rows = con.execute("""
        SELECT DISTINCT pdf_url, agency, filename
        FROM far_part_pdfs
        WHERE is_dod = FALSE
        ORDER BY agency, filename
    """).fetchall()
    on_disk = on_disk_originals()
    missing = [r for r in rows if r[2] not in on_disk]
    console.print(f"[cyan]Non-DoD missing: {len(missing)}[/]")
    if not missing:
        return 0

    urls = [r[0] for r in missing]
    paths = await pe.download_many(urls, concurrency=4)

    inserted = 0
    failed = 0
    for pdf_url, agency, filename in missing:
        path = paths.get(pdf_url)
        if not path or not path.exists():
            console.print(f"  [red]download FAIL[/] {agency} {filename}")
            failed += 1
            continue
        text = pe.extract_text(path)
        if not text or len(text) < 200:
            console.print(f"  [yellow]text too short[/] {agency} {filename}")
            failed += 1
            continue
        eff = pe.find_effective_date(text)
        dev_num = pe.find_deviation_number(text, fallback_filename=filename)
        page = await ex.extract_class_deviations(pdf_url, text[:50000])
        title, scope = None, None
        if page and page.deviations:
            d = page.deviations[0]
            title = d.title
            scope = d.scope
            if not eff:
                eff = d.effective_date
            if not dev_num and d.deviation_number:
                dev_num = d.deviation_number
            if d.agency and d.agency.upper() in {"DOD", "DEPARTMENT OF DEFENSE"}:
                console.print(f"  [yellow]Skipping DoD-tagged {filename}[/]")
                continue
        else:
            title = filename.replace(".pdf", "").replace("_", " ")

        rid = f"{agency}|{dev_num or filename}"
        con.execute(
            """INSERT OR REPLACE INTO far_class_deviations
               (id, agency, deviation_number, title, effective_date, scope, link, scraped_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            [rid, agency, dev_num, title, eff, scope, pdf_url, datetime.now()],
        )
        inserted += 1
        console.print(f"  [green]{agency}[/] {dev_num or '(no num)'} eff={eff}")
    con.close()
    console.print(f"[bold]inserted={inserted} failed={failed}[/]")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
