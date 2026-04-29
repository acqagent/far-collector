"""FAR-mode orchestrator. Deterministic seed + LLM extraction.

Phases:
  1. Read manifest (far_parts, far_part_pdfs) produced by far_seed.py
  2. For each Part overview page, fetch HTML and use the 26B worker to extract
     FAR provisions/clauses (52.X-Y rows) into far_provisions_clauses.
  3. For each agency PDF (excluding DoD), download + parse PDF text. Use
     deterministic regex first; fall back to the worker model for missing fields.
     Write into far_class_deviations.
"""
from __future__ import annotations

import asyncio
import sys
from datetime import datetime

import httpx
from rich.console import Console

import db
import extract as ex
import fetch as ft
import pdf_extract as pe

console = Console()


async def crawl_part_pages(only_part: int | None = None) -> int:
    con = db.get()
    rows = con.execute("SELECT part_number, overview_url FROM far_parts ORDER BY part_number").fetchall()
    if only_part:
        rows = [r for r in rows if r[0] == only_part]
    if not rows:
        console.print("[red]No Part rows in manifest. Run: python far_seed.py[/]")
        con.close()
        return 0

    urls = [r[1] for r in rows]
    console.print(f"[cyan]Fetching {len(urls)} Part overview page(s)...[/]")
    fetched = await ft.fetch_many(urls, concurrency=4)
    inserted = 0
    for (part_num, url), (_, html, err) in zip(rows, fetched):
        if err or not html:
            console.print(f"  [red]Part {part_num} fetch failed:[/] {err}")
            continue
        clean = ft.to_clean_text(html)
        if len(clean) < 500:
            console.print(f"  [yellow]Part {part_num}: clean text too short ({len(clean)} chars), skipping[/]")
            continue
        page = await ex.extract_far_clauses(url, clean)
        if not page or not page.clauses:
            console.print(f"  [dim]Part {part_num}: no clauses extracted[/]")
            continue
        for cl in page.clauses:
            con.execute(
                """INSERT OR REPLACE INTO far_provisions_clauses
                   (number, title, kind, effective_date, full_text, source_url, scraped_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                [cl.number, cl.title, cl.kind, cl.effective_date, cl.full_text, url, datetime.now()],
            )
            inserted += 1
        console.print(f"  [green]Part {part_num}:[/] +{len(page.clauses)} clauses (running total {inserted})")
    con.close()
    return inserted


async def crawl_pdfs() -> int:
    con = db.get()
    rows = con.execute("""
        SELECT DISTINCT pdf_url, agency, filename FROM far_part_pdfs
        WHERE is_dod = FALSE
        ORDER BY agency, filename
    """).fetchall()
    if not rows:
        console.print("[red]No non-DoD PDFs in manifest. Run: python far_seed.py[/]")
        con.close()
        return 0

    urls = [r[0] for r in rows]
    console.print(f"[cyan]Downloading {len(urls)} non-DoD agency PDFs...[/]")
    paths = await pe.download_many(urls, concurrency=4)

    inserted = 0
    for pdf_url, agency, filename in rows:
        path = paths.get(pdf_url)
        if not path or not path.exists():
            console.print(f"  [red]{agency} {filename}:[/] download failed")
            continue
        text = pe.extract_text(path)
        if not text or len(text) < 200:
            console.print(f"  [yellow]{agency} {filename}:[/] text too short, skipping")
            continue

        eff = pe.find_effective_date(text)
        dev_num = pe.find_deviation_number(text, fallback_filename=filename)

        # LLM second pass for title + scope; we already have agency + dev_num + date.
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
                console.print(f"  [yellow]Skipping DoD entry from {filename}[/]")
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
    return inserted


async def main(target: str = "all", only_part: int | None = None) -> int:
    if target in ("provisions", "all"):
        n = await crawl_part_pages(only_part=only_part)
        console.print(f"[bold]Provisions/clauses inserted: {n}[/]")
    if target in ("deviations", "all"):
        n = await crawl_pdfs()
        console.print(f"[bold]Class deviations inserted: {n}[/]")
    return 0


if __name__ == "__main__":
    target = sys.argv[1] if len(sys.argv) > 1 else "all"
    only_part = int(sys.argv[2]) if len(sys.argv) > 2 else None
    asyncio.run(main(target, only_part))
