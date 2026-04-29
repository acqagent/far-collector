"""Deterministic crawler for acquisition.gov/far-overhaul/far-part-deviation-guide.

Builds a manifest of:
  - every FAR Part overview page (e.g. far-overhaul-part-1, ...-part-52)
  - every agency deviation PDF (one row per (part, agency, pdf_url))
  - status dates extracted from the page ("Issued ... | Updated ...")

The manifest is written to DuckDB and consumed by far_collector.py for
LLM extraction. We DO NOT use search engines for FAR mode.
"""
from __future__ import annotations

import re
import sys
from dataclasses import dataclass, asdict
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup
from rich.console import Console

import db

GUIDE_URL = "https://www.acquisition.gov/far-overhaul/far-part-deviation-guide"
BASE = "https://www.acquisition.gov"

PART_LINK_RE = re.compile(r"/far-overhaul/far-part-deviation-guide/far-overhaul-part-(\d+)$")
PDF_LINK_RE = re.compile(r"/sites/default/files/page_file_uploads/.*\.pdf$", re.I)
ISSUANCE_RE = re.compile(r"Issuance Date:\s*([A-Z][a-z]+\s+\d{1,2},?\s*\d{4})", re.I)
UPDATE_RE = re.compile(r"UPDATE:\s*([A-Z][a-z]+\s+\d{1,2},?\s*\d{4})", re.I)

# Map of agency name fragments (from PDF filenames) to canonical agency labels.
# Keys are upper-case substrings. First match wins, in dict order, so put
# longer/more-specific keys before shorter ones that could collide.
AGENCY_MAP = {
    "USAID": "USAID",
    "PEACE_CORPS": "Peace Corps",
    "PEACECORPS": "Peace Corps",
    "PC_RFO": "Peace Corps",
    "TREASURY": "Treasury",
    "OSHRC": "OSHRC",
    "MSPB": "MSPB",
    "PBGC": "PBGC",
    "NARA": "NARA",
    "OPM": "OPM",
    "FMC": "FMC",
    "NLRB": "NLRB",
    "FEC": "FEC",
    "HUD": "HUD",
    "DOJ": "DOJ",
    "SSA": "SSA",
    "DOE": "DOE",
    "NRC": "NRC",
    "UDALL": "Udall Foundation",
    "HHS": "HHS",
    "NASA": "NASA",
    "DOT": "DOT",
    "CFTC": "CFTC",
    "DHS": "DHS",
    "DOS": "DOS",
    "SEC": "SEC",
    "DOC": "DOC",
    "DOL": "DOL",
    "MCC": "MCC",
    "CPSC": "CPSC",
    "GSA": "GSA",
    "DOI": "DOI",
    "EPA": "EPA",
    "VA": "VA",
    "ED": "ED",
    "USDA": "USDA",
    "DOD": "DoD",          # explicitly tracked so we can EXCLUDE
    "DEPT_OF_DEFENSE": "DoD",
}

DOD_TOKENS = ("DOD", "DEPT_OF_DEFENSE", "DEPARTMENT_OF_DEFENSE", "DEFENSE")


@dataclass
class PartEntry:
    part_number: int
    overview_url: str
    issued: str | None
    updated: str | None
    title_hint: str | None


@dataclass
class PdfEntry:
    part_numbers: list[int]   # one PDF can cover multiple Parts
    agency: str
    pdf_url: str
    filename: str
    is_dod: bool


def agency_from_filename(fname: str) -> tuple[str, bool]:
    upper = fname.upper().replace("-", "_").replace(" ", "_")
    is_dod = any(tok in upper for tok in DOD_TOKENS)
    for key, label in AGENCY_MAP.items():
        if key in upper:
            return label, is_dod
    return "Unknown", is_dod


def parts_from_filename(fname: str) -> list[int]:
    """Pull Part numbers from filenames like 'USAID_RFO_Deviation_Parts-1-6-10-11.pdf'."""
    m = re.search(r"Parts?[-_]([0-9_\-andAND]+)\.pdf", fname, re.I)
    if not m:
        m2 = re.search(r"Part[-_](\d+)", fname, re.I)
        return [int(m2.group(1))] if m2 else []
    raw = m.group(1)
    nums = re.findall(r"\d+", raw)
    return [int(n) for n in nums]


def fetch_guide_html() -> str:
    with httpx.Client(timeout=30, follow_redirects=True,
                      headers={"User-Agent": "SparkCollector/1.0"}) as c:
        r = c.get(GUIDE_URL)
        r.raise_for_status()
        return r.text


def parse_guide(html: str) -> tuple[list[PartEntry], list[PdfEntry]]:
    soup = BeautifulSoup(html, "lxml")
    parts: dict[int, PartEntry] = {}
    pdfs: list[PdfEntry] = []

    # Pass 1: find every Part overview link and the surrounding heading/status.
    for a in soup.find_all("a", href=True):
        href = a["href"].split("?")[0].split("#")[0]
        m = PART_LINK_RE.search(href)
        if m:
            n = int(m.group(1))
            if n in parts:
                continue
            full = urljoin(BASE, href)
            anchor_text = a.get_text(" ", strip=True)
            # Walk up to find <h3> or sibling div that contains the status text.
            issued, updated = None, None
            container = a
            for _ in range(3):
                container = container.parent
                if container is None:
                    break
                if container.name in ("h2", "h3", "h4"):
                    txt = container.get_text(" ", strip=True)
                    im = ISSUANCE_RE.search(txt)
                    um = UPDATE_RE.search(txt)
                    issued = im.group(1) if im else None
                    updated = um.group(1) if um else None
                    break
            parts[n] = PartEntry(n, full, issued, updated, anchor_text)

    # Pass 2: every agency PDF link.
    seen_pdfs = set()
    for a in soup.find_all("a", href=True):
        href = a["href"].split("?")[0]
        if not PDF_LINK_RE.search(href):
            continue
        if href in seen_pdfs:
            continue
        seen_pdfs.add(href)
        full = urljoin(BASE, href)
        fname = href.rsplit("/", 1)[-1]
        agency, is_dod = agency_from_filename(fname)
        pnums = parts_from_filename(fname)
        pdfs.append(PdfEntry(pnums, agency, full, fname, is_dod))

    return sorted(parts.values(), key=lambda p: p.part_number), pdfs


def write_manifest(parts: list[PartEntry], pdfs: list[PdfEntry]) -> None:
    db.init()
    con = db.get()
    con.execute("""
        CREATE TABLE IF NOT EXISTS far_parts (
            part_number INTEGER PRIMARY KEY,
            overview_url VARCHAR,
            issued VARCHAR,
            updated VARCHAR,
            title_hint VARCHAR
        );
        CREATE TABLE IF NOT EXISTS far_part_pdfs (
            pdf_url VARCHAR,
            agency VARCHAR,
            filename VARCHAR,
            part_number INTEGER,        -- -1 means unknown/unparsed
            is_dod BOOLEAN,
            PRIMARY KEY (pdf_url, part_number)
        );
    """)
    con.execute("DELETE FROM far_parts")
    con.execute("DELETE FROM far_part_pdfs")
    for p in parts:
        con.execute(
            "INSERT INTO far_parts VALUES (?, ?, ?, ?, ?)",
            [p.part_number, p.overview_url, p.issued, p.updated, p.title_hint],
        )
    for pdf in pdfs:
        targets = pdf.part_numbers or [-1]
        for n in targets:
            con.execute(
                "INSERT OR REPLACE INTO far_part_pdfs VALUES (?, ?, ?, ?, ?)",
                [pdf.pdf_url, pdf.agency, pdf.filename, n, pdf.is_dod],
            )
    con.close()


def main() -> int:
    console = Console()
    console.print(f"[cyan]Fetching {GUIDE_URL}[/]")
    html = fetch_guide_html()
    parts, pdfs = parse_guide(html)
    console.print(f"[green]Parts found:[/] {len(parts)}")
    for p in parts:
        console.print(f"  Part {p.part_number}: issued {p.issued} updated {p.updated}")
    console.print(f"[green]Agency PDFs:[/] {len(pdfs)} (excluding DoD: {sum(1 for x in pdfs if not x.is_dod)})")
    agencies = sorted({pdf.agency for pdf in pdfs if not pdf.is_dod})
    console.print(f"[green]Agencies (non-DoD):[/] {', '.join(agencies)}")
    write_manifest(parts, pdfs)
    console.print("[bold]Manifest written to DuckDB.[/]")
    return 0


if __name__ == "__main__":
    sys.exit(main())
