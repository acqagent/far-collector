"""Deterministic parser for FAR Part 52 (Solicitation Provisions and Contract Clauses).

Part 52 is huge (1.9 MB cleaned text, 495 body clauses) and exceeds the LLM's
context window. The structure is regular enough to parse with regex.

Each clause body section starts with a line like:
    52.204-7 System for Award Management.
followed by a prescribe line ('As prescribed in 4.1105(a)(1), insert the
following provision:'), then a title with effective date in parens
('Title (JAN 2026)'), then the body, ending at '(End of provision)' or
'(End of clause)' or the start of the next 52.X-Y section.
"""
from __future__ import annotations

import re
from datetime import datetime

import db
import fetch as ft

PART52_URL = "https://www.acquisition.gov/far-overhaul/far-part-deviation-guide/far-overhaul-part-52"

# Match start-of-line clause headings (the index uses [52.X-Y so bracketed lines won't match here).
SECTION_RE = re.compile(r"^(52\.\d+-\d+)\s+([^.\n]+?)\.", re.M)
DATE_RE = re.compile(r"\(([A-Z][a-z]{2,9}\.?\s+\d{4}|[A-Z]{3,9}\s+\d{4})\)")
KIND_RE = re.compile(r"insert the following (provision|clause)", re.I)
END_MARKER_RE = re.compile(r"\(End of (provision|clause|solicitation provision)\)", re.I)


def parse_part52_text(text: str) -> list[dict]:
    matches = list(SECTION_RE.finditer(text))
    out: list[dict] = []
    for i, m in enumerate(matches):
        start = m.start()
        # Section ends at next clause start, or at end-marker, or end of text.
        next_start = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        section = text[start:next_start]

        number = m.group(1)
        title = m.group(2).strip()

        # Trim section at end-marker if present.
        em = END_MARKER_RE.search(section)
        body = section[: em.end()] if em else section
        body = body.strip()

        # Determine kind from the 'insert the following provision/clause' line.
        km = KIND_RE.search(section[:600])
        if km:
            kind = km.group(1).capitalize()
        elif em:
            kind = em.group(1).capitalize() if em.group(1) != "solicitation provision" else "Provision"
        else:
            kind = "Unknown"

        # Effective date — first parenthesized date pattern after the prescribe line.
        eff = None
        dm = DATE_RE.search(section[:1500])
        if dm:
            eff = dm.group(1)

        out.append({
            "number": number,
            "title": title,
            "kind": kind,
            "effective_date": eff,
            "full_text": body,
            "source_url": PART52_URL,
        })
    # Deduplicate by number, keeping the first body occurrence.
    seen: dict[str, dict] = {}
    for c in out:
        if c["number"] not in seen:
            seen[c["number"]] = c
    return list(seen.values())


async def run() -> int:
    import asyncio  # noqa: F401  (kept for future async work)

    res = await ft.fetch_many([PART52_URL])
    url, html, err = res[0]
    if err or not html:
        print(f"Part 52 fetch failed: {err}")
        return 0
    text = ft.to_clean_text(html)
    clauses = parse_part52_text(text)
    db.init()
    con = db.get()
    for c in clauses:
        con.execute(
            """INSERT OR REPLACE INTO far_provisions_clauses
               (number, title, kind, effective_date, full_text, source_url, scraped_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            [c["number"], c["title"], c["kind"], c["effective_date"],
             c["full_text"], c["source_url"], datetime.now()],
        )
    con.close()
    print(f"Part 52 clauses inserted: {len(clauses)}")
    return len(clauses)


if __name__ == "__main__":
    import asyncio
    asyncio.run(run())
