"""Normalize the free-text effective_date in far_class_deviations into an ISO date.

Adds (idempotent) `effective_date_iso DATE` and `effective_date_kind VARCHAR`
columns to far_class_deviations and backfills them from `effective_date`.

Heuristics, in order:
  1. ISO YYYY-MM-DD                  -> as-is, kind='iso'
  2. YYYY.MM.DD                      -> dotted -> iso, kind='iso'
  3. M/D/YYYY or MM/DD/YYYY          -> us-slash -> iso, kind='iso'
  4. "Month D, YYYY" anywhere        -> dateutil parse, kind='long'
     (handles "Month D, YYYY (Effective immediately)" / "...; Alert eff..." too)
  5. "Immediate"/"Immediately"       -> NULL date, kind='immediate'
  6. "Date of issuance ... YYYY"     -> NULL date, kind='issuance'
  7. "X days from signature"         -> NULL date, kind='delta'
  8. anything else                   -> NULL date, kind='unparsed'
"""
from __future__ import annotations

import re
import sys
from datetime import date

import duckdb
from dateutil import parser as dp

import db

LONG_RE = re.compile(
    r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s*\d{4}\b",
    re.I,
)
DOTTED_RE = re.compile(r"^\s*(\d{4})\.(\d{2})\.(\d{2})\s*$")
US_SLASH_RE = re.compile(r"^\s*(\d{1,2})/(\d{1,2})/(\d{4})\s*$")
ISO_RE = re.compile(r"^\s*(\d{4})-(\d{2})-(\d{2})\s*$")


def normalize(raw: str | None) -> tuple[date | None, str]:
    if raw is None or not raw.strip():
        return None, "empty"
    s = raw.strip()
    if m := ISO_RE.match(s):
        return date(int(m[1]), int(m[2]), int(m[3])), "iso"
    if m := DOTTED_RE.match(s):
        return date(int(m[1]), int(m[2]), int(m[3])), "iso"
    if m := US_SLASH_RE.match(s):
        mo, dy, yr = int(m[1]), int(m[2]), int(m[3])
        return date(yr, mo, dy), "iso"
    if m := LONG_RE.search(s):
        try:
            return dp.parse(m.group(0)).date(), "long"
        except (ValueError, OverflowError):
            pass
    low = s.lower()
    if low.startswith(("immediate", "effective upon")):
        return None, "immediate"
    if "date of issuance" in low or "document dated" in low or "model language release" in low or "model text release" in low:
        return None, "issuance"
    if "days from" in low or "days after" in low:
        return None, "delta"
    return None, "unparsed"


def main() -> int:
    con = duckdb.connect(db.DB)
    con.execute("ALTER TABLE far_class_deviations ADD COLUMN IF NOT EXISTS effective_date_iso DATE")
    con.execute("ALTER TABLE far_class_deviations ADD COLUMN IF NOT EXISTS effective_date_kind VARCHAR")

    rows = con.execute("SELECT id, effective_date FROM far_class_deviations").fetchall()
    counts = {}
    for rid, raw in rows:
        d, kind = normalize(raw)
        counts[kind] = counts.get(kind, 0) + 1
        con.execute(
            "UPDATE far_class_deviations SET effective_date_iso=?, effective_date_kind=? WHERE id=?",
            [d, kind, rid],
        )

    print(f"Normalized {len(rows)} rows.")
    for k, v in sorted(counts.items(), key=lambda x: -x[1]):
        print(f"  {k:12s} {v:5d}  ({100*v/len(rows):.1f}%)")

    iso_count = con.execute(
        "SELECT COUNT(*) FROM far_class_deviations WHERE effective_date_iso IS NOT NULL"
    ).fetchone()[0]
    print(f"\nRows with parsed ISO date: {iso_count}/{len(rows)} ({100*iso_count/len(rows):.1f}%)")

    print("\nUnparsed samples (need manual review):")
    for r in con.execute(
        "SELECT agency, deviation_number, effective_date FROM far_class_deviations "
        "WHERE effective_date_kind='unparsed' LIMIT 15"
    ).fetchall():
        print(f"  {r[0]:12s} {r[1]:25s} {r[2]!r}")
    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
