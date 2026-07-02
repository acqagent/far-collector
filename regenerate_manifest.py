"""Regenerate the corpus manifest.csv from PDFs on disk + the DuckDB manifest.

Requires FAR_CORPUS_MANIFEST and FAR_CORPUS_PDF_DIR to be set (see config.py).

For each PDF in the corpus directory, metadata is filled from (in order):
  1. the existing manifest.csv row, if any
  2. the DuckDB far_part_pdfs manifest (keyed by on-disk filename)
  3. the filename itself (hash prefix + original name)
"""
import csv
import sys
from datetime import datetime, timezone
from pathlib import Path

import config
from pdf_extract import safe_filename

MANIFEST_PATH = config.CORPUS_MANIFEST
CORPUS_PDF_DIR = config.CORPUS_PDF_DIR

HEADER = ['on_disk_filename', 'url_hash', 'original_filename', 'agency',
          'part_number', 'is_dod', 'source_url', 'pdf_size_bytes']


def get_db_metadata() -> dict[str, dict]:
    """Metadata per on-disk filename, from the DuckDB far_part_pdfs manifest."""
    import duckdb
    meta: dict[str, dict] = {}
    try:
        if not config.DB_PATH.exists():
            return meta
        con = duckdb.connect(str(config.DB_PATH), read_only=True)
        rows = con.execute("""
            SELECT pdf_url, agency, filename, part_number, is_dod
            FROM far_part_pdfs
        """).fetchall()
        con.close()
        for pdf_url, agency, filename, part_number, is_dod in rows:
            key = safe_filename(pdf_url)
            entry = meta.setdefault(key, {
                'url_hash': key.split('_', 1)[0],
                'original_filename': filename,
                'agency': agency,
                'part_numbers': [],
                'is_dod': '1' if is_dod else '0',
                'source_url': pdf_url,
            })
            if part_number is not None and part_number >= 0:
                entry['part_numbers'].append(part_number)
    except Exception as e:
        print(f"Warning: could not read DuckDB: {e}", file=sys.stderr)
    return meta


def read_existing_manifest() -> dict[str, dict]:
    meta = {}
    if MANIFEST_PATH.exists():
        with open(MANIFEST_PATH) as f:
            for row in csv.DictReader(f):
                meta[row['on_disk_filename']] = row
    return meta


def generate_manifest(existing_meta: dict[str, dict], db_meta: dict[str, dict]) -> list[dict]:
    rows = []
    for pdf_path in sorted(CORPUS_PDF_DIR.glob("*.pdf")):
        fname = pdf_path.name
        size = pdf_path.stat().st_size
        existing = existing_meta.get(fname, {})
        db = db_meta.get(fname, {})

        parts = fname.split('_', 1)
        row = {
            'on_disk_filename': fname,
            'url_hash': existing.get('url_hash') or db.get('url_hash') or parts[0],
            'original_filename': (existing.get('original_filename')
                                  or db.get('original_filename')
                                  or (parts[1] if len(parts) > 1 else fname)),
            'agency': existing.get('agency') or db.get('agency') or '',
            'part_number': (existing.get('part_number')
                            or ';'.join(str(n) for n in sorted(set(db.get('part_numbers', []))))),
            'is_dod': existing.get('is_dod') or db.get('is_dod') or '0',
            'source_url': existing.get('source_url') or db.get('source_url') or '',
            'pdf_size_bytes': size,
        }
        rows.append(row)
    return rows


def main() -> int:
    if MANIFEST_PATH is None or CORPUS_PDF_DIR is None:
        print("Set FAR_CORPUS_MANIFEST and FAR_CORPUS_PDF_DIR to use this script "
              "(see config.py).", file=sys.stderr)
        return 2
    if not CORPUS_PDF_DIR.is_dir():
        print(f"Corpus PDF dir not found: {CORPUS_PDF_DIR}", file=sys.stderr)
        return 2

    existing_meta = read_existing_manifest()
    db_meta = get_db_metadata()
    rows = generate_manifest(existing_meta, db_meta)

    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(MANIFEST_PATH, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=HEADER)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {len(rows)} rows to {MANIFEST_PATH}")
    print(f"Date: {datetime.now(timezone.utc).strftime('%Y-%m-%d')}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
