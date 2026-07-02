"""Sync the corpus manifest.csv with the PDFs actually on disk: drop entries
whose PDF is gone, report PDFs that have no manifest entry.

Requires FAR_CORPUS_MANIFEST and FAR_CORPUS_PDF_DIR to be set (see config.py).
Run regenerate_manifest.py afterwards to create rows for the missing PDFs.
"""
import csv
import sys

import config

MANIFEST_PATH = config.CORPUS_MANIFEST
CORPUS_PDF_DIR = config.CORPUS_PDF_DIR


def main() -> int:
    if MANIFEST_PATH is None or CORPUS_PDF_DIR is None:
        print("Set FAR_CORPUS_MANIFEST and FAR_CORPUS_PDF_DIR to use this script "
              "(see config.py).", file=sys.stderr)
        return 2
    if not MANIFEST_PATH.exists():
        print(f"Manifest not found: {MANIFEST_PATH}", file=sys.stderr)
        return 2

    with open(MANIFEST_PATH) as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames
        existing = {row['on_disk_filename']: row for row in reader}
    if not existing:
        print("Manifest is empty — nothing to sync.")
        return 0

    disk_files = {p.name for p in CORPUS_PDF_DIR.glob("*.pdf")}

    removed = [fn for fn in existing if fn not in disk_files]
    missing = [fn for fn in disk_files if fn not in existing]

    if not removed and not missing:
        print("Manifest already in sync with on-disk PDFs")
        return 0

    def preview(label, names):
        print(f"{label} {len(names)}")
        for fn in names[:5]:
            print(f"  {fn}")
        if len(names) > 5:
            print(f"  ... and {len(names) - 5} more")

    preview("Removing manifest entries without on-disk PDFs:", removed)
    preview("On-disk PDFs without manifest entries (run regenerate_manifest.py):",
            sorted(missing))

    rows = [row for fn, row in existing.items() if fn in disk_files]
    rows.sort(key=lambda r: r['on_disk_filename'])

    with open(MANIFEST_PATH, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {len(rows)} rows to {MANIFEST_PATH}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
