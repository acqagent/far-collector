"""Sync manifest.csv: remove entries that don't have on-disk PDFs, add entries for PDFs on disk without manifest entries."""
import csv, hashlib
from pathlib import Path

MANIFEST_PATH = Path("/home/dgxgape/far-deviations/manifest.csv")
COLLECTOR_PDF_DIR = Path("/home/dgxgape/far-deviations/corpus/pdfs")

# Read existing manifest
existing = {}
with open(MANIFEST_PATH) as f:
    reader = csv.DictReader(f)
    for row in reader:
        existing[row['on_disk_filename']] = row

# Read on-disk PDFs
disk_files = set(p.name for p in COLLECTOR_PDF_DIR.glob("*.pdf"))

# Remove manifest entries without on-disk PDFs
removed = [fn for fn in existing if fn not in disk_files]
# Add on-disk PDFs without manifest entries  
missing = [fn for fn in disk_files if fn not in existing]

if removed or missing:
    print(f"Removing {len(removed)} manifest entries without on-disk PDFs")
    for fn in removed[:5]:
        print(f"  - {fn}")
    if len(removed) > 5:
        print(f"  ... and {len(removed)-5} more")
    
    print(f"Adding {len(missing)} on-disk PDFs without manifest entries")
    for fn in missing[:5]:
        print(f"  + {fn}")
    if len(missing) > 5:
        print(f"  ... and {len(missing)-5} more")
    
    # Keep only entries that have on-disk PDFs
    header = list(existing.values())[0].keys()
    rows = [row for fn, row in existing.items() if fn in disk_files]
    rows.sort(key=lambda r: r['on_disk_filename'])
    
    with open(MANIFEST_PATH, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {len(rows)} rows to manifest.csv")
else:
    print("Manifest already in sync with on-disk PDFs")

