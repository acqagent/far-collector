"""Regenerate manifest.csv from PDFs on disk + DuckDB dev_data."""
import csv, hashlib, os, sys
from pathlib import Path
from datetime import datetime

MANIFEST_PATH = Path("/home/dgxgape/far-deviations/manifest.csv")
COLLECTOR_PDF_DIR = Path("/home/dgxgape/far-deviations/corpus/pdfs")

# Read existing manifest to get metadata from DuckDB if available
def get_db_metadata():
    """Read metadata from collector DuckDB if it exists."""
    import duckdb
    try:
        db_path = Path("/home/dgxgape/collector/data/collector.duckdb")
        if db_path.exists():
            con = duckdb.connect(str(db_path))
            rows = con.execute("""
                SELECT id, agency, deviation_number, effective_date, 
                       pdf_size_bytes, source_url 
                FROM far_class_deviations
            """).fetchall()
            con.close()
            meta = {}
            for row in rows:
                meta[row[0]] = {
                    'agency': row[1], 'deviation_number': row[2],
                    'effective_date': row[3], 'pdf_size_bytes': row[5],
                    'source_url': row[5] if row[5] else ''
                }
            return meta
    except Exception as e:
        print(f"Warning: could not read DuckDB: {e}", file=sys.stderr)
    return {}

# Build metadata from existing manifest
def read_existing_manifest():
    meta = {}
    if MANIFEST_PATH.exists():
        with open(MANIFEST_PATH) as f:
            reader = csv.DictReader(f)
            for row in reader:
                meta[row['on_disk_filename']] = row
    return meta

# Generate new manifest from on-disk PDFs
def generate_manifest(existing_meta):
    rows = []
    for pdf_path in sorted(COLLECTOR_PDF_DIR.glob("*.pdf")):
        fname = pdf_path.name
        size = pdf_path.stat().st_size
        
        # Use existing metadata if available
        if fname in existing_meta:
            row = {
                'on_disk_filename': fname,
                'url_hash': existing_meta[fname].get('url_hash', ''),
                'original_filename': existing_meta[fname].get('original_filename', ''),
                'agency': existing_meta[fname].get('agency', ''),
                'part_number': existing_meta[fname].get('part_number', ''),
                'is_dod': existing_meta[fname].get('is_dod', ''),
                'source_url': existing_meta[fname].get('source_url', ''),
                'pdf_size_bytes': size
            }
        else:
            # Extract metadata from filename
            parts = fname.split('_', 1)
            url_hash = parts[0] if parts else hashlib.sha256(fname.encode()).hexdigest()[:16]
            orig_name = parts[1] if len(parts) > 1 else fname
            row = {
                'on_disk_filename': fname,
                'url_hash': url_hash,
                'original_filename': orig_name,
                'agency': '',
                'part_number': '',
                'is_dod': '0',
                'source_url': '',
                'pdf_size_bytes': size
            }
        rows.append(row)
    return rows

# Main
existing_meta = read_existing_manifest()
db_meta = get_db_metadata()
rows = generate_manifest(existing_meta)

# Write new manifest
header = ['on_disk_filename', 'url_hash', 'original_filename', 'agency', 
          'part_number', 'is_dod', 'source_url', 'pdf_size_bytes']
with open(MANIFEST_PATH, 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=header)
    writer.writeheader()
    writer.writerows(rows)

print(f"Wrote {len(rows)} rows to {MANIFEST_PATH}")
print(f"Date: {datetime.utcnow().strftime('%Y-%m-%d')}")
