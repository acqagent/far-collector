"""Re-extract title + scope for ALL existing PDFs using the running local model."""
import asyncio
import sys
from datetime import datetime
from pathlib import Path

import pypdf
from pydantic import BaseModel, Field

import config
import models

# Prefer the external corpus if configured, else the collector's own PDF cache.
CORPUS_DIR = config.CORPUS_PDF_DIR or config.PDF_DIR

# Same input ceiling as the main extraction path unless overridden.
MAX_INPUT_CHARS = config.LLM_MAX_INPUT_CHARS

SYSTEM = (
    "You extract structured information from Federal Acquisition Regulation "
    "(FAR) class deviation memos. Report what the document says; do not invent "
    "details that are not in the text."
)


class DeviationSummary(BaseModel):
    title: str = Field(description="Brief title describing the deviation (one line)")
    scope: str = Field(description="2-3 sentence summary of what the deviation covers")


async def extract_from_pdf(path: Path) -> dict | None:
    """Extract text from a PDF and ask the model for title + scope."""
    try:
        pdf = pypdf.PdfReader(str(path))
        text = ""
        for page in pdf.pages:
            text += page.extract_text() or ""
        if len(text) < 200:
            return None

        fname = path.name
        filename_part = fname.split("_", 1)[1] if "_" in fname else fname

        result = await models.complete_json(
            DeviationSummary,
            [
                {"role": "system", "content": SYSTEM},
                {"role": "user", "content": (
                    f"Document filename: {filename_part}\n\n"
                    f"Document text:\n{text[:MAX_INPUT_CHARS]}"
                )},
            ],
            temperature=0.1,
            max_tokens=2048,
        )
        return {"title": result.title, "scope": result.scope}
    except Exception as e:
        print(f"Error processing {path.name}: {e}")
        return None


async def main():
    import duckdb

    ok, detail = models.probe()
    if not ok:
        print(f"LLM endpoint not usable: {detail}", file=sys.stderr)
        return 2
    print(f"LLM: {detail}")

    con = duckdb.connect(str(config.DB_PATH))
    
    # Get all deviations that need title/scope
    rows = con.execute("""
        SELECT id, agency, title, effective_date 
        FROM far_class_deviations 
        WHERE title LIKE '%RFO%Part%' AND scope IS NULL
        LIMIT 50
    """).fetchall()
    
    if not rows:
        print("No rows need enrichment")
        con.close()
        return 0
    
    print(f"Enriching {len(rows)} rows...")
    
    for row in rows:
        row_id, agency, current_title, eff_date = row
        print(f"\n[{agency}] {row_id[:60]}...")
        
        # Reconstruct filename from row_id
        parts = row_id.split('|', 1)
        if len(parts) == 2:
            agency_name, dev_num = parts
            # Find matching PDF in corpus
            for pdf_path in CORPUS_DIR.glob(f'*_{agency_name}*{dev_num}*'):
                result = await extract_from_pdf(pdf_path)
                if result and (result['title'] or result['scope']):
                    con.execute(
                        "UPDATE far_class_deviations SET title=?, scope=?, scraped_at=? WHERE id=?",
                        [result['title'], result['scope'], datetime.now(), row_id]
                    )
                    print(f"  Updated: title={result['title'][:60] if result['title'] else 'None'}")
                    break
            else:
                print(f"  No matching PDF found for {dev_num}")
        else:
            print(f"  Bad row_id format: {row_id}")
    
    con.close()
    print("\nDone!")
    return 0


if __name__ == '__main__':
    sys.exit(asyncio.run(main()))
