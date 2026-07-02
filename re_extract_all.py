"""Re-extract title + scope for ALL existing PDFs using the running Qwen model."""
import asyncio
import json
import sys
import pypdf
import httpx
from datetime import datetime
from pathlib import Path

import config

# Setup model
from openai import OpenAI
client = OpenAI(base_url=config.LLM_BASE_URL, api_key=config.LLM_API_KEY)

# Prefer the external corpus if configured, else the collector's own PDF cache.
CORPUS_DIR = config.CORPUS_PDF_DIR or config.PDF_DIR

# System prompt
SYSTEM = (
    "You are an assistant that extracts structured information from Federal "
    "Acquisition Regulation (FAR) class deviation memos. Return ONLY a JSON "
    "object with these fields:\n"
    "  - \"title\": brief title describing the deviation (one line)\n"
    "  - \"scope\": 2-3 sentence summary of what the deviation covers\n"
    "Do not include any text outside the JSON object.\n"
)

async def extract_from_pdf(path: Path) -> dict | None:
    """Extract text from PDF and send to LLM for title + scope."""
    try:
        pdf = pypdf.PdfReader(str(path))
        text = ''
        for page in pdf.pages:
            text += page.extract_text() or ''
        if len(text) < 200:
            return None
        
        fname = path.name
        filename_part = fname.split('_', 1)[1] if '_' in fname else fname
        
        response = client.chat.completions.create(
            model=config.LLM_MODEL,
            messages=[
                {'role': 'system', 'content': SYSTEM},
                {'role': 'user', 'content': (
                    f"Document filename: {filename_part}\n\n"
                    f"Document text (first 12000 chars):\n{text[:12000]}"
                )},
            ],
            temperature=0.1,
            max_tokens=300,
        )
        
        content = response.choices[0].message.content or ''
        try:
            # Parse JSON from response (may contain reasoning prefix)
            import re
            json_match = re.search(r'\{.*\}', content, re.DOTALL)
            if json_match:
                result = json.loads(json_match.group())
                return {
                    'title': result.get('title', ''),
                    'scope': result.get('scope', ''),
                }
        except (json.JSONDecodeError, AttributeError):
            pass
        
        return {
            'title': '',
            'scope': '',
        }
    except Exception as e:
        print(f"Error processing {path.name}: {e}")
        return None


async def main():
    import duckdb
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
        return
    
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


if __name__ == '__main__':
    asyncio.run(main())
