"""PDF download + text extraction with effective-date heuristics."""
from __future__ import annotations

import asyncio
import hashlib
import re
from pathlib import Path

import httpx
from pypdf import PdfReader
from tenacity import retry, stop_after_attempt, wait_exponential

PDF_DIR = Path(__file__).parent / "data" / "pdfs"
PDF_DIR.mkdir(parents=True, exist_ok=True)

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; SparkCollector/1.0)"}

EFFECTIVE_PATTERNS = [
    re.compile(r"effective\s+date[:\s]+([A-Za-z]+ \d{1,2},? \d{4})", re.I),
    re.compile(r"effective\s+(?:on\s+)?([A-Za-z]+ \d{1,2},? \d{4})", re.I),
    re.compile(r"effective[:\s]+(\d{1,2}/\d{1,2}/\d{2,4})", re.I),
    re.compile(r"effective\s+(?:on\s+)?(\d{4}-\d{2}-\d{2})", re.I),
]
DEVIATION_NUM_PATTERNS = [
    re.compile(r"(?:Class\s+Deviation|CD)[\s#:]*([A-Z]{2,}-\d{4}-\d+[A-Z\-]*)", re.I),
    re.compile(r"Class\s+Deviation\s*\(?\s*Number\s+(\d{2,4}-\d+)\)?", re.I),
    re.compile(r"Class\s+Deviation\s+(\d{2,4}-\d+)", re.I),
    re.compile(r"Deviation\s+(?:No\.?|Number)[\s:]*([A-Z0-9\-/]+)", re.I),
]


def pdf_path(url: str) -> Path:
    h = hashlib.sha256(url.encode()).hexdigest()[:16]
    fname = url.rsplit("/", 1)[-1]
    safe = re.sub(r"[^A-Za-z0-9._-]", "_", fname)
    return PDF_DIR / f"{h}_{safe}"


@retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=10))
async def download_pdf(url: str, client: httpx.AsyncClient) -> Path | None:
    p = pdf_path(url)
    if p.exists() and p.stat().st_size > 1024:
        return p
    r = await client.get(url, headers=HEADERS, timeout=60, follow_redirects=True)
    if r.status_code != 200:
        return None
    p.write_bytes(r.content)
    return p


def extract_text(path: Path, max_pages: int = 60) -> str:
    try:
        reader = PdfReader(str(path))
    except Exception as e:
        return f"[PDF parse error: {e}]"
    chunks = []
    for i, page in enumerate(reader.pages[:max_pages]):
        try:
            chunks.append(page.extract_text() or "")
        except Exception:
            continue
    return "\n".join(chunks)


def find_effective_date(text: str) -> str | None:
    for rx in EFFECTIVE_PATTERNS:
        m = rx.search(text)
        if m:
            return m.group(1).strip()
    return None


def find_deviation_number(text: str, fallback_filename: str | None = None) -> str | None:
    for rx in DEVIATION_NUM_PATTERNS:
        m = rx.search(text)
        if m:
            return m.group(1).strip()
    return fallback_filename


async def download_many(urls: list[str], concurrency: int = 4) -> dict[str, Path | None]:
    sem = asyncio.Semaphore(concurrency)
    out: dict[str, Path | None] = {}
    async with httpx.AsyncClient() as client:
        async def bound(u):
            async with sem:
                await asyncio.sleep(0.3)
                try:
                    out[u] = await download_pdf(u, client)
                except Exception as e:
                    print(f"PDF download failed {u}: {e}")
                    out[u] = None
        await asyncio.gather(*[bound(u) for u in urls])
    return out
