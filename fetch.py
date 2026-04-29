"""Polite async fetcher with raw-HTML cache + trafilatura cleaning."""
import asyncio
import hashlib
from pathlib import Path

import httpx
import trafilatura
from tenacity import retry, stop_after_attempt, wait_exponential

RAW = Path(__file__).parent / "data" / "raw"
RAW.mkdir(parents=True, exist_ok=True)

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; SparkCollector/1.0)"}


@retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=10))
async def fetch_one(url, client):
    try:
        r = await client.get(url, headers=HEADERS, timeout=20, follow_redirects=True)
        if r.status_code != 200 or "text/html" not in r.headers.get("content-type", ""):
            return url, None, f"status {r.status_code}"
        h = hashlib.sha256(url.encode()).hexdigest()[:16]
        (RAW / f"{h}.html").write_text(r.text, errors="ignore")
        return url, r.text, None
    except Exception as e:
        return url, None, str(e)


async def fetch_many(urls, concurrency=8):
    sem = asyncio.Semaphore(concurrency)
    async with httpx.AsyncClient() as client:
        async def bound(u):
            async with sem:
                await asyncio.sleep(0.5)
                return await fetch_one(u, client)
        return await asyncio.gather(*[bound(u) for u in urls])


def to_clean_text(html: str) -> str:
    return trafilatura.extract(html, include_links=True, include_comments=False) or ""
