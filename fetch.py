"""Polite async fetcher with raw-HTML cache + trafilatura cleaning.

Fetched pages are always written to the cache in config.RAW_DIR. Cache reads
are opt-in: pass use_cache=True (or set FAR_FETCH_USE_CACHE=1) to serve
previously fetched URLs from disk — useful when re-running extraction after
prompt/model changes without hammering acquisition.gov.
"""
import asyncio
import hashlib

import httpx
import trafilatura
from tenacity import retry, stop_after_attempt, wait_exponential

import config

RAW = config.RAW_DIR
RAW.mkdir(parents=True, exist_ok=True)

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; SparkCollector/1.0)"}


def cache_path(url):
    h = hashlib.sha256(url.encode()).hexdigest()[:16]
    return RAW / f"{h}.html"


@retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=10))
async def fetch_one(url, client, use_cache=False):
    p = cache_path(url)
    if use_cache and p.exists():
        return url, p.read_text(errors="ignore"), None
    try:
        r = await client.get(url, headers=HEADERS, timeout=20, follow_redirects=True)
        if r.status_code != 200 or "text/html" not in r.headers.get("content-type", ""):
            return url, None, f"status {r.status_code}"
        p.write_text(r.text, errors="ignore")
        return url, r.text, None
    except Exception as e:
        return url, None, str(e)


async def fetch_many(urls, concurrency=8, use_cache=None):
    if use_cache is None:
        use_cache = config.FETCH_USE_CACHE
    sem = asyncio.Semaphore(concurrency)
    async with httpx.AsyncClient() as client:
        async def bound(u):
            async with sem:
                await asyncio.sleep(0.5)
                return await fetch_one(u, client, use_cache=use_cache)
        return await asyncio.gather(*[bound(u) for u in urls])


def to_clean_text(html: str) -> str:
    return trafilatura.extract(html, include_links=True, include_comments=False) or ""
