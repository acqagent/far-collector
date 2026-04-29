"""Search-engine seeding for generic collection runs (not used in FAR mode)."""
from googlesearch import search as gsearch
from ddgs import DDGS
import tldextract

SKIP_DOMAINS = {"facebook.com", "twitter.com", "x.com", "pinterest.com", "instagram.com"}


def google(query: str, n: int = 20) -> list[str]:
    try:
        return list(gsearch(query, num_results=n, lang="en"))
    except Exception as e:
        print(f"Google failed: {e}")
        return []


def duckduckgo(query: str, n: int = 20) -> list[str]:
    try:
        with DDGS() as d:
            return [r["href"] for r in d.text(query, max_results=n)]
    except Exception as e:
        print(f"DDG failed: {e}")
        return []


def multi_search(query: str, n: int = 20) -> list[str]:
    seen, out = set(), []
    for engine in (google, duckduckgo):
        for url in engine(query, n):
            domain = tldextract.extract(url).registered_domain
            if url not in seen and domain not in SKIP_DOMAINS:
                seen.add(url)
                out.append(url)
    return out
