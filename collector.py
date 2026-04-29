"""Generic search-driven orchestrator (per doc Step 10)."""
import asyncio
import sys
import uuid
from datetime import datetime

from rich.console import Console

import agent
import db
import extract
import fetch
import search

console = Console()


async def run(prompt: str, target_pages: int = 50, max_depth: int = 1):
    db.init()
    con = db.get()
    run_id = str(uuid.uuid4())[:8]
    con.execute("INSERT INTO runs (run_id, prompt) VALUES (?, ?)", [run_id, prompt])
    console.print(f"[bold green]Run {run_id}[/]: {prompt}")

    console.print("[cyan]Planning search queries with Gemma 4 31B...[/]")
    queries = await agent.plan_searches(prompt, n=6)
    for q in queries:
        console.print(f"  - {q}")

    frontier = []
    for q in queries:
        frontier.extend(search.multi_search(q, n=15))
    frontier = list(dict.fromkeys(frontier))
    console.print(f"[cyan]Frontier seeded with {len(frontier)} URLs[/]")

    collected = 0
    depth = 0
    relevances = []

    while frontier and collected < target_pages and depth <= max_depth:
        batch = frontier[:20]
        frontier = frontier[20:]

        already = con.execute("SELECT url FROM urls WHERE url = ANY(?)", [batch]).fetchall()
        seen = {r[0] for r in already}
        batch = [u for u in batch if u not in seen]
        if not batch:
            continue

        for u in batch:
            con.execute("INSERT OR IGNORE INTO urls (url, status, depth) VALUES (?, 'pending', ?)", [u, depth])

        results = await fetch.fetch_many(batch)

        extract_tasks = []
        for url, html, err in results:
            if err or not html:
                con.execute("UPDATE urls SET status='failed', error=? WHERE url=?", [err, url])
                continue
            clean = fetch.to_clean_text(html)
            extract_tasks.append((url, clean, extract.extract_page(url, clean, prompt)))

        new_links = []
        for url, clean, task in extract_tasks:
            page = await task
            if not page:
                con.execute("UPDATE urls SET status='extract_failed' WHERE url=?", [url])
                continue
            rel = await agent.score_relevance(prompt, page.title, page.body)
            if rel.keep and rel.score >= 0.4:
                con.execute(
                    """INSERT OR REPLACE INTO pages
                       (url, title, author, published, body, topics, relevance)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    [url, page.title, page.author, page.published, page.body, page.topics, rel.score],
                )
                con.execute("UPDATE urls SET status='done', fetched_at=? WHERE url=?", [datetime.now(), url])
                collected += 1
                relevances.append(rel.score)
                new_links.extend(page.follow_links[:5])
                console.print(f"  [green]check[/] {page.title[:60]} (rel={rel.score:.2f})")
            else:
                con.execute("UPDATE urls SET status='low_relevance' WHERE url=?", [url])
                console.print(f"  [dim]skip {page.title[:60]} (rel={rel.score:.2f}: {rel.reasoning})[/]")

        if depth < max_depth:
            frontier.extend(new_links)
            frontier = list(dict.fromkeys(frontier))

        avg_rel = sum(relevances) / len(relevances) if relevances else 0
        console.print(f"[dim]Collected {collected}/{target_pages}, avg rel {avg_rel:.2f}, frontier {len(frontier)}[/]")

        if not await agent.should_continue(collected, target_pages, avg_rel):
            break
        if not frontier:
            depth += 1

    con.execute(
        "UPDATE runs SET ended_at=?, urls_collected=? WHERE run_id=?",
        [datetime.now(), collected, run_id],
    )
    con.close()
    console.print(f"[bold]Done. Collected {collected} pages.[/]")


if __name__ == "__main__":
    prompt = " ".join(sys.argv[1:]) or "small business tax deductions 2025"
    asyncio.run(run(prompt, target_pages=50, max_depth=1))
