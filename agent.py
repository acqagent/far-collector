"""Planner + relevance scorer for the generic search-driven mode."""
from pydantic import BaseModel, Field

from models import complete_json


class Plan(BaseModel):
    queries: list[str] = Field(description="5-7 diverse search queries")
    rationale: str = Field(description="Why these queries cover the topic well")


class Relevance(BaseModel):
    score: float = Field(description="0.0-1.0, how relevant the page is to the user's prompt")
    reasoning: str = Field(description="One sentence justification")
    keep: bool


async def plan_searches(user_prompt: str, n: int = 6) -> list[str]:
    plan = await complete_json(
        Plan,
        [
            {"role": "system", "content": (
                f"You generate {n} diverse, specific search queries to collect data on a topic.\n"
                "Cover different angles (overview, recent news, technical detail, opposing views, primary sources).\n"
                "Vary recency, source type, and specificity. Avoid duplicate phrasings."
            )},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0.5,
        max_tokens=2048,
    )
    print(f"Plan rationale: {plan.rationale}")
    return plan.queries


async def score_relevance(user_prompt: str, page_title: str, page_body: str) -> Relevance:
    return await complete_json(
        Relevance,
        [
            {"role": "system", "content": (
                f'Score page relevance for the user\'s collection goal: "{user_prompt}".\n'
                "Be strict. Off-topic, shallow, or low-quality pages should score below 0.4."
            )},
            {"role": "user", "content": f"TITLE: {page_title}\n\nBODY (first 3000 chars):\n{page_body[:3000]}"},
        ],
        temperature=0.1,
        max_tokens=1024,
    )


def should_continue(collected: int, target: int, avg_relevance: float) -> bool:
    if collected >= target:
        return False
    if collected > 20 and avg_relevance < 0.35:
        return False
    return True
