"""31B Dense planner + relevance scorer."""
from pydantic import BaseModel, Field

from models import planner, PLANNER_MODEL


class Plan(BaseModel):
    queries: list[str] = Field(description="5-7 diverse search queries")
    rationale: str = Field(description="Why these queries cover the topic well")


class Relevance(BaseModel):
    score: float = Field(description="0.0-1.0, how relevant the page is to the user's prompt")
    reasoning: str = Field(description="One sentence justification")
    keep: bool


async def plan_searches(user_prompt: str, n: int = 6) -> list[str]:
    resp = await planner.chat.completions.create(
        model=PLANNER_MODEL,
        messages=[
            {"role": "system", "content": (
                f"You generate {n} diverse, specific search queries to collect data on a topic.\n"
                "Cover different angles (overview, recent news, technical detail, opposing views, primary sources).\n"
                "Vary recency, source type, and specificity. Avoid duplicate phrasings."
            )},
            {"role": "user", "content": user_prompt},
        ],
        response_format={"type": "json_schema", "json_schema": {
            "name": "Plan", "schema": Plan.model_json_schema()
        }},
        temperature=0.5,
    )
    plan = Plan.model_validate_json(resp.choices[0].message.content)
    print(f"Plan rationale: {plan.rationale}")
    return plan.queries


async def score_relevance(user_prompt: str, page_title: str, page_body: str) -> Relevance:
    resp = await planner.chat.completions.create(
        model=PLANNER_MODEL,
        messages=[
            {"role": "system", "content": (
                f'Score page relevance for the user\'s collection goal: "{user_prompt}".\n'
                "Be strict. Off-topic, shallow, or low-quality pages should score below 0.4."
            )},
            {"role": "user", "content": f"TITLE: {page_title}\n\nBODY (first 3000 chars):\n{page_body[:3000]}"},
        ],
        response_format={"type": "json_schema", "json_schema": {
            "name": "Relevance", "schema": Relevance.model_json_schema()
        }},
        temperature=0.1,
    )
    return Relevance.model_validate_json(resp.choices[0].message.content)


async def should_continue(collected: int, target: int, avg_relevance: float) -> bool:
    if collected >= target:
        return False
    if collected > 20 and avg_relevance < 0.35:
        return False
    return True
