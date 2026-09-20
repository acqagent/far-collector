"""LLM extraction with Pydantic schemas. Generic Page schema + FAR-specific schemas.

Every call goes through models.complete_json(), which owns the request
plumbing (auth, thinking budget, concurrency, constrained-decoding fallback).
"""
from pydantic import BaseModel, Field

import config
from models import complete_json

# Single source of truth for how much source text goes to the model, overridable
# with FAR_LLM_MAX_INPUT_CHARS. Qwen3.8 on SGLang serves a 262k context, so the
# binding constraint is generation time, not the window. Longer inputs are
# truncated here — callers should pass the full text.
MAX_INPUT_CHARS = config.LLM_MAX_INPUT_CHARS


class Page(BaseModel):
    title: str = Field(description="Page or article title")
    author: str | None = None
    published: str | None = Field(None, description="Publication date if visible")
    body: str = Field(description="Main content, summarized to under 2000 words")
    topics: list[str] = Field(description="3-7 topic tags")
    follow_links: list[str] = Field(default_factory=list, description="On-page URLs worth following next")


class FARClause(BaseModel):
    number: str = Field(description="Full FAR number, e.g. '52.204-7' or '52.232-25'")
    title: str = Field(description="Official title without leading number")
    kind: str = Field(description="'Provision' or 'Clause' or 'Unknown' if not stated")
    effective_date: str | None = Field(None, description="Effective/revision date, e.g. 'JAN 2026'")
    full_text: str = Field(description="Verbatim text of the clause body, including paragraphs (a),(b),(c)...")


class FARClausePage(BaseModel):
    clauses: list[FARClause] = Field(default_factory=list)


class ClassDeviation(BaseModel):
    agency: str = Field(description="Issuing agency, e.g. 'GSA', 'NASA', 'DOE'")
    deviation_number: str = Field(description="Deviation identifier, e.g. 'CD-2025-04'")
    title: str = Field(description="Title or subject of the deviation")
    effective_date: str | None = Field(None, description="Effective date as written, e.g. '2025-10-15'")
    scope: str | None = Field(None, description="One-paragraph summary of what the deviation covers")
    link: str | None = Field(None, description="URL to the deviation document if present on the page")


class ClassDeviationPage(BaseModel):
    deviations: list[ClassDeviation] = Field(default_factory=list)


def _messages(system: str, url: str, clean_text: str, max_chars: int) -> list[dict[str, str]]:
    return [
        {"role": "system", "content": system},
        {"role": "user", "content": f"URL: {url}\n\nCONTENT:\n{clean_text[:max_chars]}"},
    ]


async def extract_page(url: str, clean_text: str, user_prompt: str, max_chars: int = MAX_INPUT_CHARS) -> Page | None:
    if len(clean_text) < 200:
        return None
    sys = (
        f'Extract structured info from web pages. The user is collecting data for: "{user_prompt}".\n'
        "Return strict JSON matching the schema. Only suggest follow_links that look directly useful."
    )
    try:
        return await complete_json(
            Page, _messages(sys, url, clean_text, max_chars), temperature=0.1,
        )
    except Exception as e:
        print(f"Extract failed for {url}: {e}")
        return None


async def extract_far_clauses(url: str, clean_text: str, max_chars: int = MAX_INPUT_CHARS) -> FARClausePage | None:
    if len(clean_text) < 200:
        return None
    sys = (
        "Extract every FAR provision or clause that appears on this page. "
        "FAR clauses are numbered like '52.204-7'. Capture the FULL verbatim text "
        "of each clause body, including all lettered/numbered subparagraphs. "
        "If multiple clauses appear, return them all. Do not invent fields."
    )
    try:
        return await complete_json(
            FARClausePage, _messages(sys, url, clean_text, max_chars), temperature=0.0,
        )
    except Exception as e:
        print(f"FAR clause extract failed for {url}: {e}")
        return None


async def extract_class_deviations(url: str, clean_text: str, max_chars: int = MAX_INPUT_CHARS) -> ClassDeviationPage | None:
    if len(clean_text) < 200:
        return None
    sys = (
        "Extract every agency FAR Class Deviation listed on this page. "
        "Capture: agency, deviation number, title, effective date, scope, and link if available. "
        "EXCLUDE any DoD / Department of Defense / DFARS deviations. "
        "If a row mentions DoD, skip it entirely."
    )
    try:
        return await complete_json(
            ClassDeviationPage, _messages(sys, url, clean_text, max_chars), temperature=0.0,
        )
    except Exception as e:
        print(f"Class deviation extract failed for {url}: {e}")
        return None
