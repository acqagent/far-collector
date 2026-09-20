"""Shared OpenAI-compatible client plus the JSON-completion helper every
extraction pass goes through.

The default target is the local SGLang server from acqagent/opencode-sglang
(Qwen3.8-27B-NVFP4 + DSpark). Three things about that backend shape this file:

  * it is started with --api-key, so requests carry a bearer token;
  * it applies a Qwen3 chat template with a thinking budget, so calls say
    explicitly whether they want thinking (extraction does not);
  * it decodes a small batch at a time, so all callers share one semaphore.

Nothing here is SGLang-specific in a way that breaks other servers: vLLM and
llama.cpp ignore the chat_template_kwargs they don't know, and a server that
rejects json_schema constrained decoding falls back to plain JSON mode.
"""
from __future__ import annotations

import asyncio
import json
import re
from typing import Any, TypeVar

import httpx
from openai import AsyncOpenAI, BadRequestError
from pydantic import BaseModel, ValidationError

import config

client = AsyncOpenAI(
    base_url=config.LLM_BASE_URL,
    api_key=config.LLM_API_KEY,
    timeout=config.LLM_TIMEOUT,
    max_retries=config.LLM_MAX_RETRIES,
)
MODEL = config.LLM_MODEL

T = TypeVar("T", bound=BaseModel)

# One shared budget of in-flight requests, so two callers in the same process
# can't stack up past what the GPU will actually batch. Created per event loop
# because a Semaphore binds to the loop that first waits on it, and some entry
# points (incremental_extract) have run more than one loop in a process.
_slots: dict[int, asyncio.Semaphore] = {}


def _acquire_slot() -> asyncio.Semaphore:
    loop_id = id(asyncio.get_running_loop())
    sem = _slots.get(loop_id)
    if sem is None:
        sem = _slots[loop_id] = asyncio.Semaphore(config.LLM_CONCURRENCY)
    return sem

# Flipped off the first time a server rejects a json_schema response format, so
# we don't pay for that round trip on every later call.
_schema_mode = True

_THINK_RE = re.compile(r"<think>.*?</think>", re.DOTALL)
_FENCE_RE = re.compile(r"```(?:json)?\s*(.*?)```", re.DOTALL)


class LLMError(RuntimeError):
    """A call reached the server but produced nothing usable."""


def _thinking_body() -> dict[str, Any]:
    """Request-body extras that set the Qwen3.8 thinking budget."""
    if config.LLM_THINKING == "off":
        return {"chat_template_kwargs": {"enable_thinking": False}}
    return {
        "reasoning_effort": config.LLM_THINKING,
        "chat_template_kwargs": {"enable_thinking": True},
    }


def strip_thinking(text: str) -> str:
    """Drop chain-of-thought a server left inline.

    With --reasoning-parser qwen3 SGLang splits it into `reasoning_content` and
    this is a no-op, but a server without a reasoning parser (or a response cut
    off mid-block) hands back a raw <think> block that would break JSON parsing.
    """
    text = _THINK_RE.sub("", text).strip()
    if text.startswith("<think>"):
        text = text.partition("</think>")[2].strip()
    return text


def json_slice(text: str) -> str:
    """Narrow a completion down to the JSON object it contains.

    Constrained decoding returns bare JSON, so this only matters on the
    json_object fallback path, where models like to add a code fence.
    """
    fence = _FENCE_RE.search(text)
    if fence:
        text = fence.group(1).strip()
    if text[:1] in "{[":
        return text
    start, end = text.find("{"), text.rfind("}")
    if start != -1 and end > start:
        return text[start:end + 1]
    return text


def _completion_text(resp: Any) -> str:
    msg = resp.choices[0].message
    text = strip_thinking(msg.content or "")
    if text:
        return text
    # Empty content with reasoning present means the token budget went entirely
    # to thinking — the usual cause is an undersized max_tokens.
    if getattr(msg, "reasoning_content", None):
        raise LLMError(
            "model produced only reasoning and no answer; raise FAR_LLM_MAX_TOKENS "
            f"(currently {config.LLM_MAX_TOKENS}) or set FAR_LLM_THINKING=off"
        )
    raise LLMError(f"empty completion (finish_reason={resp.choices[0].finish_reason})")


def _schema_hint(messages: list[dict[str, str]], schema: dict[str, Any]) -> list[dict[str, str]]:
    """Put the schema in the prompt, for servers that can't constrain decoding."""
    hint = (
        "Return a single JSON object and nothing else. It must validate against "
        f"this JSON Schema:\n{json.dumps(schema)}"
    )
    out = [dict(m) for m in messages]
    out[0]["content"] = f"{out[0]['content']}\n\n{hint}"
    return out


async def complete_json(
    schema_model: type[T],
    messages: list[dict[str, str]],
    *,
    name: str | None = None,
    temperature: float = 0.0,
    max_tokens: int | None = None,
) -> T:
    """One structured-output call, validated into `schema_model`.

    Raises LLMError on an unusable response; callers decide whether that is
    fatal for their row.
    """
    global _schema_mode
    schema_name = name or schema_model.__name__
    schema = schema_model.model_json_schema()
    body = {"max_tokens": max_tokens or config.LLM_MAX_TOKENS, **_thinking_body()}

    async with _acquire_slot():
        text: str | None = None
        if _schema_mode:
            try:
                resp = await client.chat.completions.create(
                    model=MODEL,
                    messages=messages,
                    response_format={
                        "type": "json_schema",
                        "json_schema": {"name": schema_name, "schema": schema},
                    },
                    temperature=temperature,
                    extra_body=body,
                )
                text = _completion_text(resp)
            except BadRequestError as e:
                _schema_mode = False
                print(f"[llm] json_schema rejected by server ({e}); using json_object mode")

        if text is None:
            resp = await client.chat.completions.create(
                model=MODEL,
                messages=_schema_hint(messages, schema),
                response_format={"type": "json_object"},
                temperature=temperature,
                extra_body=body,
            )
            text = _completion_text(resp)

    try:
        return schema_model.model_validate_json(json_slice(text))
    except ValidationError as e:
        raise LLMError(f"response did not validate as {schema_name}: {e}") from e
    except json.JSONDecodeError as e:
        raise LLMError(f"response was not JSON: {e}") from e


def probe(timeout: float | None = None) -> tuple[bool, str]:
    """Check the endpoint the way the pipeline will use it.

    Returns (ok, detail). Authenticates, because an --api-key server answers
    /v1/models with 401 and a probe that skipped the header would read that as
    "server down" and silently downgrade the run to regex-only extraction.
    """
    url = config.LLM_BASE_URL.rstrip("/") + "/models"
    headers = {"Authorization": f"Bearer {config.LLM_API_KEY}"}
    try:
        r = httpx.get(url, headers=headers, timeout=timeout or config.LLM_PROBE_TIMEOUT)
    except Exception as e:
        return False, f"{url} unreachable: {type(e).__name__}: {e}"
    if r.status_code in (401, 403):
        return False, (
            f"{url} returned {r.status_code}: set FAR_LLM_API_KEY, or point "
            f"FAR_LLM_API_KEY_FILE at the server's key (tried {config.LLM_API_KEY_FILE})"
        )
    if r.status_code != 200:
        return False, f"{url} returned HTTP {r.status_code}"
    try:
        served = [m["id"] for m in r.json().get("data", [])]
    except Exception:
        return False, f"{url} returned a /models body this client can't parse"
    if MODEL not in served:
        return False, (
            f"FAR_LLM_MODEL={MODEL!r} is not served here; the server reports "
            f"{served or '[]'} — set FAR_LLM_MODEL to one of those"
        )
    return True, f"{config.LLM_BASE_URL} serving {', '.join(served)}"


def endpoint_ready(timeout: float | None = None) -> bool:
    return probe(timeout)[0]
