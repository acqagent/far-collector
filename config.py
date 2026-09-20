"""Central configuration. Every path and endpoint can be overridden with an
environment variable so the pipeline is portable across machines.

Defaults keep everything inside the repo directory (data/, logs/), matching
the layout described in the README. The optional FAR_CORPUS_* settings point
at an external corpus directory (a second copy of the PDFs plus a CSV
manifest) used by the corpus-maintenance utilities; they are unset by default.

The LLM defaults target the local SGLang box built by acqagent/opencode-sglang
(Qwen3.8-27B-NVFP4 + DSpark on a DGX Spark, OpenAI-compatible on port 30000).
Any other OpenAI-compatible server works — point FAR_LLM_BASE_URL and
FAR_LLM_MODEL at it.
"""
from __future__ import annotations

import os
from pathlib import Path

ROOT = Path(__file__).parent


def _path(env: str, default: Path) -> Path:
    v = os.environ.get(env)
    return Path(v) if v else default


def _opt_path(env: str) -> Path | None:
    v = os.environ.get(env)
    return Path(v) if v else None


def _int(env: str, default: int) -> int:
    v = os.environ.get(env)
    try:
        return int(v) if v else default
    except ValueError:
        return default


def _float(env: str, default: float) -> float:
    v = os.environ.get(env)
    try:
        return float(v) if v else default
    except ValueError:
        return default


DATA_DIR = _path("FAR_DATA_DIR", ROOT / "data")
RAW_DIR = _path("FAR_RAW_DIR", DATA_DIR / "raw")
PDF_DIR = _path("FAR_PDF_DIR", DATA_DIR / "pdfs")
LOG_DIR = _path("FAR_LOG_DIR", ROOT / "logs")
DB_PATH = _path("FAR_DB_PATH", DATA_DIR / "collector.duckdb")

# Optional second location to mirror downloaded deviation PDFs into
# (e.g. a RAG corpus maintained outside this repo). Unset = no mirroring.
CORPUS_PDF_DIR = _opt_path("FAR_CORPUS_PDF_DIR")
# CSV manifest for that corpus, used by sync_manifest.py / regenerate_manifest.py.
CORPUS_MANIFEST = _opt_path("FAR_CORPUS_MANIFEST")

# ---------------------------------------------------------------------------
# LLM backend (OpenAI-compatible: SGLang, vLLM, llama.cpp, anything else)
# ---------------------------------------------------------------------------

LLM_BASE_URL = os.environ.get("FAR_LLM_BASE_URL", "http://localhost:30000/v1")

# Must match the id the server reports at /v1/models exactly — SGLang's
# --served-model-name, not the HuggingFace checkpoint path. `python llm_check.py`
# tells you when these disagree.
LLM_MODEL = os.environ.get("FAR_LLM_MODEL", "qwen3.8-27b")

# SGLang runs with --api-key, so every request needs a bearer token (it accepts
# Authorization: Bearer only). Resolution order: explicit env var, then the key
# file opencode-sglang's installer writes, then a placeholder for servers that
# don't check credentials at all.
LLM_API_KEY_FILE = _path("FAR_LLM_API_KEY_FILE", Path.home() / ".config" / "qwen38" / "api-key")


def read_api_key(key_file: Path | None = None) -> str:
    explicit = os.environ.get("FAR_LLM_API_KEY")
    if explicit:
        return explicit
    try:
        key = (key_file or LLM_API_KEY_FILE).read_text().strip()
    except OSError:
        return "local"
    return key or "local"


LLM_API_KEY = read_api_key()

# Whole-request ceiling. A single extraction can generate tens of thousands of
# verbatim tokens at ~30-40 tok/s, so the SDK's 10-minute default is far too
# short for Part-52 pages.
LLM_TIMEOUT = _float("FAR_LLM_TIMEOUT", 1800.0)
LLM_MAX_RETRIES = _int("FAR_LLM_MAX_RETRIES", 2)
LLM_PROBE_TIMEOUT = _float("FAR_LLM_PROBE_TIMEOUT", 10.0)

# How many extraction calls may be in flight at once. The box decodes a couple
# of requests at a time (--cuda-graph-max-bs 4) and DSpark's acceptance rate
# falls off as the batch grows, so more concurrency here buys nothing.
LLM_CONCURRENCY = _int("FAR_LLM_CONCURRENCY", 2)

# Output ceiling per call. Verbatim clause bodies are the long pole.
LLM_MAX_TOKENS = _int("FAR_LLM_MAX_TOKENS", 32768)

# How much source text goes to the model. The old 20k ceiling existed because
# vLLM served a 16k context; Qwen3.8 on SGLang has 262k, so the limiting factor
# is now generation time rather than context.
LLM_MAX_INPUT_CHARS = _int("FAR_LLM_MAX_INPUT_CHARS", 60_000)

# Qwen3.8 thinking budget: "off", or one of the effort tiers the served chat
# template accepts. Extraction is transcription rather than reasoning, so
# thinking mostly burns tokens the whole pipeline then waits on — off by
# default, but available when a pass needs the judgment.
_THINKING_MODES = {"off", "low", "medium", "xhigh"}
LLM_THINKING = os.environ.get("FAR_LLM_THINKING", "off").strip().lower()
if LLM_THINKING not in _THINKING_MODES:
    LLM_THINKING = "off"

# When "1", fetch_many() serves pages from the raw-HTML cache in RAW_DIR
# instead of re-fetching URLs it has seen before.
FETCH_USE_CACHE = os.environ.get("FAR_FETCH_USE_CACHE", "") == "1"
