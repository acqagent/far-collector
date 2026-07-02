"""Central configuration. Every path and endpoint can be overridden with an
environment variable so the pipeline is portable across machines.

Defaults keep everything inside the repo directory (data/, logs/), matching
the layout described in the README. The optional FAR_CORPUS_* settings point
at an external corpus directory (a second copy of the PDFs plus a CSV
manifest) used by the corpus-maintenance utilities; they are unset by default.
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

# Local vLLM endpoint (OpenAI-compatible).
LLM_BASE_URL = os.environ.get("FAR_LLM_BASE_URL", "http://localhost:8000/v1")
LLM_MODEL = os.environ.get("FAR_LLM_MODEL", "nvidia/Qwen3.6-35B-A3B-NVFP4")
LLM_API_KEY = os.environ.get("FAR_LLM_API_KEY", "local")

# When "1", fetch_many() serves pages from the raw-HTML cache in RAW_DIR
# instead of re-fetching URLs it has seen before.
FETCH_USE_CACHE = os.environ.get("FAR_FETCH_USE_CACHE", "") == "1"
