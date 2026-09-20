"""Preflight for the LLM backend: run this before a long collection.

Checks, in order, the three things that actually break runs against a local
OpenAI-compatible server:

  1. the endpoint answers and the bearer token is accepted;
  2. the configured model id matches what the server reports at /v1/models
     (SGLang's --served-model-name, which is not the checkpoint path);
  3. constrained decoding produces valid JSON for one of the real schemas.

Usage:
    python llm_check.py            # probe + a live structured-output round trip
    python llm_check.py --probe    # probe only, no generation
"""
from __future__ import annotations

import argparse
import asyncio
import sys
import time

import config
import models


def show_settings() -> None:
    key = config.LLM_API_KEY
    masked = "local (no auth)" if key == "local" else f"{key[:4]}...{key[-4:]} ({len(key)} chars)"
    print("backend settings")
    print(f"  base url     {config.LLM_BASE_URL}")
    print(f"  model        {config.LLM_MODEL}")
    print(f"  api key      {masked}")
    print(f"  key file     {config.LLM_API_KEY_FILE}")
    print(f"  thinking     {config.LLM_THINKING}")
    print(f"  concurrency  {config.LLM_CONCURRENCY}")
    print(f"  max tokens   {config.LLM_MAX_TOKENS}")
    print(f"  input chars  {config.LLM_MAX_INPUT_CHARS}")
    print(f"  timeout      {config.LLM_TIMEOUT:.0f}s")


async def round_trip() -> bool:
    """One real extraction against a synthetic deviation memo."""
    import extract as ex

    sample = (
        "DEPARTMENT OF ENERGY\n"
        "CLASS DEVIATION CD-2025-04\n"
        "SUBJECT: Class Deviation from FAR Part 15 Source Selection Procedures\n"
        "Effective Date: October 15, 2025\n\n"
        "Pursuant to FAR 1.404, a class deviation is authorized for all Department "
        "of Energy contracting activities. This deviation implements the "
        "Revolutionary FAR Overhaul model text for Part 15 in place of the "
        "current FAR Part 15 procedures. It applies to all solicitations issued "
        "on or after the effective date and remains in effect until the "
        "Revolutionary FAR Overhaul final rule is published.\n"
    ) * 3

    t0 = time.monotonic()
    page = await ex.extract_class_deviations("https://example.invalid/cd-2025-04.pdf", sample)
    elapsed = time.monotonic() - t0

    if not page or not page.deviations:
        print(f"FAIL structured output returned nothing after {elapsed:.1f}s")
        return False
    d = page.deviations[0]
    print(f"OK   structured output in {elapsed:.1f}s")
    print(f"       agency          {d.agency}")
    print(f"       deviation       {d.deviation_number}")
    print(f"       effective date  {d.effective_date}")
    print(f"       title           {(d.title or '')[:70]}")
    return True


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--probe", action="store_true", help="skip the generation round trip")
    args = ap.parse_args()

    show_settings()
    print()

    ok, detail = models.probe()
    print(f"{'OK  ' if ok else 'FAIL'} {detail}")
    if not ok:
        return 1
    if args.probe:
        return 0

    print("\nsending one extraction request (this warms the server; first call is slowest)...")
    return 0 if asyncio.run(round_trip()) else 1


if __name__ == "__main__":
    sys.exit(main())
