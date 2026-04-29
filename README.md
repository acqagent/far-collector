# FAR Collector

A local LLM-driven scraper for the [acquisition.gov FAR Overhaul deviation guide](https://www.acquisition.gov/far-overhaul/far-part-deviation-guide). Builds a structured dataset of every FAR Part overview page and every agency class-deviation PDF, normalizes effective dates, and exports two Excel workbooks.

---

## What this is for

In April 2025, Executive Order **14275 — "Restoring Common Sense to Federal Procurement"** kicked off the *Revolutionary FAR Overhaul (RFO)*: a top-to-bottom rewrite of the Federal Acquisition Regulation. OMB issued memo **M-25-26** directing every executive agency to publish class deviations from the existing FAR while the rewrite is staged in. The result is a moving target — hundreds of agency-issued PDF deviations posted to a single guide page on acquisition.gov, plus the new FAR Volume I that becomes effective **April 17, 2026**.

Tracking this manually doesn't scale. This tool gives you:

1. **A canonical manifest** of every Part page and every agency PDF (1,100+ deviations across 35+ agencies as of April 2026), built from the official guide page deterministically.
2. **Structured extraction** of the contents — agency, deviation number, title, scope, raw effective date, and an ISO-normalized effective date — so you can sort, filter, and join.
3. **Verbatim Part 52 clause/provision text** captured from the FAR Overhaul Part-52 page.
4. **Two Excel workbooks** that drop straight into a sharepoint/SharePoint workflow: `far_class_deviations.xlsx` and `far_provisions_clauses.xlsx`.

Downstream uses include RAG over the corpus, dependency / cross-reference mapping, propositional-logic formalization, and audit-trail evidence for compliance reviews.

---

## AI stack

Everything runs **locally** on a single workstation (built and tested on an NVIDIA DGX Spark, ARM64 Grace-Blackwell GB10, 128 GB unified LPDDR5X). No paid API calls; no data leaves the box.

| Layer | Tool | Why |
|---|---|---|
| Inference server | **[vLLM](https://github.com/vllm-project/vllm)** | OpenAI-compatible server with continuous batching; saturates the GPU at FP8 |
| Model | **Google Gemma 4 26B-A4B-Instruct** (MoE, 3.8B active params), FP8 weights + FP8 KV cache | Sweet spot of capability vs. memory bandwidth on Spark — ~37 tok/s vs. 3 tok/s for a 70B dense model |
| Client | **AsyncOpenAI** (`openai` Python SDK pointed at `localhost:8000/v1`) | Drop-in API; concurrent extraction calls without writing custom HTTP plumbing |
| Structured output | **Pydantic v2 schemas** + vLLM's `response_format={"type":"json_schema"}` | Constrained decoding gives valid JSON the first time; no regex post-processing |
| HTML pipeline | **httpx** (async) → **trafilatura** | Polite concurrent fetches with retry, then mainline-content extraction that strips boilerplate |
| PDF pipeline | **httpx** (async) → **pypdf** | Streamed PDF download with content-hash caching; per-page text extraction |
| Storage | **DuckDB** (single file) | Analytical SQL on a laptop; trivial backup; no server to manage |
| Export | **openpyxl** | Excel with header styling, frozen panes, autosizing, and proper date columns |
| Retries | **tenacity** | Exponential backoff for transient HTTP failures |

There are two extraction tasks — both go through the same 26B model:

- **`extract_far_clauses`** — given the cleaned text of a FAR Overhaul Part page, emit every `52.X-Y` clause / provision with verbatim body text.
- **`extract_class_deviations`** — given a deviation PDF's text, emit `{agency, deviation_number, title, effective_date, scope, link}`. Regex first attempts to grab the date and deviation number; the LLM fills in title and scope and any field the regex missed.

A separate script (`normalize_dates.py`) post-processes the free-text effective dates into ISO `DATE` values, classifying each as `iso`, `long`, `immediate`, `delta` (e.g. "14 days from signature"), `issuance`, or `unparsed`. About 86% of deviations resolve to a real ISO date; the rest are intentionally NULL because the source text doesn't pin down a calendar date.

---

## Layout

```
collector/
  models.py            AsyncOpenAI clients pointed at the local vLLM endpoint
  db.py                DuckDB schema (urls, pages, runs, FAR tables)
  fetch.py             async httpx + trafilatura with raw-HTML cache
  pdf_extract.py       async PDF download + pypdf + regex date heuristics
  extract.py           Pydantic schemas: Page, FARClause, ClassDeviation
  far_seed.py          deterministic crawl of the acquisition.gov guide → manifest
  far_collector.py     orchestrator: Part-page clauses + agency-PDF deviations
  retry_missing.py     re-download manifest entries that didn't land on disk
  normalize_dates.py   add effective_date_iso + effective_date_kind columns
  export_far.py        openpyxl writer for the two .xlsx outputs
  agent.py             planner + relevance scorer (used in generic mode only)
  collector.py         generic search-driven orchestrator (legacy/optional)
  search.py            Google + DDG seeding for generic mode
  part52_parser.py     bottom-up parser for Part 52 anchor structure
  auto_export.sh       waits on a long-running PID, then runs export_far.py

  data/
    raw/               cached HTML (one file per URL hash)
    pdfs/              cached agency deviation PDFs
    collector.duckdb   single-file DuckDB (the source of truth)
  output/              .xlsx exports
  logs/                run logs
```

Everything under `data/`, `output/`, and `logs/` is `.gitignore`d.

---

## Setup

```bash
python -m venv .venv && source .venv/bin/activate
pip install httpx trafilatura pypdf duckdb openpyxl pydantic openai \
            tenacity python-dateutil rich beautifulsoup4 lxml

# vLLM is the heavy install — follow the official instructions for your CUDA
# version: https://docs.vllm.ai/en/latest/getting_started/installation.html
# For NVIDIA DGX Spark (sm_121 / GB10), build from source against PyTorch 2.11+cu130.
```

Gemma 4 is gated; accept the licence on the [Hugging Face model page](https://huggingface.co/google/gemma-4-26B-A4B-it) once and run `huggingface-cli login`.

Start the model server in a separate terminal:

```bash
vllm serve google/gemma-4-26B-A4B-it \
  --served-model-name gemma-26b --port 8000 \
  --gpu-memory-utilization 0.50 --max-model-len 16384 \
  --quantization fp8 --kv-cache-dtype fp8 \
  --enable-auto-tool-choice --tool-call-parser hermes
```

Verify: `curl http://localhost:8000/v1/models` should return one model named `gemma-26b`.

---

## Run

```bash
python db.py                      # init schema (idempotent)
python far_seed.py                # crawl the deviation guide → manifest
python far_collector.py all       # provisions + class deviations
python normalize_dates.py         # add effective_date_iso column
python export_far.py all          # writes output/*.xlsx

# Subsets
python far_collector.py provisions       # only Part overview pages
python far_collector.py deviations       # only agency PDFs
python far_collector.py provisions 52    # only Part 52
python retry_missing.py                  # redownload manifest gaps
```

For long deviation runs, fire-and-forget:

```bash
nohup python far_collector.py deviations > logs/far_deviations_run.log 2>&1 &
nohup ./auto_export.sh $! >> logs/auto_export.log 2>&1 &
```

`auto_export.sh` polls until the deviation PID exits and then triggers `export_far.py all`.

---

## Outputs

`output/far_class_deviations.xlsx` — one row per agency class deviation:

| Agency | Deviation # | Title | Effective Date (raw) | Effective Date (ISO) | Date Kind | Scope | Link | Scraped At |

Sorted newest-first by ISO date. About 1,100 rows.

`output/far_provisions_clauses.xlsx` — one row per FAR Part 52 clause/provision:

| Number | Title | Type | Effective Date | Full Text | Source URL | Scraped At |

About 495 rows (396 Clauses + 99 Provisions).

Both are regenerated from the DuckDB on every `export_far.py` run.

---

## Notes

- The collector explicitly **excludes DoD** deviations because DFARS lives at a separate publication path (the OUSD(A&S) "Class Deviations" page) and follows a different cadence; pulling DFARS belongs in a separate collector.
- The deviation guide page occasionally lists PDFs whose URLs return HTTP 404 from acquisition.gov; these get logged but skipped.
- `find_effective_date()` uses a regex over the PDF text. For PDFs where the first matched "effective <date>" refers to a *predecessor* deviation rather than the current one, the LLM second-pass usually corrects it — but spot-check date outliers after every run.
