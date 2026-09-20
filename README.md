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
| Inference server | **[SGLang](https://github.com/sgl-project/sglang)**, installed by [`acqagent/opencode-sglang`](https://github.com/acqagent/opencode-sglang) | OpenAI-compatible server on `:30000`, boot-persistent systemd unit, memory-capped so a runaway can't freeze the host |
| Model | **Qwen3.8-27B-NVFP4** + **DSpark** speculative decoding (`RadixArk/Qwen3.8-27B-NVFP4`, served as `qwen3.8-27b`) | 28-40 tok/s on GB10 for structured/agentic output — roughly 1.3x a stable-MTP engine at the same NVFP4 quality floor, with ~3x faster prefill |
| Client | **AsyncOpenAI** (`openai` Python SDK pointed at `localhost:30000/v1`) | Drop-in API; bearer auth, long timeouts and a shared concurrency budget live in `models.py` |
| Structured output | **Pydantic v2 schemas** + `response_format={"type":"json_schema"}` | Constrained decoding gives valid JSON the first time; falls back to `json_object` + an in-prompt schema on servers that don't support it |
| HTML pipeline | **httpx** (async) → **trafilatura** | Polite concurrent fetches with retry, then mainline-content extraction that strips boilerplate |
| PDF pipeline | **httpx** (async) → **pypdf** | Streamed PDF download with content-hash caching; per-page text extraction |
| Storage | **DuckDB** (single file) | Analytical SQL on a laptop; trivial backup; no server to manage |
| Export | **openpyxl** | Excel with header styling, frozen panes, autosizing, and proper date columns |
| Retries | **tenacity** | Exponential backoff for transient HTTP failures |

Nothing above is SGLang-specific in a way that locks you in. `FAR_LLM_BASE_URL` / `FAR_LLM_MODEL` point the pipeline at any OpenAI-compatible server (vLLM, llama.cpp, LM Studio, a hosted endpoint); the Qwen3.8 thinking controls are sent as request-body extras that other servers ignore, and constrained decoding degrades gracefully. Run `python llm_check.py` after changing backends.

There are two extraction tasks — both go through the same served model:

- **`extract_far_clauses`** — given the cleaned text of a FAR Overhaul Part page, emit every `52.X-Y` clause / provision with verbatim body text.
- **`extract_class_deviations`** — given a deviation PDF's text, emit `{agency, deviation_number, title, effective_date, scope, link}`. Regex first attempts to grab the date and deviation number; the LLM fills in title and scope and any field the regex missed.

A separate script (`normalize_dates.py`) post-processes the free-text effective dates into ISO `DATE` values, classifying each as `iso`, `long`, `immediate`, `delta` (e.g. "14 days from signature"), `issuance`, or `unparsed`. About 86% of deviations resolve to a real ISO date; the rest are intentionally NULL because the source text doesn't pin down a calendar date.

### Thinking is off by default

Qwen3.8 is a reasoning model, and the served chat template defaults to `xhigh` effort. Extraction is transcription, not reasoning: thinking tokens would multiply the wall-clock cost of a 1,100-PDF run for no accuracy gain, and they count against `max_tokens` — undersize that budget with thinking on and you get an empty completion instead of an answer.

So the pipeline sends `chat_template_kwargs: {"enable_thinking": false}` on every call. Set `FAR_LLM_THINKING=low|medium|xhigh` to turn it back on for a pass that needs the judgment; `models.py` then forwards `reasoning_effort` too, and raises a clear error if the model spends the whole budget thinking.

## Layout

```
collector/
  config.py            central paths + endpoints, all overridable via env vars
  models.py            shared OpenAI-compatible client + the JSON-completion helper
  llm_check.py         preflight: endpoint, auth, model id, structured output
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
  incremental_pull.py  cron-safe incremental PDF discovery and download
  incremental_extract.py cron-safe incremental extraction over new PDFs
  auto_pull.sh         convenience wrapper for scheduled pulls
  auto_pull.cron       cron configuration for incremental pulls
  re_extract_all.py    re-run extraction for existing PDFs (ad-hoc)
  sync_manifest.py     reconcile the external corpus manifest with on-disk PDFs
  regenerate_manifest.py rebuild the corpus manifest from disk + DuckDB
  tests/               pytest suite for the deterministic parsing layers

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

### 1. The model server

The pipeline expects an OpenAI-compatible endpoint. The reference setup is
[`acqagent/opencode-sglang`](https://github.com/acqagent/opencode-sglang), which installs
SGLang + Qwen3.8-27B-NVFP4 + DSpark as a boot-persistent service on a DGX Spark:

```bash
git clone https://github.com/acqagent/opencode-sglang.git
cd opencode-sglang
./install.sh          # image + checkpoints + systemd unit, starts at every boot
```

That gives you `http://localhost:30000/v1`, model id `qwen3.8-27b`, and an API key at
`~/.config/qwen38/api-key`. The collector reads that key file automatically — the same box
serving OpenCode serves this pipeline, no extra configuration.

Check it from a client's point of view:

```bash
systemctl status qwen38-sglang
curl -H "Authorization: Bearer $(cat ~/.config/qwen38/api-key)" \
     http://localhost:30000/v1/models
```

> SGLang's `--api-key` accepts `Authorization: Bearer` only, never `x-api-key`, and it
> guards `/v1/models` too. A probe that skips the header gets a 401, which is why
> `models.probe()` authenticates rather than treating any non-200 as "server down".

Running the collector on a **different machine** from the GPU box? Point it at the tailnet
address and copy the key across:

```bash
export FAR_LLM_BASE_URL=http://spark.your-tailnet.ts.net:30000/v1
export FAR_LLM_API_KEY=...          # contents of ~/.config/qwen38/api-key on the Spark
```

(The SGLang unit binds `0.0.0.0`, so it is already reachable on the tailnet, guarded by the
bearer token. `opencode-sglang`'s README explains how to lock it to loopback if you'd rather
it weren't.)

### 2. The collector

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Optional extras:
pip install -r requirements-generic.txt   # legacy search-driven generic mode
pip install -r requirements-dev.txt       # pytest, for running the test suite

python llm_check.py                       # preflight before any long run
```

`llm_check.py` verifies the four things that actually break runs — the endpoint answers,
the token is accepted, `FAR_LLM_MODEL` matches what the server reports at `/v1/models`
(SGLang's `--served-model-name`, *not* the checkpoint path), and constrained decoding
returns a valid `ClassDeviationPage`. It also prints the effective settings.

### Configuration

Everything defaults to paths inside the repo and `localhost:30000`; override with
environment variables (see `config.py`):

| Variable | Default | Purpose |
|---|---|---|
| `FAR_DATA_DIR` | `./data` | root for the DuckDB + caches |
| `FAR_RAW_DIR` / `FAR_PDF_DIR` | `$FAR_DATA_DIR/raw`, `.../pdfs` | HTML / PDF caches |
| `FAR_DB_PATH` | `$FAR_DATA_DIR/collector.duckdb` | the DuckDB file |
| `FAR_LOG_DIR` | `./logs` | run logs + incremental-pull manifests |
| `FAR_CORPUS_PDF_DIR` | *(unset)* | optional second directory to mirror PDFs into |
| `FAR_CORPUS_MANIFEST` | *(unset)* | corpus manifest CSV for `sync_manifest.py` / `regenerate_manifest.py` |
| `FAR_FETCH_USE_CACHE` | *(unset)* | set to `1` to serve HTML re-fetches from the raw cache |
| `FAR_LLM_BASE_URL` | `http://localhost:30000/v1` | OpenAI-compatible endpoint |
| `FAR_LLM_MODEL` | `qwen3.8-27b` | served model id — must match `/v1/models` exactly |
| `FAR_LLM_API_KEY` | *(from key file)* | bearer token; overrides the key file |
| `FAR_LLM_API_KEY_FILE` | `~/.config/qwen38/api-key` | where to read the token when the env var is unset |
| `FAR_LLM_THINKING` | `off` | `off`, or `low` / `medium` / `xhigh` reasoning effort |
| `FAR_LLM_MAX_TOKENS` | `32768` | output ceiling per call |
| `FAR_LLM_MAX_INPUT_CHARS` | `60000` | source text sent per call (was 20k under a 16k-context server) |
| `FAR_LLM_CONCURRENCY` | `2` | extraction calls in flight at once |
| `FAR_LLM_TIMEOUT` | `1800` | whole-request ceiling, seconds |
| `FAR_LLM_MAX_RETRIES` | `2` | SDK-level retries on transport errors |
| `FAR_LLM_PROBE_TIMEOUT` | `10` | seconds for the `/v1/models` health check |

Nothing needs a key file to exist: with no `FAR_LLM_API_KEY` and no readable key file, the
client sends the placeholder `local`, which is what a keyless vLLM or llama.cpp expects.

#### Tuning notes

- **`FAR_LLM_MAX_INPUT_CHARS`** used to be capped at 20k characters because vLLM served a
  16,384-token context. Qwen3.8 serves 262,144, so the ceiling is now generation time, not
  the window. 60k characters (~15k tokens) covers essentially every deviation PDF whole.
  Raise it for the long Part-52 pages; each extra 1k characters of clause text you ask for
  verbatim is another ~250 tokens to decode at ~30-40 tok/s.
- **`FAR_LLM_CONCURRENCY`** stays low on purpose. The service runs with
  `--cuda-graph-max-bs 4`, and DSpark's acceptance rate — the whole speed advantage — falls
  off as the batch grows. All callers in a process share one semaphore, so raising this is
  the only lever.
- **`FAR_LLM_TIMEOUT`** defaults to 30 minutes because a Part-52 page asking for verbatim
  clause bodies can legitimately generate for that long. The `openai` SDK's own 10-minute
  default would kill those requests mid-stream.

---

## Run

```bash
python llm_check.py               # preflight (endpoint, auth, model id, JSON)
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

# Incremental (cron-friendly)
python incremental_pull.py              # discover + download new PDFs only
python incremental_extract.py           # extract new PDFs into DuckDB
python incremental_extract.py --no-llm  # regex-only, no model needed

# Tests (no network, no GPU, no model server needed)
python -m pytest
```

`incremental_extract.py` degrades rather than failing: if the preflight can't reach the
model it logs why and writes regex-only rows, so a cron run still lands the new deviations
and you can enrich them later with `re_extract_all.py`.

For long deviation runs, fire-and-forget:

```bash
nohup python far_collector.py deviations > logs/far_deviations_run.log 2>&1 &
nohup ./auto_export.sh $! >> logs/auto_export.log 2>&1 &
```

`auto_export.sh` polls until the deviation PID exits and then triggers `export_far.py all`.

---

## Where this fits: the three-repo chain

This repo is the *ingest* stage. Two repos downstream consume what it publishes, in series:

```
  acquisition.gov guide page
            │
            ▼
   ┌──────────────────┐   PDFs + manifest.csv     ┌──────────────────┐
   │  far-collector   │ ────────────────────────► │  rfo-deviations  │
   │  (this repo)     │   dated .xlsx exports     │  (corpus repo)   │
   └──────────────────┘                           └──────────────────┘
      scrape · extract · normalize                  archive · release        │
      local Qwen3.8 on SGLang                                                │
                                                       1,192 deviation PDFs  │
                                                                             ▼
                                                    ┌───────────────────────────────┐
                                                    │ all-civ-agency-far-cds-matrix │
                                                    │  per-agency Part 52 tracker   │
                                                    └───────────────────────────────┘
                                                      33 agency tabs · 702 clause rows
```

### Stage 1 → 2: far-collector feeds [`rfo-deviations`](https://github.com/acqagent/rfo-deviations)

`rfo-deviations` is the versioned public archive of the corpus. Three artifacts land there,
and they map one-to-one onto this repo's outputs:

| What lands in `rfo-deviations` | Produced here by | Wired through |
|---|---|---|
| `manifest.csv` (1,256 rows) | `regenerate_manifest.py` / `sync_manifest.py` | `FAR_CORPUS_MANIFEST` |
| `pdfs/` → dated release zips | `far_collector.py`, `incremental_pull.py` | `FAR_CORPUS_PDF_DIR` |
| `far_class_deviations-<date>.xlsx`, `far_provisions_clauses-<date>.xlsx` | `export_far.py` | copied from `output/`, renamed with the scrape date |

The manifest columns are exactly the ones `regenerate_manifest.py` writes —
`on_disk_filename, url_hash, original_filename, agency, part_number, is_dod, source_url,
pdf_size_bytes` — so a corpus checkout is a valid `FAR_CORPUS_*` target with no adapter:

```bash
export FAR_CORPUS_PDF_DIR=../rfo-deviations/pdfs        # gitignored; ships via Releases
export FAR_CORPUS_MANIFEST=../rfo-deviations/manifest.csv
python sync_manifest.py && python regenerate_manifest.py
```

`sync_manifest.py` drops manifest rows whose PDF is gone and reports PDFs with no row;
`regenerate_manifest.py` rebuilds the CSV from disk plus the DuckDB. Neither repo imports
the other — the directory and the CSV are the whole contract.

### Stage 2 → 3: `rfo-deviations` feeds [`all-civ-agency-far-cds-matrix`](https://github.com/acqagent/all-civ-agency-far-cds-matrix)

The matrix repo does **not** read this repo. It reads the corpus: every PDF assigned to an
agency in `manifest.csv`, plus a blank WarU Provision & Clause template. Per agency, it
bundles that agency's PDF text into one structured-output call, stamps the extracted
effective dates and per-clause overrides into a copy of the template, and merges the 33
agency workbooks into a 34-tab master.

So the manifest is load-bearing twice over: `agency` and `part_number` decide which PDFs go
into which agency's bundle, and a wrong `part_number` silently mis-stamps a whole FAR Part
of clauses three repos downstream. That is why the seeding layer is deterministic and
regex-driven rather than model-driven, and why `tests/` covers it.

That build step runs on a hosted frontier model, not the local box — it is a different
workload (whole-agency reconciliation across dozens of PDFs at once, not per-document
transcription). Nothing stops it from pointing at `FAR_LLM_BASE_URL` instead; it would
trade wall-clock for keeping the data on the box.

### The loop-back, and a filename collision

The matrix master is copied back into `rfo-deviations` as a dated snapshot. As of
2026-05-02 that copy is **byte-identical** to `far_provisions_clauses_matrix.xlsx`
(sha256 `5719af51…`) — but it is filed under this repo's export name:

| File in `rfo-deviations` | Sheets | Actually produced by |
|---|---|---|
| `far_class_deviations-2026-04-27.xlsx` | 1 — `Class Deviations`, 1,102 rows | `export_far.py` |
| `far_class_deviations-2026-05-02.xlsx` | **34 — `README` + 33 agency tabs** | the matrix repo |
| `far_class_deviations-2026-06-23.xlsx` | 1 — `Class Deviations`, 1,221 rows | `export_far.py` |

Anything that globs `far_class_deviations-*.xlsx` and opens the `Class Deviations` sheet
works on two of those three and raises on the middle one. The corpus README describes the
05-02 schema change as permanent ("starting 2026-05-03 the file is the per-agency
clause-level workbook"), but 06-23 is a plain collector export again, so that note no
longer matches the directory. Worth renaming the odd one out to
`far_part52_matrix-2026-05-02.xlsx` in the corpus repo; nothing in *this* repo reads those
files, so it is a rename there and a README correction, not a code change here.

### The shared GPU

The model server is the resource all of this contends for: one SGLang unit serves OpenCode,
this collector, and anything else on the box, a small batch at a time. Run a 1,100-PDF
extraction and an interactive coding session together and they queue behind each other —
that is what `FAR_LLM_CONCURRENCY` is protecting.

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
- `find_effective_date()` uses a regex over the PDF text. For PDFs where the first matched "effective <date>" refers to a *predecessor* deviation rather than the current one, the LLM second-pass usually corrects it — but spot-check date outliers after every run.- Backend problems surface as a `FAIL` line from `python llm_check.py`, not as silently
  empty columns. The three that actually happen: a 401 (key file unreadable, or the key
  rotated), a model-id mismatch after re-pinning the checkpoint, and an empty completion
  when `FAR_LLM_THINKING` is on with too small a `FAR_LLM_MAX_TOKENS`.
- Changing servers doesn't need code changes, but it does need a preflight: constrained
  decoding support varies, and `models.complete_json()` falls back to `json_object` plus an
  in-prompt schema the first time a server rejects a `json_schema` response format.
