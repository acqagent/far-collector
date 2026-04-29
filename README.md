# Spark Collector — Two-Model Gemma 4 Web Data Collector

Built on `/home/dgxgape/` per `spark_collector_final.docx`.
Generic search-driven collector + FAR-overhaul-specific deterministic crawler.

## Layout

```
collector/
  models.py            two OpenAI clients pointed at vLLM :8000 + :8001
  db.py                DuckDB schema (urls, pages, runs, FAR tables)
  search.py            Google + DDG seeding (generic mode only)
  fetch.py             async httpx + trafilatura
  extract.py           Pydantic schemas: Page, FARClause, ClassDeviation
  agent.py             31B planner + relevance scorer
  collector.py         generic search-driven orchestrator (per doc)
  far_seed.py          deterministic crawl of acquisition.gov deviation guide
  pdf_extract.py       PDF download + pypdf text + regex date heuristics
  far_collector.py     FAR-mode orchestrator (Part pages + agency PDFs)
  export_far.py        openpyxl writer for the two .xlsx outputs
  data/
    raw/               cached HTML (one file per URL hash)
    pdfs/              cached agency deviation PDFs
    collector.duckdb   single-file DuckDB
  output/              .xlsx exports
  logs/
```

## Two model servers (Gemma 4, FP8 vLLM)

```bash
source .venv/bin/activate
huggingface-cli login    # accept gating on both model pages first

# Terminal 1 — 26B MoE worker
vllm serve google/gemma-4-26B-A4B-it \
  --served-model-name gemma-26b --port 8000 \
  --gpu-memory-utilization 0.25 --max-model-len 32768 \
  --quantization fp8 --kv-cache-dtype fp8 \
  --enable-auto-tool-choice --tool-call-parser hermes

# Terminal 2 — 31B Dense planner
vllm serve google/gemma-4-31B-it \
  --served-model-name gemma-31b --port 8001 \
  --gpu-memory-utilization 0.30 --max-model-len 32768 \
  --quantization fp8 --kv-cache-dtype fp8 \
  --enable-auto-tool-choice --tool-call-parser hermes
```

Verify: `curl http://localhost:8000/v1/models`, `curl http://localhost:8001/v1/models`.

## FAR mode (the actual goal)

```bash
python db.py                  # init schema
python far_seed.py            # crawl deviation guide → manifest in DuckDB
python far_collector.py all   # provisions + class deviations (uses LLMs)
python export_far.py all      # writes output/*.xlsx

# Subsets
python far_collector.py provisions       # only Part overview pages
python far_collector.py deviations       # only agency PDFs
python far_collector.py provisions 52    # only Part 52
```

## Generic mode (per the doc)

```bash
python collector.py "best practices for fine-tuning Llama 3"
```
