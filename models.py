"""Centralized clients for the Gemma 4 vLLM endpoints.

For FAR mode we run only the 26B-A4B (3.8B active MoE) on port 8000 and use it
for both extraction and planning roles. The 31B Dense planner can be added
later by reverting PLANNER_* to point at port 8001.
"""
from openai import AsyncOpenAI

# Fast extraction worker (Gemma 4 26B MoE on port 8000)
worker = AsyncOpenAI(base_url="http://localhost:8000/v1", api_key="local")
WORKER_MODEL = "gemma-26b"

# Planner / scorer — temporarily routed to the 26B (no separate 31B server today)
planner = AsyncOpenAI(base_url="http://localhost:8000/v1", api_key="local")
PLANNER_MODEL = "gemma-26b"
