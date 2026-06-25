"""Centralized clients for the Qwen3.6 vLLM endpoint."""
from openai import AsyncOpenAI

# Fast extraction worker (Qwen3.6 35B MoE on port 8000)
worker = AsyncOpenAI(base_url="http://localhost:8000/v1", api_key="local")
WORKER_MODEL = "nvidia/Qwen3.6-35B-A3B-NVFP4"

# Planner / scorer — same model on port 8000
planner = AsyncOpenAI(base_url="http://localhost:8000/v1", api_key="local")
PLANNER_MODEL = "nvidia/Qwen3.6-35B-A3B-NVFP4"
