"""Shared AsyncOpenAI client for the local vLLM endpoint (see config.py)."""
from openai import AsyncOpenAI

import config

client = AsyncOpenAI(base_url=config.LLM_BASE_URL, api_key=config.LLM_API_KEY)
MODEL = config.LLM_MODEL
