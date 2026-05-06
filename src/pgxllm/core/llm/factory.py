"""
pgxllm.core.llm.factory
------------------------
LLMProviderFactory — config 기반 provider 생성.
"""
from __future__ import annotations

import os

from pgxllm.config import LLMConfig
from .base import LLMProvider


def create_llm_provider(cfg: LLMConfig) -> LLMProvider:
    """
    LLMConfig 를 기반으로 적절한 LLMProvider 인스턴스를 생성한다.

    provider 값:
        ollama    → OllamaProvider
        vllm      → VLLMProvider  (OpenAI-compatible)
        lmstudio  → VLLMProvider  (OpenAI-compatible)
        openai    → VLLMProvider  (api.openai.com)
        anthropic → AnthropicProvider
        watsonx   → WatsonXProvider (IBM watsonx.ai / IBM Cloud IAM)

    api_key 우선순위: cfg.api_key > 환경변수
    """
    provider = cfg.provider.lower()

    if provider == "ollama":
        from .ollama import OllamaProvider
        return OllamaProvider(
            base_url=cfg.base_url,
            model=cfg.model,
            timeout=cfg.timeout,
            temperature=cfg.temperature,
        )

    elif provider in ("vllm", "lmstudio"):
        from .vllm import VLLMProvider
        api_key = cfg.api_key or os.environ.get("OPENAI_API_KEY", "dummy")
        return VLLMProvider(
            base_url=cfg.base_url,
            model=cfg.model,
            api_key=api_key,
            timeout=cfg.timeout,
        )

    elif provider == "openai":
        from .vllm import VLLMProvider
        base = cfg.base_url or "https://api.openai.com/v1"
        api_key = cfg.api_key or os.environ.get("OPENAI_API_KEY", "")
        return VLLMProvider(
            base_url=base,
            model=cfg.model,
            api_key=api_key,
            timeout=cfg.timeout,
        )

    elif provider == "anthropic":
        from .anthropic_provider import AnthropicProvider
        api_key = cfg.api_key or os.environ.get("ANTHROPIC_API_KEY", "")
        return AnthropicProvider(
            api_key=api_key,
            model=cfg.model,
            timeout=cfg.timeout,
        )

    elif provider == "watsonx":
        from .watsonx import WatsonXProvider
        api_key    = cfg.api_key    or os.environ.get("WATSONX_API_KEY", "")
        project_id = cfg.project_id or os.environ.get("WATSONX_PROJECT_ID", "")
        base_url   = cfg.base_url   or "https://us-south.ml.cloud.ibm.com"
        username   = cfg.username   or os.environ.get("WATSONX_USERNAME", "")
        return WatsonXProvider(
            api_key=api_key,
            project_id=project_id,
            model=cfg.model,
            base_url=base_url,
            timeout=cfg.timeout,
            username=username,
            verify_ssl=cfg.verify_ssl,
        )

    else:
        raise ValueError(
            f"Unknown LLM provider '{cfg.provider}'. "
            "Choose from: ollama, vllm, lmstudio, openai, anthropic, watsonx"
        )
