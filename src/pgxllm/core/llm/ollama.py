"""
pgxllm.core.llm.ollama
-----------------------
Ollama LLM provider.
"""
from __future__ import annotations

import json
import logging
import urllib.request
import urllib.error
from typing import Optional

from .base import LLMProvider, LLMResponse

log = logging.getLogger(__name__)


class OllamaProvider(LLMProvider):
    """
    Ollama HTTP API provider.
    기본 엔드포인트: http://localhost:11434
    """

    def __init__(
        self,
        base_url:    str   = "http://localhost:11434",
        model:       str   = "qwen2.5-coder:32b",
        timeout:     int   = 120,
        temperature: float = 0.0,
    ):
        self._base_url    = base_url.rstrip("/")
        self._model       = model
        self._timeout     = timeout
        self._temperature = temperature

    @property
    def model_name(self) -> str:
        return self._model

    def complete(
        self,
        system:      str,
        user:        str,
        *,
        temperature: float = 0.0,
        max_tokens:  int   = 2048,
    ) -> LLMResponse:
        payload = {
            "model":  self._model,
            "stream": False,
            "options": {
                "temperature": temperature,
                "num_predict": max_tokens,
            },
            "messages": [
                {"role": "system",    "content": system},
                {"role": "user",      "content": user},
            ],
        }
        url  = f"{self._base_url}/api/chat"
        data = json.dumps(payload).encode()
        req  = urllib.request.Request(url, data=data,
                                      headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                body = json.loads(resp.read())
        except urllib.error.URLError as e:
            raise RuntimeError(f"Ollama connection error: {e}") from e

        text = body.get("message", {}).get("content", "")
        return LLMResponse(
            text=text,
            model=self._model,
            input_tokens=body.get("prompt_eval_count", 0),
            output_tokens=body.get("eval_count", 0),
            raw=body,
        )
