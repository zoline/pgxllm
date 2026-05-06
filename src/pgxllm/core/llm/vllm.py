"""
pgxllm.core.llm.vllm
---------------------
vLLM OpenAI-compatible endpoint provider.
"""
from __future__ import annotations

import json
import logging
import urllib.request
import urllib.error

from .base import LLMProvider, LLMResponse

log = logging.getLogger(__name__)


class VLLMProvider(LLMProvider):
    """
    vLLM / LM Studio / any OpenAI-compatible endpoint.
    """

    def __init__(
        self,
        base_url:    str   = "http://localhost:8001/v1",
        model:       str   = "qwen2.5-coder-32b",
        api_key:     str   = "dummy",
        timeout:     int   = 120,
    ):
        self._base_url = base_url.rstrip("/")
        self._model    = model
        self._api_key  = api_key
        self._timeout  = timeout

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
            "model": self._model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user",   "content": user},
            ],
            "temperature": temperature,
            "max_tokens":  max_tokens,
        }
        url  = f"{self._base_url}/chat/completions"
        data = json.dumps(payload).encode()
        req  = urllib.request.Request(url, data=data, headers={
            "Content-Type":  "application/json",
            "Authorization": f"Bearer {self._api_key}",
        })
        try:
            with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                body = json.loads(resp.read())
        except urllib.error.URLError as e:
            raise RuntimeError(f"vLLM connection error: {e}") from e

        choice = body["choices"][0]
        text   = choice["message"]["content"]
        usage  = body.get("usage", {})
        return LLMResponse(
            text=text,
            model=self._model,
            input_tokens=usage.get("prompt_tokens", 0),
            output_tokens=usage.get("completion_tokens", 0),
            raw=body,
        )
