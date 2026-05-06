"""
pgxllm.core.llm.anthropic_provider
------------------------------------
Anthropic Claude API provider.
"""
from __future__ import annotations

import json
import logging
import os
import urllib.request
import urllib.error

from .base import LLMProvider, LLMResponse

log = logging.getLogger(__name__)


class AnthropicProvider(LLMProvider):
    """
    Anthropic Claude API provider.
    ANTHROPIC_API_KEY 환경변수 또는 생성자 파라미터로 키 설정.
    """

    API_URL = "https://api.anthropic.com/v1/messages"

    def __init__(
        self,
        api_key: str   = "",
        model:   str   = "claude-3-5-sonnet-20241022",
        timeout: int   = 120,
    ):
        self._api_key = api_key or os.environ.get("ANTHROPIC_API_KEY", "")
        self._model   = model
        self._timeout = timeout

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
        if not self._api_key:
            raise RuntimeError("ANTHROPIC_API_KEY 가 설정되지 않았습니다.")

        payload = {
            "model":      self._model,
            "max_tokens": max_tokens,
            "temperature": temperature,
            "system":     system,
            "messages":   [{"role": "user", "content": user}],
        }
        data = json.dumps(payload).encode()
        req  = urllib.request.Request(self.API_URL, data=data, headers={
            "Content-Type":      "application/json",
            "x-api-key":         self._api_key,
            "anthropic-version": "2023-06-01",
        })
        try:
            with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                body = json.loads(resp.read())
        except urllib.error.URLError as e:
            raise RuntimeError(f"Anthropic API error: {e}") from e

        text  = body["content"][0]["text"]
        usage = body.get("usage", {})
        return LLMResponse(
            text=text,
            model=self._model,
            input_tokens=usage.get("input_tokens", 0),
            output_tokens=usage.get("output_tokens", 0),
            raw=body,
        )
