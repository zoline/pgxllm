"""
pgxllm.core.llm.base
---------------------
LLMProvider ABC — provider-agnostic 추상화.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional


@dataclass
class LLMResponse:
    text:        str
    model:       str
    input_tokens:  int = 0
    output_tokens: int = 0
    raw:           dict = None

    def __post_init__(self):
        if self.raw is None:
            self.raw = {}


class LLMProvider(ABC):
    """
    Provider-agnostic LLM interface.
    구현체: OllamaProvider, VLLMProvider, AnthropicProvider
    """

    @abstractmethod
    def complete(
        self,
        system:  str,
        user:    str,
        *,
        temperature: float = 0.0,
        max_tokens:  int   = 2048,
    ) -> LLMResponse:
        """단일 system+user 턴 completion."""
        ...

    @property
    @abstractmethod
    def model_name(self) -> str:
        ...

    def ping(self) -> bool:
        """Provider 연결 테스트. True = 정상."""
        try:
            resp = self.complete("ping", "respond with 'ok'", max_tokens=10)
            return bool(resp.text)
        except Exception:
            return False
