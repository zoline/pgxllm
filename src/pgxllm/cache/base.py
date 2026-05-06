"""
pgxllm.cache.base
-----------------
SemanticCache ABC.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional

from pgxllm.core.models import PipelineResult


class SemanticCache(ABC):
    """
    Semantic Cache for PipelineResult.

    Key: normalized question string (SqlParser.normalize)
    Value: PipelineResult (execution_ok=True 만 저장)
    """

    @abstractmethod
    def get(self, key: str, db_alias: str) -> Optional[PipelineResult]:
        """캐시에서 결과를 조회한다."""
        ...

    @abstractmethod
    def set(self, key: str, db_alias: str, result: PipelineResult) -> None:
        """execution_ok=True 인 결과를 캐시에 저장한다."""
        ...

    @abstractmethod
    def invalidate(self, db_alias: str) -> int:
        """특정 DB의 캐시를 전부 무효화한다. 삭제 건수 반환."""
        ...
