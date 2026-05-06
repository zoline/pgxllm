"""
pgxllm.cache.tfidf_cache
-------------------------
TF-IDF 기반 SemanticCache.

내부 DB의 verified_queries 테이블을 사용한다.
SQL 정규화 키로 exact match 를 먼저 시도하고,
miss 시 pg_trgm 유사도 검색으로 fallback.
"""
from __future__ import annotations

import json
import logging
from typing import Optional

from pgxllm.cache.base import SemanticCache
from pgxllm.core.models import PipelineRequest, PipelineResult
from pgxllm.db.connections import ConnectionRegistry

log = logging.getLogger(__name__)


class TfidfSemanticCache(SemanticCache):
    """
    PostgreSQL pg_trgm 기반 유사 질문 캐시.

    HIT 조건:
      1. normalized question 완전 일치
      2. 또는 pg_trgm similarity >= threshold (기본 0.75)
    """

    def __init__(
        self,
        registry:  ConnectionRegistry,
        threshold: float = 0.75,
        top_k:     int   = 5,
    ):
        self._registry  = registry
        self._threshold = threshold
        self._top_k     = top_k

    def get(self, key: str, db_alias: str) -> Optional[PipelineResult]:
        """
        캐시 조회.
        1. exact match (question = key)
        2. trgm similarity fallback
        """
        try:
            with self._registry.internal.connection() as conn:
                # 1. Exact match
                row = conn.execute_one(
                    """
                    SELECT question, sql, execution_ok
                    FROM verified_queries
                    WHERE db_alias = %s
                      AND question = %s
                      AND execution_ok = TRUE
                    LIMIT 1
                    """,
                    (db_alias, key)
                )
                if row:
                    log.debug("Cache exact hit: %s", key[:60])
                    return self._row_to_result(key, db_alias, row)

                # 2. Trigram similarity (requires pg_trgm)
                row = conn.execute_one(
                    """
                    SELECT question, sql, execution_ok,
                           similarity(question, %s) AS sim
                    FROM verified_queries
                    WHERE db_alias = %s
                      AND execution_ok = TRUE
                      AND similarity(question, %s) >= %s
                    ORDER BY sim DESC
                    LIMIT 1
                    """,
                    (key, db_alias, key, self._threshold)
                )
                if row:
                    log.debug("Cache trgm hit (sim=%.2f): %s", row["sim"], key[:60])
                    return self._row_to_result(key, db_alias, row)

        except Exception as e:
            log.warning("Cache get error: %s", e)

        return None

    def set(self, key: str, db_alias: str, result: PipelineResult) -> None:
        if not result.execution_ok or not result.final_sql:
            return
        try:
            with self._registry.internal.connection() as conn:
                conn.execute(
                    """
                    INSERT INTO verified_queries (db_alias, question, sql, execution_ok, source)
                    VALUES (%s, %s, %s, TRUE, 'pipeline')
                    ON CONFLICT DO NOTHING
                    """,
                    (db_alias, key, result.final_sql)
                )
            log.debug("Cache set: %s", key[:60])
        except Exception as e:
            log.warning("Cache set error: %s", e)

    def invalidate(self, db_alias: str) -> int:
        try:
            with self._registry.internal.connection() as conn:
                rows = conn.execute(
                    "DELETE FROM verified_queries WHERE db_alias=%s AND source='pipeline' RETURNING id",
                    (db_alias,)
                )
            return len(rows)
        except Exception as e:
            log.warning("Cache invalidate error: %s", e)
            return 0

    def _row_to_result(self, key: str, db_alias: str, row: dict) -> PipelineResult:
        from pgxllm.core.models import PipelineRequest
        req = PipelineRequest(question=key, db_alias=db_alias)
        return PipelineResult(
            request=req,
            final_sql=row["sql"],
            execution_ok=True,
            cache_hit=True,
        )
