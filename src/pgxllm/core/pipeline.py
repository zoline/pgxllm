"""
pgxllm.core.pipeline
---------------------
PipelineRunner — Core Pipeline 전체 흐름을 조율한다.

  SemanticCache 확인
    HIT → 즉시 반환
    MISS → S1 → S2 → S3 → S4 (→ loop) → 결과 캐시
"""
from __future__ import annotations

import logging
import time
from typing import Optional

from pgxllm.cache.tfidf_cache import TfidfSemanticCache
from pgxllm.config import AppConfig
from pgxllm.core.llm.factory import create_llm_provider
from pgxllm.core.models import (
    PipelineRequest, PipelineResult, StageLog,
)
from pgxllm.core.s1_understanding import QuestionUnderstanding
from pgxllm.core.s2_schema_linking import SchemaLinker
from pgxllm.core.s3_generation import SQLGenerator
from pgxllm.core.s4_validation import SQLValidator
from pgxllm.db.connections import ConnectionRegistry
from pgxllm.parser.facade import SqlParser

log = logging.getLogger(__name__)


class PipelineRunner:
    """
    Core Pipeline 실행기.

    Usage::

        runner = PipelineRunner(registry, config)
        result = runner.run(PipelineRequest(
            question="2023년 부서별 매출 합계는?",
            db_alias="mydb",
        ))
        print(result.final_sql)
    """

    def __init__(
        self,
        registry: ConnectionRegistry,
        config:   AppConfig,
    ):
        self._registry  = registry
        self._config    = config
        self._parser    = SqlParser()

        # Components (lazy-initialized)
        self._llm       = None
        self._s1        = QuestionUnderstanding(registry)
        self._s2        = SchemaLinker(registry, config.graph)
        self._s3        = None   # needs LLM
        self._s4        = SQLValidator(registry)
        self._cache     = TfidfSemanticCache(
            registry,
            threshold=config.cache.tfidf.similarity_threshold,
            top_k=config.cache.tfidf.top_k,
        )

    def _ensure_llm(self):
        if self._llm is None:
            self._llm = create_llm_provider(self._config.llm)
            self._s3  = SQLGenerator(self._llm, self._registry)

    def run(self, request: PipelineRequest) -> PipelineResult:
        """
        전체 파이프라인을 실행한다.

        Returns:
            PipelineResult (execution_ok=True 이면 성공)
        """
        t_start   = time.perf_counter()
        stage_logs: list[StageLog] = []
        result     = PipelineResult(request=request)

        def elapsed_ms() -> int:
            return int((time.perf_counter() - t_start) * 1000)

        # ── 캐시 확인 ──────────────────────────────────────────
        cache_key = self._parser.normalize(request.question)
        cached    = self._cache.get(cache_key, request.db_alias)
        if cached:
            log.info("[Pipeline] cache hit for: %s", request.question[:60])
            cached.duration_ms = elapsed_ms()
            return cached

        # ── S1 Question Understanding ─────────────────────────
        t1 = time.perf_counter()
        try:
            analysis = self._s1.run(
                request.question,
                request.db_alias,
                top_k_tables=10,
            )
            stage_logs.append(StageLog(
                stage="s1", ok=True,
                duration_ms=int((time.perf_counter()-t1)*1000),
                detail={
                    "keywords":  analysis.keywords,
                    "tables":    analysis.candidate_tables,
                    "patterns":  [
                        {"name": p.name, "score": round(p.score, 3),
                         "instruction": p.instruction}
                        for p in analysis.matched_patterns
                    ],
                },
            ))
        except Exception as e:
            log.exception("[S1] failed")
            result.error      = f"S1 failed: {e}"
            result.stage_logs = stage_logs
            result.duration_ms = elapsed_ms()
            return result

        # ── S2 Schema Linking ─────────────────────────────────
        t2 = time.perf_counter()
        try:
            schema = self._s2.run(analysis, request.db_alias)
            stage_logs.append(StageLog(
                stage="s2", ok=True,
                duration_ms=int((time.perf_counter()-t2)*1000),
                detail={
                    "tables": [
                        {"table": t.table, "schema": t.schema,
                         "columns": [c["name"] for c in t.columns]}
                        for t in schema.tables
                    ],
                    "join_hint":     schema.join_hint,
                    "sample_context": schema.sample_context,
                    "schema_prompt": schema.to_prompt_text(),
                    "rules":         len(schema.dialect_rules),
                },
            ))
        except Exception as e:
            log.exception("[S2] failed")
            result.error      = f"S2 failed: {e}"
            result.stage_logs = stage_logs
            result.duration_ms = elapsed_ms()
            return result

        # ── S3 + S4 loop ──────────────────────────────────────
        self._ensure_llm()
        max_loops   = request.max_loops
        prev_sql    = None
        hint        = None
        final_sql   = None
        explanation = ""

        for attempt in range(1, max_loops + 1):
            # S3 Generate
            t3 = time.perf_counter()
            try:
                candidate = self._s3.run(
                    request.question, analysis, schema,
                    attempt=attempt,
                    prev_sql=prev_sql,
                    correction_hint=hint,
                    top_k_fewshot=request.top_k,
                )
                stage_logs.append(StageLog(
                    stage=f"s3_attempt{attempt}", ok=True,
                    duration_ms=int((time.perf_counter()-t3)*1000),
                    detail={
                        "sql":           candidate.sql,
                        "explanation":   candidate.explanation,
                        "raw_response":  candidate.raw_response,
                        "system_prompt": candidate.system_prompt,
                        "user_prompt":   candidate.user_prompt,
                    },
                ))
            except Exception as e:
                log.exception("[S3] attempt %d failed", attempt)
                stage_logs.append(StageLog(
                    stage=f"s3_attempt{attempt}", ok=False,
                    duration_ms=int((time.perf_counter()-t3)*1000),
                    detail={"error": str(e)},
                ))
                if attempt == max_loops:
                    result.error = f"S3 failed: {e}"
                break

            # S4 Validate
            t4 = time.perf_counter()
            validation = self._s4.validate(candidate, schema, db_alias=request.db_alias)
            stage_logs.append(StageLog(
                stage=f"s4_attempt{attempt}", ok=validation.ok,
                duration_ms=int((time.perf_counter()-t4)*1000),
                detail={
                    "ok":     validation.ok,
                    "issues": [
                        {"rule": i.rule, "message": i.message, "severity": i.severity}
                        for i in validation.issues
                    ],
                    "error":  validation.error,
                    "correction_hint": self._s4.correction_hint(validation) if not validation.ok else None,
                },
            ))

            if validation.ok:
                final_sql   = candidate.sql
                explanation = candidate.explanation
                log.info("[Pipeline] S4 passed on attempt %d", attempt)
                break
            else:
                # 실패 이력 기록
                self._s4.record_failure(request, candidate, validation)
                prev_sql = candidate.sql
                hint     = self._s4.correction_hint(validation)
                log.info("[Pipeline] S4 fail attempt %d: %s", attempt, hint)

                if attempt == max_loops:
                    # max loops 도달 — 마지막 SQL이라도 반환
                    final_sql   = candidate.sql
                    explanation = f"[검증 실패, {max_loops}회 시도] {hint}"
                    result.error = hint

        # ── 결과 구성 ─────────────────────────────────────────
        result.final_sql   = final_sql
        result.explanation = explanation
        result.stage_logs  = stage_logs
        result.duration_ms = elapsed_ms()

        if final_sql and not result.error:
            result.execution_ok = True
            # 캐시 저장
            self._cache.set(cache_key, request.db_alias, result)
            # verified_queries 에도 저장 (cache_key 사용으로 정규화 통일)
            self._save_verified(request, final_sql, cache_key)

        log.info("[Pipeline] %s", result.summary())
        return result

    def _save_verified(self, request: PipelineRequest, sql: str, key: str = "") -> None:
        """실행 성공한 SQL을 verified_queries 에 저장.
        key: 정규화된 캐시 키 (cache.set()과 동일한 값으로 저장해 삭제 일관성 유지).
        """
        question = key or request.question
        try:
            with self._registry.internal.connection() as conn:
                conn.execute(
                    """
                    INSERT INTO verified_queries
                        (db_alias, question, sql, execution_ok, source)
                    VALUES (%s, %s, %s, TRUE, 'pipeline')
                    ON CONFLICT DO NOTHING
                    """,
                    (request.db_alias, question, sql)
                )
        except Exception as e:
            log.debug("[Pipeline] save_verified error: %s", e)

    def _save_log(self, result: PipelineResult) -> None:
        """pipeline_logs 에 실행 이력 저장."""
        import json
        try:
            stage_json = json.dumps({
                s.stage: {"ok": s.ok, "ms": s.duration_ms, **s.detail}
                for s in result.stage_logs
            })
            with self._registry.internal.connection() as conn:
                conn.execute(
                    """
                    INSERT INTO pipeline_logs
                        (db_alias, question, final_sql, execution_ok,
                         stage_logs, duration_ms, cache_hit)
                    VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s)
                    """,
                    (
                        result.request.db_alias,
                        result.request.question,
                        result.final_sql,
                        result.execution_ok,
                        stage_json,
                        result.duration_ms,
                        result.cache_hit,
                    )
                )
        except Exception as e:
            log.debug("[Pipeline] save_log error: %s", e)
