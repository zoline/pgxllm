"""
pgxllm.core.s1_understanding
------------------------------
S1 Question Understanding

수행:
  1. DynamicPatternEngine 으로 Top-N / GROUP BY 등 SQL 구조 패턴 감지
  2. 키워드 추출 (질문에서 테이블/컬럼 힌트)
  3. schema_catalog 에서 후보 테이블 검색 (pg_trgm fulltext)
"""
from __future__ import annotations

import logging
import re
from typing import Optional

from pgxllm.core.models import MatchedPattern, QuestionAnalysis
from pgxllm.db.connections import ConnectionRegistry
from pgxllm.intelligence.pattern_engine import DynamicPatternEngine
from pgxllm.intelligence.pattern_engine import MatchedPattern as IntelMatchedPattern

log = logging.getLogger(__name__)

# 한국어 / 영어 불용어 — 테이블명 검색에서 제외
_STOPWORDS = {
    "의", "은", "는", "이", "가", "을", "를", "에", "도", "로", "과", "와",
    "및", "에서", "으로", "한", "하는", "있는", "없는", "모든",
    "the", "a", "an", "of", "in", "for", "with", "and", "or",
    "show", "me", "get", "find", "list", "what", "how", "many",
    "알려줘", "보여줘", "구해줘", "있나요", "얼마나", "어떤",
}


class QuestionUnderstanding:
    """
    S1 — 질문에서 패턴과 후보 테이블을 추출한다.
    """

    def __init__(self, registry: ConnectionRegistry):
        self._registry = registry
        self._pattern_engine = DynamicPatternEngine(registry)

    def run(
        self,
        question: str,
        db_alias: str,
        *,
        top_k_tables:  int   = 10,
        min_pattern_score: float = 0.5,
    ) -> QuestionAnalysis:
        """
        Args:
            question:          자연어 질문
            db_alias:          target DB alias
            top_k_tables:      후보 테이블 최대 수
            min_pattern_score: 패턴 감지 최소 점수

        Returns:
            QuestionAnalysis
        """
        log.info("[S1] question: %s", question[:80])

        # ── 1. 패턴 감지 ────────────────────────────────────────
        raw_patterns = self._pattern_engine.match(
            question, db_alias, min_score=min_pattern_score
        )
        matched = [
            MatchedPattern(
                pattern_id=p.pattern_id,
                name=p.name,
                score=p.score,
                instruction=p.instruction,
                example_bad=p.example_bad,
                example_good=p.example_good,
            )
            for p in raw_patterns
        ]
        if matched:
            log.info("[S1] patterns: %s", [p.name for p in matched])

        # ── 2. 키워드 추출 ───────────────────────────────────────
        keywords = self._extract_keywords(question)
        log.debug("[S1] keywords: %s", keywords)

        # ── 3. 후보 테이블 검색 ──────────────────────────────────
        candidate_tables = self._search_tables(
            db_alias, keywords, top_k=top_k_tables
        )
        log.info("[S1] candidate tables: %s", candidate_tables)

        return QuestionAnalysis(
            question=question,
            matched_patterns=matched,
            candidate_tables=candidate_tables,
            keywords=keywords,
        )

    # ── Helpers ────────────────────────────────────────────────

    def _extract_keywords(self, question: str) -> list[str]:
        """
        질문에서 의미있는 단어를 추출한다.
        한국어 단어 경계를 단순하게 처리 (공백/특수문자 기준 분리).
        """
        # Remove common question markers
        text = re.sub(r'[?？!！.,，。]', ' ', question)
        tokens = re.split(r'[\s\u3000]+', text)
        keywords = [
            t.lower() for t in tokens
            if len(t) >= 2 and t.lower() not in _STOPWORDS
        ]
        # Deduplicate while preserving order
        seen = set()
        result = []
        for k in keywords:
            if k not in seen:
                seen.add(k)
                result.append(k)
        return result

    def _search_tables(
        self,
        db_alias: str,
        keywords: list[str],
        top_k:    int,
    ) -> list[str]:
        """
        schema_catalog 에서 GIN fulltext 또는 trgm 유사도로
        관련 테이블을 검색한다. alias.schema.table 형식 반환.
        """
        if not keywords:
            return self._fallback_tables(db_alias, top_k)

        search_text = " ".join(keywords)
        try:
            with self._registry.internal.connection() as conn:
                rows = conn.execute(
                    """
                    SELECT DISTINCT db_alias, schema_name, table_name,
                        ts_rank(
                            to_tsvector('english',
                                COALESCE(table_name,'') || ' ' ||
                                COALESCE(column_name,'') || ' ' ||
                                COALESCE(comment_text,'')
                            ),
                            plainto_tsquery('english', %s)
                        ) AS rank
                    FROM schema_catalog
                    WHERE db_alias = %s
                      AND to_tsvector('english',
                            COALESCE(table_name,'') || ' ' ||
                            COALESCE(column_name,'') || ' ' ||
                            COALESCE(comment_text,'')
                          ) @@ plainto_tsquery('english', %s)
                    ORDER BY rank DESC
                    LIMIT %s
                    """,
                    (search_text, db_alias, search_text, top_k)
                )

            if rows:
                return [
                    f"{r['db_alias']}.{r['schema_name']}.{r['table_name']}"
                    for r in rows
                ]

            # fallback: ILIKE
            return self._ilike_search(db_alias, keywords, top_k)

        except Exception as e:
            log.warning("[S1] table search error: %s", e)
            return self._fallback_tables(db_alias, top_k)

    def _ilike_search(
        self, db_alias: str, keywords: list[str], top_k: int
    ) -> list[str]:
        """pg_trgm 없는 환경 fallback — ILIKE."""
        try:
            conditions = " OR ".join(
                ["table_name ILIKE %s OR column_name ILIKE %s"] * len(keywords)
            )
            params = []
            for kw in keywords:
                params += [f"%{kw}%", f"%{kw}%"]
            params += [db_alias, top_k]

            with self._registry.internal.connection() as conn:
                rows = conn.execute(
                    f"""
                    SELECT DISTINCT db_alias, schema_name, table_name
                    FROM schema_catalog
                    WHERE ({conditions}) AND db_alias = %s
                    LIMIT %s
                    """,
                    params
                )
            return [
                f"{r['db_alias']}.{r['schema_name']}.{r['table_name']}"
                for r in rows
            ]
        except Exception as e:
            log.warning("[S1] ilike search error: %s", e)
            return self._fallback_tables(db_alias, top_k)

    def _fallback_tables(self, db_alias: str, top_k: int) -> list[str]:
        """스키마 검색 실패 시 — row_count 기준 상위 테이블 반환."""
        try:
            with self._registry.internal.connection() as conn:
                rows = conn.execute(
                    """
                    SELECT DISTINCT db_alias, schema_name, table_name
                    FROM schema_catalog
                    WHERE db_alias = %s AND column_name IS NULL
                    LIMIT %s
                    """,
                    (db_alias, top_k)
                )
            return [
                f"{r['db_alias']}.{r['schema_name']}.{r['table_name']}"
                for r in rows
            ]
        except Exception:
            return []
