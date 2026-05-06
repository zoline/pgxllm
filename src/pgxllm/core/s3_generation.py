"""
pgxllm.core.s3_generation
--------------------------
S3 SQL Generation

수행:
  1. SYSTEM_PROMPT 조립 (base + dialect_rules + dynamic_patterns)
  2. USER_PROMPT 조립 (schema + few-shot + question)
  3. LLM 호출 → SQL 파싱
"""
from __future__ import annotations

import logging
import re
from typing import Optional

from pgxllm.core.llm.base import LLMProvider
from pgxllm.core.models import (
    FewShotItem, LinkedSchema, QuestionAnalysis,
    SQLCandidate,
)
from pgxllm.db.connections import ConnectionRegistry
from pgxllm.intelligence.rule_engine import RuleEngine
from pgxllm.intelligence.pattern_engine import DynamicPatternEngine

log = logging.getLogger(__name__)

# ── Base system prompt ────────────────────────────────────────
_BASE_SYSTEM = """\
당신은 PostgreSQL 전문가입니다.
아래 지침을 철저히 따르세요.

## 핵심 규칙
- 반드시 유효한 PostgreSQL SQL만 생성합니다.
- SELECT / WITH / EXPLAIN 만 허용됩니다. INSERT, UPDATE, DELETE, DROP 등은 절대 금지.
- **[CRITICAL] 테이블명과 컬럼명은 반드시 아래 "스키마 정보"에 나열된 이름을 그대로 사용합니다. 절대로 이름을 추측하거나 변형하지 마세요.**
  예) 스키마에 "rental"이 있으면 "rental"을 사용. "rentals"로 바꾸지 말 것.
- **[CRITICAL] Use ONLY the exact table names and column names from the schema below. Do NOT guess or modify names.**
  Example: if the schema shows table "rental", use "rental" — NOT "rentals".
- 데이터 타입을 확인하고 적절한 함수를 사용합니다.
- 결과는 반드시 다음 형식으로 반환합니다:

```sql
<SQL here>
```

설명: <한 문장 설명>
"""


class SQLGenerator:
    """
    S3 — LinkedSchema + QuestionAnalysis 를 기반으로 SQL을 생성한다.
    """

    def __init__(
        self,
        llm:      LLMProvider,
        registry: ConnectionRegistry,
    ):
        self._llm          = llm
        self._registry     = registry
        self._rule_engine  = RuleEngine(registry)
        self._pattern_eng  = DynamicPatternEngine(registry)

    def run(
        self,
        question:  str,
        analysis:  QuestionAnalysis,
        schema:    LinkedSchema,
        *,
        top_k_fewshot: int   = 3,
        attempt:       int   = 1,
        prev_sql:      Optional[str]   = None,
        correction_hint: Optional[str] = None,
    ) -> SQLCandidate:
        """
        Args:
            question:         자연어 질문
            analysis:         S1 결과
            schema:           S2 LinkedSchema
            top_k_fewshot:    few-shot 예시 수
            attempt:          현재 시도 횟수 (self-correction)
            prev_sql:         이전 시도 SQL (correction 시)
            correction_hint:  이전 실패 이유 (correction 시)

        Returns:
            SQLCandidate
        """
        log.info("[S3] attempt=%d, question=%s", attempt, question[:60])

        # ── System prompt 조립 ──────────────────────────────────
        system = self._build_system(schema, analysis)

        # ── User prompt 조립 ────────────────────────────────────
        user   = self._build_user(
            question, schema, analysis,
            top_k_fewshot=top_k_fewshot,
            attempt=attempt,
            prev_sql=prev_sql,
            correction_hint=correction_hint,
        )

        log.debug("[S3] system(%d) user(%d)", len(system), len(user))

        # ── LLM 호출 ─────────────────────────────────────────────
        resp = self._llm.complete(
            system, user,
            temperature=0.0 if attempt == 1 else 0.2,
            max_tokens=2048,
        )
        log.debug("[S3] raw response: %s", resp.text[:200])

        # ── SQL 파싱 ──────────────────────────────────────────────
        sql, explanation = self._parse_response(resp.text)

        return SQLCandidate(
            sql=sql,
            explanation=explanation,
            raw_response=resp.text,
            attempt=attempt,
            system_prompt=system,
            user_prompt=user,
        )

    # ── Prompt builders ────────────────────────────────────────

    def _build_system(
        self,
        schema:   LinkedSchema,
        analysis: QuestionAnalysis,
    ) -> str:
        parts = [_BASE_SYSTEM]

        # Dialect rules
        if schema.dialect_rules:
            rule_text = self._rule_engine.build_prompt_injection(schema.dialect_rules)
            if rule_text:
                parts.append("\n## Dialect 규칙\n" + rule_text)

        # Dynamic pattern instructions
        if analysis.matched_patterns:
            pattern_lines = []
            for p in analysis.matched_patterns:
                pattern_lines.append(f"\n### 패턴: {p.name}")
                pattern_lines.append(p.instruction)
                if p.example_bad:
                    pattern_lines.append(f"❌ 잘못된 예: {p.example_bad}")
                if p.example_good:
                    pattern_lines.append(f"✅ 올바른 예: {p.example_good}")
            parts.append("\n## SQL 구조 패턴\n" + "\n".join(pattern_lines))

        return "\n".join(parts)

    def _build_user(
        self,
        question: str,
        schema:   LinkedSchema,
        analysis: QuestionAnalysis,
        *,
        top_k_fewshot:   int,
        attempt:         int,
        prev_sql:        Optional[str],
        correction_hint: Optional[str],
    ) -> str:
        parts = []

        # Schema
        parts.append(schema.to_prompt_text())

        # Few-shot examples
        few_shots = self._load_few_shots(
            schema.db_alias, question, top_k=top_k_fewshot
        )
        if few_shots:
            parts.append("### 유사 질문 예시\n")
            for fs in few_shots:
                parts.append(f"Q: {fs.question}\n```sql\n{fs.sql}\n```\n")

        # Correction context (attempt > 1)
        if attempt > 1 and prev_sql:
            parts.append(f"### 이전 시도 ({attempt-1}차) — 수정 필요\n")
            parts.append(f"```sql\n{prev_sql}\n```\n")
            if correction_hint:
                parts.append(f"**오류/문제점:** {correction_hint}\n")
            parts.append("위 문제를 수정하여 올바른 SQL을 생성해 주세요.\n")

        # Question
        parts.append(f"### 질문\n{question}")
        if analysis.keywords:
            parts.append(f"\n*키워드: {', '.join(analysis.keywords[:10])}*")

        return "\n".join(parts)

    def _load_few_shots(
        self, db_alias: str, question: str, top_k: int
    ) -> list[FewShotItem]:
        """verified_queries 에서 유사 few-shot 예시 로드."""
        if top_k <= 0:
            return []
        try:
            with self._registry.internal.connection() as conn:
                rows = conn.execute(
                    """
                    SELECT question, sql
                    FROM verified_queries
                    WHERE db_alias = %s AND execution_ok = TRUE
                    ORDER BY similarity(question, %s) DESC
                    LIMIT %s
                    """,
                    (db_alias, question, top_k)
                )
            return [FewShotItem(question=r["question"], sql=r["sql"]) for r in rows]
        except Exception as e:
            log.debug("[S3] few-shot load error: %s", e)
            # Fallback without similarity
            try:
                with self._registry.internal.connection() as conn:
                    rows = conn.execute(
                        """
                        SELECT question, sql FROM verified_queries
                        WHERE db_alias = %s AND execution_ok = TRUE
                        ORDER BY created_at DESC LIMIT %s
                        """,
                        (db_alias, top_k)
                    )
                return [FewShotItem(question=r["question"], sql=r["sql"]) for r in rows]
            except Exception:
                return []

    # ── Response parser ────────────────────────────────────────

    def _parse_response(self, text: str) -> tuple[str, str]:
        """
        LLM 응답에서 SQL 블록과 설명을 추출한다.

        형식:
            ```sql
            SELECT ...
            ```
            설명: ...
        """
        # ```sql ... ``` 또는 ``` ... ``` 블록 추출
        sql_match = re.search(
            r"```(?:sql)?\s*\n?(.*?)```",
            text, re.DOTALL | re.IGNORECASE
        )
        if sql_match:
            sql = sql_match.group(1).strip()
        else:
            # 블록 없으면 전체 텍스트에서 SELECT / WITH 시작 부분 추출
            lines = text.strip().splitlines()
            sql_lines = []
            in_sql = False
            for line in lines:
                upper = line.strip().upper()
                if upper.startswith(("SELECT", "WITH", "EXPLAIN")):
                    in_sql = True
                if in_sql:
                    if line.strip().startswith("설명") or line.strip().startswith("Explanation"):
                        break
                    sql_lines.append(line)
            sql = "\n".join(sql_lines).strip() if sql_lines else text.strip()

        # 설명 추출
        explanation = ""
        expl_match = re.search(r"설명:\s*(.+)", text)
        if not expl_match:
            expl_match = re.search(r"Explanation:\s*(.+)", text, re.IGNORECASE)
        if expl_match:
            explanation = expl_match.group(1).strip()

        return sql, explanation
