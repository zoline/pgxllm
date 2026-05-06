"""
pgxllm.intelligence.pattern_engine
-------------------------------------
DynamicPatternEngine — SQL 구조 패턴을 학습하고
S1 Question Understanding 시 동적으로 감지한다.

학습 경로:
  1. verified SQL → SqlParser.analyze_structure() → 패턴 후보 자동 학습
  2. 수동 등록 (CLI / Web UI)
  3. S4 실패 → patterns promote → 실패 학습
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Optional

from pgxllm.db.connections import ConnectionRegistry
from pgxllm.parser.facade import SqlParser
from pgxllm.parser.models import SqlStructure, TopNPattern

log = logging.getLogger(__name__)


# ── Matched pattern ───────────────────────────────────────────

@dataclass
class MatchedPattern:
    pattern_id:   str
    name:         str
    score:        float
    instruction:  str
    example_bad:  str
    example_good: str


# ── Engine ────────────────────────────────────────────────────

class DynamicPatternEngine:
    """
    Manages sql_patterns table and provides runtime pattern matching.

    Usage::

        engine = DynamicPatternEngine(registry)

        # 런타임: S1에서 질문 패턴 감지
        patterns = engine.match(question="부서별 매출 상위 5명은?", db_alias="mydb")
        prompt   = engine.build_prompt_injection(patterns)

        # 학습: verified SQL에서 패턴 추출
        engine.learn_from_verified(db_alias="mydb")

        # 수동 등록
        engine.add_pattern(name="top_n_then_detail", ...)

        # 실패 학습
        engine.promote_from_log(execution_id="...", instruction="...")
    """

    def __init__(self, registry: ConnectionRegistry):
        self._registry = registry
        self._parser   = SqlParser()

    # ── Runtime: match ────────────────────────────────────────

    def match(
        self,
        question: str,
        db_alias: str,
        *,
        min_score: float = 0.5,
    ) -> list[MatchedPattern]:
        """
        Match active patterns against a natural language question.
        Called by S1 Question Understanding.
        """
        with self._registry.internal.connection() as conn:
            patterns = conn.execute(
                """
                SELECT * FROM sql_patterns
                WHERE enabled = TRUE
                  AND (db_alias IS NULL OR db_alias = %s)
                ORDER BY hit_count DESC
                """,
                (db_alias,)
            )

        matched: list[MatchedPattern] = []
        question_lower = question.lower()

        for p in patterns:
            score = self._score_pattern(question_lower, p)
            if score >= min_score:
                matched.append(MatchedPattern(
                    pattern_id=str(p["id"]),
                    name=p["name"],
                    score=score,
                    instruction=p["instruction"],
                    example_bad=p.get("example_bad", ""),
                    example_good=p.get("example_good", ""),
                ))

        # Sort by score descending
        matched.sort(key=lambda x: -x.score)
        return matched

    def _score_pattern(self, question: str, pattern: dict) -> float:
        keywords   = json.loads(pattern["detect_keywords"]) \
                     if isinstance(pattern["detect_keywords"], str) \
                     else (pattern["detect_keywords"] or [])
        exclusions = json.loads(pattern["detect_exclusions"]) \
                     if isinstance(pattern["detect_exclusions"], str) \
                     else (pattern["detect_exclusions"] or [])

        if not keywords:
            return 0.0

        hits = sum(1 for kw in keywords if kw.lower() in question)
        exc  = sum(1 for ex in exclusions if ex.lower() in question)

        if hits == 0:
            return 0.0

        # Score: at least 0.6 if any keyword matches, up to 1.0
        score = 0.6 + 0.4 * (hits / len(keywords)) - exc * 0.3
        return max(0.0, min(1.0, score))

    # ── Prompt injection ──────────────────────────────────────

    def build_prompt_injection(self, patterns: list[MatchedPattern]) -> str:
        if not patterns:
            return ""

        lines = ["## SQL 구조 패턴 가이드"]
        lines.append("")
        for i, p in enumerate(patterns, 1):
            lines.append(f"### 패턴 {i}: {p.name}")
            lines.append(p.instruction)
            if p.example_bad:
                lines.append(f"❌ 나쁜 예:\n{p.example_bad}")
            if p.example_good:
                lines.append(f"✅ 좋은 예:\n{p.example_good}")
            lines.append("")
        return "\n".join(lines)

    # ── Learning: from verified SQL ───────────────────────────

    def learn_from_verified(
        self,
        db_alias:        str,
        *,
        min_confidence:  float = 0.8,
        min_occurrences: int   = 3,
    ) -> int:
        """
        Analyze verified_queries and generate pattern candidates.
        Patterns are created with enabled=FALSE until reviewed.

        Returns number of new patterns created.
        """
        with self._registry.internal.connection() as conn:
            queries = conn.execute(
                """
                SELECT question, sql FROM verified_queries
                WHERE db_alias = %s AND execution_ok = TRUE
                ORDER BY created_at DESC
                LIMIT 500
                """,
                (db_alias,)
            )

        if not queries:
            return 0

        # Analyze structure for each query
        pattern_counts: dict[str, dict] = {}

        for q in queries:
            structure = self._parser.analyze_structure(q["sql"])

            # Extract pattern signals
            signals = self._extract_signals(structure)
            for signal_key, signal_data in signals.items():
                if signal_key not in pattern_counts:
                    pattern_counts[signal_key] = {
                        "count": 0, "questions": [], "data": signal_data
                    }
                pattern_counts[signal_key]["count"] += 1
                if len(pattern_counts[signal_key]["questions"]) < 5:
                    pattern_counts[signal_key]["questions"].append(q["question"])

        # Create pattern candidates for frequent patterns
        created = 0
        for key, info in pattern_counts.items():
            if info["count"] < min_occurrences:
                continue
            if self._pattern_exists(key):
                continue

            self._create_candidate(db_alias, key, info)
            created += 1

        log.info("[%s] Pattern learning: %d new candidates", db_alias, created)
        return created

    def _extract_signals(self, structure: SqlStructure) -> dict[str, dict]:
        """Extract learnable signals from SqlStructure."""
        signals = {}

        # Top-N patterns
        if structure.top_n_pattern == TopNPattern.THEN_DETAIL:
            signals["top_n_then_detail"] = {
                "keywords": ["상위", "top", "최고", "최대", "가장 많"],
                "exclusions": [],
                "instruction": (
                    "상위 N개 추출 후 상세 집계 패턴입니다.\n"
                    "먼저 CTE/inline view로 상위 N개를 추출한 뒤,\n"
                    "그 결과에 대해 JOIN하여 상세 집계하세요.\n"
                    "최종 SELECT에 LIMIT을 직접 적용하지 마세요."
                ),
                "example_bad": (
                    "SELECT dept, month, SUM(sales)\n"
                    "FROM orders GROUP BY dept, month\n"
                    "LIMIT 5"
                ),
                "example_good": (
                    "WITH top_depts AS (\n"
                    "    SELECT dept FROM orders\n"
                    "    GROUP BY dept ORDER BY SUM(sales) DESC LIMIT 5\n"
                    ")\n"
                    "SELECT o.dept, o.month, SUM(o.sales)\n"
                    "FROM orders o JOIN top_depts t ON o.dept = t.dept\n"
                    "GROUP BY o.dept, o.month"
                ),
            }

        if structure.top_n_pattern == TopNPattern.PER_GROUP:
            signals["top_n_per_group"] = {
                "keywords": ["각", "별", "그룹별", "부서별", "per", "by"],
                "exclusions": [],
                "instruction": (
                    "그룹별 상위 N개 패턴입니다.\n"
                    "RANK() 또는 ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...) 를 사용하세요.\n"
                    "LIMIT 은 그룹별 제한에 사용할 수 없습니다."
                ),
                "example_good": (
                    "SELECT * FROM (\n"
                    "    SELECT *,\n"
                    "        RANK() OVER (PARTITION BY dept ORDER BY sales DESC) AS rnk\n"
                    "    FROM employees\n"
                    ") ranked WHERE rnk <= 3"
                ),
                "example_bad": (
                    "SELECT dept, name, sales FROM employees\n"
                    "ORDER BY dept, sales DESC LIMIT 3"
                ),
            }

        return signals

    def _pattern_exists(self, name: str) -> bool:
        with self._registry.internal.connection() as conn:
            row = conn.execute_one(
                "SELECT id FROM sql_patterns WHERE name=%s", (name,)
            )
        return row is not None

    def _create_candidate(self, db_alias: str, name: str, info: dict) -> None:
        data = info["data"]
        with self._registry.internal.connection() as conn:
            conn.execute(
                """
                INSERT INTO sql_patterns (
                    name, description, scope, dialect, db_alias,
                    detect_keywords, detect_exclusions,
                    instruction, example_bad, example_good,
                    auto_detected, confidence, enabled
                ) VALUES (%s,%s,'db','postgresql',%s,%s,%s,%s,%s,%s,TRUE,%s,FALSE)
                """,
                (
                    name,
                    f"Auto-detected from {info['count']} verified queries",
                    db_alias,
                    json.dumps(data.get("keywords", [])),
                    json.dumps(data.get("exclusions", [])),
                    data.get("instruction", ""),
                    data.get("example_bad", ""),
                    data.get("example_good", ""),
                    min(1.0, info["count"] / 10.0),
                )
            )

    # ── Learning: promote from failed log ─────────────────────

    def promote_from_log(
        self,
        execution_id: str,
        *,
        instruction:  Optional[str] = None,
        db_alias:     Optional[str] = None,
    ) -> Optional[str]:
        """
        Create a pattern candidate from a failed pipeline execution.
        Returns the created pattern name, or None if log not found.
        """
        with self._registry.internal.connection() as conn:
            log_row = conn.execute_one(
                "SELECT * FROM pipeline_logs WHERE id = %s",
                (execution_id,)
            )
        if not log_row:
            log.warning("Execution log not found: %s", execution_id)
            return None

        failed_sql = log_row.get("final_sql") or ""
        question   = log_row.get("question", "")
        db         = db_alias or log_row.get("db_alias", "")

        if failed_sql:
            structure = self._parser.analyze_structure(failed_sql)
            signals   = self._extract_signals(structure)
        else:
            signals   = {}

        if not signals:
            # Create a generic "needs review" pattern
            name = f"promoted_from_{execution_id[:8]}"
            with self._registry.internal.connection() as conn:
                conn.execute(
                    """
                    INSERT INTO sql_patterns (
                        name, description, scope, db_alias,
                        detect_keywords, detect_exclusions,
                        instruction, auto_detected, confidence, enabled
                    ) VALUES (%s,%s,'db',%s,'[]','[]',%s,TRUE,0.5,FALSE)
                    """,
                    (
                        name,
                        f"Promoted from failed execution {execution_id}",
                        db,
                        instruction or f"질문 '{question}' 에서 실패한 패턴. 검토 필요.",
                    )
                )
            return name

        # Use first detected signal
        signal_name = next(iter(signals))
        if not self._pattern_exists(signal_name):
            self._create_candidate(db, signal_name, {
                "count": 1,
                "data":  signals[signal_name],
                "questions": [question],
            })
        return signal_name

    # ── Manual add ────────────────────────────────────────────

    def add_pattern(
        self,
        *,
        name:         str,
        keywords:     list[str],
        instruction:  str,
        example_bad:  str = "",
        example_good: str = "",
        exclusions:   list[str] = None,
        db_alias:     Optional[str] = None,
        enabled:      bool = True,
    ) -> None:
        with self._registry.internal.connection() as conn:
            conn.execute(
                """
                INSERT INTO sql_patterns (
                    name, scope, db_alias,
                    detect_keywords, detect_exclusions,
                    instruction, example_bad, example_good,
                    auto_detected, confidence, enabled
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, FALSE, 1.0, %s)
                ON CONFLICT (name) DO UPDATE SET
                    detect_keywords  = EXCLUDED.detect_keywords,
                    detect_exclusions= EXCLUDED.detect_exclusions,
                    instruction      = EXCLUDED.instruction,
                    example_bad      = EXCLUDED.example_bad,
                    example_good     = EXCLUDED.example_good,
                    enabled          = EXCLUDED.enabled,
                    updated_at       = NOW()
                """,
                (
                    name,
                    "db" if db_alias else "global",
                    db_alias,
                    json.dumps(keywords),
                    json.dumps(exclusions or []),
                    instruction, example_bad, example_good,
                    enabled,
                )
            )
        log.info("Pattern added: %s", name)

    def approve_pattern(self, name: str) -> None:
        with self._registry.internal.connection() as conn:
            conn.execute(
                "UPDATE sql_patterns SET enabled=TRUE, updated_at=NOW() WHERE name=%s",
                (name,)
            )

    def list_patterns(
        self,
        db_alias: Optional[str] = None,
        enabled_only: bool = False,
    ) -> list[dict]:
        sql = "SELECT * FROM sql_patterns WHERE 1=1"
        params: list = []
        if enabled_only:
            sql += " AND enabled=TRUE"
        if db_alias:
            sql += " AND (db_alias=%s OR db_alias IS NULL)"
            params.append(db_alias)
        sql += " ORDER BY hit_count DESC, name"
        with self._registry.internal.connection() as conn:
            return conn.execute(sql, params)

    def record_hit(self, pattern_id: str, was_correct: bool) -> None:
        """Update hit_count and success_count after pattern application."""
        with self._registry.internal.connection() as conn:
            conn.execute(
                """
                UPDATE sql_patterns
                SET hit_count     = hit_count + 1,
                    success_count = success_count + %s,
                    updated_at    = NOW()
                WHERE id = %s
                """,
                (1 if was_correct else 0, pattern_id)
            )
