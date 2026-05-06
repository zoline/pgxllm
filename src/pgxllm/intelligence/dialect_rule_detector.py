"""
pgxllm.intelligence.dialect_rule_detector
------------------------------------------
DialectRuleDetector — 샘플 데이터 패턴을 분석하여
Dialect Rule을 자동으로 생성한다.

감지 패턴:
  - YYYYMM   (6자리 숫자 문자열) → SUBSTR 사용 rule
  - YYYYMMDD (8자리 숫자 문자열) → SUBSTR 사용 rule
  - YYYY-MM  (날짜 형식 문자열)  → SUBSTR 또는 LEFT 사용 rule
  - YYYY-MM-DD                   → ::date 캐스팅 허용 rule

생성된 Rule은 pgxllm.dialect_rules 에 저장되며
auto_detected=TRUE 로 표시된다.
"""
from __future__ import annotations

import json
import logging
import re
import uuid
from dataclasses import dataclass
from typing import Optional

from pgxllm.db.connections import ConnectionRegistry
from pgxllm.intelligence.sample_extractor import SampleResult

log = logging.getLogger(__name__)

# ── 패턴 정의 ─────────────────────────────────────────────────

@dataclass
class PatternDef:
    pattern_id:     str
    regex:          str
    min_match_ratio: float   # 샘플 중 이 비율 이상 매칭되어야 rule 생성
    forbidden_funcs: list[str]
    required_func:   Optional[str]
    instruction:     str
    example_bad:     str
    example_good:    str


_PATTERNS: list[PatternDef] = [
    PatternDef(
        pattern_id="text_date_yyyymm",
        regex=r"^\d{6}$",
        min_match_ratio=0.8,
        forbidden_funcs=["EXTRACT", "BETWEEN", "::date", "::timestamp"],
        required_func="SUBSTR",
        instruction=(
            "이 컬럼은 TEXT 타입의 YYYYMM 형식입니다.\n"
            "날짜 비교 시 SUBSTR을 사용하세요.\n"
            "EXTRACT, BETWEEN, ::date 캐스팅은 절대 사용하지 마세요."
        ),
        example_bad="EXTRACT(YEAR FROM {col}) = 2023",
        example_good="SUBSTR({col}, 1, 4) = '2023'",
    ),
    PatternDef(
        pattern_id="text_date_yyyymmdd",
        regex=r"^\d{8}$",
        min_match_ratio=0.8,
        forbidden_funcs=["EXTRACT", "BETWEEN"],
        required_func="SUBSTR",
        instruction=(
            "이 컬럼은 TEXT 타입의 YYYYMMDD 형식입니다.\n"
            "연도는 SUBSTR({col},1,4), 월은 SUBSTR({col},5,2)로 추출하세요."
        ),
        example_bad="EXTRACT(YEAR FROM {col}) = 2023",
        example_good="SUBSTR({col}, 1, 4) = '2023'",
    ),
    PatternDef(
        pattern_id="text_date_yyyy_mm",
        regex=r"^\d{4}-\d{2}$",
        min_match_ratio=0.8,
        forbidden_funcs=["EXTRACT", "BETWEEN"],
        required_func="LEFT",
        instruction=(
            "이 컬럼은 TEXT 타입의 YYYY-MM 형식입니다.\n"
            "연도 비교는 LEFT({col},4), 월 비교는 SUBSTRING({col},6,2)를 사용하세요."
        ),
        example_bad="EXTRACT(YEAR FROM {col}) = 2023",
        example_good="LEFT({col}, 4) = '2023'",
    ),
    PatternDef(
        pattern_id="text_date_yyyy_mm_dd",
        regex=r"^\d{4}-\d{2}-\d{2}$",
        min_match_ratio=0.8,
        forbidden_funcs=["BETWEEN"],
        required_func=None,
        instruction=(
            "이 컬럼은 TEXT 타입의 YYYY-MM-DD 형식입니다.\n"
            "::date 캐스팅 또는 TO_DATE()를 사용할 수 있습니다."
        ),
        example_bad="{col} BETWEEN '2023-01-01' AND '2023-12-31'",
        example_good="{col}::date BETWEEN '2023-01-01'::date AND '2023-12-31'::date",
    ),
]


# ── Detector ──────────────────────────────────────────────────

class DialectRuleDetector:
    """
    Analyzes sample values from SampleDataExtractor results
    and auto-generates DialectRules.

    Usage::

        detector = DialectRuleDetector(registry)
        rules = detector.detect(db_alias="mydb", samples=sample_results)
        detector.save(db_alias="mydb", rules=rules)
    """

    def __init__(self, registry: ConnectionRegistry):
        self._registry = registry

    def detect(
        self,
        db_alias: str,
        samples:  list[SampleResult],
    ) -> list[dict]:
        """
        Detect dialect rules from sample data.

        Returns:
            list of rule dicts ready for insert into dialect_rules
        """
        detected: list[dict] = []

        for sample in samples:
            if not sample.sample_values:
                continue

            for pat in _PATTERNS:
                ratio = self._match_ratio(sample.sample_values, pat.regex)
                if ratio < pat.min_match_ratio:
                    continue

                col_placeholder = f"{sample.schema_name}.{sample.table_name}.{sample.column_name}"
                rule = {
                    "rule_id":        f"{db_alias}__{sample.schema_name}__{sample.table_name}__{sample.column_name}__{pat.pattern_id}",
                    "scope":          "column",
                    "dialect":        "postgresql",
                    "db_alias":       db_alias,
                    "schema_name":    sample.schema_name,
                    "table_name":     sample.table_name,
                    "column_name":    sample.column_name,
                    "condition_json": json.dumps({
                        "data_type": sample.data_type,
                        "pattern":   pat.regex,
                        "match_ratio": ratio,
                    }),
                    "forbidden_funcs": pat.forbidden_funcs,  # list, not JSON string
                    "required_func":   pat.required_func,
                    "instruction":     pat.instruction.replace("{col}", col_placeholder),
                    "example_bad":     pat.example_bad.replace("{col}", sample.column_name),
                    "example_good":    pat.example_good.replace("{col}", sample.column_name),
                    "auto_detected":   True,
                    "enabled":         True,
                }
                detected.append(rule)
                log.info(
                    "Auto-detected rule [%s] for %s.%s.%s (ratio=%.2f)",
                    pat.pattern_id,
                    sample.schema_name, sample.table_name, sample.column_name,
                    ratio,
                )

        return detected

    def save(self, db_alias: str, rules: list[dict]) -> int:
        """
        Save detected rules to pgxllm.dialect_rules.
        Returns number of rules saved.
        """
        if not rules:
            return 0

        saved = 0
        with self._registry.internal.connection() as conn:
            for rule in rules:
                conn.execute(
                    """
                    INSERT INTO dialect_rules (
                        rule_id, scope, dialect, db_alias, schema_name,
                        table_name, column_name, condition_json,
                        forbidden_funcs, required_func, instruction,
                        example_bad, example_good, auto_detected, enabled
                    ) VALUES (
                        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
                    )
                    ON CONFLICT (rule_id) DO UPDATE SET
                        condition_json  = EXCLUDED.condition_json,
                        instruction     = EXCLUDED.instruction,
                        example_bad     = EXCLUDED.example_bad,
                        example_good    = EXCLUDED.example_good,
                        enabled         = EXCLUDED.enabled,
                        updated_at      = NOW()
                    """,
                    (
                        rule["rule_id"],
                        rule["scope"], rule["dialect"],
                        rule["db_alias"], rule["schema_name"],
                        rule["table_name"], rule["column_name"],
                        rule["condition_json"],
                        # forbidden_funcs may be list or already JSON string
                        json.dumps(rule["forbidden_funcs"])
                            if isinstance(rule["forbidden_funcs"], list)
                            else rule["forbidden_funcs"],
                        rule["required_func"],
                        rule["instruction"],
                        rule["example_bad"], rule["example_good"],
                        rule["auto_detected"], rule["enabled"],
                    )
                )
                saved += 1
        log.info("Saved %d auto-detected rules for %s", saved, db_alias)
        return saved

    def load_rules_for_column(
        self,
        db_alias:    str,
        schema_name: str,
        table_name:  str,
        column_name: str,
    ) -> list[dict]:
        """Load enabled dialect rules for a specific column."""
        with self._registry.internal.connection() as conn:
            return conn.execute(
                """
                SELECT *
                FROM dialect_rules
                WHERE enabled = TRUE
                  AND (
                    scope = 'global'
                    OR (scope = 'db'     AND db_alias    = %s)
                    OR (scope = 'table'  AND db_alias    = %s
                                         AND schema_name = %s
                                         AND table_name  = %s)
                    OR (scope = 'column' AND db_alias    = %s
                                         AND schema_name = %s
                                         AND table_name  = %s
                                         AND column_name = %s)
                  )
                ORDER BY scope DESC
                """,
                (
                    db_alias,
                    db_alias, schema_name, table_name,
                    db_alias, schema_name, table_name, column_name,
                )
            )

    # ── Helpers ───────────────────────────────────────────────

    @staticmethod
    def _match_ratio(values: list[str], pattern: str) -> float:
        if not values:
            return 0.0
        compiled = re.compile(pattern)
        matched  = sum(1 for v in values if compiled.match(str(v)))
        return matched / len(values)
