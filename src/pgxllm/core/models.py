"""
pgxllm.core.models
------------------
Core Pipeline 데이터 클래스.

흐름:
  PipelineRequest
    → S1 → QuestionAnalysis
    → S2 → LinkedSchema
    → S3 → SQLCandidate[]
    → S4 → PipelineResult
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Optional


# ── Request ───────────────────────────────────────────────────

@dataclass
class PipelineRequest:
    """파이프라인 입력."""
    question:   str
    db_alias:   str
    hint:       Optional[str] = None   # BIRD hint or user note
    top_k:      int           = 3      # few-shot 수
    max_loops:  int           = 3      # S4 self-correction 최대 횟수


# ── S1 출력 ───────────────────────────────────────────────────

@dataclass
class MatchedPattern:
    """DynamicPatternMatcher 가 감지한 패턴."""
    pattern_id:  str
    name:        str
    score:       float
    instruction: str
    example_bad:  str = ""
    example_good: str = ""


@dataclass
class QuestionAnalysis:
    """S1 Question Understanding 결과."""
    question:         str
    matched_patterns: list[MatchedPattern]   = field(default_factory=list)
    candidate_tables: list[str]              = field(default_factory=list)
    # alias.schema.table 형식
    keywords:         list[str]              = field(default_factory=list)


# ── S2 출력 ───────────────────────────────────────────────────

@dataclass
class TableInfo:
    """Schema Linking 결과의 개별 테이블 정보."""
    address:     str               # alias.schema.table
    schema:      str
    table:       str
    columns:     list[dict]        # [{name, type, pk, fk, samples}, ...]
    comment:     Optional[str] = None
    row_count:   Optional[int] = None


@dataclass
class LinkedSchema:
    """S2 Schema Linking 결과 — LLM에 전달할 스키마 정보."""
    db_alias:    str
    tables:      list[TableInfo]   = field(default_factory=list)
    join_hint:   str               = ""    # LLM prompt용 JOIN 경로 텍스트
    dialect_rules: list[dict]      = field(default_factory=list)
    sample_context: str            = ""    # 샘플 데이터 요약 텍스트

    def to_prompt_text(self) -> str:
        """LLM prompt에 삽입할 스키마 텍스트."""
        lines = ["### 스키마 정보\n"]
        fk_pairs: list[str] = []

        for t in self.tables:
            lines.append(f"**{t.schema}.{t.table}**"
                         + (f"  — {t.comment}" if t.comment else ""))
            for c in t.columns:
                tags = []
                if c.get("pk"):   tags.append("PK")
                if c.get("fk"):
                    ref = c.get("fk_ref", "")
                    tags.append(f"FK→{ref}")
                    fk_pairs.append(
                        f"  {t.table}.{c['name']} → {ref}"
                    )
                tag_str = f" [{', '.join(tags)}]" if tags else ""
                samples = ""
                if c.get("samples"):
                    try:
                        import json
                        sv = c["samples"] if isinstance(c["samples"], list) else json.loads(c["samples"])
                        if sv:
                            samples = f"  예: {', '.join(str(v) for v in sv[:5])}"
                    except Exception:
                        pass
                lines.append(f"  - {c['name']} {c['type']}{tag_str}{samples}")
            lines.append("")

        # FK JOIN 조건 요약 — 명시적으로 제공해 LLM이 JOIN 조건 선택 시 우선 사용
        if fk_pairs:
            lines.append("### FK 관계 (JOIN 조건 기준)\n"
                         "아래 FK 관계를 JOIN 조건으로 우선 사용하세요:\n"
                         + "\n".join(fk_pairs) + "\n")

        if self.join_hint:
            lines.append("### JOIN 경로 힌트\n" + self.join_hint + "\n")

        if self.sample_context:
            lines.append("### 샘플 데이터\n" + self.sample_context + "\n")

        return "\n".join(lines) if lines else "(스키마 없음)"


# ── S3 출력 ───────────────────────────────────────────────────

@dataclass
class SQLCandidate:
    """S3 SQL Generation 결과 후보."""
    sql:           str
    explanation:   str  = ""
    raw_response:  str  = ""
    attempt:       int  = 1
    system_prompt: str  = ""
    user_prompt:   str  = ""


# ── S4 출력 ───────────────────────────────────────────────────

@dataclass
class ValidationIssue:
    """ValidationVisitor 가 감지한 구조 위반."""
    rule:    str
    message: str
    severity: str = "error"   # error | warning


@dataclass
class ValidationResult:
    """S4 Validation 결과."""
    sql:     str
    ok:      bool
    issues:  list[ValidationIssue] = field(default_factory=list)
    error:   Optional[str]         = None


# ── Pipeline 컨텍스트 / 결과 ──────────────────────────────────

@dataclass
class StageLog:
    """각 Stage 실행 로그."""
    stage:        str
    duration_ms:  int
    ok:           bool
    detail:       dict = field(default_factory=dict)


@dataclass
class PipelineResult:
    """Core Pipeline 최종 결과."""
    request:       PipelineRequest
    final_sql:     Optional[str]       = None
    explanation:   str                 = ""
    execution_ok:  bool                = False
    cache_hit:     bool                = False
    stage_logs:    list[StageLog]      = field(default_factory=list)
    error:         Optional[str]       = None
    duration_ms:   int                 = 0

    @property
    def ok(self) -> bool:
        return self.final_sql is not None and self.execution_ok

    def summary(self) -> str:
        if self.cache_hit:
            return f"[CACHE HIT] {self.duration_ms}ms"
        if self.ok:
            return f"[OK] {self.duration_ms}ms"
        return f"[FAILED] {self.error or 'unknown'} {self.duration_ms}ms"


# ── Few-shot 아이템 ────────────────────────────────────────────

@dataclass
class FewShotItem:
    """verified_queries 에서 로드한 검증된 SQL 예시."""
    question: str
    sql:      str
    score:    float = 1.0
