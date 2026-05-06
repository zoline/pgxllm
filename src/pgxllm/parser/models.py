"""
pgxllm.parser.models
--------------------
Data classes for SQL parsing results.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


# ── Enums ─────────────────────────────────────────────────

class JoinType(str, Enum):
    INNER    = "INNER"
    LEFT     = "LEFT"
    RIGHT    = "RIGHT"
    FULL     = "FULL"
    CROSS    = "CROSS"
    IMPLICIT = "IMPLICIT"   # comma-separated FROM / WHERE condition


class JoinSource(str, Enum):
    EXPLICIT_JOIN  = "explicit_join"
    IMPLICIT_JOIN  = "implicit_join"   # FROM a, b WHERE a.id = b.id
    CTE            = "cte"
    SUBQUERY       = "subquery"


class LimitPosition(str, Enum):
    NONE        = "none"
    FINAL       = "final"        # outermost SELECT
    INLINE_VIEW = "inline_view"  # subquery in FROM
    CTE         = "cte"          # inside CTE body


class TopNPattern(str, Enum):
    NONE        = "none"
    SIMPLE      = "simple"        # ORDER BY + LIMIT  (단순 상위 N)
    THEN_DETAIL = "then_detail"   # 상위 N 추출 후 상세 집계
    PER_GROUP   = "per_group"     # RANK/ROW_NUMBER + PARTITION BY


class DateFuncType(str, Enum):
    EXTRACT   = "EXTRACT"
    SUBSTR    = "SUBSTR"
    BETWEEN   = "BETWEEN"
    CAST      = "CAST_DATE"
    OTHER     = "OTHER"


class Severity(str, Enum):
    ERROR   = "error"
    WARNING = "warning"
    INFO    = "info"


# ── Extracted Relation ────────────────────────────────────

@dataclass
class ExtractedRelation:
    """A table-to-table relationship found in a SQL statement."""
    from_table:  str
    from_column: str
    to_table:    str
    to_column:   str
    join_type:   JoinType  = JoinType.INNER
    source:      JoinSource = JoinSource.EXPLICIT_JOIN
    confidence:  float      = 1.0
    raw_sql:     str        = ""   # snippet that produced this relation

    def __hash__(self) -> int:
        return hash((
            self.from_table.lower(), self.from_column.lower(),
            self.to_table.lower(),   self.to_column.lower()
        ))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ExtractedRelation):
            return False
        return (
            self.from_table.lower()  == other.from_table.lower()  and
            self.from_column.lower() == other.from_column.lower() and
            self.to_table.lower()    == other.to_table.lower()    and
            self.to_column.lower()   == other.to_column.lower()
        )

    def reversed(self) -> "ExtractedRelation":
        """Return the same relation with from/to swapped."""
        return ExtractedRelation(
            from_table=self.to_table,
            from_column=self.to_column,
            to_table=self.from_table,
            to_column=self.from_column,
            join_type=self.join_type,
            source=self.source,
            confidence=self.confidence,
        )


# ── Date Function Usage ───────────────────────────────────

@dataclass
class DateFuncUsage:
    column:    str
    table:     Optional[str]
    func_type: DateFuncType
    pattern:   str   # e.g. "YYYYMM", "YYYYMMDD", "date_column"


# ── SQL Structure ─────────────────────────────────────────

@dataclass
class SqlStructure:
    """Structural analysis of a SQL statement."""

    # Tables and aliases
    tables:  list[str]            = field(default_factory=list)
    aliases: dict[str, str]       = field(default_factory=dict)   # alias → table

    # Aggregation
    has_group_by:    bool         = False
    has_having:      bool         = False
    has_window_func: bool         = False
    window_funcs:    list[str]    = field(default_factory=list)    # RANK, ROW_NUMBER, …

    # Top-N
    limit_position:  LimitPosition = LimitPosition.NONE
    has_order_by:    bool           = False
    top_n_pattern:   TopNPattern    = TopNPattern.NONE
    top_n_value:     Optional[int]  = None   # the N in "상위 N개"

    # Function usage
    date_funcs: list[DateFuncUsage] = field(default_factory=list)

    # Structure
    has_cte:        bool = False
    cte_count:      int  = 0
    has_subquery:   bool = False
    subquery_depth: int  = 0
    join_count:     int  = 0
    has_union:      bool = False

    def table_count(self) -> int:
        return len(set(self.tables))


# ── Validation ────────────────────────────────────────────

@dataclass
class Violation:
    rule_id:    str
    severity:   Severity
    location:   str        # "line:col" or description
    message:    str
    suggestion: str = ""


@dataclass
class ValidationResult:
    is_valid:   bool
    violations: list[Violation] = field(default_factory=list)

    @property
    def errors(self) -> list[Violation]:
        return [v for v in self.violations if v.severity == Severity.ERROR]

    @property
    def warnings(self) -> list[Violation]:
        return [v for v in self.violations if v.severity == Severity.WARNING]

    def add(self, violation: Violation) -> None:
        self.violations.append(violation)
        if violation.severity == Severity.ERROR:
            self.is_valid = False
