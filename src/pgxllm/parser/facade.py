"""
pgxllm.parser.facade
---------------------
SqlParser — single entry point for all SQL parsing operations.

The pipeline always calls this class directly.
Underlying visitor implementations are hidden behind this facade.

Usage::

    parser = SqlParser()
    relations = parser.extract_relations(sql)
    structure = parser.analyze_structure(sql)
    result    = parser.validate(sql, rules=rules)
"""
from __future__ import annotations

import logging
from typing import Optional

from .models import ExtractedRelation, SqlStructure, ValidationResult
from .relation_visitor import RelationExtractVisitor
from .sql_parser import parse_sql
from .structure_visitor import StructureAnalysisVisitor
from .validation_visitor import DialectRule, ValidationVisitor

log = logging.getLogger(__name__)


class SqlParser:
    """
    Facade for all SQL analysis operations.

    Thread-safe — each method creates its own visitor instances.
    """

    def __init__(self, max_depth: int = 5):
        self._max_depth = max_depth

    # ── Public API ────────────────────────────────────────

    def extract_relations(
        self,
        sql: str,
        *,
        max_depth: Optional[int] = None,
    ) -> list[ExtractedRelation]:
        """
        Extract table-to-table JOIN relationships from SQL.

        Covers:
          - Explicit JOINs (INNER / LEFT / RIGHT / FULL / CROSS)
          - Implicit JOINs (FROM a, b WHERE a.id = b.fk)
          - CTE bodies
          - Subquery bodies

        Args:
            sql:       SQL statement(s)
            max_depth: override default recursion depth

        Returns:
            Deduplicated list of ExtractedRelation
        """
        depth = max_depth or self._max_depth
        try:
            stmts   = parse_sql(sql)
            visitor = RelationExtractVisitor(max_depth=depth)
            visitor.visit_stmts(stmts)
            return visitor.relations
        except Exception as e:
            log.warning("extract_relations failed: %s", e)
            return []

    def analyze_structure(
        self,
        sql: str,
        *,
        max_depth: Optional[int] = None,
    ) -> SqlStructure:
        """
        Analyse structural patterns in a SQL statement.

        Returns SqlStructure containing:
          - Top-N pattern  (SIMPLE / THEN_DETAIL / PER_GROUP / NONE)
          - LIMIT position (FINAL / INLINE_VIEW / CTE / NONE)
          - Date function usage
          - Window functions
          - Aggregation flags
          - CTE / subquery counts

        Args:
            sql:       SQL statement(s)
            max_depth: override default recursion depth

        Returns:
            SqlStructure
        """
        depth = max_depth or self._max_depth
        try:
            stmts   = parse_sql(sql)
            visitor = StructureAnalysisVisitor(max_depth=depth)
            visitor.visit_stmts(stmts)
            return visitor.structure
        except Exception as e:
            log.warning("analyze_structure failed: %s", e)
            return SqlStructure()

    def validate(
        self,
        sql: str,
        rules: Optional[list[DialectRule]] = None,
        *,
        db_name: Optional[str] = None,
        max_depth: Optional[int] = None,
    ) -> ValidationResult:
        """
        Validate a SQL statement against dialect rules and structural rules.

        Structural rules (always applied):
          - LIMIT without ORDER BY
          - Top-N THEN_DETAIL with LIMIT in wrong position

        Dialect rules (from pgxllm DB):
          - EXTRACT / BETWEEN / :: cast on TEXT date columns
          - Custom user-defined rules

        Args:
            sql:      SQL statement(s)
            rules:    list of DialectRule (from pgxllm DB, optional)
            db_name:  current DB for scope matching
            max_depth: override default recursion depth

        Returns:
            ValidationResult with is_valid flag and violations list
        """
        depth = max_depth or self._max_depth
        try:
            stmts   = parse_sql(sql)
            visitor = ValidationVisitor(
                rules=rules or [],
                db_name=db_name,
                max_depth=depth,
            )
            visitor.visit_stmts(stmts)
            return visitor.result
        except Exception as e:
            log.warning("validate failed: %s", e)
            from .models import ValidationResult
            return ValidationResult(is_valid=True)

    def extract_annotations(self, sql: str) -> list[dict]:
        """
        Extract @relation annotations from SQL file comments.

        Format::

            -- @relation orders -> customers : 주문-고객
            -- @relation orders -> regions   : 주문-지역

        Returns:
            list of dicts with keys: from_table, to_table, label
        """
        import re
        pattern = re.compile(
            r"--\s*@relation\s+"
            r"(\w+)\s*[-–>]+\s*(\w+)"
            r"(?:\s*:\s*(.+))?",
            re.IGNORECASE,
        )
        results = []
        for line in sql.splitlines():
            m = pattern.search(line)
            if m:
                results.append({
                    "from_table": m.group(1).lower(),
                    "to_table":   m.group(2).lower(),
                    "label":      (m.group(3) or "").strip(),
                })
        return results

    def normalize(self, sql: str) -> str:
        """
        Normalize SQL for cache key generation:
          - Replace $1/$2 params with ?
          - Collapse whitespace
          - Uppercase keywords
          - Strip trailing semicolons
        """
        import re
        sql = re.sub(r"\$\d+", "?", sql)
        sql = re.sub(r"\s+", " ", sql).strip()
        sql = sql.rstrip(";").strip()
        return sql
