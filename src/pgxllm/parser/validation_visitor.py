"""
pgxllm.parser.validation_visitor
----------------------------------
ValidationVisitor — validates a SQL statement against a set of rules
(dialect rules + structural rules) and returns violations.

Rules are evaluated against:
  1. SqlStructure patterns (structural rules — always applied)
  2. DialectRule objects   (column-level rules — from pgxllm DB)
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional, Protocol

from .ast import (
    BetweenExpr, ColumnRef, FunctionCall, SelectStmt,
    TypeCast, WindowFuncCall,
)
from .base_visitor import BaseVisitor
from .models import Severity, SqlStructure, TopNPattern, Violation, ValidationResult
from .structure_visitor import StructureAnalysisVisitor, analyze_structure

log = logging.getLogger(__name__)


# ── Rule interface ────────────────────────────────────────

@dataclass
class DialectRule:
    """
    A rule that defines forbidden/required SQL patterns for a column.
    Loaded from pgxllm.dialect_rules table.
    """
    rule_id:     str
    scope:       str                  # global | dialect | db | table | column
    dialect:     str                  # postgresql
    db_name:     Optional[str]
    table_name:  Optional[str]
    column_name: Optional[str]
    forbidden_funcs: list[str]        # e.g. ["EXTRACT", "BETWEEN"]
    required_func:   Optional[str]    # e.g. "SUBSTR"
    instruction:     str
    example_bad:     str = ""
    example_good:    str = ""
    severity:        Severity = Severity.ERROR


# ── Built-in structural rules ─────────────────────────────

def _check_structural(structure: SqlStructure) -> list[Violation]:
    violations: list[Violation] = []

    # Rule: LIMIT without ORDER BY (non-deterministic)
    if (structure.limit_position == "final" and
            structure.limit_position != "none" and
            not structure.has_order_by):
        violations.append(Violation(
            rule_id="limit_without_order_by",
            severity=Severity.WARNING,
            location="SELECT",
            message="LIMIT 이 ORDER BY 없이 사용됨 — 비결정적 결과",
            suggestion="ORDER BY 절을 추가하세요.",
        ))

    # Rule: TOP-N THEN_DETAIL — LIMIT should be in subquery, not final
    if structure.top_n_pattern == TopNPattern.THEN_DETAIL:
        from .models import LimitPosition
        if structure.limit_position == LimitPosition.FINAL:
            violations.append(Violation(
                rule_id="top_n_limit_position",
                severity=Severity.ERROR,
                location="SELECT",
                message="상위 N 개 추출 후 상세 집계 패턴: LIMIT 은 inline view/CTE 안에 있어야 합니다.",
                suggestion=(
                    "WITH top_n AS (SELECT ... ORDER BY ... LIMIT N)\n"
                    "SELECT ... FROM main JOIN top_n ... 패턴을 사용하세요."
                ),
            ))

    # Rule: WINDOW function without PARTITION BY (might be unintentional)
    if structure.has_window_func and not any(
        f in ("RANK", "DENSE_RANK", "ROW_NUMBER") for f in structure.window_funcs
    ):
        pass  # aggregation window without partition is valid

    return violations


# ── Validation Visitor ────────────────────────────────────

class ValidationVisitor(BaseVisitor):
    """
    Validates a SQL AST against dialect rules and structural rules.

    Usage::

        rules = [DialectRule(...)]   # from pgxllm DB
        visitor = ValidationVisitor(rules=rules)
        visitor.visit_stmts(parse_sql(sql))
        result = visitor.result
    """

    def __init__(
        self,
        rules: Optional[list[DialectRule]] = None,
        db_name: Optional[str] = None,
        max_depth: int = 5,
    ):
        super().__init__(max_depth=max_depth)
        self._rules   = rules or []
        self._db_name = db_name
        self._violations: list[Violation] = []
        self._structure_visitor = StructureAnalysisVisitor(max_depth=max_depth)

    @property
    def result(self) -> ValidationResult:
        errors = [v for v in self._violations if v.severity == Severity.ERROR]
        return ValidationResult(
            is_valid=len(errors) == 0,
            violations=list(self._violations),
        )

    def visit_stmts(self, stmts) -> None:
        # First pass: structural analysis
        self._structure_visitor.visit_stmts(stmts)
        structure = self._structure_visitor.structure

        # Structural rule violations
        self._violations.extend(_check_structural(structure))

        # Second pass: dialect rule checking
        super().visit_stmts(stmts)

    # ── Hooks ─────────────────────────────────────────────

    def on_function_call(self, call: FunctionCall) -> None:
        name = call.name.upper()
        # Check if this function is used on a column that has a rule forbidding it
        if call.args:
            col = self._first_column_arg(call)
            if col:
                self._check_func_on_column(name, col.column.lower(), col.table)

    def on_window_func(self, call: WindowFuncCall) -> None:
        self.on_function_call(call.func)

    def on_between(self, expr: BetweenExpr) -> None:
        if isinstance(expr.expr, ColumnRef):
            col = expr.expr
            self._check_func_on_column("BETWEEN", col.column.lower(), col.table)

    def on_type_cast(self, expr) -> None:
        if isinstance(expr, TypeCast):
            t = expr.type_name.upper()
            if t in ("DATE", "TIMESTAMP", "TIMESTAMPTZ"):
                if isinstance(expr.expr, ColumnRef):
                    col = expr.expr
                    self._check_func_on_column(f"::{t}", col.column.lower(), col.table)

    # ── Rule matching ─────────────────────────────────────

    def _check_func_on_column(
        self,
        func_name: str,
        column: str,
        table: Optional[str],
    ) -> None:
        resolved_table = self._resolve(table) if table else None

        for rule in self._rules:
            if not self._rule_matches(rule, column, resolved_table):
                continue
            if func_name.upper() in [f.upper() for f in rule.forbidden_funcs]:
                self._violations.append(Violation(
                    rule_id=rule.rule_id,
                    severity=rule.severity,
                    location=f"column:{resolved_table or '?'}.{column}",
                    message=(
                        f"컬럼 '{resolved_table or ''}.{column}' 에 "
                        f"'{func_name}' 사용 금지 — {rule.instruction}"
                    ),
                    suggestion=(
                        f"대신 '{rule.required_func}' 를 사용하세요.\n"
                        f"예시: {rule.example_good}" if rule.required_func else rule.instruction
                    ),
                ))

    def _rule_matches(
        self,
        rule: DialectRule,
        column: str,
        table: Optional[str],
    ) -> bool:
        if rule.scope == "global":
            return True
        if rule.scope == "db":
            return rule.db_name == self._db_name
        if rule.scope == "table" and table:
            return (rule.table_name or "").lower() == table.lower()
        if rule.scope == "column":
            col_match   = (rule.column_name or "").lower() == column.lower()
            table_match = (
                table is None or
                rule.table_name is None or
                rule.table_name.lower() == table.lower()
            )
            return col_match and table_match
        return False

    # ── Helpers ───────────────────────────────────────────

    @staticmethod
    def _first_column_arg(call: FunctionCall) -> Optional[ColumnRef]:
        for arg in call.args:
            if isinstance(arg, ColumnRef):
                return arg
        return None


# ── Convenience function ──────────────────────────────────

def validate_sql(
    sql: str,
    rules: Optional[list[DialectRule]] = None,
    db_name: Optional[str] = None,
    max_depth: int = 5,
) -> ValidationResult:
    """
    Validate a SQL string against dialect rules and structural rules.

    Args:
        sql:      SQL statement(s) to validate
        rules:    list of DialectRule objects (from pgxllm DB)
        db_name:  current DB name (for scope matching)
        max_depth: recursion depth limit

    Returns:
        ValidationResult with is_valid flag and violations list
    """
    from .sql_parser import parse_sql

    stmts   = parse_sql(sql)
    visitor = ValidationVisitor(rules=rules, db_name=db_name, max_depth=max_depth)
    visitor.visit_stmts(stmts)
    return visitor.result
