"""
pgxllm.parser.relation_visitor
--------------------------------
RelationExtractVisitor — extracts table-to-table JOIN relationships
from SQL statements, including:
  - Explicit JOINs (INNER / LEFT / RIGHT / FULL / CROSS)
  - Implicit JOINs  (FROM a, b WHERE a.id = b.id)
  - CTE bodies (recursively)
  - Subquery bodies (recursively)
"""
from __future__ import annotations

import logging
from collections import defaultdict
from typing import Optional

from .ast import (
    BinaryExpr, ColumnRef, CTE, Expr, FunctionCall, JoinedTable,
    SelectStmt, SubqueryRef, TableRef, WindowFuncCall,
)
from .base_visitor import BaseVisitor
from .models import ExtractedRelation, JoinSource, JoinType

log = logging.getLogger(__name__)


def _join_type_str(s: str) -> JoinType:
    m = {
        "INNER":   JoinType.INNER,
        "LEFT":    JoinType.LEFT,
        "RIGHT":   JoinType.RIGHT,
        "FULL":    JoinType.FULL,
        "CROSS":   JoinType.CROSS,
        "NATURAL": JoinType.INNER,
    }
    return m.get(s.upper(), JoinType.INNER)


class RelationExtractVisitor(BaseVisitor):
    """
    Walks the SQL AST and collects all table-to-table relations.

    Usage::

        visitor = RelationExtractVisitor()
        visitor.visit_stmts(parse_sql(sql))
        relations = visitor.relations   # list[ExtractedRelation]
    """

    def __init__(self, max_depth: int = 5):
        super().__init__(max_depth=max_depth)
        self._relations: list[ExtractedRelation] = []
        self._seen:      set[ExtractedRelation]  = set()
        # implicit join tracking: tables in current FROM scope
        self._implicit_tables: list[str] = []
        self._in_implicit_scope = False

    @property
    def relations(self) -> list[ExtractedRelation]:
        return list(self._relations)

    def _add(self, rel: ExtractedRelation) -> None:
        if rel not in self._seen:
            self._seen.add(rel)
            self._relations.append(rel)

    # ── Override hooks ────────────────────────────────────

    def on_join(self, join: JoinedTable) -> None:
        """Called when we encounter a JOIN node."""
        pass  # relation extracted in on_join_condition

    def on_join_condition(self, expr: Expr, join: JoinedTable) -> None:
        """Extract relation from JOIN ON / USING condition."""
        join_type = _join_type_str(join.join_type)
        source    = self._current_source()

        # ON expr
        if expr is not None:
            self._extract_from_expr(expr, join_type, source)

        # USING columns
        if join.using_cols:
            left_table  = self._get_table_name(join.left)
            right_table = self._get_table_name(join.right)
            if left_table and right_table:
                for col in join.using_cols:
                    self._add(ExtractedRelation(
                        from_table=left_table,
                        from_column=col,
                        to_table=right_table,
                        to_column=col,
                        join_type=join_type,
                        source=source,
                    ))

    def on_where(self, expr: Expr) -> None:
        """Extract implicit JOINs from WHERE conditions."""
        self._extract_from_expr(expr, JoinType.IMPLICIT, JoinSource.IMPLICIT_JOIN)

    def on_cte_enter(self, cte: CTE) -> None:
        log.debug("entering CTE: %s", cte.name)

    # ── Relation extraction from expressions ─────────────

    def _extract_from_expr(
        self,
        expr: Expr,
        join_type: JoinType,
        source: JoinSource,
    ) -> None:
        """
        Recursively scan an expression for equality comparisons
        between column references from different tables.
        e.g.  a.id = b.a_id  →  ExtractedRelation(a.id → b.a_id)
        """
        if isinstance(expr, BinaryExpr):
            if expr.op == "AND":
                self._extract_from_expr(expr.left,  join_type, source)
                self._extract_from_expr(expr.right, join_type, source)
                return

            if expr.op in ("=", "EQ") and isinstance(expr.left, ColumnRef) and isinstance(expr.right, ColumnRef):
                lc = expr.left
                rc = expr.right

                lt = self._resolve_table(lc.table)
                rt = self._resolve_table(rc.table)

                # Both sides must have table references
                if lt and rt:
                    # Skip if exact same alias (a.id = a.id — not a join)
                    if lc.table and rc.table and lc.table.lower() == rc.table.lower():
                        return
                    self._add(ExtractedRelation(
                        from_table=lt,
                        from_column=lc.column.lower(),
                        to_table=rt,
                        to_column=rc.column.lower(),
                        join_type=join_type,
                        source=source,
                    ))

    def _resolve_table(self, alias: Optional[str]) -> Optional[str]:
        if alias is None:
            return None
        return self._resolve(alias)

    def _current_source(self) -> JoinSource:
        # Walk scope stack from innermost (skip root scope)
        for scope in reversed(self._scope_stack[1:]):
            if scope.name.startswith("cte:"):
                return JoinSource.CTE
            if scope.name == "subquery":
                return JoinSource.SUBQUERY
        return JoinSource.EXPLICIT_JOIN

    def _get_table_name(self, fi) -> Optional[str]:
        if isinstance(fi, TableRef):
            return fi.name.name.lower()
        return None

    def visit_from_item(self, fi) -> None:
        """Override to track implicit join table list."""
        super().visit_from_item(fi)


# ── Convenience function ──────────────────────────────────

def extract_relations(
    sql: str,
    max_depth: int = 5,
) -> list[ExtractedRelation]:
    """
    Extract all table relations from a SQL string.

    Args:
        sql:       SQL statement(s) to analyse
        max_depth: maximum CTE/subquery recursion depth

    Returns:
        Deduplicated list of ExtractedRelation objects
    """
    from .sql_parser import parse_sql

    stmts = parse_sql(sql)
    visitor = RelationExtractVisitor(max_depth=max_depth)
    visitor.visit_stmts(stmts)
    return visitor.relations
