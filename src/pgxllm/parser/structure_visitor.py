"""
pgxllm.parser.structure_visitor
---------------------------------
StructureAnalysisVisitor — analyses the structural pattern of a SQL statement:
  - Top-N pattern detection  (SIMPLE / THEN_DETAIL / PER_GROUP)
  - LIMIT position            (FINAL / INLINE_VIEW / CTE / NONE)
  - Date function usage       (EXTRACT / SUBSTR / BETWEEN on date-like columns)
  - Window function usage     (RANK, ROW_NUMBER, DENSE_RANK …)
  - Aggregation               (GROUP BY / HAVING)
  - CTE / subquery depth
"""
from __future__ import annotations

import re
import logging
from typing import Optional

from .ast import (
    BetweenExpr, CTE, ColumnRef, FunctionCall, JoinedTable,
    SelectCore, SelectStmt, SubqueryRef, TableRef, TypeCast,
    WindowFuncCall,
)
from .base_visitor import BaseVisitor
from .models import (
    DateFuncType, DateFuncUsage, LimitPosition, SqlStructure,
    TopNPattern,
)

log = logging.getLogger(__name__)

# Patterns that suggest a TEXT column stores dates
_YYYYMM_PAT   = re.compile(r"^\d{6}$")       # 202301
_YYYYMMDD_PAT = re.compile(r"^\d{8}$")       # 20230101
_YYYY_MM_PAT  = re.compile(r"^\d{4}-\d{2}")  # 2023-01


class StructureAnalysisVisitor(BaseVisitor):
    """
    Analyses SQL structure to identify patterns that the Dynamic Pattern
    system and the LLM prompt injector need.

    Usage::

        visitor = StructureAnalysisVisitor()
        visitor.visit_stmts(parse_sql(sql))
        structure = visitor.structure
    """

    def __init__(self, max_depth: int = 5):
        super().__init__(max_depth=max_depth)

        # Accumulation
        self._tables:        list[str]         = []
        self._window_funcs:  list[str]         = []
        self._date_funcs:    list[DateFuncUsage] = []

        # Flags
        self._has_group_by   = False
        self._has_having     = False
        self._has_order_by   = False
        self._has_union      = False
        self._has_cte        = False
        self._has_subquery   = False
        self._cte_count      = 0
        self._join_count     = 0
        self._max_subq_depth = 0

        # Limit tracking — (position, value)
        self._limits: list[tuple[LimitPosition, Optional[int]]] = []

        # Partition-by tracking for per-group detection
        self._partition_cols: list[str] = []

    @property
    def structure(self) -> SqlStructure:
        aliases = {}
        for s in self._scope_stack:
            aliases.update(s._aliases)

        limit_pos = LimitPosition.NONE
        top_n_val: Optional[int] = None
        if self._limits:
            # Outermost (root) limit reported first
            limit_pos = self._limits[0][0]
            top_n_val = self._limits[0][1]

        top_n = self._compute_top_n()

        return SqlStructure(
            tables=list(dict.fromkeys(self._tables)),   # dedup, order preserved
            aliases=aliases,
            has_group_by=self._has_group_by,
            has_having=self._has_having,
            has_window_func=bool(self._window_funcs),
            window_funcs=list(dict.fromkeys(self._window_funcs)),
            limit_position=limit_pos,
            has_order_by=self._has_order_by,
            top_n_pattern=top_n,
            top_n_value=top_n_val,
            date_funcs=self._date_funcs,
            has_cte=self._has_cte,
            cte_count=self._cte_count,
            has_subquery=self._has_subquery,
            subquery_depth=self._max_subq_depth,
            join_count=self._join_count,
            has_union=self._has_union,
        )

    # ── Top-N pattern computation ─────────────────────────

    def _compute_top_n(self) -> TopNPattern:
        has_limit  = bool(self._limits)
        has_rank   = any(f in ("RANK", "DENSE_RANK", "ROW_NUMBER")
                         for f in self._window_funcs)
        has_partition = bool(self._partition_cols)
        limit_pos  = self._limits[0][0] if self._limits else LimitPosition.NONE

        if has_rank and has_partition:
            return TopNPattern.PER_GROUP       # 부서별 상위 N

        if has_limit:
            if limit_pos == LimitPosition.INLINE_VIEW or self._has_subquery:
                return TopNPattern.THEN_DETAIL  # 상위 N 추출 후 상세
            if self._has_group_by or self._has_order_by:
                return TopNPattern.SIMPLE
            return TopNPattern.SIMPLE

        if has_rank:
            return TopNPattern.PER_GROUP

        return TopNPattern.NONE

    # ── Overriding visit_select to track limits ───────────

    def visit_select(self, stmt: SelectStmt) -> None:
        # Track UNION
        if stmt.set_ops:
            self._has_union = True

        # Determine limit position
        if stmt.limit is not None:
            pos = self._limit_position_for_depth()
            val = self._extract_limit_value(stmt.limit)
            self._limits.append((pos, val))

        # Track ORDER BY at root
        if stmt.order_by and self._depth == 0:
            self._has_order_by = True

        super().visit_select(stmt)

    def _limit_position_for_depth(self) -> LimitPosition:
        if self._depth == 0:
            return LimitPosition.FINAL
        for s in reversed(self._scope_stack):
            if s.name.startswith("cte:"):
                return LimitPosition.CTE
            if s.name == "subquery":
                return LimitPosition.INLINE_VIEW
        return LimitPosition.FINAL

    def _extract_limit_value(self, expr) -> Optional[int]:
        from .ast import Literal
        if isinstance(expr, Literal):
            try:
                return int(expr.value)
            except (ValueError, TypeError):
                pass
        return None

    # ── Hooks ─────────────────────────────────────────────

    def on_cte_enter(self, cte: CTE) -> None:
        self._has_cte = True
        self._cte_count += 1

    def on_table_ref(self, ref: TableRef) -> None:
        self._tables.append(ref.name.name.lower())

    def on_subquery_ref(self, ref: SubqueryRef) -> None:
        self._has_subquery = True
        depth = self._depth
        if depth > self._max_subq_depth:
            self._max_subq_depth = depth

    def on_inline_subquery(self) -> None:
        """IN / scalar subquery."""
        self._has_subquery = True
        depth = self._depth + 1
        if depth > self._max_subq_depth:
            self._max_subq_depth = depth

    def on_join(self, join: JoinedTable) -> None:
        self._join_count += 1

    def on_window_func(self, call: WindowFuncCall) -> None:
        name = call.func.name.upper()
        self._window_funcs.append(name)

        # Track PARTITION BY columns for PER_GROUP detection
        for expr in call.partition_by:
            if isinstance(expr, ColumnRef):
                self._partition_cols.append(expr.column.lower())

    def on_function_call(self, call: FunctionCall) -> None:
        name = call.func_name_upper if hasattr(call, "func_name_upper") else call.name.upper()

        # EXTRACT(YEAR/MONTH/DAY FROM col) — date column usage
        if name == "EXTRACT" and len(call.args) >= 2:
            col_expr = call.args[1]
            if isinstance(col_expr, ColumnRef):
                self._date_funcs.append(DateFuncUsage(
                    column=col_expr.column.lower(),
                    table=col_expr.table,
                    func_type=DateFuncType.EXTRACT,
                    pattern="EXTRACT",
                ))

        # SUBSTR / SUBSTRING on column
        elif name in ("SUBSTR", "SUBSTRING") and call.args:
            col_expr = call.args[0]
            if isinstance(col_expr, ColumnRef):
                self._date_funcs.append(DateFuncUsage(
                    column=col_expr.column.lower(),
                    table=col_expr.table,
                    func_type=DateFuncType.SUBSTR,
                    pattern="SUBSTR",
                ))

    def on_between(self, expr: BetweenExpr) -> None:
        if isinstance(expr.expr, ColumnRef):
            col = expr.expr
            # Detect if the BETWEEN bounds look like date strings
            low_val  = self._literal_value(expr.low)
            high_val = self._literal_value(expr.high)
            pattern  = self._detect_date_pattern(low_val or "")
            if pattern:
                self._date_funcs.append(DateFuncUsage(
                    column=col.column.lower(),
                    table=col.table,
                    func_type=DateFuncType.BETWEEN,
                    pattern=pattern,
                ))

    def on_cast(self, expr) -> None:
        from .ast import CastExpr
        if isinstance(expr, CastExpr):
            t = expr.type_name.upper()
            if t in ("DATE", "TIMESTAMP", "TIMESTAMPTZ"):
                col = expr.expr
                if isinstance(col, ColumnRef):
                    self._date_funcs.append(DateFuncUsage(
                        column=col.column.lower(),
                        table=col.table,
                        func_type=DateFuncType.CAST,
                        pattern=f"::{t}",
                    ))

    def on_type_cast(self, expr) -> None:
        from .ast import TypeCast
        if isinstance(expr, TypeCast):
            t = expr.type_name.upper()
            if t in ("DATE", "TIMESTAMP", "TIMESTAMPTZ"):
                col = expr.expr
                if isinstance(col, ColumnRef):
                    self._date_funcs.append(DateFuncUsage(
                        column=col.column.lower(),
                        table=col.table,
                        func_type=DateFuncType.CAST,
                        pattern=f"::{t}",
                    ))

    def _visit_select_core(self, core: SelectCore) -> None:
        # Detect flags BEFORE calling super (which recurses into subqueries)
        if core.group_by:
            self._has_group_by = True
        if core.having is not None:
            self._has_having = True
        super()._visit_select_core(core)

    # ── Helpers ───────────────────────────────────────────

    @staticmethod
    def _literal_value(expr) -> Optional[str]:
        from .ast import Literal
        if isinstance(expr, Literal):
            return expr.value.strip("'\"")
        return None

    @staticmethod
    def _detect_date_pattern(val: str) -> Optional[str]:
        if _YYYYMM_PAT.match(val):
            return "YYYYMM"
        if _YYYYMMDD_PAT.match(val):
            return "YYYYMMDD"
        if _YYYY_MM_PAT.match(val):
            return "YYYY-MM"
        return None


# ── Convenience function ──────────────────────────────────

def analyze_structure(sql: str, max_depth: int = 5) -> SqlStructure:
    """
    Analyse the structural pattern of a SQL string.

    Returns a SqlStructure with Top-N pattern, limit position,
    date function usage, window functions, etc.
    """
    from .sql_parser import parse_sql

    stmts = parse_sql(sql)
    visitor = StructureAnalysisVisitor(max_depth=max_depth)
    visitor.visit_stmts(stmts)
    return visitor.structure
