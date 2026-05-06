"""
pgxllm.parser.base_visitor
---------------------------
BaseVisitor — alias resolution, CTE/subquery scope management.
All concrete visitors inherit from this class.
"""
from __future__ import annotations

import logging
from typing import Optional

from .ast import (
    BetweenExpr, BinaryExpr, CTE, CaseExpr, CastExpr, ColumnRef,
    DeleteStmt, Expr, FromItem, FunctionCall, InExpr, InsertStmt,
    IsNullExpr, JoinedTable, Literal, OrderItem, Param, SelectCore,
    SelectItem, SelectStmt, Star, SubqueryExpr, SubqueryRef, TableRef,
    TypeCast, UnaryExpr, UpdateStmt, WindowFuncCall, WithClause,
    SqlStmt,
)

log = logging.getLogger(__name__)


class Scope:
    """
    Tracks alias → table mappings within a single SELECT scope.
    Supports nested scopes (CTE bodies, subqueries).
    """

    def __init__(self, parent: Optional["Scope"] = None, name: str = ""):
        self._aliases: dict[str, str] = {}   # alias → real table name (lower)
        self._tables:  set[str]       = set() # tables in this scope
        self.parent = parent
        self.name   = name  # e.g. "cte:cte_name", "subquery"

    def register(self, alias: str, table: str) -> None:
        a = alias.lower()
        t = table.lower()
        self._aliases[a] = t
        self._tables.add(t)

    def resolve(self, alias: str) -> str:
        """Resolve alias → table name, walking up the scope chain."""
        a = alias.lower()
        if a in self._aliases:
            return self._aliases[a]
        if self.parent:
            return self.parent.resolve(alias)
        return alias   # unresolved → return as-is

    def tables(self) -> set[str]:
        return set(self._tables)

    def all_tables(self) -> set[str]:
        """Collect tables from this scope and all ancestors."""
        result = set(self._tables)
        if self.parent:
            result |= self.parent.all_tables()
        return result


class BaseVisitor:
    """
    Base class for SQL AST visitors.
    Provides:
      - Alias registration and resolution
      - Recursive scope management for CTEs and subqueries
      - max_depth protection
      - visit_* dispatch helpers
    """

    def __init__(self, max_depth: int = 5):
        self.max_depth    = max_depth
        self._scope_stack: list[Scope] = [Scope(name="root")]
        self._depth       = 0

    # ── Scope helpers ─────────────────────────────────────

    @property
    def _scope(self) -> Scope:
        return self._scope_stack[-1]

    def _push_scope(self, name: str = "") -> Scope:
        s = Scope(parent=self._scope, name=name)
        self._scope_stack.append(s)
        return s

    def _pop_scope(self) -> Scope:
        if len(self._scope_stack) > 1:
            return self._scope_stack.pop()
        return self._scope_stack[0]

    def _register_alias(self, alias: str, table: str) -> None:
        self._scope.register(alias, table)

    def _resolve(self, alias: str) -> str:
        return self._scope.resolve(alias)

    def _enter_depth(self) -> bool:
        """Returns False if max_depth exceeded."""
        if self._depth >= self.max_depth:
            return False
        self._depth += 1
        return True

    def _exit_depth(self) -> None:
        if self._depth > 0:
            self._depth -= 1

    # ── Main dispatch ─────────────────────────────────────

    def visit_stmts(self, stmts: list[SqlStmt]) -> None:
        for stmt in stmts:
            self.visit_stmt(stmt)

    def visit_stmt(self, stmt: SqlStmt) -> None:
        if isinstance(stmt, SelectStmt):
            self.visit_select(stmt)
        elif isinstance(stmt, InsertStmt):
            self.visit_insert(stmt)
        elif isinstance(stmt, UpdateStmt):
            self.visit_update(stmt)
        elif isinstance(stmt, DeleteStmt):
            self.visit_delete(stmt)

    # ── SELECT ────────────────────────────────────────────

    def visit_select(self, stmt: SelectStmt) -> None:
        # Process WITH clause (CTEs) first
        if stmt.with_:
            self._visit_with(stmt.with_)

        for core in stmt.cores:
            self._visit_select_core(core)

        for item in stmt.order_by:
            self._visit_order_item(item)

        if stmt.limit:
            self.visit_expr(stmt.limit)

    def _visit_with(self, with_: WithClause) -> None:
        for cte in with_.ctes:
            self._visit_cte(cte)

    def _visit_cte(self, cte: CTE) -> None:
        if not self._enter_depth():
            return
        self._push_scope(name=f"cte:{cte.name}")
        try:
            self.on_cte_enter(cte)
            self.visit_select(cte.query)
            self.on_cte_exit(cte)
        finally:
            self._pop_scope()
            self._exit_depth()

    def _visit_select_core(self, core: SelectCore) -> None:
        # Pre-pass: register ALL aliases from entire FROM tree
        for fi in core.from_items:
            self._register_from_aliases(fi)

        # Visit select list
        for item in core.select_list:
            self._visit_select_item(item)

        # Visit FROM items (fires on_join, on_table_ref, join conditions)
        for fi in core.from_items:
            self.visit_from_item(fi)

        # WHERE (implicit JOIN detection)
        if core.where:
            self.visit_where(core.where)

        for e in core.group_by:
            self.visit_expr(e)

        if core.having:
            self.visit_expr(core.having)

    def _register_from_aliases(self, fi: FromItem) -> None:
        """Pre-pass: register all aliases in a FROM item."""
        if isinstance(fi, TableRef):
            table = fi.name.name.lower()
            if fi.alias:
                self._register_alias(fi.alias, table)
            else:
                self._register_alias(table, table)
        elif isinstance(fi, SubqueryRef):
            if fi.alias:
                self._register_alias(fi.alias, f"__subquery_{fi.alias}")
        elif isinstance(fi, JoinedTable):
            self._register_from_aliases(fi.left)
            self._register_from_aliases(fi.right)

    def _visit_select_item(self, item: SelectItem) -> None:
        self.visit_expr(item.expr)

    def visit_from_item(self, fi: FromItem) -> None:
        if isinstance(fi, TableRef):
            self.on_table_ref(fi)
        elif isinstance(fi, SubqueryRef):
            self.on_subquery_ref(fi)
            if self._enter_depth():
                self._push_scope(name="subquery")
                try:
                    # Register subquery alias in parent scope before visiting
                    if fi.alias:
                        self._scope_stack[-2].register(
                            fi.alias, f"__subquery_{fi.alias}"
                        )
                    self.visit_select(fi.query)
                finally:
                    self._pop_scope()
                    self._exit_depth()
        elif isinstance(fi, JoinedTable):
            # Register ALL aliases in this join tree BEFORE visiting conditions
            self._register_from_aliases(fi)
            self.on_join(fi)
            self.visit_from_item(fi.left)
            self.visit_from_item(fi.right)
            if fi.on_expr:
                self.visit_join_condition(fi.on_expr, fi)
            elif fi.using_cols:
                # USING clause — no expr but still a join condition
                self.on_join_condition(None, fi)  # type: ignore[arg-type]

    def visit_where(self, expr: Expr) -> None:
        self.on_where(expr)
        self.visit_expr(expr)

    def visit_join_condition(self, expr: Expr, join: JoinedTable) -> None:
        self.on_join_condition(expr, join)
        self.visit_expr(expr)

    def _visit_order_item(self, item: OrderItem) -> None:
        self.visit_expr(item.expr)

    # ── Expressions ───────────────────────────────────────

    def visit_expr(self, expr: Expr) -> None:
        if isinstance(expr, Literal):
            self.on_literal(expr)
        elif isinstance(expr, Param):
            self.on_param(expr)
        elif isinstance(expr, ColumnRef):
            self.on_column_ref(expr)
        elif isinstance(expr, Star):
            pass
        elif isinstance(expr, BinaryExpr):
            self.visit_expr(expr.left)
            self.visit_expr(expr.right)
        elif isinstance(expr, UnaryExpr):
            self.visit_expr(expr.expr)
        elif isinstance(expr, BetweenExpr):
            self.on_between(expr)
            self.visit_expr(expr.expr)
            self.visit_expr(expr.low)
            self.visit_expr(expr.high)
        elif isinstance(expr, InExpr):
            self.visit_expr(expr.expr)
            for v in expr.values:
                self.visit_expr(v)
            if expr.subquery:
                self.on_inline_subquery()   # notify subclasses
                if self._enter_depth():
                    self._push_scope("subquery")
                    try:
                        self.visit_select(expr.subquery)
                    finally:
                        self._pop_scope()
                        self._exit_depth()
        elif isinstance(expr, IsNullExpr):
            self.visit_expr(expr.expr)
        elif isinstance(expr, CastExpr):
            self.on_cast(expr)
            self.visit_expr(expr.expr)
        elif isinstance(expr, TypeCast):
            self.on_type_cast(expr)
            self.visit_expr(expr.expr)
        elif isinstance(expr, FunctionCall):
            self.on_function_call(expr)
            for a in expr.args:
                self.visit_expr(a)
        elif isinstance(expr, WindowFuncCall):
            self.on_window_func(expr)
            for a in expr.func.args:
                self.visit_expr(a)
            for p in expr.partition_by:
                self.visit_expr(p)
            for o in expr.order_by:
                self.visit_expr(o.expr)
        elif isinstance(expr, CaseExpr):
            if expr.operand:
                self.visit_expr(expr.operand)
            for cond, res in expr.whens:
                self.visit_expr(cond)
                self.visit_expr(res)
            if expr.else_:
                self.visit_expr(expr.else_)
        elif isinstance(expr, SubqueryExpr):
            if self._enter_depth():
                self._push_scope("subquery")
                try:
                    self.visit_select(expr.subquery)
                finally:
                    self._pop_scope()
                    self._exit_depth()
        elif isinstance(expr, SelectStmt):
            # scalar subquery
            if self._enter_depth():
                self._push_scope("subquery")
                try:
                    self.visit_select(expr)
                finally:
                    self._pop_scope()
                    self._exit_depth()

    # ── DML ───────────────────────────────────────────────

    def visit_insert(self, stmt: InsertStmt) -> None:
        if isinstance(stmt.source, SelectStmt):
            self.visit_select(stmt.source)

    def visit_update(self, stmt: UpdateStmt) -> None:
        for fi in stmt.from_items:
            self._register_from_aliases(fi)
            self.visit_from_item(fi)
        for _, expr in stmt.set_items:
            self.visit_expr(expr)
        if stmt.where:
            self.visit_where(stmt.where)

    def visit_delete(self, stmt: DeleteStmt) -> None:
        for fi in stmt.using:
            self._register_from_aliases(fi)
            self.visit_from_item(fi)
        if stmt.where:
            self.visit_where(stmt.where)

    # ── Hook methods (override in subclasses) ─────────────

    def on_cte_enter(self, cte: CTE) -> None:
        pass

    def on_cte_exit(self, cte: CTE) -> None:
        pass

    def on_table_ref(self, ref: TableRef) -> None:
        pass

    def on_subquery_ref(self, ref: SubqueryRef) -> None:
        pass

    def on_inline_subquery(self) -> None:
        """Called for scalar/IN subqueries (not FROM subqueries)."""
        pass

    def on_join(self, join: JoinedTable) -> None:
        pass

    def on_join_condition(self, expr: Expr, join: JoinedTable) -> None:
        pass

    def on_where(self, expr: Expr) -> None:
        pass

    def on_function_call(self, call: FunctionCall) -> None:
        pass

    def on_window_func(self, call: WindowFuncCall) -> None:
        pass

    def on_between(self, expr: BetweenExpr) -> None:
        pass

    def on_cast(self, expr: CastExpr) -> None:
        pass

    def on_type_cast(self, expr: TypeCast) -> None:
        pass

    def on_column_ref(self, ref: ColumnRef) -> None:
        pass

    def on_literal(self, lit: Literal) -> None:
        pass

    def on_param(self, param: Param) -> None:
        pass
