"""
pgxllm.parser.ast
-----------------
Lightweight AST node definitions for PostgreSQL SQL.
Mirrors the structure of PostgreSQLParser.g4.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, Union


# ── Base ──────────────────────────────────────────────────

class Node:
    """Base AST node."""
    pass


# ── Identifiers ───────────────────────────────────────────

@dataclass
class Identifier(Node):
    name:   str
    quoted: bool = False

    def __str__(self) -> str:
        return self.name

    def lower(self) -> str:
        return self.name.lower()


@dataclass
class QualifiedName(Node):
    """schema.table  or  table  or  schema.table.column"""
    parts: list[str]   # [schema?, table] or [table?, column]

    @property
    def name(self) -> str:
        return self.parts[-1]

    @property
    def qualifier(self) -> Optional[str]:
        return self.parts[-2] if len(self.parts) >= 2 else None

    def __str__(self) -> str:
        return ".".join(self.parts)

    def lower(self) -> str:
        return ".".join(p.lower() for p in self.parts)


# ── Literals & Expressions (simplified) ───────────────────

@dataclass
class Literal(Node):
    value: str

@dataclass
class Param(Node):
    """$1, $2 pg_stat_statements placeholder"""
    index: int

@dataclass
class ColumnRef(Node):
    table:  Optional[str]
    column: str

    def __str__(self) -> str:
        if self.table:
            return f"{self.table}.{self.column}"
        return self.column

@dataclass
class Star(Node):
    table: Optional[str] = None   # table.* or just *


@dataclass
class BinaryExpr(Node):
    op:    str
    left:  "Expr"
    right: "Expr"

@dataclass
class UnaryExpr(Node):
    op:   str
    expr: "Expr"

@dataclass
class BetweenExpr(Node):
    expr:  "Expr"
    low:   "Expr"
    high:  "Expr"
    negated: bool = False

@dataclass
class InExpr(Node):
    expr:    "Expr"
    values:  list["Expr"]
    subquery: Optional["SelectStmt"] = None
    negated: bool = False

@dataclass
class IsNullExpr(Node):
    expr:    "Expr"
    negated: bool = False   # IS NOT NULL

@dataclass
class CastExpr(Node):
    expr:      "Expr"
    type_name: str
    style:     str = "CAST"   # "CAST" | "::"

@dataclass
class FunctionCall(Node):
    name:     str
    args:     list["Expr"]
    distinct: bool = False
    star:     bool = False
    filter_where: Optional["Expr"] = None

@dataclass
class WindowFuncCall(Node):
    func:        FunctionCall
    partition_by: list["Expr"]    = field(default_factory=list)
    order_by:    list["OrderItem"] = field(default_factory=list)

@dataclass
class CaseExpr(Node):
    operand:  Optional["Expr"]
    whens:    list[tuple["Expr", "Expr"]]
    else_:    Optional["Expr"] = None

@dataclass
class SubqueryExpr(Node):
    op:       str   # ALL | ANY | SOME | EXISTS
    subquery: "SelectStmt"

@dataclass
class TypeCast(Node):
    expr:      "Expr"
    type_name: str

Expr = Union[
    Literal, Param, ColumnRef, Star, BinaryExpr, UnaryExpr,
    BetweenExpr, InExpr, IsNullExpr, CastExpr, TypeCast,
    FunctionCall, WindowFuncCall, CaseExpr, SubqueryExpr,
    "SelectStmt",   # scalar subquery
]


# ── FROM clause ───────────────────────────────────────────

@dataclass
class TableRef(Node):
    name:  QualifiedName
    alias: Optional[str] = None

    @property
    def effective_name(self) -> str:
        return self.alias or self.name.name.lower()

    @property
    def table_name(self) -> str:
        return self.name.name.lower()


@dataclass
class SubqueryRef(Node):
    query: "SelectStmt"
    alias: Optional[str] = None


@dataclass
class JoinedTable(Node):
    join_type:  str              # INNER | LEFT | RIGHT | FULL | CROSS | NATURAL
    left:       "FromItem"
    right:      "FromItem"
    on_expr:    Optional[Expr]   = None
    using_cols: list[str]        = field(default_factory=list)


FromItem = Union[TableRef, SubqueryRef, JoinedTable]


# ── ORDER BY ──────────────────────────────────────────────

@dataclass
class OrderItem(Node):
    expr:      Expr
    direction: str          = "ASC"    # ASC | DESC
    nulls:     Optional[str] = None   # FIRST | LAST


# ── SELECT ────────────────────────────────────────────────

@dataclass
class SelectItem(Node):
    expr:  Expr
    alias: Optional[str] = None


@dataclass
class WithClause(Node):
    recursive: bool
    ctes:      list["CTE"]


@dataclass
class CTE(Node):
    name:  str
    query: "SelectStmt"


@dataclass
class SelectCore(Node):
    distinct:     bool
    select_list:  list[SelectItem]
    from_items:   list[FromItem]    = field(default_factory=list)
    where:        Optional[Expr]    = None
    group_by:     list[Expr]        = field(default_factory=list)
    having:       Optional[Expr]    = None


@dataclass
class SelectStmt(Node):
    cores:     list[SelectCore]
    set_ops:   list[str]            = field(default_factory=list)  # UNION/INTERSECT/EXCEPT
    order_by:  list[OrderItem]      = field(default_factory=list)
    limit:     Optional[Expr]       = None
    offset:    Optional[Expr]       = None
    with_:     Optional[WithClause] = None

    @property
    def has_limit(self) -> bool:
        return self.limit is not None

    @property
    def has_order_by(self) -> bool:
        return bool(self.order_by)


# ── DML ───────────────────────────────────────────────────

@dataclass
class InsertStmt(Node):
    table:   QualifiedName
    alias:   Optional[str]
    columns: list[str]
    source:  Union[list[list[Expr]], SelectStmt]


@dataclass
class UpdateStmt(Node):
    table:      QualifiedName
    alias:      Optional[str]
    set_items:  list[tuple[str, Expr]]
    from_items: list[FromItem]
    where:      Optional[Expr]


@dataclass
class DeleteStmt(Node):
    table:      QualifiedName
    alias:      Optional[str]
    using:      list[FromItem]
    where:      Optional[Expr]


SqlStmt = Union[SelectStmt, InsertStmt, UpdateStmt, DeleteStmt]
