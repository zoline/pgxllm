"""
pgxllm.parser.sql_parser
-------------------------
Recursive-descent PostgreSQL SQL parser.
Mirrors the structure of PostgreSQLParser.g4.
Produces AST nodes defined in pgxllm.parser.ast.
"""
from __future__ import annotations

import logging
from typing import Optional

from .ast import (
    BetweenExpr, BinaryExpr, CTE, CaseExpr, CastExpr, ColumnRef,
    DeleteStmt, Expr, FromItem, FunctionCall, InExpr, InsertStmt,
    IsNullExpr, JoinedTable, Literal, OrderItem, Param, QualifiedName,
    SelectCore, SelectItem, SelectStmt, Star, SubqueryExpr, SubqueryRef,
    TableRef, TypeCast, UnaryExpr, UpdateStmt, WindowFuncCall,
    WithClause, SqlStmt,
)
from .tokenizer import TT, Token, tokenize

log = logging.getLogger(__name__)


class ParseError(Exception):
    def __init__(self, msg: str, token: Optional[Token] = None):
        loc = f" at {token.line}:{token.col} ({token.value!r})" if token else ""
        super().__init__(f"ParseError{loc}: {msg}")
        self.token = token


WINDOW_FUNCS = frozenset({
    "RANK", "DENSE_RANK", "ROW_NUMBER", "NTILE",
    "LAG", "LEAD", "FIRST_VALUE", "LAST_VALUE", "NTH_VALUE",
    "PERCENT_RANK", "CUME_DIST",
})

AGG_FUNCS = frozenset({
    "COUNT", "SUM", "AVG", "MIN", "MAX",
    "ARRAY_AGG", "STRING_AGG", "JSON_AGG",
})


class Parser:
    """
    Recursive-descent parser for PostgreSQL SELECT/DML statements.
    Tolerant of unknown constructs — skips unrecognised tokens
    rather than aborting, so pg_stat_statements output (which may
    include unusual expressions) still yields useful relations.
    """

    def __init__(self, tokens: list[Token]):
        self._tokens = tokens
        self._pos    = 0

    # ── Token stream helpers ──────────────────────────────

    @property
    def _cur(self) -> Token:
        return self._tokens[self._pos]

    def _peek(self, offset: int = 1) -> Token:
        idx = self._pos + offset
        if idx >= len(self._tokens):
            return Token(TT.EOF, "")
        return self._tokens[idx]

    def _advance(self) -> Token:
        t = self._cur
        if t.type != TT.EOF:
            self._pos += 1
        return t

    def _expect(self, *values: str) -> Token:
        t = self._cur
        # Support both keyword and punctuation tokens
        cur_val = t.value.upper()
        expected = {v.upper() for v in values}
        if cur_val not in expected:
            # Also check by token type for punctuation
            type_vals = {"(": TT.LPAREN, ")": TT.RPAREN, ",": TT.COMMA, ";": TT.SEMI}
            match = any(
                (v in type_vals and t.type == type_vals[v]) or
                (v not in type_vals and cur_val == v)
                for v in expected
            )
            if not match:
                raise ParseError(f"Expected {values}", t)
        return self._advance()

    def _match(self, *values: str) -> bool:
        cur_val = self._cur.value.upper()
        if cur_val in {v.upper() for v in values}:
            self._advance()
            return True
        # punctuation match by type
        type_map = {"(": TT.LPAREN, ")": TT.RPAREN, ",": TT.COMMA, ";": TT.SEMI}
        for v in values:
            if v in type_map and self._cur.type == type_map[v]:
                self._advance()
                return True
        return False

    def _match_kw(self, *kws: str) -> bool:
        upper_kws = {k.upper() for k in kws}
        # Keywords
        if self._cur.type == TT.KEYWORD and self._cur.value in upper_kws:
            self._advance()
            return True
        # Also handle punctuation tokens referenced by name
        punct_map = {"COMMA": TT.COMMA, "SEMI": TT.SEMI,
                     "LPAREN": TT.LPAREN, "RPAREN": TT.RPAREN}
        for kw in upper_kws:
            if kw in punct_map and self._cur.type == punct_map[kw]:
                self._advance()
                return True
        return False

    def _match_comma(self) -> bool:
        if self._cur.type == TT.COMMA:
            self._advance()
            return True
        return False

    def _match_semi(self) -> bool:
        if self._cur.type == TT.SEMI:
            self._advance()
            return True
        return False

    def _at(self, *values: str) -> bool:
        return self._cur.value.upper() in {v.upper() for v in values}

    def _at_kw(self, *kws: str) -> bool:
        upper_kws = {k.upper() for k in kws}
        if self._cur.type == TT.KEYWORD and self._cur.value in upper_kws:
            return True
        return False

    def _at_id(self) -> bool:
        return self._cur.type in (TT.IDENTIFIER, TT.KEYWORD)

    def _skip_to(self, *stop: str) -> None:
        """Skip tokens until one of the stop values (or EOF)."""
        while self._cur.type != TT.EOF and self._cur.value.upper() not in {s.upper() for s in stop}:
            if self._cur.type == TT.LPAREN:
                self._skip_parens()
            else:
                self._advance()

    def _skip_parens(self) -> None:
        """Skip a balanced parenthesised group."""
        if self._cur.type != TT.LPAREN:
            return
        self._advance()
        depth = 1
        while self._cur.type != TT.EOF and depth > 0:
            if self._cur.type == TT.LPAREN:
                depth += 1
            elif self._cur.type == TT.RPAREN:
                depth -= 1
            self._advance()

    # ── Entry point ───────────────────────────────────────

    def parse(self) -> list[SqlStmt]:
        stmts: list[SqlStmt] = []
        while self._cur.type != TT.EOF:
            self._match_semi()
            if self._cur.type == TT.EOF:
                break
            try:
                stmt = self._parse_stmt()
                if stmt:
                    stmts.append(stmt)
            except ParseError as e:
                log.debug("parse error (skipped): %s", e)
                # skip to next statement
                while self._cur.type not in (TT.EOF,) and self._cur.type != TT.SEMI:
                    if self._cur.type == TT.LPAREN:
                        self._skip_parens()
                    else:
                        self._advance()
        return stmts

    def _parse_stmt(self) -> Optional[SqlStmt]:
        if self._at_kw("WITH"):
            return self._parse_with_or_select()
        elif self._at_kw("SELECT"):
            return self._parse_select()
        elif self._at_kw("INSERT"):
            return self._parse_insert()
        elif self._at_kw("UPDATE"):
            return self._parse_update()
        elif self._at_kw("DELETE"):
            return self._parse_delete()
        else:
            # Unknown statement: skip
            self._skip_to("SEMI")
            return None

    # ── WITH / CTE ────────────────────────────────────────

    def _parse_with_or_select(self) -> SelectStmt:
        self._expect("WITH")
        recursive = self._match_kw("RECURSIVE")
        ctes: list[CTE] = []
        while True:
            name = self._cur.value
            self._advance()
            self._expect("AS")
            self._expect("(")
            query = self._parse_select()
            self._expect(")")
            ctes.append(CTE(name=name, query=query))
            if not self._match_comma():
                break
        stmt = self._parse_select()
        stmt.with_ = WithClause(recursive=recursive, ctes=ctes)
        return stmt

    # ── SELECT ────────────────────────────────────────────

    def _parse_select(self) -> SelectStmt:
        cores:   list[SelectCore] = []
        set_ops: list[str]        = []

        cores.append(self._parse_select_core())
        while self._at_kw("UNION", "INTERSECT", "EXCEPT"):
            op = self._cur.value
            self._advance()
            if self._at_kw("ALL"):
                op += " ALL"
                self._advance()
            set_ops.append(op)
            cores.append(self._parse_select_core())

        order_by: list[OrderItem] = []
        limit:    Optional[Expr]  = None
        offset:   Optional[Expr]  = None

        if self._at_kw("ORDER_BY", "ORDER"):
            if self._at_kw("ORDER"):
                self._advance()
                self._expect("BY")
            else:
                self._advance()
            order_by = self._parse_order_by_items()

        if self._at_kw("LIMIT"):
            self._advance()
            if not self._at_kw("ALL"):
                limit = self._parse_simple_expr()
            else:
                self._advance()

        if self._at_kw("OFFSET"):
            self._advance()
            offset = self._parse_simple_expr()
            self._match_kw("ROW", "ROWS")

        if self._at_kw("FETCH"):
            self._advance()
            self._match_kw("FIRST", "NEXT")
            if not self._at_kw("ROW", "ROWS"):
                limit = self._parse_simple_expr()
            self._match_kw("ROW", "ROWS")
            self._match_kw("ONLY", "TIES")

        return SelectStmt(cores=cores, set_ops=set_ops, order_by=order_by,
                          limit=limit, offset=offset)

    def _parse_select_core(self) -> SelectCore:
        self._expect("SELECT")
        distinct = False
        if self._at_kw("DISTINCT"):
            distinct = True
            self._advance()
            if self._at_kw("ON"):
                self._advance()
                self._skip_parens()
        elif self._at_kw("ALL"):
            self._advance()

        select_list = self._parse_select_list()

        from_items: list[FromItem] = []
        if self._at_kw("FROM"):
            self._advance()
            from_items = self._parse_from_list()

        where: Optional[Expr] = None
        if self._at_kw("WHERE"):
            self._advance()
            where = self._parse_expr()

        group_by: list[Expr] = []
        if self._at_kw("GROUP_BY", "GROUP"):
            if self._at_kw("GROUP"):
                self._advance()
                self._expect("BY")
            else:
                self._advance()
            group_by = self._parse_expr_list()

        having: Optional[Expr] = None
        if self._at_kw("HAVING"):
            self._advance()
            having = self._parse_expr()

        # WINDOW clause (skip)
        if self._at_kw("WINDOW"):
            self._skip_to("ORDER", "LIMIT", "OFFSET", "FETCH",
                          "UNION", "INTERSECT", "EXCEPT",
                          "RPAREN", "SEMI")

        return SelectCore(
            distinct=distinct,
            select_list=select_list,
            from_items=from_items,
            where=where,
            group_by=group_by,
            having=having,
        )

    # ── SELECT list ───────────────────────────────────────

    def _parse_select_list(self) -> list[SelectItem]:
        items: list[SelectItem] = []
        while True:
            item = self._parse_select_item()
            if item:
                items.append(item)
            if not self._match_comma():
                break
        return items

    def _parse_select_item(self) -> Optional[SelectItem]:
        if self._cur.type == TT.STAR:
            self._advance()
            return SelectItem(expr=Star())
        try:
            expr = self._parse_expr()
        except ParseError:
            self._skip_to("COMMA", "FROM", "WHERE", "GROUP", "HAVING",
                          "ORDER", "LIMIT", "UNION", "INTERSECT", "EXCEPT",
                          "RPAREN", "SEMI")
            return None
        alias: Optional[str] = None
        if self._at_kw("AS"):
            self._advance()
            alias = self._cur.value
            self._advance()
        elif self._cur.type in (TT.IDENTIFIER,) and not self._at_kw(
            "FROM", "WHERE", "GROUP", "HAVING", "ORDER", "LIMIT",
            "UNION", "INTERSECT", "EXCEPT", "RPAREN", "SEMI", "COMMA",
        ):
            alias = self._cur.value
            self._advance()
        return SelectItem(expr=expr, alias=alias)

    # ── FROM clause ───────────────────────────────────────

    def _parse_from_list(self) -> list[FromItem]:
        """Parse comma-separated from items (implicit cross join)."""
        items: list[FromItem] = []
        items.append(self._parse_from_item())
        while self._cur.type == TT.COMMA:
            self._advance()
            items.append(self._parse_from_item())
        return items

    def _parse_from_item(self) -> FromItem:
        primary = self._parse_from_primary()
        # JOIN chain
        while self._at_kw(
            "JOIN", "INNER_JOIN", "LEFT_JOIN", "LEFT_OUTER",
            "RIGHT_JOIN", "RIGHT_OUTER", "FULL_JOIN", "FULL_OUTER",
            "CROSS_JOIN", "NATURAL_JOIN",
            "INNER", "LEFT", "RIGHT", "FULL", "CROSS", "NATURAL",
        ):
            join_type, right = self._parse_join()
            on_expr:    Optional[Expr] = None
            using_cols: list[str]      = []

            if self._at_kw("ON"):
                self._advance()
                on_expr = self._parse_expr()
            elif self._at_kw("USING"):
                self._advance()
                if self._cur.type == TT.LPAREN:
                    self._advance()
                    while self._cur.type != TT.RPAREN and self._cur.type != TT.EOF:
                        using_cols.append(self._cur.value.lower())
                        self._advance()
                        self._match_comma()
                    if self._cur.type == TT.RPAREN:
                        self._advance()

            primary = JoinedTable(
                join_type=join_type,
                left=primary,
                right=right,
                on_expr=on_expr,
                using_cols=using_cols,
            )
        return primary

    def _parse_join(self) -> tuple[str, FromItem]:
        """Parse JOIN keyword(s) and return (join_type_str, right_from_item)."""
        kw = self._cur.value.upper()
        self._advance()

        if kw in ("INNER_JOIN", "INNER"):
            if kw == "INNER":
                self._match_kw("JOIN")
            return "INNER", self._parse_from_primary()

        elif kw in ("LEFT_JOIN", "LEFT", "LEFT_OUTER"):
            self._match_kw("OUTER", "JOIN")
            if self._at_kw("JOIN"):
                self._advance()
            return "LEFT", self._parse_from_primary()

        elif kw in ("RIGHT_JOIN", "RIGHT", "RIGHT_OUTER"):
            self._match_kw("OUTER", "JOIN")
            if self._at_kw("JOIN"):
                self._advance()
            return "RIGHT", self._parse_from_primary()

        elif kw in ("FULL_JOIN", "FULL", "FULL_OUTER"):
            self._match_kw("OUTER", "JOIN")
            if self._at_kw("JOIN"):
                self._advance()
            return "FULL", self._parse_from_primary()

        elif kw in ("CROSS_JOIN", "CROSS"):
            self._match_kw("JOIN")
            return "CROSS", self._parse_from_primary()

        elif kw in ("NATURAL_JOIN", "NATURAL"):
            self._match_kw("JOIN")
            return "NATURAL", self._parse_from_primary()

        else:  # bare JOIN → INNER
            return "INNER", self._parse_from_primary()

    def _parse_from_primary(self) -> FromItem:
        # LATERAL keyword (optional, skip)
        self._match_kw("LATERAL")

        if self._cur.type == TT.LPAREN:
            self._advance()
            if self._at_kw("SELECT", "WITH"):
                query = self._parse_select() if self._at_kw("SELECT") else self._parse_with_or_select()
                self._expect(")")
                alias = self._parse_optional_alias()
                return SubqueryRef(query=query, alias=alias)
            else:
                # parenthesised FROM item
                item = self._parse_from_item()
                self._expect(")")
                return item

        # table reference
        name = self._parse_qualified_name()
        alias = self._parse_optional_alias()
        return TableRef(name=name, alias=alias)

    def _parse_optional_alias(self) -> Optional[str]:
        if self._at_kw("AS"):
            self._advance()
            alias = self._cur.value
            self._advance()
            return alias
        # implicit alias — identifier not followed by keyword
        if (self._cur.type == TT.IDENTIFIER and not self._at_kw(
            "ON", "USING", "WHERE", "GROUP", "HAVING", "ORDER",
            "LIMIT", "UNION", "INTERSECT", "EXCEPT", "JOIN",
            "INNER", "LEFT", "RIGHT", "FULL", "CROSS", "NATURAL",
            "INNER_JOIN", "LEFT_JOIN", "RIGHT_JOIN", "FULL_JOIN",
            "CROSS_JOIN", "NATURAL_JOIN", "RPAREN", "SEMI", "COMMA",
        )):
            alias = self._cur.value
            self._advance()
            return alias
        return None

    def _parse_qualified_name(self) -> QualifiedName:
        parts: list[str] = []
        parts.append(self._cur.value)
        self._advance()
        while self._cur.type == TT.DOT:
            self._advance()
            parts.append(self._cur.value)
            self._advance()
        return QualifiedName(parts=parts)

    # ── ORDER BY items ────────────────────────────────────

    def _parse_order_by_items(self) -> list[OrderItem]:
        items: list[OrderItem] = []
        while True:
            expr = self._parse_simple_expr()
            direction = "ASC"
            nulls = None
            if self._at_kw("ASC"):
                self._advance()
            elif self._at_kw("DESC"):
                direction = "DESC"
                self._advance()
            if self._at_kw("NULLS"):
                self._advance()
                if self._at_kw("FIRST"):
                    nulls = "FIRST"
                    self._advance()
                elif self._at_kw("LAST"):
                    nulls = "LAST"
                    self._advance()
            items.append(OrderItem(expr=expr, direction=direction, nulls=nulls))
            if not self._match_comma():
                break
        return items

    # ── Expressions ───────────────────────────────────────

    def _parse_expr_list(self) -> list[Expr]:
        exprs: list[Expr] = []
        exprs.append(self._parse_expr())
        while self._cur.type == TT.COMMA:
            self._advance()
            exprs.append(self._parse_expr())
        return exprs

    def _parse_expr(self) -> Expr:
        return self._parse_or_expr()

    def _parse_or_expr(self) -> Expr:
        left = self._parse_and_expr()
        while self._at_kw("OR"):
            self._advance()
            right = self._parse_and_expr()
            left = BinaryExpr(op="OR", left=left, right=right)
        return left

    def _parse_and_expr(self) -> Expr:
        left = self._parse_not_expr()
        while self._at_kw("AND"):
            self._advance()
            right = self._parse_not_expr()
            left = BinaryExpr(op="AND", left=left, right=right)
        return left

    def _parse_not_expr(self) -> Expr:
        if self._at_kw("NOT"):
            self._advance()
            return UnaryExpr(op="NOT", expr=self._parse_not_expr())
        return self._parse_comparison()

    def _parse_comparison(self) -> Expr:
        left = self._parse_range_expr()
        while True:
            op = self._cur.value.upper()
            if self._cur.type == TT.OP and op in ("=", "<>", "!=", "<", ">", "<=", ">="):
                self._advance()
                right = self._parse_range_expr()
                left = BinaryExpr(op=op, left=left, right=right)
            elif self._at_kw("IS"):
                self._advance()
                negated = self._match_kw("NOT")
                self._match_kw("NULL", "DISTINCT")
                left = IsNullExpr(expr=left, negated=negated)
            elif self._at_kw("IN") or self._at_kw("NOT_IN"):
                negated = self._cur.value == "NOT_IN"
                self._advance()
                values: list[Expr] = []
                subq: Optional[SelectStmt] = None
                if self._cur.type == TT.LPAREN:
                    self._advance()
                    if self._at_kw("SELECT", "WITH"):
                        subq = self._parse_select() if self._at_kw("SELECT") else self._parse_with_or_select()
                    else:
                        values = self._parse_expr_list()
                    self._expect(")")
                left = InExpr(expr=left, values=values, subquery=subq, negated=negated)
            elif self._at_kw("BETWEEN") or self._at_kw("NOT_BETWEEN"):
                negated = self._cur.value == "NOT_BETWEEN"
                self._advance()
                low  = self._parse_range_expr()
                self._expect("AND")
                high = self._parse_range_expr()
                left = BetweenExpr(expr=left, low=low, high=high, negated=negated)
            elif self._at_kw("LIKE", "NOT_LIKE", "ILIKE", "SIMILAR"):
                op2 = self._cur.value
                self._advance()
                right = self._parse_range_expr()
                left = BinaryExpr(op=op2, left=left, right=right)
            else:
                break
        return left

    def _parse_range_expr(self) -> Expr:
        return self._parse_add_expr()

    def _parse_add_expr(self) -> Expr:
        left = self._parse_mul_expr()
        while self._cur.type == TT.OP and self._cur.value in ("+", "-"):
            op = self._cur.value
            self._advance()
            right = self._parse_mul_expr()
            left = BinaryExpr(op=op, left=left, right=right)
        if self._cur.type == TT.KEYWORD and self._cur.value == "CONCAT":
            self._advance()
            right = self._parse_mul_expr()
            left = BinaryExpr(op="||", left=left, right=right)
        return left

    def _parse_mul_expr(self) -> Expr:
        left = self._parse_unary_expr()
        while (
            (self._cur.type == TT.OP and self._cur.value in ("/", "%")) or
            (self._cur.type == TT.STAR)   # * as multiplication
        ):
            op = self._cur.value
            self._advance()
            right = self._parse_unary_expr()
            left = BinaryExpr(op=op, left=left, right=right)
        return left

    def _parse_unary_expr(self) -> Expr:
        if self._cur.type == TT.OP and self._cur.value == "-":
            self._advance()
            return UnaryExpr(op="-", expr=self._parse_postfix_expr())
        return self._parse_postfix_expr()

    def _parse_postfix_expr(self) -> Expr:
        expr = self._parse_primary()
        # type cast ::
        while self._cur.type == TT.OP and self._cur.value == "::":
            self._advance()
            type_name = self._cur.value
            self._advance()
            # optional [] for arrays
            if self._cur.type == TT.OP and self._cur.value == "[":
                type_name += "[]"
                self._advance()
                if self._cur.type == TT.OP and self._cur.value == "]":
                    self._advance()
            expr = TypeCast(expr=expr, type_name=type_name)
        return expr

    def _parse_primary(self) -> Expr:
        t = self._cur

        # Parenthesised expression or scalar subquery
        if t.type == TT.LPAREN:
            self._advance()
            if self._at_kw("SELECT", "WITH"):
                subq = self._parse_select() if self._at_kw("SELECT") else self._parse_with_or_select()
                self._expect(")")
                return subq
            expr = self._parse_expr()
            self._expect(")")
            return expr

        # EXISTS
        if self._at_kw("EXISTS"):
            self._advance()
            self._expect("(")
            subq = self._parse_select()
            self._expect(")")
            return SubqueryExpr(op="EXISTS", subquery=subq)

        # ANY / ALL / SOME
        if self._at_kw("ANY", "ALL", "SOME"):
            op = self._cur.value
            self._advance()
            self._expect("(")
            subq = self._parse_select()
            self._expect(")")
            return SubqueryExpr(op=op, subquery=subq)

        # CASE
        if self._at_kw("CASE"):
            return self._parse_case()

        # CAST
        if self._at_kw("CAST"):
            return self._parse_cast()

        # NULL literal
        if self._at_kw("NULL"):
            self._advance()
            return Literal(value="NULL")

        # Boolean literals
        if self._at_kw("TRUE"):
            self._advance()
            return Literal(value="TRUE")
        if self._at_kw("FALSE"):
            self._advance()
            return Literal(value="FALSE")

        # Numeric / string literals
        if t.type == TT.LITERAL:
            self._advance()
            return Literal(value=t.value)

        # $1 param
        if t.type == TT.PARAM:
            self._advance()
            return Param(index=int(t.value[1:]))

        # * (bare star in COUNT(*))
        if t.type == TT.STAR:
            self._advance()
            return Star()

        # Identifier / function call / window function
        if t.type in (TT.IDENTIFIER, TT.KEYWORD):
            return self._parse_id_or_func()

        raise ParseError("Unexpected token in expression", t)

    def _parse_id_or_func(self) -> Expr:
        """Parse identifier, qualified name, function call, or window function."""
        # Collect identifier chain (a, a.b, a.b.c)
        parts: list[str] = [self._cur.value]
        self._advance()

        while self._cur.type == TT.DOT:
            self._advance()
            if self._cur.type == TT.STAR:
                self._advance()
                return Star(table=parts[-1])
            parts.append(self._cur.value)
            self._advance()

        func_name = parts[-1].upper()
        qualifier = ".".join(parts[:-1]) if len(parts) > 1 else None

        # EXTRACT(field FROM expr)
        if func_name == "EXTRACT" and self._cur.type == TT.LPAREN:
            self._advance()
            # field (YEAR, MONTH, etc.)
            field_name = self._cur.value
            self._advance()
            self._expect("FROM")
            arg = self._parse_expr()
            self._expect(")")
            return FunctionCall(
                name="EXTRACT",
                args=[Literal(value=field_name), arg],
            )

        # SUBSTRING / SUBSTR
        if func_name in ("SUBSTRING", "SUBSTR") and self._cur.type == TT.LPAREN:
            self._advance()
            args: list[Expr] = []
            args.append(self._parse_expr())
            if self._at_kw("FROM"):
                self._advance()
                args.append(self._parse_expr())
                if self._at_kw("FOR"):
                    self._advance()
                    args.append(self._parse_expr())
            elif self._cur.type == TT.COMMA:
                while self._cur.type == TT.COMMA:
                    self._advance()
                    args.append(self._parse_expr())
            self._expect(")")
            return FunctionCall(name=func_name, args=args)

        # Generic function call or window function
        if self._cur.type == TT.LPAREN:
            self._advance()
            distinct = False
            star     = False
            args2: list[Expr] = []

            if self._cur.type == TT.STAR:
                star = True
                self._advance()
            elif self._at_kw("DISTINCT"):
                distinct = True
                self._advance()
                args2 = self._parse_expr_list()
            elif self._cur.type != TT.RPAREN:
                args2 = self._parse_expr_list()

            self._expect(")")

            # FILTER clause
            filter_where: Optional[Expr] = None
            if self._at_kw("FILTER"):
                self._advance()
                self._expect("(")
                self._expect("WHERE")
                filter_where = self._parse_expr()
                self._expect(")")

            fc = FunctionCall(
                name=func_name,
                args=args2,
                distinct=distinct,
                star=star,
                filter_where=filter_where,
            )

            # OVER clause → window function
            if self._at_kw("OVER"):
                self._advance()
                self._expect("(")
                partition_by: list[Expr] = []
                order_by2: list[OrderItem] = []

                if self._at_kw("PARTITION_BY", "PARTITION"):
                    if self._at_kw("PARTITION"):
                        self._advance()
                        self._expect("BY")
                    else:
                        self._advance()
                    partition_by = self._parse_expr_list()

                if self._at_kw("ORDER_BY", "ORDER"):
                    if self._at_kw("ORDER"):
                        self._advance()
                        self._expect("BY")
                    else:
                        self._advance()
                    order_by2 = self._parse_order_by_items()

                # skip ROWS/RANGE/GROUPS frame clause
                if self._at_kw("ROWS", "RANGE", "GROUPS"):
                    self._skip_to("RPAREN")

                self._expect(")")
                return WindowFuncCall(
                    func=fc,
                    partition_by=partition_by,
                    order_by=order_by2,
                )

            return fc

        # Plain column reference
        if len(parts) == 1:
            return ColumnRef(table=None, column=parts[0])
        elif len(parts) == 2:
            return ColumnRef(table=parts[0], column=parts[1])
        else:
            # schema.table.column → table.column
            return ColumnRef(table=parts[-2], column=parts[-1])

    def _parse_simple_expr(self) -> Expr:
        """Parse a single primary + postfix (no boolean operators)."""
        return self._parse_postfix_expr()

    # ── CASE ──────────────────────────────────────────────

    def _parse_case(self) -> CaseExpr:
        self._expect("CASE")
        operand: Optional[Expr] = None
        if not self._at_kw("WHEN"):
            operand = self._parse_expr()
        whens: list[tuple[Expr, Expr]] = []
        while self._at_kw("WHEN"):
            self._advance()
            cond = self._parse_expr()
            self._expect("THEN")
            result = self._parse_expr()
            whens.append((cond, result))
        else_: Optional[Expr] = None
        if self._at_kw("ELSE"):
            self._advance()
            else_ = self._parse_expr()
        self._expect("END")
        return CaseExpr(operand=operand, whens=whens, else_=else_)

    # ── CAST ──────────────────────────────────────────────

    def _parse_cast(self) -> CastExpr:
        self._expect("CAST")
        self._expect("(")
        expr = self._parse_expr()
        self._expect("AS")
        type_name = self._cur.value
        self._advance()
        if self._cur.type == TT.LPAREN:
            self._skip_parens()
            type_name += "(...)"
        self._expect(")")
        return CastExpr(expr=expr, type_name=type_name)

    # ── DML ───────────────────────────────────────────────

    def _parse_insert(self) -> InsertStmt:
        self._expect("INSERT")
        self._expect("INTO")
        name = self._parse_qualified_name()
        alias = self._parse_optional_alias()
        columns: list[str] = []
        if self._cur.type == TT.LPAREN and self._peek(1).type != TT.KEYWORD:
            self._advance()
            while self._cur.type != TT.RPAREN and self._cur.type != TT.EOF:
                columns.append(self._cur.value.lower())
                self._advance()
                self._match_kw("COMMA")
            self._expect(")")
        if self._at_kw("VALUES"):
            self._advance()
            # skip values
            self._skip_parens()
            source: list[list[Expr]] = []
        else:
            source = self._parse_select()  # type: ignore
        return InsertStmt(table=name, alias=alias, columns=columns, source=source)

    def _parse_update(self) -> UpdateStmt:
        self._expect("UPDATE")
        name = self._parse_qualified_name()
        alias = self._parse_optional_alias()
        self._expect("SET")
        set_items: list[tuple[str, Expr]] = []
        while True:
            col = self._cur.value.lower()
            self._advance()
            self._expect("=")
            val = self._parse_expr()
            set_items.append((col, val))
            if not self._cur.type == TT.COMMA:
                break
            self._advance()
        from_items: list[FromItem] = []
        if self._at_kw("FROM"):
            self._advance()
            from_items = self._parse_from_list()
        where: Optional[Expr] = None
        if self._at_kw("WHERE"):
            self._advance()
            where = self._parse_expr()
        return UpdateStmt(table=name, alias=alias, set_items=set_items,
                          from_items=from_items, where=where)

    def _parse_delete(self) -> DeleteStmt:
        self._expect("DELETE")
        self._expect("FROM")
        name = self._parse_qualified_name()
        alias = self._parse_optional_alias()
        using: list[FromItem] = []
        if self._at_kw("USING"):
            self._advance()
            using = self._parse_from_list()
        where: Optional[Expr] = None
        if self._at_kw("WHERE"):
            self._advance()
            where = self._parse_expr()
        return DeleteStmt(table=name, alias=alias, using=using, where=where)


# ── Public helper ─────────────────────────────────────────

def parse_sql(sql: str) -> list[SqlStmt]:
    """
    Parse one or more SQL statements.
    Returns a list of AST nodes.
    Tolerant — partial results returned on error.
    """
    tokens = tokenize(sql)
    return Parser(tokens).parse()
