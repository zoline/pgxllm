"""
pgxllm.parser.tokenizer
------------------------
Lightweight PostgreSQL SQL tokenizer.
Handles pg_stat_statements $1/$2 parameters, quoted identifiers,
dollar-quoted strings, and inline comments.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum, auto
from typing import Iterator


class TT(Enum):
    """Token Type"""
    KEYWORD    = auto()
    IDENTIFIER = auto()
    LITERAL    = auto()
    PARAM      = auto()    # $1, $2 …  (pg_stat_statements)
    OP         = auto()    # operators and punctuation
    LPAREN     = auto()
    RPAREN     = auto()
    COMMA      = auto()
    DOT        = auto()
    SEMI       = auto()
    STAR       = auto()
    EOF        = auto()


@dataclass(slots=True)
class Token:
    type:  TT
    value: str
    line:  int = 0
    col:   int = 0

    def is_kw(self, *words: str) -> bool:
        return self.type == TT.KEYWORD and self.value.upper() in {w.upper() for w in words}

    def is_id(self) -> bool:
        return self.type in (TT.IDENTIFIER, TT.KEYWORD)

    def upper(self) -> str:
        return self.value.upper()

    def __repr__(self) -> str:
        return f"Token({self.type.name}, {self.value!r}, {self.line}:{self.col})"


# ── Keywords ──────────────────────────────────────────────
KEYWORDS: frozenset[str] = frozenset({
    "ALL", "AND", "ANY", "AS", "ASC", "BETWEEN", "BY", "CASE",
    "CAST", "COALESCE", "CROSS", "CURRENT", "DELETE", "DESC",
    "DENSE_RANK", "DISTINCT", "ELSE", "END", "EXCEPT", "EXISTS",
    "EXTRACT", "FALSE", "FETCH", "FILTER", "FIRST", "FOLLOWING",
    "FROM", "FULL", "GROUP", "GROUPS", "HAVING", "ILIKE", "IN",
    "INNER", "INSERT", "INTERSECT", "INTO", "IS", "JOIN", "LAG",
    "LAST", "LEAD", "LEFT", "LIKE", "LIMIT", "NATURAL", "NOT",
    "NULL", "NULLS", "NTILE", "OFFSET", "ON", "OR", "ORDER",
    "OUTER", "OVER", "PARTITION", "PERCENT_RANK", "PRECEDING",
    "RANGE", "RANK", "RECURSIVE", "RIGHT", "ROW", "ROWS",
    "ROW_NUMBER", "SELECT", "SET", "SIMILAR", "SOME", "SUBSTR",
    "SUBSTRING", "THEN", "TIES", "TO", "TRUE", "UNBOUNDED",
    "UNION", "UPDATE", "USING", "VALUES", "WHEN", "WHERE",
    "WITH", "WITHIN", "COUNT", "SUM", "AVG", "MIN", "MAX",
    "ARRAY_AGG", "STRING_AGG", "JSON_AGG", "CUME_DIST",
    "FIRST_VALUE", "LAST_VALUE", "NTH_VALUE", "ONLY",
    "RETURNING", "USING", "WINDOW", "LATERAL",
})

# Multi-word tokens normalized to single token
COMPOUND_KW: dict[tuple[str, ...], str] = {
    ("ROW", "NUMBER"):     "ROW_NUMBER",
    ("DENSE", "RANK"):     "DENSE_RANK",
    ("FIRST", "VALUE"):    "FIRST_VALUE",
    ("LAST", "VALUE"):     "LAST_VALUE",
    ("NTH", "VALUE"):      "NTH_VALUE",
    ("PERCENT", "RANK"):   "PERCENT_RANK",
    ("CUME", "DIST"):      "CUME_DIST",
    ("ARRAY", "AGG"):      "ARRAY_AGG",
    ("STRING", "AGG"):     "STRING_AGG",
    ("JSON", "AGG"):       "JSON_AGG",
    ("NOT", "IN"):         "NOT_IN",
    ("NOT", "LIKE"):       "NOT_LIKE",
    ("NOT", "BETWEEN"):    "NOT_BETWEEN",
    ("NOT", "EXISTS"):     "NOT_EXISTS",
    ("IS", "NOT"):         "IS_NOT",
    ("ORDER", "BY"):       "ORDER_BY",
    ("GROUP", "BY"):       "GROUP_BY",
    ("PARTITION", "BY"):   "PARTITION_BY",
    ("LEFT", "JOIN"):      "LEFT_JOIN",
    ("LEFT", "OUTER"):     "LEFT_OUTER",
    ("RIGHT", "JOIN"):     "RIGHT_JOIN",
    ("RIGHT", "OUTER"):    "RIGHT_OUTER",
    ("FULL", "JOIN"):      "FULL_JOIN",
    ("FULL", "OUTER"):     "FULL_OUTER",
    ("INNER", "JOIN"):     "INNER_JOIN",
    ("CROSS", "JOIN"):     "CROSS_JOIN",
    ("NATURAL", "JOIN"):   "NATURAL_JOIN",
}

_TOKEN_RE = re.compile(
    r"""
    (?P<BLOCK_COMMENT>  /\* .*? \*/ )
  | (?P<LINE_COMMENT>   -- [^\r\n]* )
  | (?P<DOLLAR_STR>     \$(?:[A-Za-z_]\w*)?\$.*?\$(?:[A-Za-z_]\w*)?\$ )
  | (?P<E_STRING>       [Ee]' (?: [^\\'] | \\. | '' )* ' )
  | (?P<STRING>         ' (?: [^'] | '' )* ' )
  | (?P<QUOTED_ID>      " (?: [^"] | "" )* " )
  | (?P<PARAM>          \$ \d+ )
  | (?P<FLOAT>          \d+ \. \d* | \. \d+ )
  | (?P<INT>            \d+ )
  | (?P<DOUBLE_COLON>   :: )
  | (?P<ARROW2>         ->> )
  | (?P<ARROW>          -> )
  | (?P<CONCAT>         \|\| )
  | (?P<NEQ>            <> | != )
  | (?P<LTE>            <= )
  | (?P<GTE>            >= )
  | (?P<OP>             [=<>+\-/%] )
  | (?P<LPAREN>         \( )
  | (?P<RPAREN>         \) )
  | (?P<LBRACKET>       \[ )
  | (?P<RBRACKET>       \] )
  | (?P<COMMA>          , )
  | (?P<DOT>            \. )
  | (?P<SEMI>           ; )
  | (?P<STAR>           \* )
  | (?P<WORD>           [a-zA-Z_\u0080-\uFFFF][a-zA-Z_0-9\u0080-\uFFFF$]* )
  | (?P<WS>             [ \t\r\n]+ )
    """,
    re.VERBOSE | re.DOTALL | re.UNICODE,
)


def tokenize(sql: str) -> list[Token]:
    """
    Tokenize a PostgreSQL SQL string.
    Returns a flat list of Token objects (whitespace and comments stripped).
    pg_stat_statements $1/$2 params are preserved as PARAM tokens.
    """
    tokens: list[Token] = []
    line = 1
    line_start = 0

    for m in _TOKEN_RE.finditer(sql):
        kind = m.lastgroup
        val  = m.group()
        col  = m.start() - line_start

        # count newlines for line tracking
        newlines = val.count("\n")
        if newlines:
            line += newlines
            line_start = m.start() + val.rfind("\n") + 1

        if kind in ("WS", "LINE_COMMENT", "BLOCK_COMMENT"):
            continue

        if kind == "WORD":
            up = val.upper()
            tt = TT.KEYWORD if up in KEYWORDS else TT.IDENTIFIER
            tokens.append(Token(tt, up if tt == TT.KEYWORD else val, line, col))

        elif kind == "QUOTED_ID":
            # Strip quotes, preserve case
            inner = val[1:-1].replace('""', '"')
            tokens.append(Token(TT.IDENTIFIER, inner, line, col))

        elif kind in ("STRING", "E_STRING", "DOLLAR_STR"):
            tokens.append(Token(TT.LITERAL, val, line, col))

        elif kind in ("INT", "FLOAT"):
            tokens.append(Token(TT.LITERAL, val, line, col))

        elif kind == "PARAM":
            tokens.append(Token(TT.PARAM, val, line, col))

        elif kind == "LPAREN":
            tokens.append(Token(TT.LPAREN, val, line, col))

        elif kind == "RPAREN":
            tokens.append(Token(TT.RPAREN, val, line, col))

        elif kind == "COMMA":
            tokens.append(Token(TT.COMMA, val, line, col))

        elif kind == "DOT":
            tokens.append(Token(TT.DOT, val, line, col))

        elif kind == "SEMI":
            tokens.append(Token(TT.SEMI, val, line, col))

        elif kind == "STAR":
            tokens.append(Token(TT.STAR, val, line, col))

        else:
            tokens.append(Token(TT.OP, val, line, col))

    tokens.append(Token(TT.EOF, "", line, 0))
    return _merge_compound(tokens)


def _merge_compound(tokens: list[Token]) -> list[Token]:
    """
    Merge adjacent keyword tokens into compound keywords.
    e.g. ROW + NUMBER → ROW_NUMBER,  ORDER + BY → ORDER_BY
    """
    result: list[Token] = []
    i = 0
    while i < len(tokens):
        t = tokens[i]
        if t.type == TT.KEYWORD and i + 1 < len(tokens):
            nxt = tokens[i + 1]
            if nxt.type == TT.KEYWORD:
                pair = (t.value, nxt.value)
                if pair in COMPOUND_KW:
                    merged = COMPOUND_KW[pair]
                    result.append(Token(TT.KEYWORD, merged, t.line, t.col))
                    i += 2
                    continue
        result.append(t)
        i += 1
    return result
