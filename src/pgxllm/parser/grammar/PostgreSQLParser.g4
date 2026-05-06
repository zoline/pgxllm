// PostgreSQL Parser Grammar for pgxllm
// Based on grammars-v4 PostgreSQLParser.g4 (MIT License)
// Focused on SELECT/DML constructs needed for SQL analysis

parser grammar PostgreSQLParser;

options {
    tokenVocab = PostgreSQLLexer;
}

// ── Entry point ───────────────────────────────────────────
root
    : sql_stmt (SEMI sql_stmt)* SEMI? EOF
    ;

sql_stmt
    : select_stmt
    | insert_stmt
    | update_stmt
    | delete_stmt
    | with_stmt
    ;

// ── WITH (CTE) ────────────────────────────────────────────
with_stmt
    : WITH RECURSIVE? cte_list select_stmt
    ;

cte_list
    : cte_definition (COMMA cte_definition)*
    ;

cte_definition
    : cte_name AS LPAREN select_stmt RPAREN
    ;

cte_name
    : IDENTIFIER
    ;

// ── SELECT ────────────────────────────────────────────────
select_stmt
    : select_core (set_op select_core)*
      order_by_clause?
      limit_clause?
      offset_clause?
      fetch_clause?
    ;

set_op
    : UNION ALL?
    | INTERSECT
    | EXCEPT
    ;

select_core
    : SELECT distinct_clause? select_list
      from_clause?
      where_clause?
      group_by_clause?
      having_clause?
      window_clause?
    ;

distinct_clause
    : DISTINCT (ON LPAREN expr_list RPAREN)?
    | ALL
    ;

select_list
    : select_item (COMMA select_item)*
    ;

select_item
    : STAR
    | table_ref_name DOT STAR
    | expr (AS? alias_name)?
    ;

alias_name
    : IDENTIFIER
    | STRING_LITERAL
    ;

// ── FROM ──────────────────────────────────────────────────
from_clause
    : FROM from_item (COMMA from_item)*
    ;

from_item
    : from_primary join_clause*
    ;

from_primary
    : table_factor
    | LPAREN select_stmt RPAREN (AS? alias_name)?   # subquery_primary
    | LPAREN from_item RPAREN                         # paren_from
    ;

table_factor
    : table_ref_name (AS? alias_name)?
    ;

table_ref_name
    : (schema_name DOT)? table_name
    ;

schema_name : IDENTIFIER ;
table_name  : IDENTIFIER ;

join_clause
    : join_type? JOIN from_primary join_condition
    | NATURAL join_type? JOIN from_primary
    | CROSS JOIN from_primary
    ;

join_type
    : INNER
    | LEFT OUTER?
    | RIGHT OUTER?
    | FULL OUTER?
    ;

join_condition
    : ON expr                         # join_on
    | USING LPAREN column_list RPAREN # join_using
    ;

column_list
    : column_ref (COMMA column_ref)*
    ;

// ── WHERE ─────────────────────────────────────────────────
where_clause
    : WHERE expr
    ;

// ── GROUP BY / HAVING ─────────────────────────────────────
group_by_clause
    : GROUP BY expr_list
    ;

having_clause
    : HAVING expr
    ;

// ── WINDOW ────────────────────────────────────────────────
window_clause
    : // reserved for future
    ;

// ── ORDER BY ──────────────────────────────────────────────
order_by_clause
    : ORDER BY order_item (COMMA order_item)*
    ;

order_item
    : expr (ASC | DESC)? (NULLS (FIRST | LAST))?
    ;

// ── LIMIT / OFFSET / FETCH ────────────────────────────────
limit_clause
    : LIMIT (expr | ALL)
    ;

offset_clause
    : OFFSET expr (ROW_NUMBER | ROWS)?
    ;

fetch_clause
    : FETCH (FIRST | NEXT) expr? (ROW_NUMBER | ROWS) (ONLY | WITH TIES)
    ;

// ── INSERT / UPDATE / DELETE ──────────────────────────────
insert_stmt
    : INSERT INTO table_ref_name (AS? alias_name)?
      (LPAREN column_list RPAREN)?
      (VALUES values_list | select_stmt)
    ;

values_list
    : LPAREN expr_list RPAREN (COMMA LPAREN expr_list RPAREN)*
    ;

update_stmt
    : UPDATE table_ref_name (AS? alias_name)?
      SET update_item (COMMA update_item)*
      (FROM from_clause)?
      where_clause?
    ;

update_item
    : column_ref EQ expr
    ;

delete_stmt
    : DELETE FROM table_ref_name (AS? alias_name)?
      (USING from_clause)?
      where_clause?
    ;

// ── EXPRESSIONS ───────────────────────────────────────────
expr
    : literal_expr                                   # literal
    | column_expr                                    # column
    | function_expr                                  # function
    | cast_expr                                      # cast
    | case_expr                                      # case_expression
    | subquery_expr                                  # subquery_expression
    | exists_expr                                    # exists_expression
    | window_func_expr                               # window_function
    | expr DOUBLE_COLON type_name                   # type_cast
    | LPAREN expr RPAREN                            # paren_expr
    | LPAREN select_stmt RPAREN                     # scalar_subquery
    | NOT expr                                       # not_expr
    | MINUS expr                                     # unary_minus
    | expr (STAR | SLASH | PERCENT) expr            # arith_expr
    | expr (PLUS | MINUS | CONCAT) expr             # arith_expr2
    | expr (EQ | NEQ | LT | GT | LTE | GTE) expr   # compare_expr
    | expr BETWEEN expr AND expr                     # between_expr
    | expr NOT? LIKE expr                            # like_expr
    | expr NOT? ILIKE expr                           # ilike_expr
    | expr NOT? IN LPAREN (expr_list | select_stmt) RPAREN # in_expr
    | expr IS NOT? NULL                             # is_null_expr
    | expr AND expr                                  # and_expr
    | expr OR expr                                   # or_expr
    | PARAM                                         # param_expr
    ;

literal_expr
    : INTEGER_LITERAL
    | NUMERIC_LITERAL
    | STRING_LITERAL
    | TRUE
    | FALSE
    | NULL
    ;

column_expr
    : column_ref
    ;

column_ref
    : (table_ref_name DOT)? column_name
    ;

column_name : IDENTIFIER ;

type_name
    : IDENTIFIER (LPAREN INTEGER_LITERAL (COMMA INTEGER_LITERAL)? RPAREN)?
    | IDENTIFIER LBRACKET RBRACKET
    ;

// ── FUNCTION CALLS ────────────────────────────────────────
function_expr
    : function_name LPAREN (DISTINCT? expr_list | STAR)? RPAREN
      filter_clause?
    ;

function_name
    : IDENTIFIER
    | COUNT | SUM | AVG | MIN | MAX
    | ARRAY_AGG | STRING_AGG | JSON_AGG
    | COALESCE | EXTRACT | SUBSTR | SUBSTRING
    ;

filter_clause
    : FILTER LPAREN WHERE expr RPAREN
    ;

// ── EXTRACT ───────────────────────────────────────────────
// Handled as function_expr above; extract field detection
// done in visitor via function name check

// ── CAST ──────────────────────────────────────────────────
cast_expr
    : CAST LPAREN expr AS type_name RPAREN
    ;

// ── CASE ──────────────────────────────────────────────────
case_expr
    : CASE expr? when_clause+ else_clause? END
    ;

when_clause
    : WHEN expr THEN expr
    ;

else_clause
    : ELSE expr
    ;

// ── SUBQUERY ──────────────────────────────────────────────
subquery_expr
    : (ALL | ANY | SOME) LPAREN select_stmt RPAREN
    ;

exists_expr
    : EXISTS LPAREN select_stmt RPAREN
    ;

// ── WINDOW FUNCTION ───────────────────────────────────────
window_func_expr
    : window_func_name LPAREN expr_list? RPAREN
      OVER LPAREN partition_clause? order_by_clause? frame_clause? RPAREN
    ;

window_func_name
    : RANK | DENSE_RANK | ROW_NUMBER | NTILE
    | LAG | LEAD | FIRST_VALUE | LAST_VALUE | NTH_VALUE
    | PERCENT_RANK | CUME_DIST
    | COUNT | SUM | AVG | MIN | MAX
    ;

partition_clause
    : PARTITION BY expr_list
    ;

frame_clause
    : (RANGE | ROWS | GROUPS) frame_bound
    | (RANGE | ROWS | GROUPS) BETWEEN frame_bound AND frame_bound
    ;

frame_bound
    : UNBOUNDED PRECEDING
    | CURRENT ROW
    | expr PRECEDING
    | UNBOUNDED FOLLOWING
    | expr FOLLOWING
    ;

// keywords used as identifiers in frame
ROWS        : R O W S ;
GROUPS      : G R O U P S ;
RANGE       : R A N G E ;
UNBOUNDED   : U N B O U N D E D ;
PRECEDING   : P R E C E D I N G ;
FOLLOWING   : F O L L O W I N G ;
CURRENT     : C U R R E N T ;
ROW         : R O W ;
ROWS2       : R O W S ; // alias; lexer handles
TIES        : T I E S ;
RECURSIVE   : R E C U R S I V E ;
SOME        : S O M E ;
ANY         : A N Y ;
INTO        : I N T O ;
VALUES      : V A L U E S ;
ROWS3       : R O W S ;

// ── Helper ────────────────────────────────────────────────
expr_list
    : expr (COMMA expr)*
    ;
