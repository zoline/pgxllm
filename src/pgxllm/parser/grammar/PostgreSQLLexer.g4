// PostgreSQL Lexer Grammar for pgxllm
// Based on grammars-v4 PostgreSQLLexer.g4 (MIT License)
// Focused on SELECT/DML constructs needed for SQL analysis

lexer grammar PostgreSQLLexer;

options {
    superClass = PostgreSQLLexerBase;
}

// ── Keywords ──────────────────────────────────────────────
ALL         : A L L ;
AND         : A N D ;
AS          : A S ;
ASC         : A S C ;
BETWEEN     : B E T W E E N ;
BY          : B Y ;
CASE        : C A S E ;
CAST        : C A S T ;
COALESCE    : C O A L E S C E ;
CROSS       : C R O S S ;
CTE         : C T E ;  // not a real keyword, placeholder
DELETE      : D E L E T E ;
DESC        : D E S C ;
DENSE_RANK  : D E N S E '_' R A N K ;
DISTINCT    : D I S T I N C T ;
ELSE        : E L S E ;
END         : E N D ;
EXCEPT      : E X C E P T ;
EXISTS      : E X I S T S ;
EXTRACT     : E X T R A C T ;
FALSE       : F A L S E ;
FETCH       : F E T C H ;
FILTER      : F I L T E R ;
FIRST       : F I R S T ;
FROM        : F R O M ;
FULL        : F U L L ;
GROUP       : G R O U P ;
HAVING      : H A V I N G ;
ILIKE       : I L I K E ;
IN          : I N ;
INNER       : I N N E R ;
INSERT      : I N S E R T ;
INTERSECT   : I N T E R S E C T ;
IS          : I S ;
JOIN        : J O I N ;
LAST        : L A S T ;
LEFT        : L E F T ;
LIKE        : L I K E ;
LIMIT       : L I M I T ;
NATURAL     : N A T U R A L ;
NOT         : N O T ;
NULL        : N U L L ;
NULLS       : N U L L S ;
OFFSET      : O F F S E T ;
ON          : O N ;
OR          : O R ;
ORDER       : O R D E R ;
OUTER       : O U T E R ;
OVER        : O V E R ;
PARTITION   : P A R T I T I O N ;
RANK        : R A N K ;
RIGHT       : R I G H T ;
ROW_NUMBER  : R O W '_' N U M B E R ;
SELECT      : S E L E C T ;
SET         : S E T ;
SIMILAR     : S I M I L A R ;
SUBSTR      : S U B S T R ;
SUBSTRING   : S U B S T R I N G ;
THEN        : T H E N ;
TO          : T O ;
TRUE        : T R U E ;
UNION       : U N I O N ;
UPDATE      : U P D A T E ;
USING       : U S I N G ;
WHEN        : W H E N ;
WHERE       : W H E R E ;
WITH        : W I T H ;
WITHIN      : W I T H I N ;
NTILE       : N T I L E ;
LAG         : L A G ;
LEAD        : L E A D ;
FIRST_VALUE : F I R S T '_' V A L U E ;
LAST_VALUE  : L A S T '_' V A L U E ;
NTH_VALUE   : N T H '_' V A L U E ;
PERCENT_RANK: P E R C E N T '_' R A N K ;
CUME_DIST   : C U M E '_' D I S T ;

// ── Aggregate functions ───────────────────────────────────
COUNT       : C O U N T ;
SUM         : S U M ;
AVG         : A V G ;
MIN         : M I N ;
MAX         : M A X ;
ARRAY_AGG   : A R R A Y '_' A G G ;
STRING_AGG  : S T R I N G '_' A G G ;
JSON_AGG    : J S O N '_' A G G ;

// ── Symbols ───────────────────────────────────────────────
STAR        : '*' ;
COMMA       : ',' ;
DOT         : '.' ;
SEMI        : ';' ;
COLON       : ':' ;
DOUBLE_COLON: '::' ;
LPAREN      : '(' ;
RPAREN      : ')' ;
LBRACKET    : '[' ;
RBRACKET    : ']' ;
EQ          : '=' ;
NEQ         : '<>' | '!=' ;
LT          : '<' ;
GT          : '>' ;
LTE         : '<=' ;
GTE         : '>=' ;
PLUS        : '+' ;
MINUS       : '-' ;
SLASH       : '/' ;
PERCENT     : '%' ;
CONCAT      : '||' ;
ARROW       : '->' ;
ARROW2      : '->>' ;

// ── Literals ──────────────────────────────────────────────
INTEGER_LITERAL
    : DIGIT+ ;

NUMERIC_LITERAL
    : DIGIT+ '.' DIGIT*
    | '.' DIGIT+
    | DIGIT+ ('.' DIGIT*)? ([eE] [+-]? DIGIT+)?
    ;

STRING_LITERAL
    : '\'' ( ~'\'' | '\'\'' )* '\''
    | E_STRING_LITERAL
    ;

E_STRING_LITERAL
    : [Ee] '\'' ( ~[\\'] | '\\' . | '\'\'' )* '\'' ;

DOLLAR_STRING
    : '$' TAG? '$' .*? '$' TAG? '$' ;

fragment TAG : [a-zA-Z_][a-zA-Z_0-9]* ;

// ── Identifier ────────────────────────────────────────────
IDENTIFIER
    : [a-zA-Z_\u0080-\uFFFF] [a-zA-Z_0-9\u0080-\uFFFF$]*
    | QUOTED_IDENTIFIER
    ;

QUOTED_IDENTIFIER
    : '"' ( ~'"' | '""' )* '"' ;

// ── Param placeholder (pg_stat_statements uses $1, $2 ...) ─
PARAM
    : '$' DIGIT+ ;

// ── Whitespace & Comments ─────────────────────────────────
WS          : [ \t\r\n]+ -> skip ;

LINE_COMMENT
    : '--' ~[\r\n]* -> skip ;

BLOCK_COMMENT
    : '/*' .*? '*/' -> skip ;

// ── Annotation (pgxllm SQL file registration) ─────────────
// -- @relation orders -> customers : 주문-고객
// Handled at the SQL pre-processor level, not here

// ── Fragments ─────────────────────────────────────────────
fragment DIGIT : [0-9] ;
fragment A : [aA] ; fragment B : [bB] ; fragment C : [cC] ;
fragment D : [dD] ; fragment E : [eE] ; fragment F : [fF] ;
fragment G : [gG] ; fragment H : [hH] ; fragment I : [iI] ;
fragment J : [jJ] ; fragment K : [kK] ; fragment L : [lL] ;
fragment M : [mM] ; fragment N : [nN] ; fragment O : [oO] ;
fragment P : [pP] ; fragment Q : [qQ] ; fragment R : [rR] ;
fragment S : [sS] ; fragment T : [tT] ; fragment U : [uU] ;
fragment V : [vV] ; fragment W : [wW] ; fragment X : [xX] ;
fragment Y : [yY] ; fragment Z : [zZ] ;
