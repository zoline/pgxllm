# ADR-003: ANTLR4 Parser 설계

**상태:** 확정 + 구현 완료  
**날짜:** 2025  
**관련 Phase:** Phase 2  
**구현 위치:** `src/pgxllm/parser/`  
**테스트:** `tests/parser/test_parser.py` — 39/39 통과 ✅

---

## 배경

pgxllm 전체에서 SQL 분석이 필요한 곳이 여러 곳에 걸쳐 있다:

- pg_stat_statements 분석 → JOIN 관계 추출 → graph_edges
- SQL 파일 등록 → @relation 어노테이션 파싱
- Reverse Inference → SQL 구조 패턴 분석
- S4 Validation → 생성된 SQL 구조 검증
- Dynamic Pattern 학습 → verified SQL 패턴 추출

이 모두를 하나의 일관된 파서로 처리해야 한다.

---

## 결정

### Grammar 선택

`antlr/grammars-v4` 레포지토리의 PostgreSQL grammar를 베이스로 한다.

```
src/pgxllm/parser/grammar/
  PostgreSQLLexer.g4    ← grammars-v4 기반
  PostgreSQLParser.g4   ← grammars-v4 기반
```

> **주의:** grammars-v4 PostgreSQL grammar는 비관용적 ANTLR 구조를 가지고 있어
> Visitor 작성 시 null 체크 등 추가 처리가 필요하다.
> ANTLR4 jar로 generate: `make generate-parser`

### Visitor 계층 설계

```
BaseVisitor (공통 기반)
  │  alias 해석, CTE/Subquery scope 진입·퇴출, max_depth 관리
  │
  ├── RelationExtractVisitor    ← pg_stat_statements, SQL 파일 등록
  │     명시적 JOIN / implicit JOIN / CTE / Subquery 재귀
  │
  ├── StructureAnalysisVisitor  ← Reverse Inference, Pattern 학습
  │     Top-N 패턴, LIMIT 위치, 집계 구조, 함수 사용 패턴
  │
  └── ValidationVisitor         ← S4 Validation
        구조 규칙 위반, Dialect Rule 위반 감지

SqlParser (facade) ← 파이프라인에서 항상 이것만 호출
```

### SqlParser 공개 API (facade)

```python
parser = SqlParser()

# 1. 관계 추출 (pg_stat_statements, SQL 파일)
relations: list[ExtractedRelation] = parser.extract_relations(sql)

# 2. 구조 분석 (Pattern 학습, Reverse Inference)
structure: SqlStructure = parser.analyze_structure(sql)

# 3. 유효성 검사 (S4 Validation)
result: ValidationResult = parser.validate(sql, rules=dialect_rules)

# 4. @relation 어노테이션 추출 (SQL 파일 등록)
annotations: list[dict] = parser.extract_annotations(sql)

# 5. SQL 정규화 (캐시 키 생성)
normalized: str = parser.normalize(sql)
```

---

## 주요 추출 데이터 구조

### ExtractedRelation

```python
@dataclass
class ExtractedRelation:
    from_table:  str
    from_column: str
    to_table:    str
    to_column:   str
    join_type:   JoinType   # INNER/LEFT/RIGHT/FULL/CROSS/IMPLICIT
    source:      JoinSource # explicit_join/implicit_join/cte/subquery
    confidence:  float      # 등장 횟수 기반
```

### SqlStructure

```python
@dataclass
class SqlStructure:
    tables:          list[str]
    has_group_by:    bool
    has_window_func: bool
    window_funcs:    list[str]        # RANK, ROW_NUMBER, DENSE_RANK ...
    limit_position:  LimitPosition    # final/inline_view/cte/none
    top_n_pattern:   TopNPattern      # simple/then_detail/per_group/none
    top_n_value:     Optional[int]
    date_funcs:      list[DateFuncUsage]  # EXTRACT/SUBSTR/BETWEEN 사용
    has_cte:         bool
    subquery_depth:  int
    join_count:      int
```

---

## pg_stat_statements 특별 처리

pg_stat_statements는 리터럴을 `$1`, `$2`로 대체한다.
파서는 이를 `PARAM` 토큰으로 인식하여 정상적으로 파싱한다.

```sql
-- pg_stat_statements 출력 예시
SELECT o.id, c.name FROM orders o
JOIN customers c ON o.customer_id = c.id
WHERE o.status = $1 AND c.region = $2
```

---

## 발견된 주요 버그 (참고)

구현 중 발견되어 수정된 버그들:

| 버그 | 원인 | 수정 |
|---|---|---|
| 멀티컬럼 SELECT 파싱 실패 | `_match_kw("COMMA")` — COMMA는 KEYWORD 타입이 아님 | `_match_comma()` 헬퍼 추가 |
| `COUNT(*)`·집계 함수 파싱 실패 | `*`가 `TT.OP`로 토크나이징 (OP 패턴이 STAR보다 먼저) | tokenizer regex 순서 수정 |
| USING 절 relation 미추출 | `on_expr=None`이면 `visit_join_condition` 미호출 | USING 절도 hook 호출 |
| self-join 미감지 | `lt != rt` 조건이 동일 테이블 alias 차이를 차단 | 같은 alias일 때만 제외 |
| IN (subquery) `has_subquery=False` | `InExpr` 경로에서 `on_subquery_ref` 미호출 | `on_inline_subquery()` hook 추가 |
