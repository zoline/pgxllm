# ADR-009: Dialect Rule Engine

**상태:** 확정 + 구현 완료 ✅  
**구현 위치:** `src/pgxllm/intelligence/rule_engine.py`

---

## 배경

LLM이 자주 틀리는 패턴들이 있다. 대표 예시:

```sql
-- 컬럼명이 date인데 실제 타입은 TEXT (YYYYMM 형식)
-- Gold SQL:
WHERE SUBSTR(order_date, 1, 4) = '2023'

-- LLM 오류:
WHERE EXTRACT(YEAR FROM order_date) = 2023  -- 타입 오해

-- pgxllm 기존 오류:
WHERE order_date BETWEEN '202301' AND '202312'  -- rule 오적용
```

SYSTEM_PROMPT에 하드코딩하는 방식으로는 한계가 있다.

---

## 결정

### Rule 구조

```python
@dataclass
class DialectRule:
    rule_id:         str
    scope:           str   # global | dialect | db | table | column
    dialect:         str   # postgresql
    db_alias:        Optional[str]
    table_name:      Optional[str]
    column_name:     Optional[str]
    condition_json:  dict  # 자동 감지 조건 (data_type + pattern)
    forbidden_funcs: list[str]   # ["EXTRACT", "BETWEEN"]
    required_func:   Optional[str]  # "SUBSTR"
    instruction:     str   # LLM에 주입할 rule 텍스트
    example_bad:     str
    example_good:    str
    auto_detected:   bool  # True = db refresh 시 자동 감지
```

### YYYYMM TEXT 컬럼 예시

```yaml
rule_id: text_date_yyyymm
scope: column
condition:
  data_type: text
  pattern: "^\\d{6}$"    # 샘플값이 YYYYMM 패턴
forbidden_funcs: [EXTRACT, BETWEEN, "::date"]
required_func: SUBSTR
instruction: |
  {column}은 TEXT 타입의 YYYYMM 형식입니다.
  날짜 비교 시 SUBSTR을 사용하세요.
  EXTRACT, BETWEEN, ::date 캐스팅은 절대 사용하지 마세요.
example_bad:  "EXTRACT(YEAR FROM order_date) = 2023"
example_good: "SUBSTR(order_date, 1, 4) = '2023'"
```

### S3 Prompt Injection

해당 쿼리에 관련된 컬럼들의 rule만 동적으로 주입:

```
SYSTEM_PROMPT (base)
  + 자동 감지 rule (db refresh 시 생성)
  + 수동 등록 rule
  + schema + few-shot
```

### 자동 감지

`pgxllm db refresh` 시 샘플 데이터 패턴 분석:
- 샘플값이 `^\d{6}$` → YYYYMM TEXT 컬럼 → rule 자동 생성
- `auto_detected=True`로 저장

### CLI

```bash
pgxllm rules add --dbname mydb --table orders --column order_date \
    --instruction "YYYYMM TEXT 형식, SUBSTR 사용"
pgxllm rules list --dbname mydb
pgxllm rules test --dbname mydb --question "2023년 주문 건수는?"
pgxllm db refresh --dbname mydb --show-rules   # 자동 감지 확인
```

---
---

# ADR-010: Dynamic Pattern 자가 학습 시스템

**상태:** 확정 + 구현 완료 ✅  
**구현 위치:** `src/pgxllm/intelligence/pattern_engine.py`

---

## 배경

SQL 생성 오류 패턴이 있다. 예시:

```sql
-- 질문: "매출 상위 5개 부서별 월별 합계는?"

-- 오류 패턴: 전체에 LIMIT 적용
SELECT dept, month, SUM(sales)
FROM orders GROUP BY dept, month
LIMIT 5                        -- 상위 5개 부서가 아니라 5개 행

-- 올바른 패턴:
WITH top_depts AS (
    SELECT dept FROM orders GROUP BY dept ORDER BY SUM(sales) DESC LIMIT 5
)
SELECT o.dept, o.month, SUM(o.sales)
FROM orders o JOIN top_depts t ON o.dept = t.dept
GROUP BY o.dept, o.month
```

---

## 결정

### 패턴 등록 3가지 경로

```
1. [자동 학습]  verified SQL → SqlParser.analyze_structure() → 패턴 후보
2. [수동 등록]  CLI / Web UI
3. [실패 학습]  S4 실패 → pattern_applications → 패턴 승격
```

### Dynamic Detection (런타임)

```python
class DynamicPatternMatcher:
    def match(self, question: str, db_alias: str) -> list[SqlPattern]:
        patterns = self.store.load_active(db_alias)  # pgxllm DB에서 로드
        matched = []
        for pattern in patterns:
            score = self._score(question, pattern)
            if score >= pattern.threshold:
                matched.append((pattern, score))
        return sorted by confidence
```

패턴 키워드 예시: `["상위", "top", "최고"]` → Top-N 패턴 감지

### 학습 루프

```
정상 경로:
  verified SQL 축적
    → pgxllm patterns learn
    → SQL 구조 분석 (SqlParser.analyze_structure)
    → 패턴 후보 등록 (enabled=FALSE)
    → 검토 → 승인 → enabled=TRUE

실패 경로:
  S4 Validation 실패
    → pipeline_logs 기록
    → pgxllm patterns promote --from-log <id>
    → 실패 SQL 구조 분석 → 패턴 후보 생성
    → 검토 → 승인 → 다음 실행부터 즉시 적용
```

### CLI

```bash
pgxllm patterns learn --min-confidence 0.8
pgxllm patterns list
pgxllm patterns stats         # hit_count, success_rate 기준
pgxllm patterns test --question "부서별 매출 상위 5명의 월별 실적은?"
pgxllm patterns promote --from-log <execution_id>
```

### Top-N 패턴 분류

| 패턴 | 설명 | 올바른 SQL 구조 |
|---|---|---|
| `SIMPLE` | 단순 상위 N | `ORDER BY + LIMIT` |
| `THEN_DETAIL` | 상위 N 추출 후 상세 집계 | `inline view / CTE에 LIMIT` |
| `PER_GROUP` | 그룹별 상위 N | `RANK() + PARTITION BY` |
