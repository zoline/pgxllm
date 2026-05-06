# ADR-007: 샘플 데이터 추출 전략

**상태:** 확정 + 구현 완료 ✅  
**구현 위치:** `src/pgxllm/intelligence/sample_extractor.py`

---

## 결정

### 추출 대상 선정 기준

`pg_stats` + `pg_catalog` 기반으로 자동 분류:

```python
# 코드성 컬럼: n_distinct 낮음 + text/varchar
is_code = (
    data_type in ('text', 'varchar', 'char')
    and n_distinct <= 50      # configs/default.yaml: code_distinct_max
    and n_distinct > 1
)

# Dimension 테이블: FK 참조 많이 받음
is_dimension = (
    incoming_fk_count >= 2    # dimension_fk_min
    and row_count <= 1_000_000  # dimension_row_max
)
```

### Blacklist

테이블 / 컬럼 / 패턴(glob) 3단계 제어:

```yaml
sample_data:
  blacklist:
    tables:   [audit_logs, session_tokens]
    columns:  [users.password_hash]
    patterns: ["*_hash", "*_token", "*_password", "*_secret", "*_key"]
```

### 갱신 방법

자동 스케줄 없음. CLI로 명시적 실행:

```bash
pgxllm db refresh --dbname mydb           # 전체
pgxllm db refresh --dbname mydb --table products  # 특정 테이블만
pgxllm db refresh --all                   # 등록된 전체 DB
```

---
---

# ADR-008: Graph 관계 수집 3가지 경로

**상태:** 확정, 구현 예정 (Phase 5)  
**구현 위치:** `src/pgxllm/intelligence/relation_collector.py`

---

## 결정

FK만으로는 실제 비즈니스 JOIN 관계를 모두 표현할 수 없다.  
3가지 경로로 보완한다.

### 1. pg_stat_statements 자동 분석

```bash
pgxllm db analyze --dbname mydb --top 100
```

- `SqlParser.extract_relations(sql)` 로 JOIN 조건 추출
- `call_count` 기반 confidence 계산
- Web UI에서 관계명 설정 후 승인

### 2. SQL 파일 등록

```sql
-- @relation orders -> customers  : 주문-고객
-- @relation orders -> regions    : 주문-지역
-- @description 영업 분석용 기준 쿼리

SELECT c.name, r.region_name, SUM(o.amount)
FROM orders o
JOIN customers c ON o.customer_id = c.id
JOIN regions r ON o.region_code = r.code
GROUP BY c.name, r.region_name
```

```bash
pgxllm relations import --file queries/sales.sql
pgxllm relations import --dir queries/ --recursive
pgxllm relations import --file sales.sql --dry-run  # 미리보기
```

### 3. Reverse Inference

```bash
pgxllm relations infer --dbname mydb --min-confidence 0.7
pgxllm relations infer --dbname mydb --auto-approve 0.95
```

추론 방식:
- **JOIN 빈도 분석:** A→B 50회 + B→C 30회 → A→C 간접 관계 추론
- **컬럼명 유사도:** `orders.region_code` ↔ `regions.code` (이름 유사 + 타입 일치 + 같이 JOIN됨)
- **공통 필터 패턴:** 여러 쿼리에서 같은 WHERE 조건으로 묶이는 컬럼 쌍

### relation_type 구분

```
fk        ← pg_catalog FK에서 자동 추출
analyzed  ← pg_stat_statements 분석
inferred  ← Reverse Inference
manual    ← 수동 등록
```
