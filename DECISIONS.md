# pgxllm — 설계 결정 전체 요약

> 자세한 내용은 `docs/ADR-*.md` 참조  
> 이 파일은 프로젝트 루트에서 빠르게 전체 결정 사항을 조회할 때 사용

---

## 1. 재설계 배경

기존 코드에서 BIRD benchmark 평가 로직이 Core Pipeline에 침투하여:
- few-shot 오염 (`execution_ok=True`만으로는 정확성 보장 불가)
- Baseline과 pgxllm 코드가 뒤섞여 공정한 비교 불가능
- 설계 전체가 BIRD 종속으로 변질

→ **관심사 분리:** Core Pipeline / BIRD Eval Harness 완전 분리

---

## 2. 최종 목표

```
pgxllm = 독립적으로 운영 가능한 PostgreSQL Text-to-SQL 시스템
         BIRD = 외부에서 Core Pipeline을 호출하는 품질 검증 도구
```

---

## 3. 확정된 설계 결정 요약

### 3.1 Core Pipeline — 4단계 (구현 완료 ✅)

| 단계 | 역할 | 핵심 결정 |
|---|---|---|
| S1 Question Understanding | 질문 분석 + 패턴 감지 | DynamicPatternMatcher + pg_trgm |
| S2 Schema Linking | JOIN 경로 탐색 | GraphStore (graph_paths 사전 계산) |
| S3 SQL Generation | LLM으로 SQL 생성 | Provider-agnostic + 동적 rule 주입 |
| S4 Validation | 구조 검증 + 자기 수정 | ValidationVisitor + hard limit |

### 3.2 ANTLR4 Parser (구현 완료 ✅)

- grammars-v4 PostgreSQL g4 베이스
- 3개 Visitor: RelationExtract / StructureAnalysis / Validation
- `SqlParser` facade — 파이프라인에서 항상 이것만 호출
- pg_stat_statements `$1/$2` param 정상 처리

### 3.3 Multi-DB 설계 (구현 완료 ✅)

```
internal DB (1개)   ← pgxllm 메타데이터 전용, 독립 인스턴스 가능
target DB (N개)     ← 각각 다른 host:port, 독립 connection pool

schema 범위: include (명시 목록) | exclude (전체 - blacklist)
cross-DB: mydb.public.orders → warehouse.dw.customers 허용
```

### 3.4 Internal DB 레이어 (구현 완료 ✅)

```
pgvector   → Semantic Cache, Schema Linking, Few-shot 검색
GIN/trgm   → 스키마 전문 검색
relational → db_registry, verified_queries, dialect_rules, sql_patterns, pipeline_logs
graph      → graph_nodes, graph_edges, graph_paths (사전 계산)
```

### 3.5 GraphStore (구현 완료 ✅)

- 기본: PostgreSQL (BFS 사전 계산 → graph_paths SELECT)
- 추후 교체 가능: AGE / Neo4j (인터페이스만 동일하면 됨)
- pgvector + graph = "어떤 테이블?" + "어떻게 연결?"

### 3.6 Graph 관계 수집 (구현 완료 ✅)

3가지 경로로 FK 너머의 비즈니스 관계 학습:
1. `pg_stat_statements` 자동 분석
2. SQL 파일 `@relation` 어노테이션 등록
3. 수동 등록 (Web UI / CLI)
+ Reverse Inference (간접 관계 자동 추론)

### 3.7 샘플 데이터 추출 (구현 완료 ✅)

- 코드성 컬럼 (n_distinct ≤ 50, text/varchar) + Dimension 테이블
- Blacklist: 테이블 / 컬럼 / 패턴(glob) 3단계
- 갱신: 자동 스케줄 없음, CLI로 명시적 실행 (`pgxllm db refresh`)

### 3.8 Dialect Rule Engine (구현 완료 ✅)

- scope: global | dialect | db | table | column
- 자동 감지: db refresh 시 샘플 데이터 패턴 분석
- S3 prompt injection: 관련 컬럼 rule만 동적 주입
- 예시: YYYYMM TEXT 컬럼 → EXTRACT/BETWEEN 금지, SUBSTR 사용

### 3.9 Dynamic Pattern 자가 학습 (구현 완료 ✅)

- 3가지 경로: 자동 학습 / 수동 등록 / 실패 학습
- Top-N 패턴 감지: SIMPLE / THEN_DETAIL / PER_GROUP
- S4 실패 → `pgxllm patterns promote` → 패턴 승격

### 3.10 BIRD Eval Harness (구현 완료 ✅)

- Baseline (순수 LLM) vs pgxllm (전체 파이프라인) vs Gold 3자 비교
- Core Pipeline은 BIRD를 전혀 알지 못함
- `pgxllm eval --dataset bird --output results.json`

---

## 4. Phase 완료 현황

| Phase | 내용 | 상태 | 테스트 |
|---|---|---|---|
| 2 | ANTLR4 Parser | ✅ 완료 | 39 |
| 3 | Internal DB & Multi-DB | ✅ 완료 | 31 |
| 4 | DB 등록 & 메타데이터 수집 | ✅ 완료 | 29 |
| 5 | Graph 관계 수집 & 학습 | ✅ 완료 | 35 |
| 6 | Rule Engine & Dynamic Pattern | ✅ 완료 | 35 |
| 1 | Core Pipeline | ✅ 완료 | 37 |
| 7 | BIRD Eval Harness | ✅ 완료 | 23 |
| 8 | Web UI & CLI | ✅ 완료 | — |

**전체 테스트: 194개 (전부 통과)**

---

## 5. 핵심 파일 위치

```
src/pgxllm/
  config.py                        AppConfig, TargetDBConfig, InternalDBConfig
  cli.py                           CLI (db register/refresh/list, web, eval)
  db/connections.py                ConnectionRegistry, PgPool, TableAddress
  db/schema.py                     Internal DB DDL
  parser/facade.py                 SqlParser (항상 이것만 사용)
  parser/models.py                 ExtractedRelation, SqlStructure, ValidationResult
  parser/relation_visitor.py       RelationExtractVisitor
  parser/structure_visitor.py      StructureAnalysisVisitor
  parser/validation_visitor.py     ValidationVisitor + DialectRule
  graph/base.py                    GraphStore ABC
  graph/postgresql.py              PostgreSQL 기반 GraphStore (기본)
  graph/age.py                     Apache AGE GraphStore
  graph/neo4j.py                   Neo4j GraphStore
  intelligence/db_registry.py      Target DB 등록 관리
  intelligence/schema_catalog.py   스키마 수집
  intelligence/refresh.py          pg_catalog 스캔 오케스트레이션
  intelligence/sample_extractor.py 컬럼 샘플 추출
  intelligence/relation_collector.py 관계 수집 (pg_stat / 추론 / 수동)
  intelligence/rule_engine.py      Dialect Rule Engine
  intelligence/dialect_rule_detector.py 규칙 자동 감지
  intelligence/pattern_engine.py   Dynamic Pattern 매칭
  core/pipeline.py                 PipelineRunner (S1→S2→S3→S4)
  core/s1_understanding.py         S1: 질문 이해 · 패턴 감지
  core/s2_schema_linking.py        S2: 스키마 링킹 · JOIN 경로
  core/s3_generation.py            S3: LLM SQL 생성
  core/s4_validation.py            S4: SQL 검증 · 자동 보정
  core/llm/                        LLM Provider 추상화 (ollama/vllm/anthropic/watsonx 등)
  cache/tfidf_cache.py             TF-IDF 기반 Semantic Cache
  eval/                            BIRD Eval Harness
  web/app.py                       FastAPI REST API (1,700+ lines)

configs/default.yaml               전체 설정 (env var 확장 지원)
frontend/                          React + Vite Web UI
docs/ADR-*.md                      각 결정의 상세 배경 및 이유
```

---

## 6. 변경 이력

| 날짜 | 내용 |
|---|---|
| 2025 | 초기 설계 협의 완료 (Phase 2, 3 구현) |
| 2026-04 | 전체 Phase 구현 완료 (194 tests) |
| 2026-04 | Query Plan 내보내기: DB 저장 제거 → JSON/TEXT/PNG/PPT 파일 저장으로 변경 |
