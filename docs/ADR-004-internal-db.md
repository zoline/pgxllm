# ADR-004: pgxllm Internal DB 설계

**상태:** 확정 + 구현 완료  
**날짜:** 2025  
**관련 Phase:** Phase 3  
**구현 위치:** `src/pgxllm/db/schema.py`, `src/pgxllm/config.py`

---

## 결정

PostgreSQL 단일 DB로 모든 메타데이터를 관리한다. 외부 의존성(Neo4j, SQLite, Redis 등) 없음.

### 레이어 구조

```
pgxllm DB (PostgreSQL, 별도 인스턴스 가능)
│
├── [pgvector]        semantic layer
│   ├── question_embeddings      → Semantic Cache
│   ├── schema_embeddings        → Schema Linking (S2)
│   └── graph_paths (embedding)  → 경로 의미 벡터
│
├── [GIN / pg_trgm]   텍스트 검색 layer
│   ├── schema_catalog           → 테이블/컬럼 전문 검색
│   └── sql_cache                → SQL 패턴 검색
│
├── [relational]      메타데이터 layer
│   ├── db_registry              → 등록된 target DB 목록
│   ├── verified_queries         → few-shot 소스
│   ├── dialect_rules            → Rule Engine
│   ├── sql_patterns             → Dynamic Pattern
│   ├── pattern_applications     → 패턴 적용 이력
│   └── pipeline_logs            → 실행 이력
│
└── [graph]           관계 layer
    ├── graph_nodes              → 테이블 노드
    ├── graph_edges              → 테이블 관계 (FK + 비즈니스 관계)
    └── graph_paths              → 사전 계산 JOIN 경로
```

### Schema 격리

모든 pgxllm 객체는 `pgxllm` 스키마 안에 위치한다 (설정 가능).

```yaml
internal_db:
  schema: pgxllm   # SET search_path TO pgxllm, public
```

### pgvector 선택적 지원

pgvector 미설치 환경에서도 동작하도록 설계:
- pgvector 없으면 tfidf 백엔드로 자동 fallback
- `VECTOR_ALTER_SQL` 은 vector extension 확인 후 별도 실행

---

## 초기화

```python
registry = ConnectionRegistry(config)
registry.internal.initialize_schema()  # 최초 1회
```

`InternalDBManager.initialize_schema()` 가 모든 DDL을 순서대로 실행한다.
