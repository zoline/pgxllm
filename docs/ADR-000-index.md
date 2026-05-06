# pgxllm 설계 결정 문서 (ADR) 인덱스

> **ADR** = Architecture Decision Record  
> 이 폴더는 pgxllm 설계 과정에서 협의된 모든 결정 사항을 기록합니다.  
> 코드를 이해하기 전에 이 문서들을 먼저 읽으면 Why를 알 수 있습니다.

---

## 문서 목록

| 번호 | 제목 | 상태 | 관련 Phase |
|---|---|---|---|
| [ADR-001](ADR-001-redesign-motivation.md) | 재설계 배경 및 목표 | ✅ 확정 | 전체 |
| [ADR-002](ADR-002-pipeline-design.md) | Core Pipeline 4단계 설계 | ✅ 확정 | Phase 1 |
| [ADR-003](ADR-003-antlr4-parser.md) | ANTLR4 Parser 설계 | ✅ 확정 | Phase 2 |
| [ADR-004](ADR-004-internal-db.md) | pgxllm Internal DB 설계 | ✅ 확정 | Phase 3 |
| [ADR-005](ADR-005-multi-db.md) | Multi-DB / Multi-Schema 설계 | ✅ 확정 | Phase 3 |
| [ADR-006](ADR-006-graph-store.md) | GraphStore 설계 및 백엔드 선택 | ✅ 확정 | Phase 3/5 |
| [ADR-007](ADR-007-sample-data.md) | 샘플 데이터 추출 전략 | ✅ 확정 | Phase 4 |
| [ADR-008](ADR-008-graph-relations.md) | Graph 관계 수집 3가지 경로 | ✅ 확정 | Phase 5 |
| [ADR-009](ADR-009-rule-engine.md) | Dialect Rule Engine | ✅ 확정 | Phase 6 |
| [ADR-010](ADR-010-dynamic-pattern.md) | Dynamic Pattern 자가 학습 | ✅ 확정 | Phase 6 |
| [ADR-011](ADR-011-bird-eval.md) | BIRD Eval Harness 분리 | ✅ 확정 | Phase 7 |
| [ADR-012](ADR-012-phase-order.md) | Phase 작업 순서 결정 | ✅ 확정 | 전체 |

---

## 핵심 원칙 요약

```
1. pgxllm = 독립 운영 가능한 PostgreSQL Text-to-SQL 시스템
2. BIRD는 품질 검증 도구 — Core Pipeline과 완전 분리
3. DB-agnostic: 어떤 PostgreSQL DB든 연결 즉시 동작
4. 사용할수록 정확해지는 자가 학습 구조
5. PostgreSQL 단일 스택 (외부 의존성 최소화)
```

---

## 읽는 순서 (신규 팀원 권장)

1. ADR-001 (왜 재설계했는가)
2. ADR-002 (파이프라인 전체 그림)
3. ADR-005 (Multi-DB 설계 — 가장 중요한 설정)
4. ADR-003 (파서 — 모든 분석의 기반)
5. ADR-006 (GraphStore — 테이블 관계 핵심)
6. ADR-009, ADR-010 (정확도 향상 메커니즘)
7. ADR-011 (BIRD 평가 방법)
