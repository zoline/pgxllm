# ADR-001: 재설계 배경 및 목표

**상태:** 확정  
**날짜:** 2025  
**관련 Phase:** 전체

---

## 배경

기존 pgxllm은 BIRD benchmark 평가와 실제 Text-to-SQL 서비스 목적이 하나의 파이프라인 안에 뒤섞인 구조였다.

### 원래 의도했던 구조

```
BIRD question
    ├─ [Baseline]  LLM (schema + Qwen2) → SQL  ┐
    ├─ [pgxllm]   Pipeline              → SQL  ├─ EX 비교 → gold SQL
    └─ [Gold]     BIRD gold SQL                ┘
```

세 가지가 공정하게 비교되어야 했다.

### 실제로 발생한 문제

BIRD 평가 로직이 Pipeline 내부로 침투하면서:

| 설계 결정 | BIRD 평가를 위한 것 | 실서비스 부작용 |
|---|---|---|
| `execution_ok=True` few-shot 기준 | BIRD 정답 검증 우회 | 틀린 SQL이 few-shot으로 누적 |
| `EvidenceOnlyRetriever` | BIRD hint 활용 | 실제 DB엔 hint가 없음 |
| Baseline pipeline (`cache=None`) | 공정한 벤치마크 비교 | 서비스 경로와 코드 분기 복잡 |
| `FullSchemaLinker` vs `CandidateSchemaLinker` | BIRD 스키마 다양성 대응 | 불필요한 분기, 품질 저하 |

**결과:** Baseline과 pgxllm이 코드 레벨에서 뒤섞여 공정한 비교도 불가능하고 정확도도 낮아짐.

---

## 결정

### 1. 관심사 분리 (핵심)

```
[Core Pipeline]  ← 순수 Text-to-SQL, 서비스 목적
      ↑
[Eval Harness]   ← BIRD 평가는 외부에서 Core Pipeline을 호출하는 wrapper
```

### 2. 최종 목표 정의

**pgxllm = 독립적으로 운영 가능한 PostgreSQL Text-to-SQL 시스템**

```
[Any PostgreSQL DB]
      ↓
Natural Language Question
      ↓
┌─────────────────────┐
│   Core Pipeline     │  (DB-agnostic)
└─────────────────────┘
      ↓
SQL + Explanation
      ↓
Web UI / API / CLI
```

BIRD는 품질 검증 도구일 뿐, 시스템 본체가 아님.

### 3. 독립 운영을 위한 핵심 요건

1. **DB 연결만 있으면 동작** — pg_catalog 기반 자동 schema 탐색
2. **외부 서비스 의존 최소화** — LLM 엔드포인트만 설정하면 동작
3. **점진적 학습** — 사용할수록 verified cache 축적 → few-shot 품질 향상
4. **BIRD는 플러그인** — `pgxllm eval --dataset bird` 형태

---

## 결과

- Core Pipeline에서 BIRD 종속 코드 전면 제거
- BIRD Eval Harness를 `src/pgxllm/eval/` 에 완전 분리 (→ ADR-011)
- 파이프라인은 4단계로 단순화 (→ ADR-002)
