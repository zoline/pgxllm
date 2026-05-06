# ADR-002: Core Pipeline 4단계 설계

**상태:** 확정  
**날짜:** 2025  
**관련 Phase:** Phase 1  
**구현 위치:** `src/pgxllm/core/`

---

## 결정

BIRD 종속성 제거 후 Core Pipeline을 4단계로 단순화한다.

```
S1. Question Understanding
S2. Schema Linking
S3. SQL Generation
S4. Validation & Correction
```

---

## 각 단계 상세

### S1. Question Understanding

```
입력: 자연어 질문
출력: 패턴 감지 결과 + 관련 테이블 후보

처리:
  - DynamicPatternMatcher → Top-N, GROUP BY 등 패턴 감지
  - pgvector → 질문 임베딩으로 관련 테이블 후보 추출
```

**결정 사항:** BIRD hint 의존 없이 pg_catalog + pgvector만으로 동작.

### S2. Schema Linking

```
입력: 질문 + 테이블 후보
출력: LinkedSchema (테이블 + FK + JOIN hint)

처리:
  - graph_paths SELECT → JOIN 경로 탐색 (사전 계산, 런타임 비용 없음)
  - Dialect Rule 적용 대상 컬럼 확인
  - LLM에 주입할 JOIN hint 텍스트 생성
```

**결정 사항:** GraphStore를 통해 FK 너머의 비즈니스 관계도 활용 (→ ADR-006).

### S3. SQL Generation

```
입력: schema + evidence + few-shot + rules + patterns
출력: SQLCandidate[]

처리:
  - LLM provider-agnostic (Qwen2 / Ollama / vLLM / API)
  - SYSTEM_PROMPT (base) + dialect_rules + dynamic_patterns → 동적 주입
  - verified few-shot (execution_ok=True 결과만)
```

**결정 사항:** LLM은 provider-agnostic 추상화 레이어를 통해 호환.

### S4. Validation & Correction

```
입력: SQLCandidate[]
출력: 검증된 최종 SQL

처리:
  - PREPARE validate (실행 가능 여부)
  - ValidationVisitor → 구조 규칙 위반 감지
  - LLM self-correction loop (hard limit 적용)
  - 실패 시 pattern_applications 기록 → 자동 학습 트리거
```

**결정 사항:** 무한루프 방지를 위해 `max_correction_loops` (기본값 3) 적용.

---

## 데이터 흐름

```
PipelineContext (mutable)
  question: str
  db_alias: str
  schema: LinkedSchema      ← S2 이후 채워짐
  candidates: list[SQL]     ← S3 이후 채워짐
  final_sql: str            ← S4 이후 채워짐

→ PipelineResult (immutable)
  final_sql, explanation, stage_logs
```

**결정 사항:** 각 Stage 간 결합도를 낮추기 위해 mutable context → immutable result 패턴 사용.

---

## SemanticCache 통합

```
PipelineRunner.run(question):
  cache_key = SqlParser.normalize(question)
  if cache.hit(cache_key):
      return cache.get(cache_key)   # 파이프라인 skip
  
  result = pipeline.run(question)
  
  if result.execution_ok:
      cache.set(cache_key, result)  # execution_ok=True만 저장
  
  return result
```

**결정 사항:** execution_ok=True만 캐시에 저장 (정답 여부는 별도 EX 평가로 결정).

---

## 미결 사항

- S3 LLM provider 추상화 인터페이스 상세 설계 (Phase 1 구현 시)
- S2 Schema Linking 시 multi-DB cross-schema 경로 처리 방식
