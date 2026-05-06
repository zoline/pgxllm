# ADR-011: BIRD Eval Harness 완전 분리

**상태:** 확정 + 구현 완료 ✅  
**구현 위치:** `src/pgxllm/eval/`

---

## 결정

BIRD 평가 로직은 Core Pipeline에 침투하지 않는다.  
외부에서 Core Pipeline을 호출하는 독립 harness로 구현한다.

### 구조

```
bird_item (question + db + gold_sql + hint)
      │
      ├─── BaselineSQLEngine.generate()
      │      Schema + Qwen2 direct generation
      │      파이프라인 없음 / 캐시 없음 / few-shot 없음
      │      순수 LLM 1회 호출
      │      → baseline_sql
      │
      └─── Core Pipeline.run()
             완전한 pgxllm 파이프라인 (변형 없이)
             → pgxllm_sql
      │
      ↓
EvalResult:
  ex_baseline = execute_match(baseline_sql, gold_sql)
  ex_pgxllm   = execute_match(pgxllm_sql,   gold_sql)
  → 공정한 3자 비교
```

### BaselineSQLEngine

```python
class BaselineSQLEngine:
    """Schema + Qwen2 direct generation. 파이프라인 없음."""
    def generate(self, question: str, schema: LinkedSchema) -> str:
        # LLM 1회 호출, 캐시/few-shot/rule 없음
        # 순수 LLM 능력 측정용 베이스라인
```

### BIRD hint 처리

BIRD hint는 Core Pipeline이 아닌 Harness에서 처리한다:

```python
class BIRDEvalRunner:
    def run(self, bird_item):
        # hint를 question에 자연스럽게 합성
        augmented = f"{bird_item.question}\n(참고: {bird_item.hint})"
        return core_pipeline.run(augmented, bird_item.db)
```

### CLI

```bash
pgxllm eval --dataset bird --output results/bird_eval.json
pgxllm eval --dataset bird --split dev --limit 100
```

---
---

# ADR-012: Phase 작업 순서 결정

**상태:** 확정  
**날짜:** 2025

---

## 결정

의존성을 고려한 작업 순서:

```
Phase 2  ANTLR4 Parser         ← 완료 ✅ (모든 Phase의 기반)
Phase 3  Internal DB & Multi-DB ← 완료 ✅ (저장소 스키마 확정)
Phase 4  DB 등록 & 수집         ← 진행 예정 ▶
Phase 5  Graph 관계 수집        ← Phase 2, 4 완료 후
Phase 6  Rule & Pattern         ← Phase 2, 4 완료 후
Phase 1  Core Pipeline          ← Phase 2~6 완료 후 통합
Phase 7  BIRD Eval Harness      ← Phase 1 완료 후
Phase 8  Web UI & CLI           ← 전체 진행하며 병행
```

### 순서 결정 이유

- **Phase 2를 먼저:** 파서가 없으면 관계 추출, 패턴 학습, 검증이 모두 불가능
- **Phase 3을 그 다음:** DB 스키마 확정 없이는 어떤 데이터도 저장할 수 없음
- **Phase 1을 나중에:** Core Pipeline은 2~6의 모든 컴포넌트를 조립하는 단계
- **Phase 8을 병행:** Web UI는 각 Phase 완성된 기능부터 순차적으로 노출

---

## 현재 구현 상태

| Phase | 구현 상태 | 테스트 |
|---|---|---|
| Phase 2 (ANTLR4 Parser) | ✅ 완료 | 39/39 |
| Phase 3 (config/db) | ✅ 완료 | 31/31 |
| Phase 3 (GraphStore) | 🔄 설계 완료, 구현 예정 | - |
| Phase 4 | 🔄 설계 완료, 구현 예정 | - |
| Phase 5 | 🔄 설계 완료, 구현 예정 | - |
| Phase 6 | 🔄 설계 완료, 구현 예정 | - |
| Phase 1 | 🔄 설계 완료, 구현 예정 | - |
| Phase 7 | 🔄 설계 완료, 구현 예정 | - |
| Phase 8 | 🔄 Web UI 틀 존재, 연결 예정 | - |
