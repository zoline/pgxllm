"""
pgxllm.eval.bird
-----------------
BIRD Eval Harness — Core Pipeline과 완전 분리된 평가 모듈.

3자 비교:
  BaselineSQLEngine  (순수 LLM 1회, 파이프라인 없음)
  Core Pipeline      (전체 pgxllm 파이프라인)
  Gold SQL           (BIRD 정답)

EX (Execution Match) 기준으로 비교.

Usage::

    runner = BIRDEvalRunner(registry, config)
    results = runner.run_file("bird_dev.json", db_alias="bird_db", limit=100)
    runner.save_results(results, "results/bird_eval.json")
    runner.print_summary(results)
"""
from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from pgxllm.config import AppConfig
from pgxllm.core.llm.base import LLMProvider
from pgxllm.core.llm.factory import create_llm_provider
from pgxllm.core.models import PipelineRequest
from pgxllm.core.pipeline import PipelineRunner
from pgxllm.db.connections import ConnectionRegistry

log = logging.getLogger(__name__)


# ── Data classes ──────────────────────────────────────────────

@dataclass
class BIRDItem:
    """BIRD benchmark 단일 아이템."""
    question_id:  str
    question:     str
    db_id:        str
    gold_sql:     str
    hint:         str = ""
    difficulty:   str = "simple"


@dataclass
class EvalResult:
    """단일 질문에 대한 3자 평가 결과."""
    question_id:    str
    question:       str
    db_alias:       str
    gold_sql:       str
    hint:           str

    baseline_sql:   Optional[str] = None
    pgxllm_sql:     Optional[str] = None

    ex_baseline:    Optional[bool] = None   # gold vs baseline EX match
    ex_pgxllm:      Optional[bool] = None   # gold vs pgxllm  EX match

    baseline_error: Optional[str] = None
    pgxllm_error:   Optional[str] = None

    baseline_ms:    int = 0
    pgxllm_ms:      int = 0

    difficulty:     str = "simple"

    @property
    def pgxllm_wins(self) -> bool:
        """pgxllm 이 baseline 보다 낫거나 같은가."""
        return bool(self.ex_pgxllm) and (not self.ex_baseline or self.ex_pgxllm)

    def to_dict(self) -> dict:
        return {
            "question_id":   self.question_id,
            "question":      self.question,
            "db_alias":      self.db_alias,
            "difficulty":    self.difficulty,
            "hint":          self.hint,
            "gold_sql":      self.gold_sql,
            "baseline_sql":  self.baseline_sql,
            "pgxllm_sql":    self.pgxllm_sql,
            "ex_baseline":   self.ex_baseline,
            "ex_pgxllm":     self.ex_pgxllm,
            "baseline_error": self.baseline_error,
            "pgxllm_error":  self.pgxllm_error,
            "baseline_ms":   self.baseline_ms,
            "pgxllm_ms":     self.pgxllm_ms,
        }


@dataclass
class EvalSummary:
    """전체 평가 요약."""
    total:           int
    baseline_ex:     int
    pgxllm_ex:       int
    both_correct:    int
    only_pgxllm:     int
    only_baseline:   int
    both_wrong:      int
    avg_baseline_ms: float
    avg_pgxllm_ms:   float
    by_difficulty:   dict = field(default_factory=dict)

    @property
    def baseline_acc(self) -> float:
        return self.baseline_ex / self.total if self.total else 0.0

    @property
    def pgxllm_acc(self) -> float:
        return self.pgxllm_ex / self.total if self.total else 0.0

    def __str__(self) -> str:
        return (
            f"총 {self.total}개\n"
            f"  Baseline EX:  {self.baseline_ex}/{self.total} ({self.baseline_acc:.1%})\n"
            f"  pgxllm   EX:  {self.pgxllm_ex}/{self.total}  ({self.pgxllm_acc:.1%})\n"
            f"  둘 다 정답:   {self.both_correct}\n"
            f"  pgxllm만:    {self.only_pgxllm}\n"
            f"  Baseline만:  {self.only_baseline}\n"
            f"  둘 다 오답:  {self.both_wrong}\n"
            f"  평균 latency: baseline={self.avg_baseline_ms:.0f}ms  "
            f"pgxllm={self.avg_pgxllm_ms:.0f}ms"
        )


# ── Baseline SQL Engine ───────────────────────────────────────

class BaselineSQLEngine:
    """
    순수 LLM 1회 호출 — 파이프라인 없음, 캐시 없음, few-shot 없음.
    공정한 베이스라인 측정을 위한 최소 구현.
    """

    _SYSTEM = """\
당신은 PostgreSQL 전문가입니다.
주어진 스키마와 질문을 보고 SQL을 생성합니다.
반드시 다음 형식으로만 응답하세요:

```sql
<SQL here>
```
"""

    def __init__(self, llm: LLMProvider, registry: ConnectionRegistry):
        self._llm      = llm
        self._registry = registry

    def generate(self, item: BIRDItem, db_alias: str) -> tuple[str, Optional[str]]:
        """
        Returns (sql, error). error is None on success.
        """
        schema_text = self._load_schema_text(db_alias)
        user = (
            f"{schema_text}\n\n"
            f"### 질문\n{item.question}"
            + (f"\n\n(참고: {item.hint})" if item.hint else "")
        )
        try:
            resp = self._llm.complete(self._SYSTEM, user, temperature=0.0, max_tokens=1024)
            sql  = self._extract_sql(resp.text)
            return sql, None
        except Exception as e:
            return "", str(e)

    def _load_schema_text(self, db_alias: str) -> str:
        """schema_catalog 에서 간략한 스키마 텍스트 로드."""
        try:
            with self._registry.internal.connection() as conn:
                rows = conn.execute(
                    """
                    SELECT schema_name, table_name, column_name, data_type, is_pk, is_fk
                    FROM schema_catalog
                    WHERE db_alias = %s AND column_name IS NOT NULL
                    ORDER BY schema_name, table_name, column_name
                    LIMIT 500
                    """,
                    (db_alias,)
                )
            if not rows:
                return "(스키마 없음)"
            lines = ["### 스키마"]
            cur_tbl = None
            for r in rows:
                tbl = f"{r['schema_name']}.{r['table_name']}"
                if tbl != cur_tbl:
                    cur_tbl = tbl
                    lines.append(f"\n**{tbl}**")
                tags = []
                if r["is_pk"]: tags.append("PK")
                if r["is_fk"]: tags.append("FK")
                tag = f" [{','.join(tags)}]" if tags else ""
                lines.append(f"  - {r['column_name']} {r['data_type']}{tag}")
            return "\n".join(lines)
        except Exception as e:
            log.warning("baseline schema load error: %s", e)
            return "(스키마 로드 실패)"

    @staticmethod
    def _extract_sql(text: str) -> str:
        import re
        m = re.search(r"```(?:sql)?\s*\n?(.*?)```", text, re.DOTALL | re.IGNORECASE)
        if m:
            return m.group(1).strip()
        return text.strip()


# ── Execution Match ───────────────────────────────────────────

def execution_match(
    sql:       str,
    gold_sql:  str,
    db_alias:  str,
    registry:  ConnectionRegistry,
    timeout:   int = 30,
) -> tuple[bool, Optional[str]]:
    """
    두 SQL의 실행 결과를 비교한다 (EX).
    순서 무관 집합 비교.

    Returns:
        (match: bool, error: Optional[str])
    """
    if not sql or not sql.strip():
        return False, "빈 SQL"

    try:
        mgr = registry.target(db_alias)
    except KeyError:
        return False, f"DB '{db_alias}' not registered"

    def run_sql(s: str) -> tuple[list, Optional[str]]:
        try:
            with mgr.connection() as conn:
                rows = conn.execute(s)
            # Sort for order-insensitive comparison
            return sorted(
                [tuple(str(v) for v in row.values()) for row in rows]
            ), None
        except Exception as e:
            return [], str(e)

    gold_rows, gold_err = run_sql(gold_sql)
    if gold_err:
        return False, f"gold SQL error: {gold_err}"

    pred_rows, pred_err = run_sql(sql)
    if pred_err:
        return False, f"SQL error: {pred_err}"

    return gold_rows == pred_rows, None


# ── Main Runner ───────────────────────────────────────────────

class BIRDEvalRunner:
    """
    BIRD benchmark 평가 실행기.

    Core Pipeline 과 완전 분리 — Core Pipeline 은 변형 없이 그대로 호출.
    BIRD hint 는 Runner 레벨에서 question 에 추가하여 처리.
    """

    def __init__(self, registry: ConnectionRegistry, config: AppConfig):
        self._registry = registry
        self._config   = config
        self._llm      = create_llm_provider(config.llm)
        self._baseline = BaselineSQLEngine(self._llm, registry)
        self._pipeline = PipelineRunner(registry, config)

    def run(
        self,
        items:      list[BIRDItem],
        db_alias:   str,
        *,
        limit:      Optional[int] = None,
        skip_baseline: bool = False,
    ) -> list[EvalResult]:
        """
        items 목록을 평가한다.

        Args:
            items:          BIRDItem 목록
            db_alias:       평가할 target DB alias
            limit:          평가 수 제한 (None=전체)
            skip_baseline:  baseline 평가 skip (빠른 pgxllm 단독 평가)
        """
        if limit:
            items = items[:limit]

        results: list[EvalResult] = []

        for i, item in enumerate(items, 1):
            log.info("[Eval] %d/%d  %s", i, len(items), item.question[:60])

            result = EvalResult(
                question_id=item.question_id,
                question=item.question,
                db_alias=db_alias,
                gold_sql=item.gold_sql,
                hint=item.hint,
                difficulty=item.difficulty,
            )

            # ── Baseline ─────────────────────────────────────
            if not skip_baseline:
                t0 = time.perf_counter()
                sql, err = self._baseline.generate(item, db_alias)
                result.baseline_ms = int((time.perf_counter()-t0)*1000)
                result.baseline_sql = sql
                if err:
                    result.baseline_error = err
                    result.ex_baseline = False
                else:
                    match, match_err = execution_match(
                        sql, item.gold_sql, db_alias, self._registry
                    )
                    result.ex_baseline    = match
                    result.baseline_error = match_err

            # ── pgxllm Pipeline ───────────────────────────────
            # BIRD hint는 Runner 레벨에서 question에 추가
            augmented = item.question
            if item.hint:
                augmented = f"{item.question}\n(참고: {item.hint})"

            t0  = time.perf_counter()
            res = self._pipeline.run(PipelineRequest(
                question=augmented,
                db_alias=db_alias,
            ))
            result.pgxllm_ms  = int((time.perf_counter()-t0)*1000)
            result.pgxllm_sql = res.final_sql

            if res.error and not res.final_sql:
                result.pgxllm_error = res.error
                result.ex_pgxllm    = False
            else:
                match, match_err = execution_match(
                    res.final_sql or "", item.gold_sql,
                    db_alias, self._registry
                )
                result.ex_pgxllm    = match
                result.pgxllm_error = match_err

            results.append(result)

            # 진행 상황 로그
            b_ok = "✓" if result.ex_baseline else "✗"
            p_ok = "✓" if result.ex_pgxllm   else "✗"
            log.info(
                "[Eval] baseline=%s pgxllm=%s  (%dms/%dms)",
                b_ok, p_ok, result.baseline_ms, result.pgxllm_ms
            )

        return results

    def run_file(
        self,
        path:     str,
        db_alias: str,
        *,
        limit:    Optional[int] = None,
        skip_baseline: bool = False,
    ) -> list[EvalResult]:
        """JSON 파일에서 BIRD 아이템을 로드하여 평가한다."""
        items = self.load_bird_file(path)
        return self.run(items, db_alias, limit=limit, skip_baseline=skip_baseline)

    @staticmethod
    def load_bird_file(path: str) -> list[BIRDItem]:
        """
        BIRD JSON 형식 로드.
        공식 BIRD dev.json / train.json 형식을 지원한다.
        """
        with open(path) as f:
            data = json.load(f)

        items = []
        for i, d in enumerate(data):
            items.append(BIRDItem(
                question_id=str(d.get("question_id", i)),
                question=d.get("question", ""),
                db_id=d.get("db_id", ""),
                gold_sql=d.get("SQL", d.get("gold_sql", d.get("sql", ""))),
                hint=d.get("evidence", d.get("hint", "")),
                difficulty=d.get("difficulty", "simple"),
            ))
        return items

    @staticmethod
    def summarize(results: list[EvalResult]) -> EvalSummary:
        """EvalResult 목록에서 요약 통계를 계산한다."""
        total = len(results)
        if total == 0:
            return EvalSummary(0,0,0,0,0,0,0,0,0)

        baseline_ex   = sum(1 for r in results if r.ex_baseline)
        pgxllm_ex     = sum(1 for r in results if r.ex_pgxllm)
        both_correct  = sum(1 for r in results if r.ex_baseline and r.ex_pgxllm)
        only_pgxllm   = sum(1 for r in results if not r.ex_baseline and r.ex_pgxllm)
        only_baseline = sum(1 for r in results if r.ex_baseline and not r.ex_pgxllm)
        both_wrong    = sum(1 for r in results if not r.ex_baseline and not r.ex_pgxllm)

        avg_b_ms = sum(r.baseline_ms for r in results) / total
        avg_p_ms = sum(r.pgxllm_ms  for r in results) / total

        # By difficulty
        by_diff: dict[str, dict] = {}
        for r in results:
            d = r.difficulty
            if d not in by_diff:
                by_diff[d] = {"total":0,"baseline":0,"pgxllm":0}
            by_diff[d]["total"]    += 1
            by_diff[d]["baseline"] += int(bool(r.ex_baseline))
            by_diff[d]["pgxllm"]   += int(bool(r.ex_pgxllm))

        return EvalSummary(
            total=total,
            baseline_ex=baseline_ex,
            pgxllm_ex=pgxllm_ex,
            both_correct=both_correct,
            only_pgxllm=only_pgxllm,
            only_baseline=only_baseline,
            both_wrong=both_wrong,
            avg_baseline_ms=avg_b_ms,
            avg_pgxllm_ms=avg_p_ms,
            by_difficulty=by_diff,
        )

    @staticmethod
    def save_results(results: list[EvalResult], path: str) -> None:
        """결과를 JSON 파일로 저장한다."""
        output = {
            "summary": BIRDEvalRunner.summarize(results).__dict__,
            "results": [r.to_dict() for r in results],
        }
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(output, f, ensure_ascii=False, indent=2)
        log.info("Saved %d eval results to %s", len(results), path)
