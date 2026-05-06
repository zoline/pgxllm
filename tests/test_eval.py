"""
tests/test_eval.py
------------------
Phase 7 BIRD Eval Harness unit tests.
"""
from __future__ import annotations
import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from pgxllm.config import AppConfig, InternalDBConfig, LLMConfig, TargetDBConfig
from pgxllm.db.connections import ConnectionRegistry
from pgxllm.eval.bird import (
    BIRDEvalRunner, BIRDItem, BaselineSQLEngine,
    EvalResult, EvalSummary, execution_match,
)


def make_config():
    return AppConfig(
        internal_db=InternalDBConfig(host="localhost", dbname="pgxllm"),
        target_dbs=[TargetDBConfig(alias="testdb", host="h", user="u", dbname="testdb")],
        llm=LLMConfig(provider="ollama", model="test"),
    )

def make_registry():
    return ConnectionRegistry(make_config())

def mock_conn_ctx(execute_returns=None, execute_one_returns=None):
    conn = MagicMock()
    conn.execute.return_value = execute_returns or []
    conn.execute_one.return_value = execute_one_returns
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=conn)
    ctx.__exit__  = MagicMock(return_value=False)
    return ctx, conn


# ══════════════════════════════════════════════════════════════
# BIRDItem
# ══════════════════════════════════════════════════════════════

class TestBIRDItem:

    def test_load_bird_file(self, tmp_path):
        data = [
            {"question_id": 1, "question": "주문 수는?", "db_id": "testdb",
             "SQL": "SELECT COUNT(*) FROM orders", "evidence": "힌트", "difficulty": "simple"},
        ]
        f = tmp_path / "dev.json"
        f.write_text(json.dumps(data))
        items = BIRDEvalRunner.load_bird_file(str(f))
        assert len(items) == 1
        assert items[0].question == "주문 수는?"
        assert items[0].gold_sql == "SELECT COUNT(*) FROM orders"
        assert items[0].hint    == "힌트"

    def test_load_bird_file_alternative_keys(self, tmp_path):
        """gold_sql 키 변형 지원."""
        data = [{"question_id": 1, "question": "q", "db_id": "db",
                 "gold_sql": "SELECT 1", "difficulty": "hard"}]
        f = tmp_path / "dev.json"
        f.write_text(json.dumps(data))
        items = BIRDEvalRunner.load_bird_file(str(f))
        assert items[0].gold_sql == "SELECT 1"
        assert items[0].difficulty == "hard"


# ══════════════════════════════════════════════════════════════
# BaselineSQLEngine
# ══════════════════════════════════════════════════════════════

class TestBaselineSQLEngine:

    def _make_engine(self):
        from pgxllm.core.llm.base import LLMProvider, LLMResponse
        registry = make_registry()
        llm      = MagicMock(spec=LLMProvider)
        llm.complete.return_value = LLMResponse(
            text="```sql\nSELECT COUNT(*) FROM orders;\n```",
            model="test"
        )
        return BaselineSQLEngine(llm, registry), registry, llm

    def test_generate_calls_llm(self):
        engine, registry, llm = self._make_engine()
        item = BIRDItem("1", "주문 수는?", "testdb", "SELECT COUNT(*) FROM orders")
        ctx, _ = mock_conn_ctx(execute_returns=[])
        with patch.object(registry.internal, "connection", return_value=ctx):
            sql, err = engine.generate(item, "testdb")
        llm.complete.assert_called_once()
        assert err is None
        assert "SELECT" in sql.upper()

    def test_generate_with_hint(self):
        engine, registry, llm = self._make_engine()
        item = BIRDItem("1", "주문 수는?", "testdb", "SELECT COUNT(*) FROM orders", hint="orders 테이블 사용")
        ctx, _ = mock_conn_ctx(execute_returns=[])
        with patch.object(registry.internal, "connection", return_value=ctx):
            engine.generate(item, "testdb")
        _, kwargs = llm.complete.call_args
        user_arg = llm.complete.call_args[0][1]  # positional user arg
        assert "orders 테이블 사용" in user_arg

    def test_extract_sql_with_block(self):
        sql = BaselineSQLEngine._extract_sql("```sql\nSELECT 1;\n```")
        assert sql == "SELECT 1;"

    def test_extract_sql_without_block(self):
        sql = BaselineSQLEngine._extract_sql("SELECT COUNT(*) FROM orders")
        assert "SELECT" in sql.upper()

    def test_generate_error_handling(self):
        from pgxllm.core.llm.base import LLMProvider
        registry = make_registry()
        llm      = MagicMock(spec=LLMProvider)
        llm.complete.side_effect = RuntimeError("connection refused")
        engine   = BaselineSQLEngine(llm, registry)
        item     = BIRDItem("1", "q", "db", "SELECT 1")
        ctx, _   = mock_conn_ctx(execute_returns=[])
        with patch.object(registry.internal, "connection", return_value=ctx):
            sql, err = engine.generate(item, "testdb")
        assert err is not None
        assert sql == ""


# ══════════════════════════════════════════════════════════════
# execution_match
# ══════════════════════════════════════════════════════════════

class TestExecutionMatch:

    def test_match_equal_rows(self):
        registry = make_registry()
        mgr      = registry.target("testdb")
        rows_a   = [{"id": 1, "name": "A"}, {"id": 2, "name": "B"}]
        rows_b   = [{"id": 2, "name": "B"}, {"id": 1, "name": "A"}]  # different order
        ctx_a, _ = mock_conn_ctx(execute_returns=rows_a)
        ctx_b, _ = mock_conn_ctx(execute_returns=rows_b)

        call_count = [0]
        def side_effect(*_):
            call_count[0] += 1
            return ctx_a if call_count[0] == 1 else ctx_b

        with patch.object(mgr, "connection", side_effect=side_effect):
            match, err = execution_match("SELECT 1", "SELECT 2", "testdb", registry)
        assert match is True
        assert err is None

    def test_match_different_rows(self):
        registry = make_registry()
        mgr      = registry.target("testdb")
        rows_a   = [{"cnt": 10}]
        rows_b   = [{"cnt": 20}]
        call_count = [0]
        def side_effect(*_):
            call_count[0] += 1
            return mock_conn_ctx(execute_returns=rows_a)[0] \
                if call_count[0] == 1 else mock_conn_ctx(execute_returns=rows_b)[0]
        with patch.object(mgr, "connection", side_effect=side_effect):
            match, err = execution_match("SELECT 1", "SELECT 2", "testdb", registry)
        assert match is False

    def test_match_empty_sql(self):
        registry = make_registry()
        match, err = execution_match("", "SELECT 1", "testdb", registry)
        assert match is False
        assert err is not None

    def test_match_unregistered_db(self):
        registry = make_registry()
        match, err = execution_match("SELECT 1", "SELECT 1", "nonexistent", registry)
        assert match is False
        assert "not registered" in err


# ══════════════════════════════════════════════════════════════
# EvalResult & Summary
# ══════════════════════════════════════════════════════════════

class TestEvalSummary:

    def _make_results(self):
        def r(b, p, diff="simple"):
            item = EvalResult("1","q","db","gold","hint",difficulty=diff)
            item.ex_baseline = b; item.ex_pgxllm = p
            item.baseline_ms = 100; item.pgxllm_ms = 200
            return item
        return [
            r(True,  True,  "simple"),   # both correct
            r(True,  False, "simple"),   # only baseline
            r(False, True,  "moderate"), # only pgxllm
            r(False, False, "hard"),     # both wrong
        ]

    def test_summary_counts(self):
        results = self._make_results()
        s = BIRDEvalRunner.summarize(results)
        assert s.total          == 4
        assert s.baseline_ex    == 2
        assert s.pgxllm_ex      == 2
        assert s.both_correct   == 1
        assert s.only_baseline  == 1
        assert s.only_pgxllm    == 1
        assert s.both_wrong     == 1

    def test_summary_accuracy(self):
        results = self._make_results()
        s = BIRDEvalRunner.summarize(results)
        assert s.baseline_acc == 0.5
        assert s.pgxllm_acc   == 0.5

    def test_summary_by_difficulty(self):
        results = self._make_results()
        s = BIRDEvalRunner.summarize(results)
        assert "simple"   in s.by_difficulty
        assert "moderate" in s.by_difficulty
        assert "hard"     in s.by_difficulty

    def test_summary_str(self):
        results = self._make_results()
        s = BIRDEvalRunner.summarize(results)
        text = str(s)
        assert "Baseline" in text
        assert "pgxllm"   in text
        assert "50.0%"    in text

    def test_empty_summary(self):
        s = BIRDEvalRunner.summarize([])
        assert s.total == 0
        assert s.baseline_acc == 0.0

    def test_to_dict(self):
        r = EvalResult("1","q","db","gold","hint",
            baseline_sql="SELECT 1", pgxllm_sql="SELECT 2",
            ex_baseline=True, ex_pgxllm=False)
        d = r.to_dict()
        assert d["ex_baseline"] is True
        assert d["ex_pgxllm"]   is False
        assert d["baseline_sql"] == "SELECT 1"

    def test_pgxllm_wins(self):
        r = EvalResult("1","q","db","gold","hint")
        r.ex_baseline = False; r.ex_pgxllm = True
        assert r.pgxllm_wins is True
        r.ex_baseline = True; r.ex_pgxllm = False
        assert r.pgxllm_wins is False

    def test_save_results(self, tmp_path):
        results = self._make_results()
        output  = str(tmp_path / "results" / "eval.json")
        BIRDEvalRunner.save_results(results, output)
        assert Path(output).exists()
        with open(output) as f:
            data = json.load(f)
        assert "summary" in data
        assert "results" in data
        assert len(data["results"]) == 4


# ══════════════════════════════════════════════════════════════
# BIRDEvalRunner integration
# ══════════════════════════════════════════════════════════════

class TestBIRDEvalRunner:

    def _make_runner(self):
        from pgxllm.core.llm.base import LLMProvider, LLMResponse
        config   = make_config()
        registry = make_registry()
        runner   = BIRDEvalRunner(registry, config)
        # Mock LLM
        llm = MagicMock(spec=LLMProvider)
        llm.complete.return_value = LLMResponse(
            text="```sql\nSELECT COUNT(*) FROM orders;\n```", model="t"
        )
        runner._llm      = llm
        runner._baseline = BaselineSQLEngine(llm, registry)
        # Mock pipeline
        from pgxllm.core.models import PipelineRequest, PipelineResult
        mock_pipeline = MagicMock()
        mock_pipeline.run.return_value = PipelineResult(
            request=PipelineRequest(question="q", db_alias="testdb"),
            final_sql="SELECT COUNT(*) FROM orders",
            execution_ok=True,
        )
        runner._pipeline = mock_pipeline
        return runner, registry, llm

    def test_run_single_item(self):
        runner, registry, llm = self._make_runner()
        item = BIRDItem("1", "주문 수는?", "testdb", "SELECT COUNT(*) FROM orders")
        # Mock execution_match
        with patch("pgxllm.eval.bird.execution_match", return_value=(True, None)):
            ctx, _ = mock_conn_ctx(execute_returns=[])
            with patch.object(registry.internal, "connection", return_value=ctx):
                results = runner.run([item], "testdb")
        assert len(results) == 1
        assert results[0].ex_pgxllm is True

    def test_run_with_limit(self):
        runner, registry, _ = self._make_runner()
        items = [BIRDItem(str(i), f"질문{i}", "testdb", "SELECT 1") for i in range(10)]
        with patch("pgxllm.eval.bird.execution_match", return_value=(True, None)):
            ctx, _ = mock_conn_ctx(execute_returns=[])
            with patch.object(registry.internal, "connection", return_value=ctx):
                results = runner.run(items, "testdb", limit=3)
        assert len(results) == 3

    def test_run_skip_baseline(self):
        runner, registry, llm = self._make_runner()
        item = BIRDItem("1", "q", "testdb", "SELECT 1")
        with patch("pgxllm.eval.bird.execution_match", return_value=(True, None)):
            ctx, _ = mock_conn_ctx(execute_returns=[])
            with patch.object(registry.internal, "connection", return_value=ctx):
                results = runner.run([item], "testdb", skip_baseline=True)
        assert results[0].ex_baseline is None   # not evaluated
        assert results[0].ex_pgxllm  is True

    def test_hint_appended_to_question(self):
        runner, registry, _ = self._make_runner()
        item = BIRDItem("1", "주문 수는?", "testdb", "SELECT COUNT(*) FROM orders", hint="orders 사용")
        with patch("pgxllm.eval.bird.execution_match", return_value=(True, None)):
            ctx, _ = mock_conn_ctx(execute_returns=[])
            with patch.object(registry.internal, "connection", return_value=ctx):
                runner.run([item], "testdb")
        # Pipeline 호출 시 hint가 question에 포함되어야 함
        call_args = runner._pipeline.run.call_args[0][0]
        assert "orders 사용" in call_args.question
