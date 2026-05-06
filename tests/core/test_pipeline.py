"""
tests/core/test_pipeline.py
----------------------------
Phase 1 Core Pipeline unit tests (mock 기반, LLM 연결 불필요).
"""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from pgxllm.config import AppConfig, InternalDBConfig, LLMConfig, TargetDBConfig
from pgxllm.core.models import (
    FewShotItem, LinkedSchema, MatchedPattern,
    PipelineRequest, PipelineResult, QuestionAnalysis,
    SQLCandidate, TableInfo, ValidationResult, ValidationIssue,
)
from pgxllm.db.connections import ConnectionRegistry


# ── Fixtures ─────────────────────────────────────────────────

def make_config() -> AppConfig:
    return AppConfig(
        internal_db=InternalDBConfig(host="localhost", dbname="pgxllm"),
        target_dbs=[
            TargetDBConfig(alias="testdb", host="test-host", user="u", dbname="testdb")
        ],
        llm=LLMConfig(provider="ollama", model="test-model"),
    )


def make_registry(config: AppConfig | None = None) -> ConnectionRegistry:
    return ConnectionRegistry(config or make_config())


def mock_conn_ctx(execute_returns=None, execute_one_returns=None):
    conn = MagicMock()
    conn.execute.return_value = execute_returns or []
    conn.execute_one.return_value = execute_one_returns
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=conn)
    ctx.__exit__  = MagicMock(return_value=False)
    return ctx, conn


# ══════════════════════════════════════════════════════════════
# Models
# ══════════════════════════════════════════════════════════════

class TestPipelineModels:

    def test_pipeline_request_defaults(self):
        req = PipelineRequest(question="주문 현황은?", db_alias="mydb")
        assert req.top_k == 3
        assert req.max_loops == 3

    def test_pipeline_result_ok(self):
        req = PipelineRequest(question="q", db_alias="db")
        result = PipelineResult(request=req, final_sql="SELECT 1", execution_ok=True)
        assert result.ok is True

    def test_pipeline_result_fail_no_sql(self):
        req = PipelineRequest(question="q", db_alias="db")
        result = PipelineResult(request=req, final_sql=None, execution_ok=False)
        assert result.ok is False

    def test_pipeline_result_cache_hit_summary(self):
        req = PipelineResult(
            request=PipelineRequest(question="q", db_alias="db"),
            final_sql="SELECT 1", execution_ok=True, cache_hit=True
        )
        assert "CACHE HIT" in req.summary()

    def test_linked_schema_to_prompt_text(self):
        schema = LinkedSchema(
            db_alias="mydb",
            tables=[
                TableInfo(
                    address="mydb.public.orders",
                    schema="public", table="orders",
                    columns=[
                        {"name":"id","type":"integer","pk":True,"fk":False,"fk_ref":None,
                         "comment":None,"n_distinct":None,"samples":None},
                        {"name":"status","type":"text","pk":False,"fk":False,"fk_ref":None,
                         "comment":None,"n_distinct":5,
                         "samples":json.dumps(["ACTIVE","PENDING"])},
                    ],
                )
            ],
            join_hint="orders.customer_id → customers.id",
        )
        text = schema.to_prompt_text()
        assert "orders" in text
        assert "id" in text
        assert "PK" in text
        assert "ACTIVE" in text          # samples
        assert "JOIN 경로 힌트" in text


# ══════════════════════════════════════════════════════════════
# LLM Providers
# ══════════════════════════════════════════════════════════════

class TestLLMProviders:

    def test_ollama_model_name(self):
        from pgxllm.core.llm import OllamaProvider
        p = OllamaProvider(model="qwen2.5:32b")
        assert p.model_name == "qwen2.5:32b"

    def test_vllm_model_name(self):
        from pgxllm.core.llm import VLLMProvider
        p = VLLMProvider(model="codellama")
        assert p.model_name == "codellama"

    def test_anthropic_model_name(self):
        from pgxllm.core.llm import AnthropicProvider
        p = AnthropicProvider(model="claude-3-5-sonnet-20241022")
        assert p.model_name == "claude-3-5-sonnet-20241022"

    def test_factory_ollama(self):
        from pgxllm.core.llm import create_llm_provider, OllamaProvider
        cfg = LLMConfig(provider="ollama", model="test")
        p = create_llm_provider(cfg)
        assert isinstance(p, OllamaProvider)

    def test_factory_vllm(self):
        from pgxllm.core.llm import create_llm_provider, VLLMProvider
        cfg = LLMConfig(provider="vllm", model="test")
        p = create_llm_provider(cfg)
        assert isinstance(p, VLLMProvider)

    def test_factory_anthropic(self):
        from pgxllm.core.llm import create_llm_provider, AnthropicProvider
        cfg = LLMConfig(provider="anthropic", model="claude-3")
        p = create_llm_provider(cfg)
        assert isinstance(p, AnthropicProvider)

    def test_factory_unknown_raises(self):
        from pgxllm.core.llm import create_llm_provider
        cfg = LLMConfig(provider="unknown", model="x")
        with pytest.raises(ValueError, match="Unknown LLM provider"):
            create_llm_provider(cfg)


# ══════════════════════════════════════════════════════════════
# SemanticCache
# ══════════════════════════════════════════════════════════════

class TestSemanticCache:

    def _make_cache(self):
        from pgxllm.cache import TfidfSemanticCache
        config   = make_config()
        registry = make_registry(config)
        return TfidfSemanticCache(registry, threshold=0.75), registry

    def test_cache_miss_returns_none(self):
        cache, registry = self._make_cache()
        ctx, conn = mock_conn_ctx(execute_one_returns=None)
        with patch.object(registry.internal, "connection", return_value=ctx):
            result = cache.get("SELECT count(*) FROM orders", "testdb")
        assert result is None

    def test_cache_hit_exact_match(self):
        cache, registry = self._make_cache()
        ctx, conn = mock_conn_ctx(execute_one_returns={
            "question": "주문 수는?",
            "sql": "SELECT COUNT(*) FROM orders",
            "execution_ok": True,
        })
        with patch.object(registry.internal, "connection", return_value=ctx):
            result = cache.get("주문 수는?", "testdb")
        assert result is not None
        assert result.final_sql == "SELECT COUNT(*) FROM orders"
        assert result.cache_hit is True

    def test_cache_set_skips_failed(self):
        cache, registry = self._make_cache()
        ctx, conn = mock_conn_ctx()
        req    = PipelineRequest(question="q", db_alias="testdb")
        result = PipelineResult(request=req, final_sql=None, execution_ok=False)
        with patch.object(registry.internal, "connection", return_value=ctx):
            cache.set("key", "testdb", result)
        conn.execute.assert_not_called()

    def test_cache_set_ok(self):
        cache, registry = self._make_cache()
        ctx, conn = mock_conn_ctx()
        req    = PipelineRequest(question="q", db_alias="testdb")
        result = PipelineResult(request=req, final_sql="SELECT 1", execution_ok=True)
        with patch.object(registry.internal, "connection", return_value=ctx):
            cache.set("q_normalized", "testdb", result)
        conn.execute.assert_called_once()


# ══════════════════════════════════════════════════════════════
# S1 Question Understanding
# ══════════════════════════════════════════════════════════════

class TestS1Understanding:

    def _make_s1(self):
        from pgxllm.core.s1_understanding import QuestionUnderstanding
        config   = make_config()
        registry = make_registry(config)
        return QuestionUnderstanding(registry), registry

    def test_keyword_extraction(self):
        s1, _ = self._make_s1()
        kws = s1._extract_keywords("2023년 부서별 매출 합계를 알려줘")
        assert "매출" in kws
        assert "부서별" in kws or "부서" in " ".join(kws)
        # stopwords excluded
        assert "알려줘" not in kws

    def test_keyword_extraction_english(self):
        s1, _ = self._make_s1()
        kws = s1._extract_keywords("show me the sales by department in 2023")
        assert "sales" in kws
        assert "department" in kws
        # stopwords
        assert "the" not in kws
        assert "show" not in kws

    def test_keyword_extraction_dedup(self):
        s1, _ = self._make_s1()
        kws = s1._extract_keywords("매출 매출 합계 합계")
        assert kws.count("매출") == 1

    def test_run_returns_question_analysis(self):
        s1, registry = self._make_s1()
        ctx, conn = mock_conn_ctx(execute_returns=[])
        with patch.object(registry.internal, "connection", return_value=ctx):
            with patch.object(s1._pattern_engine, "match", return_value=[]):
                result = s1.run("주문 현황은?", "testdb")
        assert result.question == "주문 현황은?"
        assert isinstance(result.candidate_tables, list)

    def test_run_matches_patterns(self):
        s1, registry = self._make_s1()
        from pgxllm.intelligence.pattern_engine import MatchedPattern as MP
        fake_pattern = MP(
            pattern_id="p1", name="top_n_then_detail",
            score=0.9, instruction="inline view에 LIMIT",
            example_bad="", example_good="",
        )
        ctx, conn = mock_conn_ctx(execute_returns=[])
        with patch.object(registry.internal, "connection", return_value=ctx):
            with patch.object(s1._pattern_engine, "match", return_value=[fake_pattern]):
                result = s1.run("상위 5개 부서의 매출은?", "testdb")
        assert len(result.matched_patterns) == 1
        assert result.matched_patterns[0].name == "top_n_then_detail"

    def test_run_finds_candidate_tables(self):
        s1, registry = self._make_s1()
        catalog_rows = [
            {"db_alias": "testdb", "schema_name": "public", "table_name": "orders"},
        ]
        ctx, conn = mock_conn_ctx(execute_returns=catalog_rows)
        with patch.object(registry.internal, "connection", return_value=ctx):
            with patch.object(s1._pattern_engine, "match", return_value=[]):
                result = s1.run("orders 현황", "testdb")
        assert any("orders" in t for t in result.candidate_tables)


# ══════════════════════════════════════════════════════════════
# S2 Schema Linking
# ══════════════════════════════════════════════════════════════

class TestS2SchemaLinking:

    def _make_s2(self):
        from pgxllm.core.s2_schema_linking import SchemaLinker
        config   = make_config()
        registry = make_registry(config)
        linker   = SchemaLinker(registry, config)
        return linker, registry

    def test_returns_linked_schema(self):
        linker, registry = self._make_s2()
        analysis = QuestionAnalysis(
            question="주문 현황",
            candidate_tables=["testdb.public.orders"],
        )
        catalog_rows = [
            {"schema_name":"public","table_name":"orders","column_name":None,
             "data_type":None,"is_pk":False,"is_fk":False,
             "fk_ref_table":None,"fk_ref_column":None,
             "comment_text":"주문 테이블","n_distinct":None,"sample_values":None},
            {"schema_name":"public","table_name":"orders","column_name":"id",
             "data_type":"integer","is_pk":True,"is_fk":False,
             "fk_ref_table":None,"fk_ref_column":None,
             "comment_text":None,"n_distinct":None,"sample_values":None},
        ]
        ctx, conn = mock_conn_ctx(execute_returns=catalog_rows)
        with patch.object(registry.internal, "connection", return_value=ctx):
            with patch.object(linker._graph, "get_join_hint", return_value=""):
                with patch.object(linker._rule_engine, "get_rules_for_query", return_value=[]):
                    result = linker.run(analysis, "testdb")

        assert result.db_alias == "testdb"
        assert len(result.tables) >= 1
        assert result.tables[0].table == "orders"

    def test_to_prompt_text_includes_join_hint(self):
        """join_hint가 있으면 to_prompt_text에 포함되어야 한다."""
        schema = LinkedSchema(
            db_alias="testdb",
            tables=[],
            join_hint="orders.customer_id → customers.id",
        )
        prompt = schema.to_prompt_text()
        assert "JOIN 경로 힌트" in prompt
        assert "customer_id" in prompt


# ══════════════════════════════════════════════════════════════
# S3 SQL Generation
# ══════════════════════════════════════════════════════════════

class TestS3Generation:

    def _make_s3(self):
        from pgxllm.core.s3_generation import SQLGenerator
        from pgxllm.core.llm.base import LLMProvider, LLMResponse
        config   = make_config()
        registry = make_registry(config)

        # Mock LLM
        llm = MagicMock(spec=LLMProvider)
        llm.complete.return_value = LLMResponse(
            text="```sql\nSELECT COUNT(*) FROM orders;\n```\n설명: 주문 수를 조회합니다.",
            model="test-model",
        )
        return SQLGenerator(llm, registry), registry, llm

    def _make_schema(self):
        return LinkedSchema(
            db_alias="testdb",
            tables=[TableInfo(
                address="testdb.public.orders",
                schema="public", table="orders",
                columns=[{"name":"id","type":"integer","pk":True,"fk":False,
                          "fk_ref":None,"comment":None,"n_distinct":None,"samples":None}],
            )],
        )

    def test_run_calls_llm(self):
        s3, registry, llm = self._make_s3()
        analysis = QuestionAnalysis(question="주문 수는?")
        schema   = self._make_schema()
        ctx, conn = mock_conn_ctx(execute_returns=[])
        with patch.object(registry.internal, "connection", return_value=ctx):
            candidate = s3.run("주문 수는?", analysis, schema)
        llm.complete.assert_called_once()
        assert "SELECT" in candidate.sql.upper()

    def test_parses_sql_block(self):
        s3, registry, llm = self._make_s3()
        analysis  = QuestionAnalysis(question="q")
        schema    = self._make_schema()
        ctx, conn = mock_conn_ctx(execute_returns=[])
        with patch.object(registry.internal, "connection", return_value=ctx):
            candidate = s3.run("q", analysis, schema)
        assert candidate.sql.strip().startswith("SELECT")
        assert "설명" in candidate.explanation or "주문" in candidate.explanation

    def test_parse_response_no_block(self):
        from pgxllm.core.s3_generation import SQLGenerator
        from pgxllm.core.llm.base import LLMResponse
        config   = make_config()
        registry = make_registry(config)
        llm      = MagicMock()
        s3       = SQLGenerator(llm, registry)
        sql, expl = s3._parse_response("SELECT id FROM orders WHERE status='A'")
        assert "SELECT" in sql.upper()

    def test_correction_attempt_raises_temperature(self):
        s3, registry, llm = self._make_s3()
        analysis  = QuestionAnalysis(question="q")
        schema    = self._make_schema()
        ctx, conn = mock_conn_ctx(execute_returns=[])
        with patch.object(registry.internal, "connection", return_value=ctx):
            s3.run("q", analysis, schema, attempt=2, prev_sql="SELECT 1", correction_hint="오류")
        _, kwargs = llm.complete.call_args
        assert kwargs.get("temperature", 0) > 0.0


# ══════════════════════════════════════════════════════════════
# S4 Validation
# ══════════════════════════════════════════════════════════════

class TestS4Validation:

    def _make_s4(self):
        from pgxllm.core.s4_validation import SQLValidator
        config   = make_config()
        registry = make_registry(config)
        return SQLValidator(registry), registry

    def _make_schema(self):
        return LinkedSchema(db_alias="testdb", tables=[], dialect_rules=[])

    def test_empty_sql_fails(self):
        s4, registry = self._make_s4()
        candidate = SQLCandidate(sql="")
        schema    = self._make_schema()
        result    = s4.validate(candidate, schema, db_alias="testdb")
        assert result.ok is False
        assert any("비어있" in i.message for i in result.issues)

    def test_valid_sql_passes_without_db(self):
        """DB 미등록 환경에서는 PREPARE를 skip하고 통과."""
        s4, registry = self._make_s4()
        candidate = SQLCandidate(sql="SELECT COUNT(*) FROM orders")
        schema    = self._make_schema()
        # _prepare_validate returns True when DB not in registry
        with patch.object(s4, "_prepare_validate", return_value=(True, None)):
            result = s4.validate(candidate, schema, db_alias="testdb")
        assert result.ok is True

    def test_syntax_error_fails(self):
        s4, registry = self._make_s4()
        candidate = SQLCandidate(sql="INVALID SQL !@#$")
        schema    = self._make_schema()
        with patch.object(s4, "_prepare_validate",
                          return_value=(False, "syntax error at or near '!'")):
            result = s4.validate(candidate, schema, db_alias="testdb")
        assert result.ok is False
        assert result.error is not None

    def test_correction_hint_from_syntax_error(self):
        s4, _ = self._make_s4()
        vr = ValidationResult(
            sql="BAD SQL", ok=False,
            error="syntax error near '!'",
        )
        hint = s4.correction_hint(vr)
        assert "syntax error" in hint.lower() or "구문" in hint

    def test_correction_hint_from_issues(self):
        s4, _ = self._make_s4()
        vr = ValidationResult(
            sql="SELECT * FROM t LIMIT 5", ok=False,
            issues=[ValidationIssue("limit_no_order", "LIMIT without ORDER BY", "error")],
        )
        hint = s4.correction_hint(vr)
        assert "LIMIT" in hint or "ORDER" in hint


# ══════════════════════════════════════════════════════════════
# PipelineRunner (통합)
# ══════════════════════════════════════════════════════════════

class TestPipelineRunner:

    def _make_runner(self):
        from pgxllm.core.pipeline import PipelineRunner
        config   = make_config()
        registry = make_registry(config)
        return PipelineRunner(registry, config), registry

    def test_cache_hit_returns_immediately(self):
        runner, registry = self._make_runner()
        req = PipelineRequest(question="캐시된 질문", db_alias="testdb")
        cached = PipelineResult(
            request=req, final_sql="SELECT 1",
            execution_ok=True, cache_hit=True
        )
        with patch.object(runner._cache, "get", return_value=cached):
            result = runner.run(req)
        assert result.cache_hit is True
        assert result.final_sql == "SELECT 1"

    def test_pipeline_success(self):
        from pgxllm.core.llm.base import LLMResponse
        runner, registry = self._make_runner()
        req = PipelineRequest(question="주문 수는?", db_alias="testdb")

        # Mock all sub-components
        analysis = QuestionAnalysis(question="주문 수는?", candidate_tables=["testdb.public.orders"])
        schema   = LinkedSchema(db_alias="testdb", tables=[], dialect_rules=[])
        candidate = SQLCandidate(sql="SELECT COUNT(*) FROM orders", explanation="주문 수 조회")
        validation = ValidationResult(sql="SELECT COUNT(*) FROM orders", ok=True)

        with patch.object(runner._cache, "get", return_value=None):
            with patch.object(runner._s1, "run", return_value=analysis):
                with patch.object(runner._s2, "run", return_value=schema):
                    runner._ensure_llm()
                    runner._llm = MagicMock()
                    runner._s3  = MagicMock()
                    runner._s3.run.return_value = candidate
                    with patch.object(runner._s4, "validate", return_value=validation):
                        with patch.object(runner._cache, "set"):
                            with patch.object(runner, "_save_verified"):
                                result = runner.run(req)

        assert result.final_sql == "SELECT COUNT(*) FROM orders"
        assert result.execution_ok is True

    def test_pipeline_s4_correction_loop(self):
        """S4 실패 시 S3 재호출 (self-correction)."""
        from pgxllm.core.llm.base import LLMResponse
        runner, registry = self._make_runner()
        req = PipelineRequest(question="q", db_alias="testdb", max_loops=2)

        analysis  = QuestionAnalysis(question="q")
        schema    = LinkedSchema(db_alias="testdb", tables=[], dialect_rules=[])
        bad_cand  = SQLCandidate(sql="BAD SQL", attempt=1)
        good_cand = SQLCandidate(sql="SELECT 1", attempt=2)
        fail_vr   = ValidationResult(sql="BAD SQL", ok=False,
                                     issues=[ValidationIssue("e","오류","error")])
        ok_vr     = ValidationResult(sql="SELECT 1", ok=True)

        with patch.object(runner._cache, "get", return_value=None):
            with patch.object(runner._s1, "run", return_value=analysis):
                with patch.object(runner._s2, "run", return_value=schema):
                    runner._ensure_llm()
                    runner._llm = MagicMock()
                    runner._s3  = MagicMock()
                    runner._s3.run.side_effect = [bad_cand, good_cand]
                    runner._s4 = MagicMock()
                    runner._s4.validate.side_effect = [fail_vr, ok_vr]
                    runner._s4.correction_hint.return_value = "오류 힌트"
                    runner._s4.record_failure = MagicMock()
                    with patch.object(runner._cache, "set"):
                        with patch.object(runner, "_save_verified"):
                            result = runner.run(req)

        # S3 두 번 호출
        assert runner._s3.run.call_count == 2
        assert result.final_sql == "SELECT 1"

    def test_pipeline_max_loops_exceeded(self):
        """max_loops 초과 시 마지막 SQL 반환 + error 설정."""
        runner, registry = self._make_runner()
        req = PipelineRequest(question="q", db_alias="testdb", max_loops=1)

        analysis  = QuestionAnalysis(question="q")
        schema    = LinkedSchema(db_alias="testdb", tables=[], dialect_rules=[])
        bad_cand  = SQLCandidate(sql="BAD SQL", attempt=1)
        fail_vr   = ValidationResult(sql="BAD SQL", ok=False,
                                     issues=[ValidationIssue("e","오류","error")])

        with patch.object(runner._cache, "get", return_value=None):
            with patch.object(runner._s1, "run", return_value=analysis):
                with patch.object(runner._s2, "run", return_value=schema):
                    runner._ensure_llm()
                    runner._llm = MagicMock()
                    runner._s3  = MagicMock()
                    runner._s3.run.return_value = bad_cand
                    runner._s4  = MagicMock()
                    runner._s4.validate.return_value = fail_vr
                    runner._s4.correction_hint.return_value = "오류"
                    runner._s4.record_failure = MagicMock()
                    result = runner.run(req)

        # 최종 SQL은 반환되지만 error도 설정
        assert result.final_sql == "BAD SQL"
        assert result.error is not None
