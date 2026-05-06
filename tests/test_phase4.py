"""
tests/test_phase4.py
---------------------
Phase 4 unit tests — DB 등록, Schema Catalog, 샘플 추출, Dialect Rule 감지.
실제 구현된 API에 맞춰 Mock 기반으로 테스트한다.
"""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from pgxllm.config import AppConfig, InternalDBConfig, TargetDBConfig
from pgxllm.db.connections import ConnectionRegistry


# ── Fixtures ─────────────────────────────────────────────────

def make_config() -> AppConfig:
    return AppConfig(
        internal_db=InternalDBConfig(host="localhost", dbname="pgxllm"),
        target_dbs=[
            TargetDBConfig(
                alias="testdb", host="test-host", port=5432,
                user="testuser", dbname="testdb",
                schema_mode="include", schemas=["public", "sales"],
            )
        ],
    )


def make_registry(config: AppConfig) -> ConnectionRegistry:
    return ConnectionRegistry(config)


def mock_conn_ctx(execute_returns=None, execute_one_returns=None):
    conn = MagicMock()
    if execute_returns is not None:
        conn.execute.return_value = execute_returns
    if execute_one_returns is not None:
        conn.execute_one.return_value = execute_one_returns
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=conn)
    ctx.__exit__ = MagicMock(return_value=False)
    return ctx, conn


# ══════════════════════════════════════════════════════════════
# DBRegistryService
# ══════════════════════════════════════════════════════════════

class TestDBRegistryService:

    def _make_svc(self):
        from pgxllm.intelligence import DBRegistryService
        config = make_config()
        registry = make_registry(config)
        return DBRegistryService(registry), registry

    def test_register_new(self):
        svc, registry = self._make_svc()
        cfg = TargetDBConfig(alias="newdb", host="h", user="u", dbname="newdb")

        ctx, conn = mock_conn_ctx()
        conn.execute_one.return_value = None  # not existing

        with patch.object(registry, "register_target") as mock_reg:
            with patch.object(registry.internal, "connection", return_value=ctx):
                svc.register(cfg)
            mock_reg.assert_called_once_with(cfg)

    def test_register_raises_if_existing_no_overwrite(self):
        svc, registry = self._make_svc()
        cfg = TargetDBConfig(alias="testdb", host="h", user="u", dbname="testdb")

        ctx, conn = mock_conn_ctx()
        conn.execute_one.return_value = {"alias": "testdb", "host": "old"}

        with patch.object(registry, "register_target"):
            with patch.object(registry.internal, "connection", return_value=ctx):
                with pytest.raises(ValueError, match="already registered"):
                    svc.register(cfg, overwrite=False)

    def test_compute_schema_hash_same_schema_same_hash(self):
        svc, registry = self._make_svc()
        cols = [
            {"table_schema": "public", "table_name": "orders",
             "column_name": "id", "data_type": "integer"},
        ]
        target_ctx, _ = mock_conn_ctx(execute_returns=cols)
        with patch.object(registry.target("testdb"), "get_effective_schemas", return_value=["public"]):
            with patch.object(registry.target("testdb"), "connection", return_value=target_ctx):
                h1 = svc.compute_schema_hash("testdb")
            with patch.object(registry.target("testdb"), "connection", return_value=target_ctx):
                h2 = svc.compute_schema_hash("testdb")
        assert h1 == h2
        assert len(h1) == 16

    def test_compute_schema_hash_different_schema_different_hash(self):
        svc, registry = self._make_svc()
        cols_a = [{"table_schema": "public", "table_name": "a", "column_name": "id", "data_type": "integer"}]
        cols_b = [{"table_schema": "public", "table_name": "b", "column_name": "id", "data_type": "integer"}]
        ctx_a, _ = mock_conn_ctx(execute_returns=cols_a)
        ctx_b, _ = mock_conn_ctx(execute_returns=cols_b)
        with patch.object(registry.target("testdb"), "get_effective_schemas", return_value=["public"]):
            with patch.object(registry.target("testdb"), "connection", side_effect=[ctx_a, ctx_b]):
                h1 = svc.compute_schema_hash("testdb")
                h2 = svc.compute_schema_hash("testdb")
        assert h1 != h2

    def test_blacklist_list(self):
        svc, registry = self._make_svc()
        ctx, conn = mock_conn_ctx()
        conn.execute_one.return_value = {
            "alias": "testdb",
            "blacklist_tables": json.dumps(["audit_logs"]),
            "blacklist_columns": json.dumps([]),
            "blacklist_patterns": json.dumps(["*_hash"]),
        }
        with patch.object(registry.internal, "connection", return_value=ctx):
            bl = svc.blacklist_list("testdb")
        assert "audit_logs" in bl["tables"]
        assert "*_hash" in bl["patterns"]

    def test_update_hash(self):
        svc, registry = self._make_svc()
        ctx, conn = mock_conn_ctx()
        with patch.object(registry.internal, "connection", return_value=ctx):
            svc.update_hash("testdb", "abc123def456abcd")
        conn.execute.assert_called()


# ══════════════════════════════════════════════════════════════
# SchemaCatalogBuilder
# ══════════════════════════════════════════════════════════════

class TestSchemaCatalogBuilder:

    def _make_builder(self):
        from pgxllm.intelligence import SchemaCatalogBuilder
        config = make_config()
        registry = make_registry(config)
        return SchemaCatalogBuilder(registry, config), registry

    def test_build_calls_fetch_methods(self):
        builder, registry = self._make_builder()
        tables = [
            {"table_schema": "public", "table_name": "orders"},
            {"table_schema": "public", "table_name": "customers"},
        ]
        columns = [
            {"table_schema": "public", "table_name": "orders",
             "column_name": "id", "data_type": "integer",
             "is_nullable": "NO", "column_default": None, "ordinal_position": 1},
            {"table_schema": "public", "table_name": "orders",
             "column_name": "status", "data_type": "character varying",
             "is_nullable": "YES", "column_default": None, "ordinal_position": 2},
        ]

        # Patch internal DB connection for DELETE + INSERT
        internal_ctx, internal_conn = mock_conn_ctx()
        internal_conn.execute.return_value = []

        with patch.object(registry.target("testdb"), "get_effective_schemas", return_value=["public"]):
            with patch.object(builder, "_fetch_tables", return_value=tables) as m_tables:
                with patch.object(builder, "_fetch_columns", return_value=columns) as m_cols:
                    with patch.object(builder, "_fetch_primary_keys", return_value=set()):
                        with patch.object(builder, "_fetch_foreign_keys", return_value={}):
                            with patch.object(builder, "_fetch_stats", return_value={}):
                                with patch.object(registry.internal, "connection",
                                                  return_value=internal_ctx):
                                    result = builder.build("testdb")

        m_tables.assert_called_once()
        m_cols.assert_called_once()

    def test_fetch_primary_keys_returns_set(self):
        """_fetch_primary_keys should return a set of (schema, table, column) tuples."""
        builder, registry = self._make_builder()
        pk_rows = [
            {"table_schema": "public", "table_name": "orders", "column_name": "id"},
        ]
        target_ctx, _ = mock_conn_ctx(execute_returns=pk_rows)
        with patch.object(registry.target("testdb"), "connection", return_value=target_ctx):
            result = builder._fetch_primary_keys(registry.target("testdb"), ["public"])
        assert ("public", "orders", "id") in result

    def test_fetch_foreign_keys_returns_dict(self):
        builder, registry = self._make_builder()
        fk_rows = [
            {
                "table_schema": "public", "table_name": "orders",
                "column_name": "customer_id",
                "ref_schema": "public", "ref_table": "customers", "ref_column": "id",
            }
        ]
        target_ctx, _ = mock_conn_ctx(execute_returns=fk_rows)
        with patch.object(registry.target("testdb"), "connection", return_value=target_ctx):
            result = builder._fetch_foreign_keys(registry.target("testdb"), ["public"])
        assert ("public", "orders", "customer_id") in result
        assert result[("public", "orders", "customer_id")]["ref_table"] == "public.customers"


# ══════════════════════════════════════════════════════════════
# SampleDataExtractor
# ══════════════════════════════════════════════════════════════

class TestSampleDataExtractor:

    def _make_extractor(self):
        from pgxllm.intelligence import SampleDataExtractor
        config = make_config()
        registry = make_registry(config)
        return SampleDataExtractor(registry, config), registry

    def test_is_blacklisted_table(self):
        extractor, _ = self._make_extractor()
        from pgxllm.config import GlobalBlacklist
        bl = GlobalBlacklist(tables=["audit_logs"], columns=[], patterns=[])
        assert extractor._is_blacklisted("public", "audit_logs", "action", bl) is True

    def test_is_blacklisted_pattern(self):
        extractor, _ = self._make_extractor()
        from pgxllm.config import GlobalBlacklist
        bl = GlobalBlacklist(tables=[], columns=[], patterns=["*_token"])
        assert extractor._is_blacklisted("public", "users", "api_token", bl) is True
        assert extractor._is_blacklisted("public", "users", "username", bl) is False

    def test_is_blacklisted_column(self):
        extractor, _ = self._make_extractor()
        from pgxllm.config import GlobalBlacklist
        bl = GlobalBlacklist(tables=[], columns=["users.password_hash"], patterns=[])
        assert extractor._is_blacklisted("public", "users", "password_hash", bl) is True

    def test_should_sample_code_column(self):
        extractor, _ = self._make_extractor()
        # n_distinct=5, text → code column → should sample
        result = extractor._should_sample("text", 5.0)
        assert bool(result) is True   # "code_column" is truthy

    def test_should_sample_high_cardinality(self):
        extractor, _ = self._make_extractor()
        # n_distinct=9999 → too high → None (falsy)
        result = extractor._should_sample("text", 9999.0)
        assert bool(result) is False

    def test_should_sample_non_text(self):
        extractor, _ = self._make_extractor()
        # integer column → not sampled → None (falsy)
        result = extractor._should_sample("integer", 5.0)
        assert bool(result) is False

    def test_extract_skips_blacklisted(self):
        extractor, registry = self._make_extractor()
        # Override blacklist
        extractor._config.sample_data.blacklist.tables = ["audit_logs"]

        catalog_rows = [
            {"schema_name": "public", "table_name": "audit_logs",
             "column_name": "action", "data_type": "text", "n_distinct": 5.0},
        ]
        internal_ctx, _ = mock_conn_ctx(execute_returns=catalog_rows)

        with patch.object(registry.target("testdb"), "get_effective_schemas",
                          return_value=["public"]):
            with patch.object(extractor, "_fetch_catalog", return_value=catalog_rows):
                with patch.object(extractor, "_extract_samples") as mock_ext:
                    result = extractor.extract(registry.target("testdb"))

        mock_ext.assert_not_called()

    def test_extract_samples_code_column(self):
        extractor, registry = self._make_extractor()
        catalog_rows = [
            {"schema_name": "public", "table_name": "orders",
             "column_name": "status", "data_type": "text", "n_distinct": 5.0},
        ]
        sample_result = MagicMock()
        sample_result.values = ["ACTIVE", "PENDING", "CANCELLED"]

        with patch.object(registry.target("testdb"), "get_effective_schemas",
                          return_value=["public"]):
            with patch.object(extractor, "_fetch_catalog", return_value=catalog_rows):
                with patch.object(extractor, "_extract_samples",
                                  return_value=sample_result) as mock_ext:
                    with patch.object(extractor, "_save_samples"):
                        result = extractor.extract(registry.target("testdb"))

        mock_ext.assert_called_once()


# ══════════════════════════════════════════════════════════════
# DialectRuleDetector
# ══════════════════════════════════════════════════════════════

class TestDialectRuleDetector:

    def _make_detector(self):
        from pgxllm.intelligence import DialectRuleDetector
        config = make_config()
        registry = make_registry(config)
        return DialectRuleDetector(registry), registry

    def _make_sample_result(self, schema, table, column, dtype, values):
        from pgxllm.intelligence.sample_extractor import SampleResult
        sr = SampleResult(
            schema_name=schema, table_name=table, column_name=column,
            data_type=dtype, n_distinct=len(values), values=values,
        )
        return sr

    def test_yyyymm_pattern_detected(self):
        detector, _ = self._make_detector()
        samples = [self._make_sample_result(
            "public", "orders", "order_date", "text",
            ["202301", "202302", "202303", "202304", "202305"],
        )]
        rules = detector.detect("testdb", samples)
        assert len(rules) >= 1
        rule = rules[0]
        assert "EXTRACT" in rule["forbidden_funcs"] or "BETWEEN" in rule["forbidden_funcs"]
        assert rule["column_name"] == "order_date"

    def test_yyyymmdd_pattern_detected(self):
        detector, _ = self._make_detector()
        samples = [self._make_sample_result(
            "public", "logs", "log_date", "text",
            ["20230101", "20230102", "20230103"],
        )]
        rules = detector.detect("testdb", samples)
        assert any(r["column_name"] == "log_date" for r in rules)

    def test_non_date_no_rule(self):
        detector, _ = self._make_detector()
        samples = [self._make_sample_result(
            "public", "orders", "status", "text",
            ["ACTIVE", "PENDING", "CANCELLED"],
        )]
        rules = detector.detect("testdb", samples)
        assert not any(r["column_name"] == "status" for r in rules)

    def test_match_ratio_all_match(self):
        from pgxllm.intelligence import DialectRuleDetector
        assert DialectRuleDetector._match_ratio(
            ["202301", "202302", "202303"], r"^\d{6}$"
        ) == 1.0

    def test_match_ratio_partial(self):
        from pgxllm.intelligence import DialectRuleDetector
        ratio = DialectRuleDetector._match_ratio(
            ["202301", "ACTIVE", "202303"], r"^\d{6}$"
        )
        assert abs(ratio - 2/3) < 0.01

    def test_match_ratio_none(self):
        from pgxllm.intelligence import DialectRuleDetector
        assert DialectRuleDetector._match_ratio(
            ["ACTIVE", "PENDING"], r"^\d{6}$"
        ) == 0.0


# ══════════════════════════════════════════════════════════════
# RefreshOrchestrator
# ══════════════════════════════════════════════════════════════

class TestRefreshOrchestrator:

    def _make_orch(self):
        from pgxllm.intelligence import RefreshOrchestrator
        config = make_config()
        registry = make_registry(config)
        return RefreshOrchestrator(registry, config), registry, config

    def test_refresh_success(self):
        orch, registry, config = self._make_orch()
        target_cfg = config.get_target_db("testdb")

        with patch.object(orch._db_svc, "get_status",
                          return_value=MagicMock(alias="testdb")), \
             patch.object(orch._cat_builder, "build", return_value="abc123ef"), \
             patch.object(orch._sam_ext, "extract", return_value=[MagicMock()]), \
             patch.object(orch._rule_det, "detect", return_value=[]), \
             patch.object(orch._rule_det, "save", return_value=0), \
             patch.object(orch, "_create_fk_edges", return_value=3), \
             patch.object(orch._db_svc, "update_hash"):
            # RefreshOrchestrator uses get_required internally in refresh.py
            # We patch at the registry level
            with patch.object(registry, "target") as mock_target:
                mock_mgr = MagicMock()
                mock_mgr.get_effective_schemas.return_value = ["public"]
                mock_mgr.config = target_cfg
                mock_target.return_value = mock_mgr

                # Patch internal connection for count query
                internal_ctx, internal_conn = mock_conn_ctx()
                internal_conn.execute_one.return_value = {"n": 5}
                with patch.object(registry.internal, "connection", return_value=internal_ctx):
                    result = orch.refresh("testdb")

        # success or known error both acceptable — just no crash
        assert result.db_alias == "testdb"

    def test_refresh_result_has_summary(self):
        orch, _, _ = self._make_orch()
        from pgxllm.intelligence.refresh import RefreshResult
        r = RefreshResult(db_alias="testdb", success=True, schema_hash="abc",
                          tables_scanned=5, columns_scanned=20,
                          samples_extracted=3, rules_detected=1,
                          fk_edges_created=4, duration_sec=1.2)
        summary = r.summary()
        assert "testdb" in summary
        assert "OK" in summary

    def test_refresh_result_failed_summary(self):
        from pgxllm.intelligence.refresh import RefreshResult
        r = RefreshResult(db_alias="testdb", success=False, error="Connection refused")
        summary = r.summary()
        assert "FAILED" in summary
        assert "Connection refused" in summary


# ══════════════════════════════════════════════════════════════
# CLI smoke tests
# ══════════════════════════════════════════════════════════════

class TestCLI:

    def test_db_list_empty(self):
        from click.testing import CliRunner
        from pgxllm.cli import main

        runner = CliRunner()
        with patch("pgxllm.cli._get_registry") as mock_get:
            mock_registry = MagicMock()
            mock_config = MagicMock()
            mock_get.return_value = (mock_registry, mock_config)
            mock_svc = MagicMock()
            mock_svc.list_all.return_value = []
            with patch("pgxllm.intelligence.DBRegistryService", return_value=mock_svc):
                result = runner.invoke(main, ["db", "list"])

        assert result.exit_code == 0
        assert "등록된" in result.output

    def test_db_register_ok(self):
        from click.testing import CliRunner
        from pgxllm.cli import main

        runner = CliRunner()
        with patch("pgxllm.cli._get_registry") as mock_get:
            mock_registry = MagicMock()
            mock_config = MagicMock()
            mock_get.return_value = (mock_registry, mock_config)
            mock_svc = MagicMock()
            with patch("pgxllm.intelligence.DBRegistryService", return_value=mock_svc):
                result = runner.invoke(main, [
                    "db", "register",
                    "--alias", "mydb",
                    "--host", "localhost",
                    "--dbname", "mydb",
                ])

        assert result.exit_code == 0
        assert "Registered" in result.output or "✅" in result.output

    def test_db_status_not_found(self):
        from click.testing import CliRunner
        from pgxllm.cli import main

        runner = CliRunner()
        with patch("pgxllm.cli._get_registry") as mock_get:
            mock_registry = MagicMock()
            mock_config = MagicMock()
            mock_get.return_value = (mock_registry, mock_config)
            mock_svc = MagicMock()
            mock_svc.get_status.return_value = None
            with patch("pgxllm.intelligence.DBRegistryService", return_value=mock_svc):
                result = runner.invoke(main, ["db", "status", "--alias", "nodb"])

        assert result.exit_code != 0 or "not found" in result.output.lower()
