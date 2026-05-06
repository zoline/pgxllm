"""
tests/test_intelligence.py
---------------------------
Unit tests for Phase 4~6:
  - SchemaCatalogBuilder (mock)
  - SampleDataExtractor  (blacklist logic)
  - DialectRuleDetector  (pattern matching)
  - RelationCollector    (SQL file parsing, reverse inference)
  - RuleEngine           (prompt injection)
  - DynamicPatternEngine (pattern matching, scoring)
  - PostgreSQLGraphStore (BFS paths, join hint)
  - RefreshOrchestrator  (flow)
"""
import json
from pathlib import Path
from unittest.mock import MagicMock, patch, PropertyMock
import pytest

from pgxllm.config import AppConfig, TargetDBConfig, GlobalBlacklist, SampleDataThresholds
from pgxllm.db.connections import ConnectionRegistry
from pgxllm.graph.base import TableEdge, TableNode, GraphPath
from pgxllm.graph.postgresql import PostgreSQLGraphStore
from pgxllm.intelligence.dialect_rule_detector import DialectRuleDetector, _PATTERNS
from pgxllm.intelligence.pattern_engine import DynamicPatternEngine
from pgxllm.intelligence.relation_collector import RelationCollector, RelationCandidate
from pgxllm.intelligence.rule_engine import RuleEngine
from pgxllm.intelligence.sample_extractor import SampleDataExtractor


# ══════════════════════════════════════════════════════════════
# Fixtures
# ══════════════════════════════════════════════════════════════

def make_config(**kwargs) -> AppConfig:
    return AppConfig(**kwargs)


def make_registry(rows_by_sql: dict = None) -> ConnectionRegistry:
    """Create a mock ConnectionRegistry."""
    rows_by_sql = rows_by_sql or {}

    conn_mock = MagicMock()

    def execute_side_effect(sql, params=None):
        for key, rows in rows_by_sql.items():
            if key.lower() in sql.lower():
                return rows
        return []

    def execute_one_side_effect(sql, params=None):
        rows = execute_side_effect(sql, params)
        return rows[0] if rows else None

    conn_mock.execute.side_effect = execute_side_effect
    conn_mock.execute_one.side_effect = execute_one_side_effect

    ctx_mock = MagicMock()
    ctx_mock.__enter__ = MagicMock(return_value=conn_mock)
    ctx_mock.__exit__ = MagicMock(return_value=False)

    internal_mock = MagicMock()
    internal_mock.connection.return_value = ctx_mock

    registry = MagicMock(spec=ConnectionRegistry)
    registry.internal = internal_mock
    return registry


# ══════════════════════════════════════════════════════════════
# SampleDataExtractor — Blacklist
# ══════════════════════════════════════════════════════════════

class TestSampleDataExtractorBlacklist:

    def _make_extractor(self, blacklist_kw=None) -> SampleDataExtractor:
        patterns = blacklist_kw or ["*_hash", "*_token", "*_password"]
        cfg = AppConfig(
            sample_data={
                "thresholds": {},
                "blacklist": {
                    "tables":   ["audit_logs"],
                    "columns":  ["users.password"],
                    "patterns": patterns,
                }
            }
        )
        registry = make_registry()
        return SampleDataExtractor(registry, cfg)

    def test_blacklist_table(self):
        ext = self._make_extractor()
        bl = ext._config.merge_blacklist(
            TargetDBConfig(alias="db", host="h", user="u")
        )
        assert ext._is_blacklisted("public", "audit_logs", "id", bl)

    def test_blacklist_column_with_table(self):
        ext = self._make_extractor()
        bl = ext._config.merge_blacklist(
            TargetDBConfig(alias="db", host="h", user="u")
        )
        assert ext._is_blacklisted("public", "users", "password", bl)

    def test_blacklist_pattern_hash(self):
        ext = self._make_extractor()
        bl = ext._config.merge_blacklist(
            TargetDBConfig(alias="db", host="h", user="u")
        )
        assert ext._is_blacklisted("public", "orders", "session_hash", bl)
        assert ext._is_blacklisted("public", "users",  "api_token",    bl)
        assert ext._is_blacklisted("public", "users",  "password_hash",bl)

    def test_not_blacklisted(self):
        ext = self._make_extractor()
        bl = ext._config.merge_blacklist(
            TargetDBConfig(alias="db", host="h", user="u")
        )
        assert not ext._is_blacklisted("public", "orders",    "status",      bl)
        assert not ext._is_blacklisted("public", "customers", "region_code", bl)

    def test_per_db_blacklist_merged(self):
        cfg = AppConfig(
            sample_data={"blacklist": {"tables": ["global_table"], "columns": [], "patterns": []}}
        )
        target = TargetDBConfig(
            alias="mydb", host="h", user="u",
            blacklist_tables=["per_db_table"],
        )
        merged = cfg.merge_blacklist(target)
        assert "global_table" in merged.tables
        assert "per_db_table" in merged.tables

    def test_code_column_detection(self):
        ext = self._make_extractor()
        thresh = SampleDataThresholds()
        # Low cardinality text → code column
        reason = ext._should_sample(
            "public", "orders", "status", "character varying", 5.0, set(), thresh
        )
        assert reason == "code_column"

    def test_high_cardinality_not_sampled(self):
        ext = self._make_extractor()
        thresh = SampleDataThresholds()
        reason = ext._should_sample(
            "public", "orders", "description", "text", 9999.0, set(), thresh
        )
        assert reason is None

    def test_dimension_table_sampled(self):
        ext = self._make_extractor()
        thresh = SampleDataThresholds()
        dim_tables = {"public.products"}
        reason = ext._should_sample(
            "public", "products", "category", "text", 200.0, dim_tables, thresh
        )
        assert reason == "dimension_table"


# ══════════════════════════════════════════════════════════════
# DialectRuleDetector
# ══════════════════════════════════════════════════════════════

class TestDialectRuleDetector:

    def _make_detector(self) -> DialectRuleDetector:
        registry = make_registry()
        # Make save() a no-op
        registry.internal.connection.return_value.__enter__.return_value.execute.return_value = []
        return DialectRuleDetector(registry)

    def _make_sample(self, values, dtype="character varying"):
        from pgxllm.intelligence.sample_extractor import SampleResult
        return SampleResult(
            schema_name="public", table_name="orders",
            column_name="order_date", data_type=dtype,
            n_distinct=len(set(values)), sample_values=values,
            reason="code_column",
        )

    def test_detect_yyyymm(self):
        det = self._make_detector()
        sample = self._make_sample(["202301", "202302", "202303", "202304", "202312"])
        rules = det.detect("mydb", [sample])
        assert any("text_date_yyyymm" in r["rule_id"] for r in rules)

    def test_detect_yyyymmdd(self):
        det = self._make_detector()
        sample = self._make_sample(["20230101", "20230201", "20231201"])
        rules = det.detect("mydb", [sample])
        assert any("text_date_yyyymmdd" in r["rule_id"] for r in rules)

    def test_detect_yyyy_mm(self):
        det = self._make_detector()
        sample = self._make_sample(["2023-01", "2023-02", "2023-12"])
        rules = det.detect("mydb", [sample])
        assert any("text_date_yyyy_mm" in r["rule_id"] for r in rules)

    def test_no_detection_for_random_text(self):
        det = self._make_detector()
        sample = self._make_sample(["active", "inactive", "pending", "cancelled"])
        rules = det.detect("mydb", [sample])
        assert rules == []

    def test_forbidden_funcs_in_rule(self):
        det = self._make_detector()
        sample = self._make_sample(["202301", "202302", "202303", "202304", "202312"])
        rules = det.detect("mydb", [sample])
        yyyymm_rule = next(r for r in rules if "text_date_yyyymm" in r["rule_id"])
        forbidden = yyyymm_rule["forbidden_funcs"]
        # forbidden may be a list or a JSON string
        if isinstance(forbidden, str):
            import json
            forbidden = json.loads(forbidden)
        assert "EXTRACT" in forbidden
        assert "BETWEEN" in forbidden

    def test_required_func_in_rule(self):
        det = self._make_detector()
        sample = self._make_sample(["202301", "202302", "202303", "202304", "202312"])
        rules = det.detect("mydb", [sample])
        yyyymm_rule = next(r for r in rules if "text_date_yyyymm" in r["rule_id"])
        assert yyyymm_rule["required_func"] == "SUBSTR"

    def test_min_match_ratio(self):
        """Mixed values should not trigger detection."""
        det = self._make_detector()
        # Only 40% YYYYMM — below threshold (0.8)
        sample = self._make_sample(["202301", "202302", "hello", "world", "test"])
        rules = det.detect("mydb", [sample])
        yyyymm_rules = [r for r in rules if "text_date_yyyymm" in r["rule_id"]]
        assert yyyymm_rules == []


# ══════════════════════════════════════════════════════════════
# RelationCollector — SQL file parsing
# ══════════════════════════════════════════════════════════════

class TestRelationCollector:

    def _make_collector(self) -> RelationCollector:
        config   = AppConfig()
        registry = make_registry({
            "schema_catalog": [
                {"table_name": "orders",    "schema_name": "public"},
                {"table_name": "customers", "schema_name": "public"},
                {"table_name": "regions",   "schema_name": "public"},
            ]
        })
        # Mock target DB connection
        target_conn = MagicMock()
        target_conn.__enter__ = MagicMock(return_value=target_conn)
        target_conn.__exit__  = MagicMock(return_value=False)
        target_conn.execute.return_value = []
        target_mgr = MagicMock()
        target_mgr.connection.return_value = target_conn
        registry.target.return_value = target_mgr
        return RelationCollector(registry, config)

    def test_parse_sql_file_with_annotation(self, tmp_path):
        sql_content = """
-- @relation orders -> customers : 주문-고객
-- @relation orders -> regions   : 주문-지역
SELECT c.name, r.region_name, SUM(o.amount)
FROM orders o
JOIN customers c ON o.customer_id = c.id
JOIN regions r ON o.region_code = r.code
GROUP BY c.name, r.region_name
"""
        f = tmp_path / "sales.sql"
        f.write_text(sql_content)

        collector = self._make_collector()
        schema_map = {"orders": "public", "customers": "public", "regions": "public"}
        candidates = collector._parse_sql_file(
            sql_content, "mydb", schema_map, str(f)
        )

        tables = {(c.from_table, c.to_table) for c in candidates}
        assert ("orders", "customers") in tables
        assert ("orders", "regions")   in tables

    def test_parse_sql_file_relations_extracted(self, tmp_path):
        sql_content = """
SELECT * FROM orders o
JOIN customers c ON o.customer_id = c.id
"""
        collector = self._make_collector()
        schema_map = {"orders": "public", "customers": "public"}
        candidates = collector._parse_sql_file(sql_content, "mydb", schema_map, "test.sql")
        assert any(
            c.from_table == "orders" and c.to_table == "customers"
            for c in candidates
        )

    def test_relation_candidate_to_edge(self):
        c = RelationCandidate(
            from_db_alias="mydb", from_schema="public",
            from_table="orders", from_column="customer_id",
            to_db_alias="mydb", to_schema="public",
            to_table="customers", to_column="id",
            relation_type="file", confidence=0.95,
            auto_approve=True,
        )
        edge = c.to_edge()
        assert edge.from_table   == "orders"
        assert edge.to_table     == "customers"
        assert edge.approved     is True
        assert edge.is_cross_db  is False

    def test_cross_db_edge(self):
        c = RelationCandidate(
            from_db_alias="mydb",      from_schema="public",
            from_table="orders",       from_column="customer_id",
            to_db_alias="warehouse",   to_schema="dw",
            to_table="customers",      to_column="id",
            relation_type="manual",    confidence=1.0,
        )
        edge = c.to_edge()
        assert edge.is_cross_db is True


# ══════════════════════════════════════════════════════════════
# RuleEngine
# ══════════════════════════════════════════════════════════════

class TestRuleEngine:

    def _make_engine(self, rules=None) -> RuleEngine:
        rules = rules or []
        registry = make_registry({"dialect_rules": rules})
        return RuleEngine(registry)

    def test_build_prompt_injection_empty(self):
        engine = self._make_engine()
        result = engine.build_prompt_injection([])
        assert result == ""

    def test_build_prompt_injection(self):
        rules = [{
            "rule_id": "text_date_yyyymm",
            "scope": "column",
            "table_name": "orders",
            "column_name": "order_date",
            "db_alias": "mydb",
            "forbidden_funcs": '["EXTRACT","BETWEEN"]',
            "required_func": "SUBSTR",
            "instruction": "YYYYMM TEXT — SUBSTR 사용",
            "example_bad":  "EXTRACT(YEAR FROM order_date)",
            "example_good": "SUBSTR(order_date, 1, 4) = '2023'",
            "severity": "error",
        }]
        engine = self._make_engine()
        prompt = engine.build_prompt_injection(rules)
        assert "SUBSTR" in prompt
        assert "EXTRACT" in prompt
        assert "규칙" in prompt

    def test_to_validation_rules(self):
        rules = [{
            "rule_id": "test_rule",
            "scope": "column",
            "dialect": "postgresql",
            "db_alias": "mydb",
            "schema_name": "public",
            "table_name": "orders",
            "column_name": "order_date",
            "forbidden_funcs": '["EXTRACT"]',
            "required_func": "SUBSTR",
            "instruction": "Use SUBSTR",
            "example_bad": "", "example_good": "",
            "severity": "error",
        }]
        engine = self._make_engine()
        vr = engine.to_validation_rules(rules)
        assert len(vr) == 1
        assert vr[0].rule_id == "test_rule"
        assert "EXTRACT" in vr[0].forbidden_funcs

    def test_scope_label_column(self):
        rule = {"scope": "column", "table_name": "orders", "column_name": "date"}
        assert "orders.date" == RuleEngine._scope_label(rule)

    def test_scope_label_global(self):
        rule = {"scope": "global"}
        assert "global" == RuleEngine._scope_label(rule)


# ══════════════════════════════════════════════════════════════
# DynamicPatternEngine — matching
# ══════════════════════════════════════════════════════════════

class TestDynamicPatternEngine:

    def _make_engine(self, patterns=None) -> DynamicPatternEngine:
        patterns = patterns or []
        registry = make_registry({"sql_patterns": patterns})
        return DynamicPatternEngine(registry)

    def test_match_empty(self):
        engine = self._make_engine()
        results = engine.match("매출 현황은?", "mydb")
        assert results == []

    def test_match_top_n_pattern(self):
        patterns = [{
            "id": "uuid-1",
            "name": "top_n_then_detail",
            "detect_keywords": '["상위","top","최고"]',
            "detect_exclusions": '[]',
            "instruction": "inline view에 LIMIT",
            "example_bad": "",
            "example_good": "",
            "db_alias": None,
            "hit_count": 10,
        }]
        # match() calls: SELECT * FROM sql_patterns WHERE enabled...
        registry = make_registry({"sql_patterns": patterns})
        engine = DynamicPatternEngine(registry)
        results = engine.match("매출 상위 5개 부서의 월별 합계는?", "mydb")
        assert len(results) >= 1
        assert results[0].name == "top_n_then_detail"

    def test_score_with_exclusions(self):
        engine = self._make_engine()
        pattern = {
            "detect_keywords": '["상위","top"]',
            "detect_exclusions": '["단순","simple"]',
        }
        # No exclusions hit
        score1 = engine._score_pattern("매출 상위 5개", pattern)
        # Exclusion hit
        score2 = engine._score_pattern("매출 상위 5개 단순 조회", pattern)
        assert score1 > score2

    def test_build_prompt_injection(self):
        from pgxllm.intelligence.pattern_engine import MatchedPattern
        patterns = [MatchedPattern(
            pattern_id="1", name="top_n_then_detail",
            score=0.9, instruction="CTE 사용",
            example_bad="bad", example_good="good",
        )]
        engine = self._make_engine()
        prompt = engine.build_prompt_injection(patterns)
        assert "CTE" in prompt
        assert "bad" in prompt
        assert "good" in prompt

    def test_build_prompt_injection_empty(self):
        engine = self._make_engine()
        assert engine.build_prompt_injection([]) == ""


# ══════════════════════════════════════════════════════════════
# PostgreSQLGraphStore — BFS + join hint
# ══════════════════════════════════════════════════════════════

class TestPostgreSQLGraphStore:

    def _make_store(self, path_rows=None, edge_rows=None) -> PostgreSQLGraphStore:
        rows_map = {}
        if path_rows:
            rows_map["graph_paths"] = path_rows
        if edge_rows:
            rows_map["graph_edges"] = edge_rows
        registry = make_registry(rows_map)
        return PostgreSQLGraphStore(registry, max_depth=4)

    def test_find_paths_returns_results(self):
        path_rows = [{
            "path_json": json.dumps([
                {"db":"mydb","schema":"public","table":"orders","column":"customer_id"},
                {"db":"mydb","schema":"public","table":"customers","column":"id"},
            ]),
            "hop_count":    1,
            "total_weight": 100,
            "join_hint":    "orders.customer_id = customers.id",
            "is_cross_db":  False,
        }]
        store = self._make_store(path_rows=path_rows)
        paths = store.find_paths("mydb.public.orders", "mydb.public.customers")
        assert len(paths) == 1
        assert paths[0].hop_count == 1
        assert "customer_id" in paths[0].join_hint

    def test_find_paths_empty(self):
        store = self._make_store()
        paths = store.find_paths("mydb.public.a", "mydb.public.b")
        assert paths == []

    def test_build_join_hint(self):
        path_json = [
            {"db":"mydb","schema":"public","table":"orders",   "column":"customer_id"},
            {"db":"mydb","schema":"public","table":"customers","column":"id"},
        ]
        hint = PostgreSQLGraphStore._build_join_hint(path_json)
        assert hint == "orders.customer_id = customers.id"

    def test_build_join_hint_multi_hop(self):
        path_json = [
            {"db":"mydb","schema":"public","table":"orders",    "column":"item_id"},
            {"db":"mydb","schema":"public","table":"items",     "column":"id"},
            # items.category_id → categories.id
        ]
        hint = PostgreSQLGraphStore._build_join_hint(path_json)
        assert "orders.item_id = items.id" in hint

    def test_is_cross_db(self):
        assert PostgreSQLGraphStore._is_cross_db("mydb.public.a", "warehouse.dw.b") is True
        assert PostgreSQLGraphStore._is_cross_db("mydb.public.a", "mydb.public.b")  is False

    def test_bfs_simple(self):
        """BFS with simple adjacency: orders → customers → regions"""
        store = self._make_store()
        adjacency = {
            "mydb.public.orders": [
                ({"from_db_alias":"mydb","from_schema":"public","from_table":"orders",
                  "from_column":"customer_id","to_db_alias":"mydb","to_schema":"public",
                  "to_table":"customers","to_column":"id","call_count":50},
                 "mydb.public.customers"),
            ],
            "mydb.public.customers": [
                ({"from_db_alias":"mydb","from_schema":"public","from_table":"orders",
                  "from_column":"customer_id","to_db_alias":"mydb","to_schema":"public",
                  "to_table":"customers","to_column":"id","call_count":50},
                 "mydb.public.orders"),
                ({"from_db_alias":"mydb","from_schema":"public","from_table":"customers",
                  "from_column":"region_id","to_db_alias":"mydb","to_schema":"public",
                  "to_table":"regions","to_column":"id","call_count":30},
                 "mydb.public.regions"),
            ],
            "mydb.public.regions": [
                ({"from_db_alias":"mydb","from_schema":"public","from_table":"customers",
                  "from_column":"region_id","to_db_alias":"mydb","to_schema":"public",
                  "to_table":"regions","to_column":"id","call_count":30},
                 "mydb.public.customers"),
            ],
        }
        paths = store._bfs("mydb.public.orders", adjacency, max_depth=4)
        destinations = {p["to_address"] for p in paths}
        assert "mydb.public.customers" in destinations
        assert "mydb.public.regions"   in destinations   # 2-hop path
