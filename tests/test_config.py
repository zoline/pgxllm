"""
tests/test_config.py
--------------------
Unit tests for AppConfig and ConnectionRegistry.
No real DB connection required — uses mocking.
"""
import os
import textwrap
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml

from pgxllm.config import (
    AppConfig,
    GlobalBlacklist,
    InternalDBConfig,
    TargetDBConfig,
    invalidate_config,
    load_config,
)
from pgxllm.db.connections import ConnectionRegistry, TableAddress


# ══════════════════════════════════════════════════════════════
# AppConfig loading
# ══════════════════════════════════════════════════════════════

class TestAppConfig:

    def test_defaults_no_file(self):
        """Config loads with defaults when no file exists."""
        cfg = load_config(path=Path("/nonexistent/path.yaml"))
        assert cfg.internal_db.dbname == "pgxllm"
        assert cfg.internal_db.schema == "pgxllm"
        assert cfg.target_dbs == []
        assert cfg.cross_db_relations.enabled is True

    def test_internal_db_dsn(self):
        cfg = AppConfig(internal_db=InternalDBConfig(
            host="db-host", port=5433, user="admin",
            password="secret", dbname="pgxllm_meta"
        ))
        dsn = cfg.internal_db.dsn
        assert "host=db-host" in dsn
        assert "port=5433" in dsn
        assert "user=admin" in dsn
        assert "dbname=pgxllm_meta" in dsn

    def test_internal_db_url(self):
        cfg = AppConfig(internal_db=InternalDBConfig(
            host="localhost", port=5432, user="u", password="p", dbname="pgxllm"
        ))
        assert cfg.internal_db.url == "postgresql://u:p@localhost:5432/pgxllm"

    def test_internal_db_url_no_password(self):
        cfg = AppConfig(internal_db=InternalDBConfig(
            host="localhost", port=5432, user="u", password="", dbname="pgxllm"
        ))
        assert cfg.internal_db.url == "postgresql://u@localhost:5432/pgxllm"

    def test_target_db_registration(self):
        cfg = AppConfig(target_dbs=[
            TargetDBConfig(alias="mydb",   host="host-a", port=5432,
                           user="u", dbname="mydb"),
            TargetDBConfig(alias="warehouse", host="host-b", port=5433,
                           user="u", dbname="wh"),
        ])
        assert len(cfg.target_dbs) == 2
        db = cfg.get_target_db("mydb")
        assert db is not None
        assert db.host == "host-a"

    def test_get_target_db_not_found(self):
        cfg = AppConfig()
        assert cfg.get_target_db("nonexistent") is None

    def test_get_target_db_required_raises(self):
        cfg = AppConfig()
        with pytest.raises(KeyError, match="nonexistent"):
            cfg.get_target_db_required("nonexistent")

    def test_target_db_default_dbname(self):
        """dbname defaults to alias when not specified."""
        db = TargetDBConfig(alias="mydb", host="localhost", user="u")
        assert db.dbname == "mydb"

    def test_env_var_expansion(self, tmp_path, monkeypatch):
        """${VAR:-default} patterns are expanded from environment."""
        monkeypatch.setenv("TEST_PG_HOST", "prod-host")
        monkeypatch.setenv("TEST_PG_PORT", "5433")

        config_yaml = textwrap.dedent("""
            internal_db:
              host: ${TEST_PG_HOST:-localhost}
              port: ${TEST_PG_PORT:-5432}
              dbname: pgxllm
        """)
        config_file = tmp_path / "test.yaml"
        config_file.write_text(config_yaml)

        cfg = load_config(path=config_file)
        assert cfg.internal_db.host == "prod-host"
        assert cfg.internal_db.port == 5433

    def test_env_var_default_used(self, tmp_path):
        """${VAR:-default} uses default when env var is absent."""
        config_yaml = textwrap.dedent("""
            internal_db:
              host: ${UNDEFINED_VAR_XYZ:-fallback-host}
              dbname: pgxllm
        """)
        config_file = tmp_path / "test.yaml"
        config_file.write_text(config_yaml)

        cfg = load_config(path=config_file)
        assert cfg.internal_db.host == "fallback-host"

    def test_load_full_yaml(self, tmp_path):
        """Full YAML config with multiple target DBs loads correctly."""
        config_yaml = textwrap.dedent("""
            internal_db:
              host: internal-host
              port: 5432
              user: pgxllm_user
              dbname: pgxllm_meta
              schema: pgxllm

            target_dbs:
              - alias: mydb
                host: app-host
                port: 5432
                user: app_user
                dbname: mydb
                schema_mode: include
                schemas:
                  - public
                  - sales

              - alias: warehouse
                host: wh-host
                port: 5433
                user: wh_user
                dbname: warehouse
                schema_mode: exclude
                schemas:
                  - pg_catalog
                  - information_schema

            cross_db_relations:
              enabled: true

            llm:
              provider: ollama
              model: qwen2.5-coder:32b
        """)
        config_file = tmp_path / "test.yaml"
        config_file.write_text(config_yaml)

        cfg = load_config(path=config_file)
        assert cfg.internal_db.host == "internal-host"
        assert cfg.internal_db.schema == "pgxllm"
        assert len(cfg.target_dbs) == 2

        mydb = cfg.get_target_db("mydb")
        assert mydb.schema_mode == "include"
        assert "public" in mydb.schemas
        assert "sales" in mydb.schemas

        wh = cfg.get_target_db("warehouse")
        assert wh.schema_mode == "exclude"
        assert wh.port == 5433
        assert cfg.llm.model == "qwen2.5-coder:32b"


# ══════════════════════════════════════════════════════════════
# TargetDBConfig schema filtering
# ══════════════════════════════════════════════════════════════

class TestTargetDBSchemaFiltering:

    def test_include_mode(self):
        db = TargetDBConfig(
            alias="mydb", host="localhost", user="u",
            schema_mode="include",
            schemas=["public", "sales"],
        )
        all_schemas = ["information_schema", "pg_catalog", "public", "sales", "hr"]
        effective = db.effective_schemas(all_schemas)
        assert effective == ["public", "sales"]

    def test_exclude_mode(self):
        db = TargetDBConfig(
            alias="mydb", host="localhost", user="u",
            schema_mode="exclude",
            schemas=["pg_catalog", "information_schema", "pg_toast"],
        )
        all_schemas = ["information_schema", "pg_catalog", "pg_toast", "public", "sales"]
        effective = db.effective_schemas(all_schemas)
        assert "public"  in effective
        assert "sales"   in effective
        assert "pg_catalog"         not in effective
        assert "information_schema" not in effective

    def test_include_preserves_order(self):
        db = TargetDBConfig(
            alias="mydb", host="localhost", user="u",
            schema_mode="include",
            schemas=["hr", "sales", "public"],
        )
        all_schemas = ["public", "sales", "hr", "pg_catalog"]
        effective = db.effective_schemas(all_schemas)
        # preserves all_schemas order
        assert effective == ["public", "sales", "hr"]


# ══════════════════════════════════════════════════════════════
# Blacklist merging
# ══════════════════════════════════════════════════════════════

class TestBlacklistMerge:

    def test_global_and_per_db(self):
        cfg = AppConfig(
            sample_data={
                "blacklist": {
                    "tables":   ["audit_logs"],
                    "columns":  ["users.password_hash"],
                    "patterns": ["*_secret"],
                }
            }
        )
        target = TargetDBConfig(
            alias="mydb", host="localhost", user="u",
            blacklist_tables=["temp_data"],
            blacklist_columns=["orders.card_number"],
            blacklist_patterns=["*_token"],
        )
        merged = cfg.merge_blacklist(target)

        assert "audit_logs"             in merged.tables
        assert "temp_data"              in merged.tables
        assert "users.password_hash"    in merged.columns
        assert "orders.card_number"     in merged.columns
        assert "*_secret"               in merged.patterns
        assert "*_token"                in merged.patterns

    def test_deduplication(self):
        """Same pattern in both global and per-DB should appear once."""
        cfg = AppConfig(
            sample_data={"blacklist": {"patterns": ["*_hash"], "tables": [], "columns": []}}
        )
        target = TargetDBConfig(
            alias="mydb", host="localhost", user="u",
            blacklist_patterns=["*_hash"],  # duplicate
        )
        merged = cfg.merge_blacklist(target)
        assert merged.patterns.count("*_hash") == 1


# ══════════════════════════════════════════════════════════════
# TableAddress
# ══════════════════════════════════════════════════════════════

class TestTableAddress:

    def test_parse_full(self):
        addr = TableAddress.parse("mydb.public.orders")
        assert addr.db_alias == "mydb"
        assert addr.schema   == "public"
        assert addr.table    == "orders"

    def test_parse_schema_table(self):
        addr = TableAddress.parse("public.orders", default_alias="mydb")
        assert addr.db_alias == "mydb"
        assert addr.schema   == "public"
        assert addr.table    == "orders"

    def test_parse_table_only(self):
        addr = TableAddress.parse("orders", default_alias="mydb")
        assert addr.db_alias == "mydb"
        assert addr.schema   == "public"
        assert addr.table    == "orders"

    def test_qualified(self):
        addr = TableAddress(db_alias="mydb", schema="public", table="orders")
        assert addr.qualified   == "mydb.public.orders"
        assert addr.schema_table == "public.orders"

    def test_cross_db_equality(self):
        a1 = TableAddress.parse("mydb.public.orders")
        a2 = TableAddress.parse("mydb.public.orders")
        a3 = TableAddress.parse("warehouse.dw.orders")
        assert a1 == a2
        assert a1 != a3

    def test_hashable(self):
        addr = TableAddress.parse("mydb.public.orders")
        s = {addr}
        assert len(s) == 1


# ══════════════════════════════════════════════════════════════
# ConnectionRegistry (mocked — no real DB)
# ══════════════════════════════════════════════════════════════

class TestConnectionRegistry:

    def _make_registry(self) -> ConnectionRegistry:
        cfg = AppConfig(
            internal_db=InternalDBConfig(host="localhost", dbname="pgxllm"),
            target_dbs=[
                TargetDBConfig(alias="mydb",      host="host-a", user="u", dbname="mydb"),
                TargetDBConfig(alias="warehouse",  host="host-b", user="u", dbname="wh"),
            ]
        )
        return ConnectionRegistry(cfg)

    def test_target_aliases(self):
        reg = self._make_registry()
        aliases = reg.target_aliases()
        assert "mydb"     in aliases
        assert "warehouse" in aliases

    def test_get_target(self):
        reg = self._make_registry()
        mgr = reg.target("mydb")
        assert mgr.alias == "mydb"

    def test_get_nonexistent_target_raises(self):
        reg = self._make_registry()
        with pytest.raises(KeyError, match="nonexistent"):
            reg.target("nonexistent")

    def test_has_target(self):
        reg = self._make_registry()
        assert reg.has_target("mydb")      is True
        assert reg.has_target("nope")      is False

    def test_register_new_target(self):
        reg = self._make_registry()
        new_db = TargetDBConfig(alias="analytics", host="host-c", user="u", dbname="analytics")
        reg.register_target(new_db)
        assert reg.has_target("analytics")
        assert reg.target("analytics").alias == "analytics"

    def test_unregister_target(self):
        reg = self._make_registry()
        reg.unregister_target("mydb")
        assert not reg.has_target("mydb")
        with pytest.raises(KeyError):
            reg.target("mydb")

    def test_all_targets(self):
        reg = self._make_registry()
        targets = reg.all_targets()
        assert len(targets) == 2

    def test_cross_db_config_propagates(self):
        cfg = AppConfig(
            cross_db_relations={"enabled": True},
            target_dbs=[
                TargetDBConfig(alias="db1", host="h1", user="u", dbname="db1"),
                TargetDBConfig(alias="db2", host="h2", user="u", dbname="db2"),
            ]
        )
        reg = ConnectionRegistry(cfg)
        assert cfg.cross_db_relations.enabled is True
        # Both DBs have independent managers
        assert reg.target("db1").config.host == "h1"
        assert reg.target("db2").config.host == "h2"

    @patch("pgxllm.db.connections.PgPool.test_connection", return_value=True)
    def test_test_all(self, mock_test):
        reg = self._make_registry()
        results = reg.test_all()
        assert "__internal__" in results
        assert "mydb"          in results
        assert "warehouse"     in results
        assert all(results.values())
