"""
pgxllm.intelligence.schema_catalog
------------------------------------
SchemaCatalogBuilder — pg_catalog 스캔 → pgxllm.schema_catalog 저장.

수행 내용:
  1. information_schema / pg_catalog 에서 테이블·컬럼·FK 메타데이터 수집
  2. pg_description 에서 코멘트 수집
  3. pg_stats 에서 n_distinct 등 통계 수집
  4. schema_catalog 테이블에 upsert
  5. schema version hash 계산 (테이블·컬럼 구조 변화 감지용)
"""
from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass, field
from typing import Optional

from pgxllm.config import AppConfig, TargetDBConfig
from pgxllm.db.connections import ConnectionRegistry, TargetDBManager

log = logging.getLogger(__name__)


# ── Data classes ──────────────────────────────────────────────

@dataclass
class ColumnMeta:
    schema_name:    str
    table_name:     str
    column_name:    str
    data_type:      str
    is_nullable:    bool
    column_default: Optional[str]
    comment_text:   Optional[str]
    n_distinct:     Optional[float]
    is_pk:          bool = False
    is_fk:          bool = False
    fk_ref_table:   Optional[str] = None   # schema.table
    fk_ref_column:  Optional[str] = None


@dataclass
class TableMeta:
    schema_name:  str
    table_name:   str
    comment_text: Optional[str]
    columns:      list[ColumnMeta] = field(default_factory=list)


# ── Builder ───────────────────────────────────────────────────

class SchemaCatalogBuilder:
    """
    Scans a target DB's pg_catalog and stores the result in
    pgxllm.schema_catalog.

    Usage::

        builder = SchemaCatalogBuilder(registry, config)
        hash_ = builder.build(target_cfg, table_filter=None)
    """

    def __init__(self, registry: ConnectionRegistry, config: AppConfig):
        self._registry = registry
        self._config   = config

    def build(
        self,
        target,
        *,
        table_filter: Optional[list[str]] = None,
    ) -> str:
        """
        Scan target DB and upsert into pgxllm.schema_catalog.

        Args:
            target:       TargetDBConfig or alias string
            table_filter: if given, only refresh these tables (schema.table)

        Returns:
            schema version hash (hex string)
        """
        # Accept alias string as well as TargetDBConfig
        if isinstance(target, str):
            alias = target
            # Try registry config first (no DB connection needed)
            cfg = self._registry._config.get_target_db(alias) if hasattr(self._registry, '_config') else None
            if cfg is None:
                # Fall back to DB lookup
                from pgxllm.intelligence.db_registry import DBRegistryService
                cfg = DBRegistryService(self._registry).get_required(alias)
            target = cfg

        mgr = self._registry.target(target.alias)
        effective_schemas = mgr.get_effective_schemas()

        log.info(
            "Building schema catalog for %s (schemas: %s)",
            target.alias, effective_schemas
        )

        tables = self._fetch_tables(mgr, effective_schemas, table_filter)
        columns = self._fetch_columns(mgr, effective_schemas, table_filter)
        pks = self._fetch_primary_keys(mgr, effective_schemas)
        fks = self._fetch_foreign_keys(mgr, effective_schemas)
        stats = self._fetch_stats(mgr, effective_schemas)

        # pks is now a set of tuples, fks is now a dict
        pk_set = pks   # already a set of (schema, table, col)
        fk_map = fks   # already a dict keyed by (schema, table, col)

        # Build schema_hash from table+column structure
        hash_input = json.dumps({
            "tables": [(t["table_schema"], t["table_name"]) for t in tables],
            "columns": [
                (c["table_schema"], c["table_name"], c["column_name"], c["data_type"])
                for c in columns
            ],
        }, sort_keys=True)
        schema_hash = hashlib.sha256(hash_input.encode()).hexdigest()[:16]

        # Upsert into pgxllm.schema_catalog
        with self._registry.internal.connection() as conn:
            # Delete old entries for this DB (rebuild)
            if table_filter:
                for tf in table_filter:
                    parts = tf.split(".")
                    if len(parts) == 2:
                        conn.execute(
                            "DELETE FROM schema_catalog "
                            "WHERE db_alias=%s AND schema_name=%s AND table_name=%s",
                            (target.alias, parts[0], parts[1])
                        )
            else:
                conn.execute(
                    "DELETE FROM schema_catalog WHERE db_alias=%s",
                    (target.alias,)
                )

            # Insert table-level entries
            for t in tables:
                conn.execute(
                    """
                    INSERT INTO schema_catalog
                        (db_alias, schema_name, table_name, comment_text)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (db_alias, schema_name, table_name, column_name)
                    DO UPDATE SET comment_text=EXCLUDED.comment_text, updated_at=NOW()
                    """,
                    (target.alias, t["table_schema"], t["table_name"],
                     t.get("obj_description"))
                )

            # Insert column-level entries
            for c in columns:
                key = (c["table_schema"], c["table_name"], c["column_name"])
                is_pk  = key in pk_set
                fk_row = fk_map.get(key)
                stat   = stats.get(key)

                conn.execute(
                    """
                    INSERT INTO schema_catalog (
                        db_alias, schema_name, table_name, column_name,
                        data_type, is_nullable, column_default, comment_text,
                        n_distinct, is_pk, is_fk, fk_ref_table, fk_ref_column
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (db_alias, schema_name, table_name, column_name)
                    DO UPDATE SET
                        data_type      = EXCLUDED.data_type,
                        is_nullable    = EXCLUDED.is_nullable,
                        column_default = EXCLUDED.column_default,
                        comment_text   = EXCLUDED.comment_text,
                        n_distinct     = EXCLUDED.n_distinct,
                        is_pk          = EXCLUDED.is_pk,
                        is_fk          = EXCLUDED.is_fk,
                        fk_ref_table   = EXCLUDED.fk_ref_table,
                        fk_ref_column  = EXCLUDED.fk_ref_column,
                        updated_at     = NOW()
                    """,
                    (
                        target.alias,
                        c["table_schema"], c["table_name"], c["column_name"],
                        c["data_type"],
                        c["is_nullable"] == "YES",
                        c.get("column_default"),
                        c.get("col_description"),
                        stat["n_distinct"] if stat else None,
                        is_pk,
                        fk_row is not None,
                        f"{fk_row['foreign_schema']}.{fk_row['foreign_table']}"
                            if fk_row else None,
                        fk_row["foreign_column"] if fk_row else None,
                    )
                )

        log.info(
            "Schema catalog built: %s — %d tables, %d columns, hash=%s",
            target.alias, len(tables), len(columns), schema_hash
        )
        return schema_hash

    # ── pg_catalog queries ────────────────────────────────────

    def _fetch_tables(
        self,
        mgr: TargetDBManager,
        schemas: list[str],
        table_filter: Optional[list[str]],
    ) -> list[dict]:
        placeholders = ",".join(["%s"] * len(schemas))
        sql = f"""
            SELECT
                t.table_schema,
                t.table_name,
                obj_description(
                    (quote_ident(t.table_schema)||'.'||quote_ident(t.table_name))::regclass,
                    'pg_class'
                ) AS obj_description
            FROM information_schema.tables t
            WHERE t.table_schema IN ({placeholders})
              AND t.table_type = 'BASE TABLE'
            ORDER BY t.table_schema, t.table_name
        """
        with mgr.connection() as conn:
            rows = conn.execute(sql, schemas)

        if table_filter:
            tf_set = set(table_filter)
            rows = [
                r for r in rows
                if f"{r['table_schema']}.{r['table_name']}" in tf_set
            ]
        return rows

    def _fetch_columns(
        self,
        mgr: TargetDBManager,
        schemas: list[str],
        table_filter: Optional[list[str]],
    ) -> list[dict]:
        placeholders = ",".join(["%s"] * len(schemas))
        sql = f"""
            SELECT
                c.table_schema,
                c.table_name,
                c.column_name,
                c.data_type,
                c.is_nullable,
                c.column_default,
                col_description(
                    (quote_ident(c.table_schema)||'.'||quote_ident(c.table_name))::regclass,
                    c.ordinal_position
                ) AS col_description
            FROM information_schema.columns c
            WHERE c.table_schema IN ({placeholders})
            ORDER BY c.table_schema, c.table_name, c.ordinal_position
        """
        with mgr.connection() as conn:
            rows = conn.execute(sql, schemas)

        if table_filter:
            tf_set = set(table_filter)
            rows = [
                r for r in rows
                if f"{r['table_schema']}.{r['table_name']}" in tf_set
            ]
        return rows

    def _fetch_primary_keys(
        self, mgr: TargetDBManager, schemas: list[str]
    ) -> set[tuple]:
        """Returns set of (schema, table, column) tuples."""
        placeholders = ",".join(["%s"] * len(schemas))
        sql = f"""
            SELECT
                kcu.table_schema,
                kcu.table_name,
                kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema    = kcu.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY'
              AND tc.table_schema IN ({placeholders})
        """
        with mgr.connection() as conn:
            rows = conn.execute(sql, schemas)
        return {
            (r["table_schema"], r["table_name"], r["column_name"])
            for r in rows
        }

    def _fetch_foreign_keys(
        self, mgr: TargetDBManager, schemas: list[str]
    ) -> dict[tuple, dict]:
        """Returns dict keyed by (schema, table, column) with ref info."""
        placeholders = ",".join(["%s"] * len(schemas))
        sql = f"""
            SELECT
                kcu.table_schema,
                kcu.table_name,
                kcu.column_name,
                ccu.table_schema AS ref_schema,
                ccu.table_name   AS ref_table,
                ccu.column_name  AS ref_column
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema    = kcu.table_schema
            JOIN information_schema.constraint_column_usage ccu
              ON ccu.constraint_name = tc.constraint_name
            WHERE tc.constraint_type = 'FOREIGN KEY'
              AND tc.table_schema IN ({placeholders})
        """
        with mgr.connection() as conn:
            rows = conn.execute(sql, schemas)
        return {
            (r["table_schema"], r["table_name"], r["column_name"]): {
                "ref_table":  f"{r['ref_schema']}.{r['ref_table']}",
                "ref_column": r["ref_column"],
                "ref_schema": r["ref_schema"],
                # legacy keys kept for backward compat with refresh.py
                "foreign_schema": r["ref_schema"],
                "foreign_table":  r["ref_table"],
                "foreign_column": r["ref_column"],
            }
            for r in rows
        }

    def _fetch_stats(
        self, mgr: TargetDBManager, schemas: list[str]
    ) -> dict[tuple, dict]:
        """Fetch n_distinct from pg_stats."""
        placeholders = ",".join(["%s"] * len(schemas))
        sql = f"""
            SELECT schemaname, tablename, attname, n_distinct
            FROM pg_stats
            WHERE schemaname IN ({placeholders})
        """
        with mgr.connection() as conn:
            rows = conn.execute(sql, schemas)
        return {
            (r["schemaname"], r["tablename"], r["attname"]): r
            for r in rows
        }
