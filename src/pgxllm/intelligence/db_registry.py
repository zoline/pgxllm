"""
pgxllm.intelligence.db_registry
---------------------------------
DBRegistryService — target DB 등록, 조회, 삭제, 상태 관리.
"""
from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from pgxllm.config import AppConfig, TargetDBConfig
from pgxllm.db.connections import ConnectionRegistry, InternalDBManager

log = logging.getLogger(__name__)


def _as_list(val) -> list:
    """JSONB 컬럼 값을 list로 반환. psycopg2가 이미 변환했거나 문자열인 경우 모두 처리."""
    if isinstance(val, list):
        return val
    return json.loads(val or "[]")


@dataclass
class DBStatus:
    alias:               str
    host:                str
    port:                int
    dbname:              str
    schema_mode:         str
    schemas:             list
    schema_version_hash: Optional[str]
    last_refresh_at:     Optional[datetime]
    is_active:           bool
    is_reachable:        bool
    table_count:         int = 0
    column_count:        int = 0


class DBRegistryService:
    def __init__(self, registry: ConnectionRegistry):
        self._registry = registry
        self._internal: InternalDBManager = registry.internal

    def register(self, cfg: TargetDBConfig, *, overwrite: bool = False) -> None:
        existing = self._get_row(cfg.alias)
        with self._internal.connection() as conn:
            if existing and not overwrite:
                raise ValueError(
                    f"DB '{cfg.alias}' already registered. Use overwrite=True."
                )
            row = {
                "alias": cfg.alias, "host": cfg.host, "port": cfg.port,
                "db_user": cfg.user, "db_password": cfg.password,
                "dbname": cfg.dbname,
                "schema_mode": cfg.schema_mode,
                "schemas":            json.dumps(cfg.schemas),
                "blacklist_tables":   json.dumps(cfg.blacklist_tables),
                "blacklist_columns":  json.dumps(cfg.blacklist_columns),
                "blacklist_patterns": json.dumps(cfg.blacklist_patterns),
                "is_active": True,
            }
            if existing:
                conn.execute("""
                    UPDATE db_registry SET
                        host=%(host)s, port=%(port)s, db_user=%(db_user)s,
                        db_password=%(db_password)s,
                        dbname=%(dbname)s, schema_mode=%(schema_mode)s,
                        schemas=%(schemas)s::jsonb,
                        blacklist_tables=%(blacklist_tables)s::jsonb,
                        blacklist_columns=%(blacklist_columns)s::jsonb,
                        blacklist_patterns=%(blacklist_patterns)s::jsonb,
                        is_active=%(is_active)s, updated_at=NOW()
                    WHERE alias=%(alias)s
                """, row)
            else:
                conn.execute("""
                    INSERT INTO db_registry
                        (alias,host,port,db_user,db_password,dbname,schema_mode,
                         schemas,blacklist_tables,blacklist_columns,blacklist_patterns,is_active)
                    VALUES
                        (%(alias)s,%(host)s,%(port)s,%(db_user)s,%(db_password)s,
                         %(dbname)s,%(schema_mode)s,
                         %(schemas)s::jsonb,%(blacklist_tables)s::jsonb,
                         %(blacklist_columns)s::jsonb,%(blacklist_patterns)s::jsonb,%(is_active)s)
                """, row)
        self._registry.register_target(cfg)
        log.info("Registered DB: %s", cfg.alias)

    def list_all(self, active_only: bool = False) -> list:
        with self._internal.connection() as conn:
            sql = "SELECT * FROM db_registry"
            if active_only:
                sql += " WHERE is_active=TRUE"
            sql += " ORDER BY alias"
            rows = conn.execute(sql)
        result = []
        for row in rows:
            reachable = False
            try:
                if self._registry.has_target(row["alias"]):
                    reachable = self._registry.target(row["alias"]).test_connection()
            except Exception:
                pass
            result.append(DBStatus(
                alias=row["alias"], host=row["host"], port=row["port"],
                dbname=row["dbname"], schema_mode=row["schema_mode"],
                schemas=_as_list(row["schemas"]),
                schema_version_hash=row["schema_version_hash"],
                last_refresh_at=row["last_refresh_at"],
                is_active=row["is_active"], is_reachable=reachable,
            ))
        return result

    def get_status(self, alias: str) -> Optional[DBStatus]:
        return next((s for s in self.list_all() if s.alias == alias), None)

    def get_required(self, alias: str) -> "TargetDBConfig":
        """Return TargetDBConfig for alias, raising KeyError if not found."""
        row = self._get_row(alias)
        if not row:
            raise KeyError(
                f"Target DB '{alias}' not registered. "
                f"Use: pgxllm db register --alias {alias} ..."
            )
        return TargetDBConfig(
            alias=row["alias"], host=row["host"], port=row["port"],
            user=row["db_user"], password=row.get("db_password", ""),
            dbname=row["dbname"],
            schema_mode=row["schema_mode"],
            schemas=_as_list(row["schemas"]),
            blacklist_tables=_as_list(row["blacklist_tables"]),
            blacklist_columns=_as_list(row["blacklist_columns"]),
            blacklist_patterns=_as_list(row["blacklist_patterns"]),
        )

    def remove(self, alias: str) -> None:
        with self._internal.connection() as conn:
            conn.execute("DELETE FROM schema_catalog WHERE db_alias=%s", [alias])
            conn.execute("DELETE FROM graph_nodes    WHERE db_alias=%s", [alias])
            conn.execute(
                "DELETE FROM graph_edges WHERE from_db_alias=%s OR to_db_alias=%s",
                [alias, alias]
            )
            conn.execute("DELETE FROM db_registry WHERE alias=%s", [alias])
        if self._registry.has_target(alias):
            self._registry.unregister_target(alias)
        log.info("Removed DB: %s", alias)

    def update_hash(self, alias: str, hash_val: str) -> None:
        with self._internal.connection() as conn:
            conn.execute("""
                UPDATE db_registry
                SET schema_version_hash=%s, last_refresh_at=NOW(), updated_at=NOW()
                WHERE alias=%s
            """, [hash_val, alias])

    def compute_schema_hash(self, alias: str) -> str:
        mgr = self._registry.target(alias)
        effective = mgr.get_effective_schemas()
        with mgr.connection() as conn:
            rows = conn.execute("""
                SELECT table_schema, table_name, column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = ANY(%s)
                ORDER BY table_schema, table_name, ordinal_position
            """, [effective])
        fingerprint = json.dumps(
            [(r["table_schema"], r["table_name"], r["column_name"], r["data_type"])
             for r in rows],
            sort_keys=True,
        )
        return hashlib.sha256(fingerprint.encode()).hexdigest()[:16]

    def blacklist_add(self, alias: str, *, table=None, column=None, pattern=None) -> None:
        row = self._get_row(alias)
        if not row:
            raise KeyError(f"DB '{alias}' not registered.")
        for field, value in [
            ("blacklist_tables", table),
            ("blacklist_columns", column),
            ("blacklist_patterns", pattern),
        ]:
            if value is None:
                continue
            current = _as_list(row.get(field))
            if value not in current:
                current.append(value)
                with self._internal.connection() as conn:
                    conn.execute(
                        f"UPDATE db_registry SET {field}=%s::jsonb, updated_at=NOW() WHERE alias=%s",
                        [json.dumps(current), alias]
                    )

    def blacklist_list(self, alias: str) -> dict:
        row = self._get_row(alias)
        if not row:
            raise KeyError(f"DB '{alias}' not registered.")
        return {
            "tables":   _as_list(row.get("blacklist_tables")),
            "columns":  _as_list(row.get("blacklist_columns")),
            "patterns": _as_list(row.get("blacklist_patterns")),
        }

    def blacklist_remove(self, alias: str, *, table=None, column=None, pattern=None) -> None:
        row = self._get_row(alias)
        if not row:
            raise KeyError(f"DB '{alias}' not registered.")
        for field, value in [
            ("blacklist_tables", table),
            ("blacklist_columns", column),
            ("blacklist_patterns", pattern),
        ]:
            if value is None:
                continue
            current = _as_list(row.get(field))
            if value in current:
                current.remove(value)
                with self._internal.connection() as conn:
                    conn.execute(
                        f"UPDATE db_registry SET {field}=%s::jsonb, updated_at=NOW() WHERE alias=%s",
                        [json.dumps(current), alias]
                    )

    def load_registered_to_config(self, config: AppConfig) -> None:
        with self._internal.connection() as conn:
            rows = conn.execute(
                "SELECT * FROM db_registry WHERE is_active=TRUE ORDER BY alias"
            )
        for row in rows:
            try:
                cfg = TargetDBConfig(
                    alias=row["alias"], host=row["host"], port=row["port"],
                    user=row["db_user"], password=row.get("db_password", ""),
                    dbname=row["dbname"],
                    schema_mode=row["schema_mode"],
                    schemas=_as_list(row["schemas"]),
                    blacklist_tables=_as_list(row["blacklist_tables"]),
                    blacklist_columns=_as_list(row["blacklist_columns"]),
                    blacklist_patterns=_as_list(row["blacklist_patterns"]),
                )
                if not self._registry.has_target(cfg.alias):
                    self._registry.register_target(cfg)
            except Exception as e:
                log.warning("Failed to load DB '%s': %s", row["alias"], e)

    def _get_row(self, alias: str) -> Optional[dict]:
        with self._internal.connection() as conn:
            return conn.execute_one(
                "SELECT * FROM db_registry WHERE alias=%s", [alias]
            )
