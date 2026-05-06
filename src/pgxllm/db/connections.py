"""
pgxllm.db.connections
---------------------
Database connection management.

Provides:
  - InternalDBManager  : connection pool for pgxllm metadata DB
  - TargetDBManager    : connection pool per registered target DB
  - ConnectionRegistry : top-level registry, used by the rest of the system

Design principles:
  - internal DB and target DBs are completely independent PG instances
  - each connection pool is lazily created on first use
  - context managers ensure connections are returned to pool
  - cross-DB queries are routed to their respective pools
"""
from __future__ import annotations

import contextlib
import logging
import threading
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Iterator, Optional

import psycopg2
import psycopg2.extras
import psycopg2.pool

from pgxllm.config import AppConfig, InternalDBConfig, TargetDBConfig

log = logging.getLogger(__name__)


# ── Connection wrapper ────────────────────────────────────────

class PgConnection:
    """
    Thin wrapper around a psycopg2 connection.
    Returned by context managers; auto-returns to pool on exit.
    """

    def __init__(self, conn: psycopg2.extensions.connection, pool: "PgPool"):
        self._conn = conn
        self._pool = pool

    @property
    def raw(self) -> psycopg2.extensions.connection:
        return self._conn

    def cursor(self, *, dict_cursor: bool = True):
        factory = psycopg2.extras.RealDictCursor if dict_cursor else None
        return self._conn.cursor(cursor_factory=factory)

    def execute(self, sql: str, params=None) -> list[dict]:
        with self.cursor() as cur:
            cur.execute(sql, params)
            if cur.description:
                return [dict(row) for row in cur.fetchall()]
            return []

    def execute_limited(self, sql: str, params=None, limit: int = 500) -> tuple[list[dict], bool]:
        """Execute SQL and fetch at most `limit` rows at the driver level.
        Returns (rows, truncated) — truncated=True means more rows exist beyond limit.
        The original SQL is sent to the DB unchanged (no wrapper added).
        """
        with self.cursor() as cur:
            cur.execute(sql, params)
            if cur.description:
                rows = cur.fetchmany(limit + 1)
                truncated = len(rows) > limit
                return [dict(row) for row in rows[:limit]], truncated
            return [], False

    def execute_one(self, sql: str, params=None) -> Optional[dict]:
        rows = self.execute(sql, params)
        return rows[0] if rows else None

    def commit(self) -> None:
        self._conn.commit()

    def rollback(self) -> None:
        self._conn.rollback()

    def __enter__(self) -> "PgConnection":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if exc_type:
            self.rollback()
        else:
            self.commit()
        self._pool.putconn(self._conn)


# ── Pool ─────────────────────────────────────────────────────

class PgPool:
    """
    Thread-safe psycopg2 connection pool with lazy initialisation.
    """

    def __init__(self, dsn: str, minconn: int = 1, maxconn: int = 5):
        self._dsn     = dsn
        self._minconn = minconn
        self._maxconn = maxconn
        self._pool:   Optional[psycopg2.pool.ThreadedConnectionPool] = None
        self._lock    = threading.Lock()

    def _ensure_pool(self) -> psycopg2.pool.ThreadedConnectionPool:
        if self._pool is None:
            with self._lock:
                if self._pool is None:
                    self._pool = psycopg2.pool.ThreadedConnectionPool(
                        self._minconn, self._maxconn, self._dsn
                    )
        return self._pool

    @contextmanager
    def connection(self) -> Iterator[PgConnection]:
        pool = self._ensure_pool()
        conn = pool.getconn()
        conn.autocommit = False
        try:
            yield PgConnection(conn, self)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            self.putconn(conn)

    def putconn(self, conn: psycopg2.extensions.connection) -> None:
        if self._pool:
            self._pool.putconn(conn)

    def close(self) -> None:
        if self._pool:
            self._pool.closeall()
            self._pool = None

    def test_connection(self) -> bool:
        """Return True if the DB is reachable."""
        try:
            with self.connection() as c:
                c.execute("SELECT 1")
            return True
        except Exception as e:
            log.warning("Connection test failed for %s: %s", self._dsn, e)
            return False


# ── Internal DB Manager ───────────────────────────────────────

class InternalDBManager:
    """
    Manages the pgxllm internal metadata DB connection pool.
    Provides schema-qualified helpers for pgxllm.* objects.
    """

    def __init__(self, cfg: InternalDBConfig):
        self._cfg  = cfg
        self._pool = PgPool(cfg.dsn, cfg.pool_min, cfg.pool_max)
        self.schema = cfg.schema      # default "pgxllm"

    @contextmanager
    def connection(self) -> Iterator[PgConnection]:
        with self._pool.connection() as conn:
            # Set search_path to pgxllm schema
            conn.execute(f"SET search_path TO {self.schema}, public")
            yield conn

    def q(self, name: str) -> str:
        """Return schema-qualified object name."""
        return f"{self.schema}.{name}"

    def initialize_schema(self) -> None:
        """Create pgxllm schema and all metadata tables if they don't exist."""
        from pgxllm.db.schema import INTERNAL_SCHEMA_SQL, SCHEMA_ALTER_SQL
        with self.connection() as conn:
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema}")
            for stmt in INTERNAL_SCHEMA_SQL:
                conn.execute(stmt)
            for stmt in SCHEMA_ALTER_SQL:
                conn.execute(stmt)
            conn.commit()
        log.info("Internal DB schema initialized: %s", self._cfg)

    def test_connection(self) -> bool:
        return self._pool.test_connection()

    def close(self) -> None:
        self._pool.close()

    @property
    def config(self) -> InternalDBConfig:
        return self._cfg


# ── Target DB Manager ─────────────────────────────────────────

class TargetDBManager:
    """
    Manages connection pool(s) for a single registered target DB.
    """

    def __init__(self, cfg: TargetDBConfig):
        self._cfg  = cfg
        self._pool = PgPool(cfg.dsn, cfg.pool_min, cfg.pool_max)

    @contextmanager
    def connection(self) -> Iterator[PgConnection]:
        with self._pool.connection() as conn:
            yield conn

    def get_all_schemas(self) -> list[str]:
        """Fetch all schema names from pg_catalog."""
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT schema_name FROM information_schema.schemata "
                "ORDER BY schema_name"
            )
        return [r["schema_name"] for r in rows]

    def get_effective_schemas(self) -> list[str]:
        """Apply include/exclude rules to determine schemas to analyse."""
        all_schemas = self.get_all_schemas()
        return self._cfg.effective_schemas(all_schemas)

    def test_connection(self) -> bool:
        return self._pool.test_connection()

    def close(self) -> None:
        self._pool.close()

    @property
    def config(self) -> TargetDBConfig:
        return self._cfg

    @property
    def alias(self) -> str:
        return self._cfg.alias


# ── Connection Registry ───────────────────────────────────────

class ConnectionRegistry:
    """
    Top-level registry for all DB connections in pgxllm.

    Usage::

        registry = ConnectionRegistry(app_config)

        # Internal DB
        with registry.internal.connection() as conn:
            conn.execute("SELECT ...")

        # Specific target DB
        with registry.target("mydb").connection() as conn:
            conn.execute("SELECT ...")

        # Iterate all target DBs
        for mgr in registry.all_targets():
            print(mgr.alias, mgr.get_effective_schemas())
    """

    def __init__(self, config: AppConfig):
        self._config   = config
        self._internal = InternalDBManager(config.internal_db)
        self._targets: dict[str, TargetDBManager] = {}
        self._lock     = threading.Lock()

        # Pre-register target DBs from config
        for db_cfg in config.target_dbs:
            self._targets[db_cfg.alias] = TargetDBManager(db_cfg)

    @property
    def internal(self) -> InternalDBManager:
        return self._internal

    def target(self, alias: str) -> TargetDBManager:
        """
        Get manager for a target DB by alias.
        Raises KeyError if not registered.
        """
        if alias not in self._targets:
            # Try loading from config (may have been registered at runtime)
            db_cfg = self._config.get_target_db(alias)
            if db_cfg is None:
                raise KeyError(
                    f"Target DB '{alias}' not registered. "
                    f"Use: pgxllm db register --alias {alias} ..."
                )
            with self._lock:
                if alias not in self._targets:
                    self._targets[alias] = TargetDBManager(db_cfg)
        return self._targets[alias]

    def register_target(self, cfg: TargetDBConfig) -> TargetDBManager:
        """
        Register a new target DB at runtime (e.g. from CLI or Web UI).
        Also adds to the in-memory config target_dbs list.
        """
        with self._lock:
            mgr = TargetDBManager(cfg)
            self._targets[cfg.alias] = mgr
            # Keep config in sync
            existing = {db.alias for db in self._config.target_dbs}
            if cfg.alias not in existing:
                self._config.target_dbs.append(cfg)
        log.info("Registered target DB: %s", cfg)
        return mgr

    def unregister_target(self, alias: str) -> None:
        with self._lock:
            if alias in self._targets:
                self._targets[alias].close()
                del self._targets[alias]
            self._config.target_dbs = [
                db for db in self._config.target_dbs
                if db.alias != alias
            ]

    def all_targets(self) -> list[TargetDBManager]:
        return list(self._targets.values())

    def target_aliases(self) -> list[str]:
        return list(self._targets.keys())

    def has_target(self, alias: str) -> bool:
        return alias in self._targets

    def test_all(self) -> dict[str, bool]:
        """Test connectivity to all registered DBs."""
        results: dict[str, bool] = {
            "__internal__": self._internal.test_connection()
        }
        for alias, mgr in self._targets.items():
            results[alias] = mgr.test_connection()
        return results

    def close_all(self) -> None:
        self._internal.close()
        for mgr in self._targets.values():
            mgr.close()
        self._targets.clear()


# ── Cross-DB helpers ──────────────────────────────────────────

@dataclass
class TableAddress:
    """
    Fully-qualified table address across DBs.

    Format: alias.schema.table
    e.g.   mydb.public.orders
           warehouse.dw.customers
    """
    db_alias: str
    schema:   str
    table:    str

    @classmethod
    def parse(cls, address: str, default_alias: str = "") -> "TableAddress":
        """
        Parse 'alias.schema.table' or 'schema.table' or 'table'.
        """
        parts = address.split(".")
        if len(parts) == 3:
            return cls(db_alias=parts[0], schema=parts[1], table=parts[2])
        elif len(parts) == 2:
            return cls(db_alias=default_alias, schema=parts[0], table=parts[1])
        else:
            return cls(db_alias=default_alias, schema="public", table=parts[0])

    @property
    def qualified(self) -> str:
        return f"{self.db_alias}.{self.schema}.{self.table}"

    @property
    def schema_table(self) -> str:
        return f"{self.schema}.{self.table}"

    def __str__(self) -> str:
        return self.qualified

    def __hash__(self) -> int:
        return hash(self.qualified.lower())

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, TableAddress):
            return False
        return self.qualified.lower() == other.qualified.lower()
