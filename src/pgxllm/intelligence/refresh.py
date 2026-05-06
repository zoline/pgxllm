"""
pgxllm.intelligence.refresh
-----------------------------
RefreshOrchestrator — `pgxllm db refresh` 의 전체 흐름을 조율한다.

흐름:
  1. pg_catalog 스캔       → schema_catalog (GIN)
  2. 샘플 데이터 추출      → schema_catalog.sample_values
  3. Dialect Rule 자동 감지 → dialect_rules
  4. FK 기반 graph_edges   → graph_edges (relation_type='fk')
  5. schema version hash 갱신 → db_registry

Usage::

    orch = RefreshOrchestrator(registry, config)
    result = orch.refresh("mydb")
    result = orch.refresh("mydb", table_filter=["public.orders"])
    result = orch.refresh_all()
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Optional

from pgxllm.config import AppConfig, TargetDBConfig
from pgxllm.db.connections import ConnectionRegistry
from pgxllm.intelligence.db_registry import DBRegistryService
from pgxllm.intelligence.dialect_rule_detector import DialectRuleDetector
from pgxllm.intelligence.sample_extractor import SampleDataExtractor
from pgxllm.intelligence.schema_catalog import SchemaCatalogBuilder

log = logging.getLogger(__name__)


# ── Result ────────────────────────────────────────────────────

@dataclass
class RefreshResult:
    db_alias:         str
    success:          bool
    schema_hash:      Optional[str]       = None
    tables_scanned:   int                 = 0
    columns_scanned:  int                 = 0
    samples_extracted: int                = 0
    rules_detected:   int                 = 0
    fk_edges_created: int                 = 0
    duration_sec:     float               = 0.0
    error:            Optional[str]       = None
    warnings:         list[str]           = field(default_factory=list)

    def summary(self) -> str:
        if not self.success:
            return f"[{self.db_alias}] FAILED: {self.error}"
        return (
            f"[{self.db_alias}] OK  "
            f"tables={self.tables_scanned} cols={self.columns_scanned} "
            f"samples={self.samples_extracted} rules={self.rules_detected} "
            f"fk_edges={self.fk_edges_created} "
            f"hash={self.schema_hash} ({self.duration_sec:.1f}s)"
        )


# ── Orchestrator ──────────────────────────────────────────────

class RefreshOrchestrator:
    """
    Coordinates the full `pgxllm db refresh` flow for one or all DBs.
    """

    def __init__(self, registry: ConnectionRegistry, config: AppConfig):
        self._registry    = registry
        self._config      = config
        self._db_svc      = DBRegistryService(registry)
        self._cat_builder = SchemaCatalogBuilder(registry, config)
        self._sam_ext     = SampleDataExtractor(registry, config)
        self._rule_det    = DialectRuleDetector(registry)

    # ── Public ────────────────────────────────────────────────

    def refresh(
        self,
        alias:        str,
        *,
        table_filter: Optional[list[str]] = None,
        skip_samples: bool = False,
        skip_rules:   bool = False,
        skip_graph:   bool = False,
    ) -> RefreshResult:
        """
        Refresh metadata for a single registered target DB.

        Args:
            alias:        target DB alias
            table_filter: if given, only refresh these tables ("schema.table")
            skip_samples: skip sample data extraction
            skip_rules:   skip dialect rule detection
            skip_graph:   skip FK-based graph edge creation
        """
        t0 = time.perf_counter()
        result = RefreshResult(db_alias=alias, success=False)

        try:
            target = self._db_svc.get_required(alias)
        except KeyError as e:
            result.error = str(e)
            return result

        try:
            # ── Step 1: pg_catalog 스캔 ──────────────────────
            log.info("[%s] Step 1: scanning pg_catalog ...", alias)
            schema_hash = self._cat_builder.build(target, table_filter=table_filter)
            result.schema_hash = schema_hash

            # Count what was stored
            counts = self._count_catalog(alias)
            result.tables_scanned  = counts["tables"]
            result.columns_scanned = counts["columns"]

            # ── Step 2: 샘플 데이터 추출 ─────────────────────
            if not skip_samples:
                log.info("[%s] Step 2: extracting sample data ...", alias)
                samples = self._sam_ext.extract(target, table_filter=table_filter)
                result.samples_extracted = len(samples)
            else:
                samples = []

            # ── Step 3: Dialect Rule 자동 감지 ───────────────
            if not skip_rules and samples:
                log.info("[%s] Step 3: detecting dialect rules ...", alias)
                rules = self._rule_det.detect(alias, samples)
                saved = self._rule_det.save(alias, rules)
                result.rules_detected = saved
            else:
                result.rules_detected = 0

            # ── Step 4: FK → graph_edges ──────────────────────
            if not skip_graph:
                log.info("[%s] Step 4: creating FK graph edges ...", alias)
                fk_count = self._create_fk_edges(target)
                result.fk_edges_created = fk_count

            # ── Step 5: schema version hash 갱신 ─────────────
            self._db_svc.update_hash(alias, schema_hash)

            result.success = True

        except Exception as e:
            log.exception("[%s] Refresh failed", alias)
            result.error = str(e)

        result.duration_sec = time.perf_counter() - t0
        log.info(result.summary())
        return result

    def refresh_all(
        self,
        *,
        skip_samples: bool = False,
        skip_rules:   bool = False,
        skip_graph:   bool = False,
    ) -> list[RefreshResult]:
        """Refresh all active registered target DBs."""
        dbs = self._db_svc.list_all(active_only=True)
        results = []
        for db in dbs:
            r = self.refresh(
                db.alias,
                skip_samples=skip_samples,
                skip_rules=skip_rules,
                skip_graph=skip_graph,
            )
            results.append(r)
        return results

    # ── FK → graph_edges ─────────────────────────────────────

    def _create_fk_edges(self, target: TargetDBConfig) -> int:
        """
        Create graph_edges entries from FK constraints.
        relation_type = 'fk', approved = TRUE (FKs are trusted).
        """
        mgr = self._registry.target(target.alias)
        effective_schemas = mgr.get_effective_schemas()
        placeholders = ",".join(["%s"] * len(effective_schemas))

        sql = f"""
            SELECT
                kcu.table_schema   AS from_schema,
                kcu.table_name     AS from_table,
                kcu.column_name    AS from_col,
                ccu.table_schema   AS to_schema,
                ccu.table_name     AS to_table,
                ccu.column_name    AS to_col
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
            fk_rows = conn.execute(sql, effective_schemas)

        if not fk_rows:
            return 0

        count = 0
        with self._registry.internal.connection() as conn:
            for r in fk_rows:
                is_cross = r["from_schema"] != r["to_schema"]
                conn.execute(
                    """
                    INSERT INTO graph_edges (
                        from_db_alias, from_schema, from_table, from_column,
                        to_db_alias,   to_schema,   to_table,   to_column,
                        relation_type, confidence, call_count,
                        approved, is_cross_db
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'fk',1.0,0,TRUE,%s)
                    ON CONFLICT (from_db_alias, from_schema, from_table, from_column,
                                 to_db_alias,   to_schema,   to_table,   to_column)
                    DO UPDATE SET
                        relation_type = 'fk',
                        approved      = TRUE,
                        updated_at    = NOW()
                    """,
                    (
                        target.alias, r["from_schema"], r["from_table"], r["from_col"],
                        target.alias, r["to_schema"],   r["to_table"],   r["to_col"],
                        is_cross,
                    )
                )
                count += 1

        return count

    # ── Helpers ───────────────────────────────────────────────

    def _count_catalog(self, db_alias: str) -> dict:
        with self._registry.internal.connection() as conn:
            t_row = conn.execute_one(
                "SELECT COUNT(DISTINCT (schema_name, table_name)) AS n "
                "FROM schema_catalog WHERE db_alias=%s AND column_name IS NULL",
                (db_alias,)
            )
            c_row = conn.execute_one(
                "SELECT COUNT(*) AS n "
                "FROM schema_catalog WHERE db_alias=%s AND column_name IS NOT NULL",
                (db_alias,)
            )
        return {
            "tables":  t_row["n"] if t_row else 0,
            "columns": c_row["n"] if c_row else 0,
        }
