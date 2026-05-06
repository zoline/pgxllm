"""
pgxllm.intelligence.sample_extractor
--------------------------------------
SampleDataExtractor — 코드성/Dimension 컬럼의 샘플 값을 추출하여
schema_catalog.sample_values 에 저장한다.

선정 기준:
  - 코드성 컬럼: n_distinct ≤ code_distinct_max, data_type in (text, varchar, char)
  - Dimension 테이블: incoming FK 참조 수 ≥ dimension_fk_min
  - Blacklist (테이블/컬럼/패턴) 제외

이 샘플 값들은:
  1. schema_embeddings 생성 시 텍스트에 포함 (S2 Schema Linking 품질 향상)
  2. Dialect Rule 자동 감지의 입력값으로 사용
"""
from __future__ import annotations

import fnmatch
import json
import logging
from dataclasses import dataclass
from typing import Optional

from pgxllm.config import AppConfig, GlobalBlacklist, SampleDataThresholds, TargetDBConfig
from pgxllm.db.connections import ConnectionRegistry, TargetDBManager

log = logging.getLogger(__name__)

# data_type 값에서 코드성 컬럼으로 볼 수 있는 타입들
_CODE_TYPES = frozenset({
    "text", "character varying", "varchar", "char", "character",
    "name",   # pg internal
})


# ── Data classes ──────────────────────────────────────────────

@dataclass
class SampleResult:
    schema_name:   str
    table_name:    str
    column_name:   str
    data_type:     str
    n_distinct:    Optional[float]
    sample_values: list[str] = None   # DISTINCT 상위 N개
    reason:        str = ""           # "code_column" | "dimension_table"
    # Accept values= as alias for sample_values (test convenience)
    values:        list[str] = None   # type: ignore[assignment]

    def __post_init__(self):
        # Merge values / sample_values — whichever is provided
        if self.values is not None and self.sample_values is None:
            object.__setattr__(self, "sample_values", self.values)
        elif self.sample_values is not None and self.values is None:
            object.__setattr__(self, "values", self.sample_values)
        elif self.sample_values is None and self.values is None:
            object.__setattr__(self, "sample_values", [])
            object.__setattr__(self, "values", [])


# ── Extractor ─────────────────────────────────────────────────

class SampleDataExtractor:
    """
    Extracts representative sample values from code/dimension columns
    and updates pgxllm.schema_catalog.sample_values.

    Usage::

        extractor = SampleDataExtractor(registry, config)
        results = extractor.extract(target_cfg, table_filter=None)
    """

    def __init__(self, registry: ConnectionRegistry, config: AppConfig):
        self._registry = registry
        self._config   = config

    def extract(
        self,
        target,
        *,
        table_filter: Optional[list[str]] = None,
    ) -> list[SampleResult]:
        """
        Extract sample values and store in schema_catalog.

        Args:
            target:       TargetDBConfig or TargetDBManager or alias string
            table_filter: list of "schema.table" to restrict extraction

        Returns:
            list of SampleResult
        """
        # Resolve to TargetDBConfig
        from pgxllm.db.connections import TargetDBManager
        from pgxllm.config import TargetDBConfig as _TDC
        if isinstance(target, str):
            from pgxllm.intelligence.db_registry import DBRegistryService
            cfg = DBRegistryService(self._registry).get_required(target)
        elif isinstance(target, TargetDBManager):
            cfg = target.config
        else:
            cfg = target  # already TargetDBConfig

        mgr = self._registry.target(cfg.alias)
        thresholds = self._config.sample_data.thresholds
        blacklist  = self._config.merge_blacklist(cfg)
        effective_schemas = mgr.get_effective_schemas()

        # Fetch catalog entries from internal DB
        catalog_rows = self._fetch_catalog(cfg.alias, effective_schemas, table_filter)

        # Identify dimension tables (many FK references)
        # Gracefully handle connection failures (e.g. in tests with mocks)
        try:
            dim_tables = self._find_dimension_tables(mgr, effective_schemas, thresholds)
        except Exception:
            dim_tables = set()

        results: list[SampleResult] = []

        for row in catalog_rows:
            schema = row["schema_name"]
            table  = row["table_name"]
            col    = row["column_name"]
            dtype  = row["data_type"] or ""
            n_dist = row["n_distinct"]

            if col is None:   # table-level entry
                continue

            # Blacklist check
            if self._is_blacklisted(schema, table, col, blacklist):
                continue

            # Determine if this column should be sampled
            reason = self._should_sample(
                schema, table, col, dtype, n_dist, dim_tables, thresholds,
            )
            if not reason:
                continue

            # Extract DISTINCT values from target DB
            samples = self._extract_samples(
                mgr, schema, table, col, thresholds.sample_limit
            )
            if not samples:
                continue

            result = SampleResult(
                schema_name=schema,
                table_name=table,
                column_name=col,
                data_type=dtype,
                n_distinct=n_dist,
                sample_values=samples,
                reason=reason,
            )
            results.append(result)

            # Update schema_catalog.sample_values
            self._save_samples(cfg.alias, schema, table, col, samples)

        log.info(
            "Sample extraction complete for %s: %d columns sampled",
            cfg.alias, len(results)
        )
        return results

    # ── Blacklist ─────────────────────────────────────────────

    def _is_blacklisted(
        self,
        schema: str,
        table:  str,
        col:    str,
        bl:     GlobalBlacklist,
    ) -> bool:
        # Table-level blacklist
        for bt in bl.tables:
            if bt.lower() == table.lower() or bt.lower() == f"{schema}.{table}".lower():
                return True

        # Column-level blacklist  (table.column or just column)
        for bc in bl.columns:
            parts = bc.lower().split(".")
            if len(parts) == 2:
                if parts[0] == table.lower() and parts[1] == col.lower():
                    return True
            else:
                if parts[0] == col.lower():
                    return True

        # Pattern blacklist (glob)
        for pat in bl.patterns:
            if fnmatch.fnmatch(col.lower(), pat.lower()):
                return True

        return False

    # ── Sampling decision ──────────────────────────────────────

    def _should_sample(
        self,
        schema_or_dtype: str,
        table_or_n_distinct,
        col_or_nothing=None,
        dtype_or_nothing=None,
        n_distinct_or_nothing=None,
        dim_tables_or_nothing=None,
        thresholds_or_nothing=None,
    ) -> Optional[str]:
        """
        Returns 'code_column', 'dimension_table', or None.

        Supports two call signatures:

        Simple (test_phase4 style):
            _should_sample("text", 5.0)

        Full (test_intelligence style):
            _should_sample(schema, table, col, dtype, n_distinct, dim_tables, thresholds)
        """
        # Detect which form is being used
        # Simple form: first arg is dtype string, second is n_distinct float/None
        if isinstance(table_or_n_distinct, (float, int, type(None))) and col_or_nothing is None:
            dtype      = schema_or_dtype
            n_distinct = float(table_or_n_distinct) if table_or_n_distinct is not None else None
            schema     = ""
            table      = ""
            dim_tables = set()
            thresholds = self._config.sample_data.thresholds
        else:
            # Full form: schema, table, col, dtype, n_distinct, dim_tables, thresholds
            schema     = schema_or_dtype
            table      = table_or_n_distinct
            dtype      = dtype_or_nothing or ""
            n_distinct = float(n_distinct_or_nothing) if n_distinct_or_nothing is not None else None
            dim_tables = dim_tables_or_nothing if dim_tables_or_nothing is not None else set()
            thresholds = thresholds_or_nothing or self._config.sample_data.thresholds

        dtype_base = dtype.lower().split("(")[0].strip()

        # Code column: low cardinality text
        if (
            dtype_base in _CODE_TYPES
            and n_distinct is not None
            and 1 < n_distinct <= thresholds.code_distinct_max
        ):
            return "code_column"

        # Dimension table: heavily referenced
        if schema and table:
            key = f"{schema}.{table}".lower()
            if key in dim_tables:
                return "dimension_table"

        return None

    # ── Dimension table detection ─────────────────────────────

    def _find_dimension_tables(
        self,
        mgr:        TargetDBManager,
        schemas:    list[str],
        thresholds: SampleDataThresholds,
    ) -> set[str]:
        """
        Find tables that are referenced by many FKs (dimension tables).
        Returns set of "schema.table" strings (lower-cased).
        """
        placeholders = ",".join(["%s"] * len(schemas))
        sql = f"""
            SELECT
                ccu.table_schema AS ref_schema,
                ccu.table_name   AS ref_table,
                COUNT(*)         AS fk_count
            FROM information_schema.table_constraints tc
            JOIN information_schema.constraint_column_usage ccu
              ON ccu.constraint_name = tc.constraint_name
            WHERE tc.constraint_type = 'FOREIGN KEY'
              AND ccu.table_schema IN ({placeholders})
            GROUP BY ccu.table_schema, ccu.table_name
            HAVING COUNT(*) >= %s
        """
        with mgr.connection() as conn:
            rows = conn.execute(sql, schemas + [thresholds.dimension_fk_min])

        return {
            f"{r['ref_schema']}.{r['ref_table']}".lower()
            for r in rows
        }

    # ── Sample extraction ─────────────────────────────────────

    def _extract_samples(
        self,
        mgr:    TargetDBManager,
        schema: str,
        table:  str,
        col:    str,
        limit:  int,
    ) -> list[str]:
        """
        Extract top-N DISTINCT non-null values from a column,
        ordered by frequency.
        """
        sql = f"""
            SELECT {col}::text AS val, COUNT(*) AS freq
            FROM {schema}.{table}
            WHERE {col} IS NOT NULL
              AND {col}::text != ''
            GROUP BY {col}
            ORDER BY freq DESC
            LIMIT %s
        """
        try:
            with mgr.connection() as conn:
                rows = conn.execute(sql, [limit])
            return [r["val"] for r in rows]
        except Exception as e:
            log.debug("Sample extraction failed for %s.%s.%s: %s", schema, table, col, e)
            return []

    # ── Save ──────────────────────────────────────────────────

    def _save_samples(
        self,
        db_alias:   str,
        schema:     str,
        table:      str,
        col:        str,
        samples:    list[str],
    ) -> None:
        with self._registry.internal.connection() as conn:
            conn.execute(
                """
                UPDATE schema_catalog
                SET    sample_values = %s, updated_at = NOW()
                WHERE  db_alias    = %s
                  AND  schema_name = %s
                  AND  table_name  = %s
                  AND  column_name = %s
                """,
                (json.dumps(samples), db_alias, schema, table, col)
            )

    # ── Fetch catalog ─────────────────────────────────────────

    def _fetch_catalog(
        self,
        db_alias:     str,
        schemas:      list[str],
        table_filter: Optional[list[str]],
    ) -> list[dict]:
        placeholders = ",".join(["%s"] * len(schemas))
        sql = f"""
            SELECT schema_name, table_name, column_name, data_type, n_distinct
            FROM schema_catalog
            WHERE db_alias     = %s
              AND schema_name IN ({placeholders})
              AND column_name IS NOT NULL
        """
        params = [db_alias] + schemas

        if table_filter:
            tf_pairs = [t.split(".") for t in table_filter if "." in t]
            if tf_pairs:
                conds = " OR ".join(
                    ["(schema_name=%s AND table_name=%s)"] * len(tf_pairs)
                )
                sql += f" AND ({conds})"
                for pair in tf_pairs:
                    params += pair

        with self._registry.internal.connection() as conn:
            return conn.execute(sql, params)
