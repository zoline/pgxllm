"""
pgxllm.core.s2_schema_linking
------------------------------
S2 Schema Linking

수행:
  1. 후보 테이블의 컬럼 상세 정보를 schema_catalog 에서 로드
  2. GraphStore 로 테이블 간 JOIN 경로 탐색 → join_hint 생성
  3. 관련 Dialect Rules 로드
  4. LinkedSchema 구성
"""
from __future__ import annotations

import json
import logging
from typing import Optional

from pgxllm.core.models import LinkedSchema, QuestionAnalysis, TableInfo
from pgxllm.db.connections import ConnectionRegistry
from pgxllm.graph.factory import GraphStoreFactory
from pgxllm.intelligence.rule_engine import RuleEngine

log = logging.getLogger(__name__)


class SchemaLinker:
    """
    S2 — 후보 테이블을 기반으로 LinkedSchema 를 구성한다.
    """

    def __init__(self, registry: ConnectionRegistry, graph_config=None):
        self._registry    = registry
        self._rule_engine = RuleEngine(registry)
        # graph_config can be GraphConfig or AppConfig
        from pgxllm.graph.factory import GraphStoreFactory
        from pgxllm.config import AppConfig, GraphConfig
        if graph_config is None:
            from pgxllm.config import load_config
            cfg = load_config()
            self._graph = GraphStoreFactory.create(registry, cfg)
        elif isinstance(graph_config, AppConfig):
            self._graph = GraphStoreFactory.create(registry, graph_config)
        else:
            # GraphConfig — wrap in minimal AppConfig
            from pgxllm.config import AppConfig as AC
            tmp = AC(graph=graph_config)
            self._graph = GraphStoreFactory.create(registry, tmp)

    def run(
        self,
        analysis:    QuestionAnalysis,
        db_alias:    str,
        *,
        max_tables:  int = 8,
    ) -> LinkedSchema:
        """
        Args:
            analysis:   S1 QuestionAnalysis 결과
            db_alias:   target DB alias
            max_tables: LinkedSchema 에 포함할 최대 테이블 수

        Returns:
            LinkedSchema
        """
        log.info("[S2] linking %d candidate tables", len(analysis.candidate_tables))

        candidate_tables = analysis.candidate_tables[:max_tables]

        # ── 1. 컬럼 정보 로드 ───────────────────────────────────
        tables = self._load_table_info(db_alias, candidate_tables)
        if not tables:
            log.warning("[S2] no tables loaded from catalog")

        # ── 2. JOIN hint 생성 ────────────────────────────────────
        addresses = [t.address for t in tables]
        join_hint = self._build_join_hint(addresses)

        # ── 3. Dialect Rules 로드 ────────────────────────────────
        table_names = [t.table for t in tables]
        col_map = {t.table: [c["name"] for c in t.columns] for t in tables}
        rules = self._rule_engine.get_rules_for_query(
            db_alias, table_names, columns=col_map
        )

        # ── 4. 샘플 데이터 요약 ──────────────────────────────────
        sample_context = self._build_sample_context(tables)

        log.info(
            "[S2] linked: %d tables, %d rules, join_hint=%s",
            len(tables), len(rules), bool(join_hint)
        )

        return LinkedSchema(
            db_alias=db_alias,
            tables=tables,
            join_hint=join_hint,
            dialect_rules=rules,
            sample_context=sample_context,
        )

    # ── Helpers ────────────────────────────────────────────────

    def _load_table_info(
        self, db_alias: str, addresses: list[str]
    ) -> list[TableInfo]:
        """schema_catalog 에서 테이블 + 컬럼 정보 로드."""
        if not addresses:
            return []

        # Parse addresses → (schema, table)
        schema_tables: list[tuple[str, str]] = []
        for addr in addresses:
            parts = addr.split(".")
            if len(parts) == 3:
                schema_tables.append((parts[1], parts[2]))
            elif len(parts) == 2:
                schema_tables.append((parts[0], parts[1]))

        if not schema_tables:
            return []

        try:
            with self._registry.internal.connection() as conn:
                placeholders = ",".join(
                    ["(%s,%s)"] * len(schema_tables)
                )
                flat_params = [v for pair in schema_tables for v in pair]
                rows = conn.execute(
                    f"""
                    SELECT schema_name, table_name, column_name,
                           data_type, is_pk, is_fk,
                           fk_ref_table, fk_ref_column,
                           comment_text, n_distinct, sample_values
                    FROM schema_catalog
                    WHERE db_alias = %s
                      AND (schema_name, table_name) IN ({placeholders})
                    ORDER BY schema_name, table_name, column_name NULLS FIRST
                    """,
                    [db_alias] + flat_params
                )
        except Exception as e:
            log.warning("[S2] catalog load error: %s", e)
            return []

        # Group into TableInfo objects
        table_map: dict[str, TableInfo] = {}
        for r in rows:
            key = f"{r['schema_name']}.{r['table_name']}"
            if key not in table_map:
                table_map[key] = TableInfo(
                    address=f"{db_alias}.{key}",
                    schema=r["schema_name"],
                    table=r["table_name"],
                    columns=[],
                    comment=None,
                )
            if r["column_name"] is None:
                table_map[key].comment = r["comment_text"]
            else:
                table_map[key].columns.append({
                    "name":     r["column_name"],
                    "type":     r["data_type"] or "",
                    "pk":       r["is_pk"],
                    "fk":       r["is_fk"],
                    "fk_ref":   f"{r['fk_ref_table']}.{r['fk_ref_column']}"
                                if r["is_fk"] else None,
                    "comment":  r["comment_text"],
                    "n_distinct": r["n_distinct"],
                    "samples":  r["sample_values"],
                })

        # Preserve order from addresses
        result = []
        for addr in addresses:
            parts = addr.split(".")
            key   = ".".join(parts[1:]) if len(parts) == 3 else ".".join(parts)
            if key in table_map:
                result.append(table_map[key])
        return result

    def _build_join_hint(self, addresses: list[str]) -> str:
        """GraphStore 에서 JOIN 경로를 조회해 hint 텍스트로 변환."""
        if len(addresses) < 2:
            return ""
        try:
            return self._graph.get_join_hint(addresses)
        except Exception as e:
            log.warning("[S2] join hint error: %s", e)
            return ""

    def _build_sample_context(self, tables: list[TableInfo]) -> str:
        """샘플 데이터를 포함한 컬럼들의 요약 텍스트."""
        lines = []
        for t in tables:
            for c in t.columns:
                if not c.get("samples"):
                    continue
                try:
                    values = json.loads(c["samples"])
                    if values:
                        lines.append(
                            f"{t.schema}.{t.table}.{c['name']}: "
                            + ", ".join(str(v) for v in values[:8])
                        )
                except Exception:
                    pass
        return "\n".join(lines)
