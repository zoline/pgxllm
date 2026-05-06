"""
pgxllm.intelligence.relation_collector
----------------------------------------
RelationCollector — 3가지 경로로 테이블 관계를 수집한다.

  1. pg_stat_statements 분석
  2. SQL 파일 @relation 어노테이션 등록
  3. Reverse Inference (간접 관계 자동 추론)

수집된 관계는 graph_edges 테이블에 저장되며,
승인(approved=TRUE) 후 graph_paths BFS 계산에 사용된다.
"""
from __future__ import annotations

import fnmatch
import json
import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from pgxllm.config import AppConfig, TargetDBConfig
from pgxllm.db.connections import ConnectionRegistry
from pgxllm.graph.base import TableEdge
from pgxllm.graph.postgresql import PostgreSQLGraphStore
from pgxllm.parser.facade import SqlParser

log = logging.getLogger(__name__)


# ── Candidate ─────────────────────────────────────────────────

@dataclass
class RelationCandidate:
    from_db_alias:   str
    from_schema:     str
    from_table:      str
    from_column:     str
    to_db_alias:     str
    to_schema:       str
    to_table:        str
    to_column:       str
    relation_type:   str    # analyzed | inferred | manual | file
    confidence:      float
    call_count:      int    = 0
    source_sql:      Optional[str] = None
    relation_name:   Optional[str] = None
    auto_approve:    bool   = False

    def to_edge(self) -> TableEdge:
        return TableEdge(
            from_db_alias=self.from_db_alias,
            from_schema=self.from_schema,
            from_table=self.from_table,
            from_column=self.from_column,
            to_db_alias=self.to_db_alias,
            to_schema=self.to_schema,
            to_table=self.to_table,
            to_column=self.to_column,
            relation_name=self.relation_name,
            relation_type=self.relation_type,
            confidence=self.confidence,
            call_count=self.call_count,
            approved=self.auto_approve,
            source_sql=self.source_sql,
            is_cross_db=(self.from_db_alias != self.to_db_alias),
        )


# ── Collector ─────────────────────────────────────────────────

class RelationCollector:
    """
    Collects table relations from three sources and stores candidates
    in graph_edges (approved=FALSE until reviewed).

    Usage::

        collector = RelationCollector(registry, config)

        # 1. pg_stat_statements
        candidates = collector.from_pg_stat_statements("mydb", top=100)

        # 2. SQL file
        candidates = collector.from_sql_file(Path("queries/sales.sql"), "mydb")

        # 3. Reverse inference
        candidates = collector.reverse_infer("mydb", min_confidence=0.7)

        # Save to graph_edges
        collector.save(candidates, auto_approve_threshold=0.95)
    """

    def __init__(self, registry: ConnectionRegistry, config: AppConfig):
        self._registry = registry
        self._config   = config
        self._parser   = SqlParser(max_depth=config.parser.max_depth)
        self._graph    = PostgreSQLGraphStore(
            registry, max_depth=config.graph.max_depth
        )

    # 시스템 스키마 / 내부 테이블 키워드 — 이 문자열이 포함된 쿼리는 제외
    _SYSTEM_KEYWORDS = (
        "pg_catalog", "pg_class", "pg_attribute", "pg_namespace",
        "pg_type", "pg_index", "pg_constraint", "pg_depend",
        "pg_statistic", "pg_stat", "pg_statio", "pg_locks",
        "pg_proc", "pg_trigger", "pg_rewrite", "pg_description",
        "pg_shdescription", "pg_database",
        "pg_am", "pg_toast", "pg_temp",
        "information_schema",
    )

    # pgxllm 내부 식별자 — 테이블명 또는 prepared statement prefix 포함 쿼리 제외
    _PGXLLM_TABLES = (
        "pgxllm_",           # PREPARE pgxllm_validate / pgxllm_probe 등 모두 차단
        "graph_edges", "graph_nodes", "graph_paths",
        "schema_catalog", "schema_embeddings",
        "db_registry", "dialect_rules",
        "query_history", "question_embeddings",
        "sql_patterns", "pattern_applications",
        "pipeline_logs", "verified_queries",
    )

    # ── 1. pg_stat_statements ─────────────────────────────────

    def from_pg_stat_statements(
        self,
        db_alias:         str,
        *,
        top:              int   = 100,
        min_calls:        int   = 5,
        auto_approve_at:  float = 0.95,
    ) -> list[RelationCandidate]:
        """
        Analyze top-N queries from pg_stat_statements.
        Requires pg_stat_statements extension enabled on target DB.
        """
        mgr = self._registry.target(db_alias)

        try:
            with mgr.connection() as conn:
                rows = conn.execute(
                    """
                    SELECT query, calls
                    FROM pg_stat_statements
                    WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
                      AND calls >= %s
                      AND query ILIKE '%%JOIN%%'
                      AND query NOT ILIKE '%%pg_catalog%%'
                      AND query NOT ILIKE '%%information_schema%%'
                      AND query NOT ILIKE '%%pg_class%%'
                      AND query NOT ILIKE '%%pg_attribute%%'
                      AND query NOT ILIKE '%%pg_namespace%%'
                      AND query NOT ILIKE '%%pg_stat%%'
                      AND query NOT ILIKE '%%pg_statio%%'
                      AND query NOT ILIKE '%%pg_type%%'
                      AND query NOT ILIKE '%%pg_index%%'
                      AND query NOT ILIKE '%%pg_constraint%%'
                      AND query NOT ILIKE '%%pg_proc%%'
                      AND query NOT ILIKE '%%pg_description%%'
                      AND query NOT ILIKE '%%pg_toast%%'
                      AND query NOT ILIKE '%%pgxllm_%%'
                    ORDER BY calls DESC
                    LIMIT %s
                    """,
                    [min_calls, top]
                )
        except Exception as e:
            log.warning("pg_stat_statements not available for %s: %s", db_alias, e)
            return []

        # schema_catalog에 등록된 테이블만 허용 (시스템 테이블 완전 차단)
        schema_map = self._build_table_schema_map(db_alias)
        known_tables = set(schema_map.keys())  # 실제 등록된 테이블명 집합
        col_map = self._build_table_column_map(db_alias)  # {table: set(columns)}

        candidates: dict[tuple, RelationCandidate] = {}

        for row in rows:
            sql      = row["query"]
            calls    = row["calls"]

            # 쿼리 텍스트에 시스템 키워드 또는 pgxllm 내부 테이블이 있으면 제외
            sql_lower = sql.lower()
            if any(kw in sql_lower for kw in self._SYSTEM_KEYWORDS):
                continue
            if any(tbl in sql_lower for tbl in self._PGXLLM_TABLES):
                continue

            relations = self._parser.extract_relations(sql)

            for rel in relations:
                ft = rel.from_table.lower()
                tt = rel.to_table.lower()
                fc = rel.from_column.lower()
                tc = rel.to_column.lower()

                # schema_catalog에 없는 테이블(시스템/미등록)은 제외
                if ft not in known_tables or tt not in known_tables:
                    continue

                # 컬럼이 실제 해당 테이블에 존재하는지 검증
                if fc not in col_map.get(ft, set()) or tc not in col_map.get(tt, set()):
                    log.debug(
                        "[%s] Skipping invalid relation %s.%s → %s.%s (column not in schema)",
                        db_alias, ft, fc, tt, tc,
                    )
                    continue

                from_schema = schema_map[ft]
                to_schema   = schema_map[tt]

                key = (ft, fc, tt, tc)
                if key in candidates:
                    candidates[key].call_count += calls
                    candidates[key].confidence  = min(
                        1.0, candidates[key].confidence + 0.01
                    )
                else:
                    confidence = min(1.0, calls / 1000.0 + 0.5)
                    candidates[key] = RelationCandidate(
                        from_db_alias=db_alias,
                        from_schema=from_schema,
                        from_table=ft,
                        from_column=fc,
                        to_db_alias=db_alias,
                        to_schema=to_schema,
                        to_table=tt,
                        to_column=tc,
                        relation_type="analyzed",
                        confidence=confidence,
                        call_count=calls,
                        source_sql=sql[:500],
                        auto_approve=False,  # analyzed edges always require manual approval
                    )

        result = list(candidates.values())
        log.info(
            "[%s] pg_stat_statements: found %d relation candidates (from %d queries)",
            db_alias, len(result), len(rows)
        )
        return result

    # ── 2. SQL file ───────────────────────────────────────────

    def from_sql_file(
        self,
        path:     Path,
        db_alias: str,
        *,
        recursive: bool = False,
    ) -> list[RelationCandidate]:
        """
        Extract relations from SQL file(s) with @relation annotations.

        Single file or directory (recursive=True).
        """
        files: list[Path] = []
        if path.is_dir():
            pattern = "**/*.sql" if recursive else "*.sql"
            files = list(path.glob(pattern))
        else:
            files = [path]

        schema_map = self._build_table_schema_map(db_alias)
        candidates: list[RelationCandidate] = []

        for f in files:
            text = f.read_text(encoding="utf-8")
            candidates.extend(
                self._parse_sql_file(text, db_alias, schema_map, str(f))
            )

        log.info(
            "[%s] SQL files (%d files): found %d relation candidates",
            db_alias, len(files), len(candidates)
        )
        return candidates

    def _parse_sql_file(
        self,
        text:       str,
        db_alias:   str,
        schema_map: dict[str, str],
        source:     str,
    ) -> list[RelationCandidate]:
        candidates: list[RelationCandidate] = []

        # @relation 어노테이션 파싱
        annotations = self._parser.extract_annotations(text)
        for ann in annotations:
            from_t  = ann["from_table"]
            to_t    = ann["to_table"]
            label   = ann["label"]
            from_s  = schema_map.get(from_t, "public")
            to_s    = schema_map.get(to_t, "public")

            # @relation only gives table names, not columns
            # Extract column from JOIN conditions in the SQL
            sql_rels = self._parser.extract_relations(text)
            matched = [
                r for r in sql_rels
                if r.from_table.lower() == from_t and r.to_table.lower() == to_t
            ]
            if matched:
                for m in matched:
                    candidates.append(RelationCandidate(
                        from_db_alias=db_alias,
                        from_schema=schema_map.get(m.from_table.lower(), "public"),
                        from_table=m.from_table.lower(),
                        from_column=m.from_column.lower(),
                        to_db_alias=db_alias,
                        to_schema=schema_map.get(m.to_table.lower(), "public"),
                        to_table=m.to_table.lower(),
                        to_column=m.to_column.lower(),
                        relation_type="file",
                        confidence=0.95,
                        relation_name=label or None,
                        source_sql=source,
                        auto_approve=True,  # file-registered relations are trusted
                    ))
            else:
                # Annotation without matching JOIN — store table-level relation
                log.debug(
                    "No JOIN found for @relation %s → %s in %s",
                    from_t, to_t, source
                )

        # SQL에서 직접 추출한 관계도 추가
        sql_rels = self._parser.extract_relations(text)
        for rel in sql_rels:
            candidates.append(RelationCandidate(
                from_db_alias=db_alias,
                from_schema=schema_map.get(rel.from_table.lower(), "public"),
                from_table=rel.from_table.lower(),
                from_column=rel.from_column.lower(),
                to_db_alias=db_alias,
                to_schema=schema_map.get(rel.to_table.lower(), "public"),
                to_table=rel.to_table.lower(),
                to_column=rel.to_column.lower(),
                relation_type="file",
                confidence=0.9,
                source_sql=source,
                auto_approve=True,
            ))

        return candidates

    # ── 3. Reverse Inference ──────────────────────────────────

    def reverse_infer(
        self,
        db_alias:         str,
        *,
        min_confidence:   float = 0.7,
        auto_approve_at:  float = 0.95,
    ) -> list[RelationCandidate]:
        """
        Infer indirect relations from existing graph_edges.

        Method 1: Transitive — A→B + B→C → A→C
        Method 2: Column name similarity
        """
        with self._registry.internal.connection() as conn:
            edges = conn.execute(
                """
                SELECT from_db_alias, from_schema, from_table, from_column,
                       to_db_alias,   to_schema,   to_table,   to_column,
                       call_count, confidence
                FROM graph_edges
                WHERE (from_db_alias=%s OR to_db_alias=%s)
                  AND approved = TRUE
                """,
                (db_alias, db_alias)
            )

        candidates: list[RelationCandidate] = []

        # Method 1: Transitive (A→B + B→C → infer A→C)
        edge_map: dict[str, list[dict]] = {}
        for e in edges:
            fa = f"{e['from_schema']}.{e['from_table']}"
            edge_map.setdefault(fa, []).append(e)

        for e1 in edges:
            mid = f"{e1['to_schema']}.{e1['to_table']}"
            for e2 in edge_map.get(mid, []):
                # Skip if direct relation already exists
                if (e1["from_table"] == e2["to_table"] and
                        e1["from_schema"] == e2["to_schema"]):
                    continue
                conf = e1["confidence"] * e2["confidence"] * 0.8
                if conf < min_confidence:
                    continue
                candidates.append(RelationCandidate(
                    from_db_alias=db_alias,
                    from_schema=e1["from_schema"],
                    from_table=e1["from_table"],
                    from_column=e1["from_column"],
                    to_db_alias=db_alias,
                    to_schema=e2["to_schema"],
                    to_table=e2["to_table"],
                    to_column=e2["to_column"],
                    relation_type="inferred",
                    confidence=conf,
                    auto_approve=conf >= auto_approve_at,
                ))

        # Method 2: Column name similarity
        schema_map = self._build_table_schema_map(db_alias)
        col_candidates = self._infer_by_column_similarity(
            db_alias, schema_map, min_confidence
        )
        candidates.extend(col_candidates)

        log.info(
            "[%s] Reverse inference: %d candidates (min_conf=%.2f)",
            db_alias, len(candidates), min_confidence
        )
        return candidates

    def _infer_by_column_similarity(
        self,
        db_alias:        str,
        schema_map:      dict[str, str],
        min_confidence:  float,
    ) -> list[RelationCandidate]:
        """
        Find potential FK relationships based on column naming conventions.
        e.g., orders.customer_id → customers.id
        """
        with self._registry.internal.connection() as conn:
            cols = conn.execute(
                """
                SELECT schema_name, table_name, column_name, data_type
                FROM schema_catalog
                WHERE db_alias = %s AND column_name IS NOT NULL
                ORDER BY schema_name, table_name
                """,
                (db_alias,)
            )

        # Build: table → set of columns
        table_cols: dict[str, set[str]] = {}
        for c in cols:
            key = f"{c['schema_name']}.{c['table_name']}"
            table_cols.setdefault(key, set()).add(c["column_name"].lower())

        candidates: list[RelationCandidate] = []

        # Heuristic: col like "*_id" → look for table named col[:-3]
        for c in cols:
            col = c["column_name"].lower()
            if not col.endswith("_id") or col == "id":
                continue
            ref_table = col[:-3]  # e.g., customer_id → customer
            # Try exact and plural
            for candidate_table in [ref_table, ref_table + "s"]:
                ref_schema = schema_map.get(candidate_table)
                if not ref_schema:
                    continue
                ref_key = f"{ref_schema}.{candidate_table}"
                if "id" not in table_cols.get(ref_key, set()):
                    continue
                # Confidence based on naming match quality
                confidence = 0.75
                candidates.append(RelationCandidate(
                    from_db_alias=db_alias,
                    from_schema=c["schema_name"],
                    from_table=c["table_name"],
                    from_column=col,
                    to_db_alias=db_alias,
                    to_schema=ref_schema,
                    to_table=candidate_table,
                    to_column="id",
                    relation_type="inferred",
                    confidence=confidence,
                    auto_approve=confidence >= 0.95,
                ))
                break   # take first match

        return [c for c in candidates if c.confidence >= min_confidence]

    # ── Save ──────────────────────────────────────────────────

    def save(
        self,
        candidates:              list[RelationCandidate],
        auto_approve_threshold:  float = 0.95,
    ) -> int:
        """
        Save candidates to graph_edges.
        Candidates with confidence >= threshold are auto-approved,
        except 'analyzed' type which always requires manual approval.

        Returns number of edges saved.
        """
        saved = 0
        for c in candidates:
            if c.relation_type != "analyzed" and c.confidence >= auto_approve_threshold:
                c.auto_approve = True
            edge = c.to_edge()
            self._graph.add_edge(edge)
            saved += 1
        return saved

    # ── Approve ───────────────────────────────────────────────

    def approve(
        self,
        db_alias: str,
        relation_type: Optional[str] = None,
        *,
        min_confidence: float = 0.0,
    ) -> int:
        """Approve pending graph_edges."""
        params: list = [db_alias]
        sql = "UPDATE graph_edges SET approved=TRUE, updated_at=NOW() WHERE from_db_alias=%s"
        if relation_type:
            sql += " AND relation_type=%s"
            params.append(relation_type)
        if min_confidence > 0:
            sql += " AND confidence>=%s"
            params.append(min_confidence)

        with self._registry.internal.connection() as conn:
            conn.execute(sql, params)
            count = conn.execute_one(
                "SELECT COUNT(*) AS n FROM graph_edges WHERE from_db_alias=%s AND approved=TRUE",
                (db_alias,)
            )
        return count["n"] if count else 0

    # ── Helpers ───────────────────────────────────────────────

    def _build_table_schema_map(self, db_alias: str) -> dict[str, str]:
        """Build table_name → schema_name mapping from schema_catalog."""
        with self._registry.internal.connection() as conn:
            rows = conn.execute(
                "SELECT DISTINCT table_name, schema_name FROM schema_catalog "
                "WHERE db_alias=%s AND column_name IS NULL",
                (db_alias,)
            )
        return {r["table_name"].lower(): r["schema_name"].lower() for r in rows}

    def _build_table_column_map(self, db_alias: str) -> dict[str, set[str]]:
        """Build table_name → set(column_names) mapping from schema_catalog."""
        with self._registry.internal.connection() as conn:
            rows = conn.execute(
                "SELECT table_name, column_name FROM schema_catalog "
                "WHERE db_alias=%s AND column_name IS NOT NULL",
                (db_alias,)
            )
        result: dict[str, set[str]] = {}
        for r in rows:
            result.setdefault(r["table_name"].lower(), set()).add(r["column_name"].lower())
        return result
