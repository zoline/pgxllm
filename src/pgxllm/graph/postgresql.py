"""
pgxllm.graph.postgresql
------------------------
PostgreSQLGraphStore — graph_nodes, graph_edges, graph_paths 테이블을
사용하는 기본 GraphStore 구현체.

런타임에는 graph_paths 테이블에서 단순 SELECT 만 수행한다.
BFS 경로 계산은 refresh_paths() 에서 오프라인으로 수행한다.
"""
from __future__ import annotations

import json
import logging
from collections import deque
from typing import Optional

from pgxllm.db.connections import ConnectionRegistry
from pgxllm.graph.base import (
    GraphPath, GraphStore, TableEdge, TableNode,
)

log = logging.getLogger(__name__)


class PostgreSQLGraphStore(GraphStore):
    """
    Default GraphStore backed by PostgreSQL tables.

    Path lookup: O(1) — simple SELECT on pre-computed graph_paths.
    Path computation: BFS at refresh time.
    """

    def __init__(self, registry: ConnectionRegistry, max_depth: int = 4):
        self._registry  = registry
        self._max_depth = max_depth

    # ── add_node ──────────────────────────────────────────────

    def add_node(self, node: TableNode) -> None:
        with self._registry.internal.connection() as conn:
            conn.execute(
                """
                INSERT INTO graph_nodes (db_alias, schema_name, table_name, row_count, metadata)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (db_alias, schema_name, table_name)
                DO UPDATE SET
                    row_count  = EXCLUDED.row_count,
                    metadata   = EXCLUDED.metadata,
                    created_at = graph_nodes.created_at
                """,
                (node.db_alias, node.schema, node.table,
                 node.row_count, json.dumps(node.metadata))
            )

    # ── add_edge ──────────────────────────────────────────────

    def add_edge(self, edge: TableEdge) -> None:
        with self._registry.internal.connection() as conn:
            conn.execute(
                """
                INSERT INTO graph_edges (
                    from_db_alias, from_schema, from_table, from_column,
                    to_db_alias,   to_schema,   to_table,   to_column,
                    relation_name, relation_type, confidence, call_count,
                    approved, source_sql, is_cross_db
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (from_db_alias, from_schema, from_table, from_column,
                             to_db_alias,   to_schema,   to_table,   to_column)
                DO UPDATE SET
                    relation_name  = COALESCE(EXCLUDED.relation_name, graph_edges.relation_name),
                    relation_type  = EXCLUDED.relation_type,
                    confidence     = GREATEST(graph_edges.confidence, EXCLUDED.confidence),
                    call_count     = graph_edges.call_count + EXCLUDED.call_count,
                    approved       = EXCLUDED.approved OR graph_edges.approved,
                    updated_at     = NOW()
                """,
                (
                    edge.from_db_alias, edge.from_schema, edge.from_table, edge.from_column,
                    edge.to_db_alias,   edge.to_schema,   edge.to_table,   edge.to_column,
                    edge.relation_name, edge.relation_type,
                    edge.confidence, edge.call_count,
                    edge.approved, edge.source_sql, edge.is_cross_db,
                )
            )

    # ── find_paths (runtime — O(1) SELECT) ───────────────────

    def find_paths(
        self,
        from_address: str,
        to_address:   str,
        max_depth:    int = 4,
    ) -> list[GraphPath]:
        """Lookup pre-computed paths. No recursion at runtime."""
        with self._registry.internal.connection() as conn:
            rows = conn.execute(
                """
                SELECT path_json, hop_count, total_weight, join_hint, is_cross_db
                FROM graph_paths
                WHERE from_address = %s AND to_address = %s
                  AND hop_count    <= %s
                ORDER BY total_weight DESC, hop_count ASC
                LIMIT 5
                """,
                (from_address, to_address, max_depth)
            )
        return [
            GraphPath(
                from_address=from_address,
                to_address=to_address,
                path_json=r["path_json"] if isinstance(r["path_json"], list)
                          else json.loads(r["path_json"]),
                hop_count=r["hop_count"],
                total_weight=r["total_weight"],
                join_hint=r["join_hint"],
                is_cross_db=r["is_cross_db"],
            )
            for r in rows
        ]

    # ── find_neighbors ────────────────────────────────────────

    def find_neighbors(
        self,
        address: str,
        depth:   int = 1,
    ) -> list[TableNode]:
        """Find directly adjacent table nodes."""
        with self._registry.internal.connection() as conn:
            rows = conn.execute(
                """
                SELECT DISTINCT
                    e.to_db_alias AS db_alias,
                    e.to_schema   AS schema_name,
                    e.to_table    AS table_name
                FROM graph_edges e
                WHERE e.from_db_alias || '.' || e.from_schema || '.' || e.from_table = %s
                  AND e.approved = TRUE
                LIMIT 50
                """,
                (address,)
            )
        return [
            TableNode(db_alias=r["db_alias"], schema=r["schema_name"], table=r["table_name"])
            for r in rows
        ]

    # ── find_related_by_embedding ─────────────────────────────

    def find_related_by_embedding(
        self,
        question_embedding: list[float],
        db_alias:           str,
        top_k:              int = 5,
    ) -> list[TableNode]:
        """
        Vector similarity search on schema_embeddings.
        Falls back to empty list if pgvector not available.
        """
        try:
            with self._registry.internal.connection() as conn:
                rows = conn.execute(
                    """
                    SELECT sc.db_alias, sc.schema_name, sc.table_name,
                           se.embedding <=> %s::vector AS distance
                    FROM schema_embeddings se
                    JOIN schema_catalog sc ON sc.id = se.catalog_id
                    WHERE sc.db_alias    = %s
                      AND sc.column_name IS NULL
                    ORDER BY distance ASC
                    LIMIT %s
                    """,
                    (question_embedding, db_alias, top_k)
                )
            return [
                TableNode(db_alias=r["db_alias"], schema=r["schema_name"], table=r["table_name"])
                for r in rows
            ]
        except Exception as e:
            log.debug("Vector search unavailable: %s", e)
            return []

    # ── refresh_paths (offline BFS) ───────────────────────────

    def refresh_paths(self, db_alias: str) -> int:
        """
        Compute all table-to-table paths using BFS and store in graph_paths.
        Called by `pgxllm db refresh` — not at query time.

        Returns number of paths computed.
        """
        log.info("[%s] Computing graph paths (BFS, max_depth=%d) ...",
                 db_alias, self._max_depth)

        # Load approved edges — fk/analyzed/file/manual only (inferred 제외)
        with self._registry.internal.connection() as conn:
            edge_rows = conn.execute(
                """
                SELECT from_db_alias, from_schema, from_table, from_column,
                       to_db_alias,   to_schema,   to_table,   to_column,
                       relation_type, call_count
                FROM graph_edges
                WHERE (from_db_alias = %s OR to_db_alias = %s)
                  AND approved = TRUE
                  AND relation_type IN ('fk', 'analyzed', 'file', 'manual')
                """,
                (db_alias, db_alias)
            )

        if not edge_rows:
            log.info("[%s] No approved edges found, skipping path computation", db_alias)
            return 0

        # Build adjacency: address → list[(edge, to_address)]
        adjacency: dict[str, list[tuple[dict, str]]] = {}
        all_nodes: set[str] = set()

        for e in edge_rows:
            fa = f"{e['from_db_alias']}.{e['from_schema']}.{e['from_table']}"
            ta = f"{e['to_db_alias']}.{e['to_schema']}.{e['to_table']}"
            adjacency.setdefault(fa, []).append((e, ta))
            adjacency.setdefault(ta, []).append((e, fa))  # bidirectional
            all_nodes.add(fa)
            all_nodes.add(ta)

        # BFS from each node
        paths: list[dict] = []
        for source in all_nodes:
            bfs_paths = self._bfs(source, adjacency, self._max_depth)
            paths.extend(bfs_paths)

        # Delete old paths for this DB and insert new ones
        with self._registry.internal.connection() as conn:
            conn.execute(
                "DELETE FROM graph_paths WHERE from_address LIKE %s OR to_address LIKE %s",
                (f"{db_alias}.%", f"{db_alias}.%")
            )
            for path in paths:
                conn.execute(
                    """
                    INSERT INTO graph_paths (
                        from_address, to_address, path_json,
                        hop_count, total_weight, join_hint, is_cross_db
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (from_address, to_address)
                    DO UPDATE SET
                        path_json    = EXCLUDED.path_json,
                        hop_count    = EXCLUDED.hop_count,
                        total_weight = EXCLUDED.total_weight,
                        join_hint    = EXCLUDED.join_hint,
                        is_cross_db  = EXCLUDED.is_cross_db,
                        computed_at  = NOW()
                    """,
                    (
                        path["from_address"], path["to_address"],
                        json.dumps(path["path_json"]),
                        path["hop_count"], path["total_weight"],
                        self._build_join_hint(path["path_json"]),
                        path["is_cross_db"],
                    )
                )

        log.info("[%s] Graph paths computed: %d paths", db_alias, len(paths))
        return len(paths)

    # ── get_join_hint ─────────────────────────────────────────

    def get_join_hint(self, addresses: list[str]) -> str:
        """
        Generate LLM-ready JOIN hint text for a set of table addresses.
        Finds paths between all pairs and formats them as natural language.
        """
        if len(addresses) < 2:
            return ""

        lines: list[str] = ["관련 테이블 및 JOIN 경로:"]
        seen_pairs: set[frozenset] = set()

        for i, src in enumerate(addresses):
            for dst in addresses[i + 1:]:
                pair = frozenset([src, dst])
                if pair in seen_pairs:
                    continue
                seen_pairs.add(pair)

                paths = self.find_paths(src, dst, max_depth=self._max_depth)
                if paths:
                    p = paths[0]
                    src_table = src.split(".")[-1]
                    dst_table = dst.split(".")[-1]
                    if p.join_hint:
                        lines.append(f"  {src_table} → {dst_table}: {p.join_hint}")

        return "\n".join(lines) if len(lines) > 1 else ""

    # ── BFS ───────────────────────────────────────────────────

    def _bfs(
        self,
        source:    str,
        adjacency: dict[str, list[tuple[dict, str]]],
        max_depth: int,
    ) -> list[dict]:
        """BFS from source node, up to max_depth hops.

        path_json stores edges (not nodes): each step is
        {from_table, from_column, to_table, to_column}
        so JOIN hints are always semantically correct.
        """
        paths: list[dict] = []
        # queue: (current_address, edge_steps_so_far, total_weight, visited)
        queue: deque = deque()
        queue.append((source, [], 0, {source}))

        while queue:
            cur, edge_steps, weight, visited = queue.popleft()

            if edge_steps:
                from_addr = source
                to_addr   = cur
                if from_addr != to_addr:
                    paths.append({
                        "from_address": from_addr,
                        "to_address":   to_addr,
                        "path_json":    edge_steps,
                        "hop_count":    len(edge_steps),
                        "total_weight": weight,
                        "is_cross_db":  self._is_cross_db(from_addr, to_addr),
                    })

            if len(edge_steps) >= max_depth:
                continue

            for edge, neighbor in adjacency.get(cur, []):
                if neighbor in visited:
                    continue
                cur_is_from = (
                    f"{edge['from_db_alias']}.{edge['from_schema']}.{edge['from_table']}" == cur
                )
                if cur_is_from:
                    step = {
                        "from_table":   edge["from_table"],
                        "from_column":  edge["from_column"],
                        "to_table":     edge["to_table"],
                        "to_column":    edge["to_column"],
                    }
                else:
                    # traversing edge in reverse direction
                    step = {
                        "from_table":   edge["to_table"],
                        "from_column":  edge["to_column"],
                        "to_table":     edge["from_table"],
                        "to_column":    edge["from_column"],
                    }
                new_weight = weight + (edge.get("call_count") or 0)
                queue.append((neighbor, edge_steps + [step], new_weight, visited | {neighbor}))

        return paths

    @staticmethod
    def _is_cross_db(addr1: str, addr2: str) -> bool:
        return addr1.split(".")[0] != addr2.split(".")[0]

    @staticmethod
    def _build_join_hint(path_json: list[dict]) -> Optional[str]:
        """path_json 을 JOIN 힌트 문자열로 변환.

        두 가지 형식 지원:
        - 노드 형식: [{table, column, ...}, ...] — 연속 쌍을 JOIN 조건으로 변환
        - 엣지 형식: [{from_table, from_column, to_table, to_column}, ...] — 직접 변환
        """
        if not path_json:
            return None
        if "from_table" in path_json[0]:
            parts = [
                f"{s['from_table']}.{s['from_column']} = {s['to_table']}.{s['to_column']}"
                for s in path_json
            ]
        else:
            parts = [
                f"{path_json[i]['table']}.{path_json[i]['column']} = "
                f"{path_json[i+1]['table']}.{path_json[i+1]['column']}"
                for i in range(len(path_json) - 1)
            ]
        return " AND ".join(parts) if parts else None
