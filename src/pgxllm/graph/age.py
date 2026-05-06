"""
pgxllm.graph.age
-----------------
AGEGraphStore — Apache AGE (openCypher) 기반 구현체.

현재는 stub. AGE 설치 후 구현 예정.

AGE 설치:
  https://github.com/apache/age
  PostgreSQL 16 기준: 소스 빌드 필요

OCP 환경:
  커스텀 PG 이미지에 AGE extension 포함 필요
"""
from __future__ import annotations

from pgxllm.graph.base import GraphPath, GraphStore, TableEdge, TableNode


class AGEGraphStore(GraphStore):
    """Apache AGE (openCypher) GraphStore. Not yet implemented."""

    def __init__(self, registry, config):
        raise NotImplementedError(
            "AGEGraphStore is not yet implemented. "
            "Use graph.backend = 'postgresql' for now."
        )

    def add_node(self, node: TableNode) -> None: ...
    def add_edge(self, edge: TableEdge) -> None: ...
    def find_paths(self, from_address, to_address, max_depth=4): return []
    def find_neighbors(self, address, depth=1): return []
    def find_related_by_embedding(self, q, db_alias, top_k=5): return []
    def refresh_paths(self, db_alias): return 0
    def get_join_hint(self, addresses): return ""
