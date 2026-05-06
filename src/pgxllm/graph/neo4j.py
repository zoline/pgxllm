"""
pgxllm.graph.neo4j
-------------------
Neo4jGraphStore — Neo4j Cypher 기반 구현체. Stub.
"""
from __future__ import annotations

from pgxllm.graph.base import GraphPath, GraphStore, TableEdge, TableNode


class Neo4jGraphStore(GraphStore):
    """Neo4j GraphStore. Not yet implemented."""

    def __init__(self, config):
        raise NotImplementedError(
            "Neo4jGraphStore is not yet implemented. "
            "Use graph.backend = 'postgresql' for now."
        )

    def add_node(self, node: TableNode) -> None: ...
    def add_edge(self, edge: TableEdge) -> None: ...
    def find_paths(self, from_address, to_address, max_depth=4): return []
    def find_neighbors(self, address, depth=1): return []
    def find_related_by_embedding(self, q, db_alias, top_k=5): return []
    def refresh_paths(self, db_alias): return 0
    def get_join_hint(self, addresses): return ""
