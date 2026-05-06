"""
pgxllm.graph.factory
---------------------
GraphStoreFactory — config.graph.backend 설정으로 구현체를 선택한다.
"""
from __future__ import annotations

import logging

from pgxllm.config import AppConfig
from pgxllm.db.connections import ConnectionRegistry
from pgxllm.graph.base import GraphStore

log = logging.getLogger(__name__)


class GraphStoreFactory:
    @staticmethod
    def create(registry: ConnectionRegistry, config: AppConfig) -> GraphStore:
        backend = config.graph.backend

        if backend == "postgresql":
            from pgxllm.graph.postgresql import PostgreSQLGraphStore
            return PostgreSQLGraphStore(registry, max_depth=config.graph.max_depth)

        elif backend == "age":
            from pgxllm.graph.age import AGEGraphStore
            return AGEGraphStore(registry, config)

        elif backend == "neo4j":
            from pgxllm.graph.neo4j import Neo4jGraphStore
            return Neo4jGraphStore(config)

        else:
            raise ValueError(
                f"Unknown graph backend: {backend!r}. "
                "Choose from: postgresql | age | neo4j"
            )
