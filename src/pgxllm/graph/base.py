"""
pgxllm.graph.base
-----------------
GraphStore ABC + 데이터 클래스 정의.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class TableNode:
    db_alias:   str
    schema:     str
    table:      str
    row_count:  Optional[int] = None
    metadata:   dict          = field(default_factory=dict)

    @property
    def address(self) -> str:
        return f"{self.db_alias}.{self.schema}.{self.table}"


@dataclass
class TableEdge:
    from_db_alias: str
    from_schema:   str
    from_table:    str
    from_column:   str
    to_db_alias:   str
    to_schema:     str
    to_table:      str
    to_column:     str
    relation_name: Optional[str] = None
    relation_type: str           = "fk"   # fk|analyzed|inferred|manual
    confidence:    float         = 1.0
    call_count:    int           = 0
    approved:      bool          = False
    is_cross_db:   bool          = False
    source_sql:    Optional[str] = None

    @property
    def from_address(self) -> str:
        return f"{self.from_db_alias}.{self.from_schema}.{self.from_table}"

    @property
    def to_address(self) -> str:
        return f"{self.to_db_alias}.{self.to_schema}.{self.to_table}"


@dataclass
class GraphPath:
    from_address: str
    to_address:   str
    path_json:    list[dict]   # [{"db":…,"schema":…,"table":…,"column":…}, …]
    hop_count:    int
    total_weight: int
    join_hint:    Optional[str] = None
    is_cross_db:  bool          = False


class GraphStore(ABC):
    """Abstract interface for table relationship graph storage."""

    @abstractmethod
    def add_node(self, node: TableNode) -> None: ...

    @abstractmethod
    def add_edge(self, edge: TableEdge) -> None: ...

    @abstractmethod
    def find_paths(
        self,
        from_address: str,
        to_address:   str,
        max_depth:    int = 4,
    ) -> list[GraphPath]: ...

    @abstractmethod
    def find_neighbors(
        self,
        address: str,
        depth:   int = 1,
    ) -> list[TableNode]: ...

    @abstractmethod
    def find_related_by_embedding(
        self,
        question_embedding: list[float],
        db_alias:           str,
        top_k:              int = 5,
    ) -> list[TableNode]: ...

    @abstractmethod
    def refresh_paths(self, db_alias: str) -> int: ...

    @abstractmethod
    def get_join_hint(self, addresses: list[str]) -> str: ...
