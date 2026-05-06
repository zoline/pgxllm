"""
pgxllm.graph
------------
GraphStore implementations.

Public API::

    from pgxllm.graph import GraphStoreFactory, TableNode, TableEdge, GraphPath

    store = GraphStoreFactory.create(registry, config)
    paths = store.find_paths("mydb.public.orders", "mydb.public.customers")
"""

from .base        import GraphStore, TableNode, TableEdge, GraphPath
from .factory     import GraphStoreFactory
from .postgresql  import PostgreSQLGraphStore

__all__ = [
    "GraphStore", "TableNode", "TableEdge", "GraphPath",
    "GraphStoreFactory",
    "PostgreSQLGraphStore",
]
