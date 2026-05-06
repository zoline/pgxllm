"""
pgxllm.db
---------
Database connection management.

Public API::

    from pgxllm.db import ConnectionRegistry, TableAddress
    from pgxllm.config import load_config

    registry = ConnectionRegistry(load_config())

    with registry.internal.connection() as conn:
        rows = conn.execute("SELECT * FROM pgxllm.db_registry")

    with registry.target("mydb").connection() as conn:
        rows = conn.execute("SELECT * FROM public.orders LIMIT 5")
"""

from .connections import (
    ConnectionRegistry,
    InternalDBManager,
    PgConnection,
    PgPool,
    TableAddress,
    TargetDBManager,
)

__all__ = [
    "ConnectionRegistry",
    "InternalDBManager",
    "TargetDBManager",
    "PgConnection",
    "PgPool",
    "TableAddress",
]
