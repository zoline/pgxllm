"""
pgxllm.db.schema
----------------
DDL for pgxllm internal metadata DB.

All objects live in the 'pgxllm' schema (configurable via internal_db.schema).
The schema is created by InternalDBManager.initialize_schema().
"""
from __future__ import annotations

# ── Target DB registry ────────────────────────────────────────
_DB_REGISTRY = """
CREATE TABLE IF NOT EXISTS db_registry (
    alias               TEXT        PRIMARY KEY,
    host                TEXT        NOT NULL,
    port                INTEGER     NOT NULL DEFAULT 5432,
    db_user             TEXT        NOT NULL,
    db_password         TEXT        NOT NULL DEFAULT '',
    dbname              TEXT        NOT NULL,
    schema_mode         TEXT        NOT NULL DEFAULT 'exclude',
    schemas             JSONB       NOT NULL DEFAULT '[]',
    blacklist_tables    JSONB       NOT NULL DEFAULT '[]',
    blacklist_columns   JSONB       NOT NULL DEFAULT '[]',
    blacklist_patterns  JSONB       NOT NULL DEFAULT '[]',
    schema_version_hash TEXT,
    last_refresh_at     TIMESTAMPTZ,
    is_active           BOOLEAN     NOT NULL DEFAULT TRUE,
    notes               TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
"""

# ── Schema catalog (GIN + pgvector) ───────────────────────────
_SCHEMA_CATALOG = """
CREATE TABLE IF NOT EXISTS schema_catalog (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    db_alias        TEXT        NOT NULL REFERENCES db_registry(alias) ON DELETE CASCADE,
    schema_name     TEXT        NOT NULL,
    table_name      TEXT        NOT NULL,
    column_name     TEXT,           -- NULL = table-level entry
    data_type       TEXT,
    is_nullable     BOOLEAN,
    column_default  TEXT,
    comment_text    TEXT,           -- pg_description
    sample_values   JSONB,          -- extracted distinct values for code columns
    n_distinct      FLOAT,          -- from pg_stats
    is_pk           BOOLEAN     DEFAULT FALSE,
    is_fk           BOOLEAN     DEFAULT FALSE,
    fk_ref_table    TEXT,           -- schema.table
    fk_ref_column   TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (db_alias, schema_name, table_name, column_name)
)
"""

_SCHEMA_CATALOG_IDX = [
    "CREATE INDEX IF NOT EXISTS idx_schema_catalog_db_table ON schema_catalog(db_alias, schema_name, table_name)",
    "CREATE INDEX IF NOT EXISTS idx_schema_catalog_gin ON schema_catalog USING GIN(to_tsvector('english', COALESCE(table_name,'') || ' ' || COALESCE(column_name,'') || ' ' || COALESCE(comment_text,'')))",
]

# ── Graph ─────────────────────────────────────────────────────
_GRAPH_NODES = """
CREATE TABLE IF NOT EXISTS graph_nodes (
    id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    db_alias    TEXT        NOT NULL,
    schema_name TEXT        NOT NULL,
    table_name  TEXT        NOT NULL,
    row_count   BIGINT,
    metadata    JSONB       NOT NULL DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (db_alias, schema_name, table_name)
)
"""

_GRAPH_EDGES = """
CREATE TABLE IF NOT EXISTS graph_edges (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    -- Source table (from_db_alias.from_schema.from_table.from_column)
    from_db_alias   TEXT        NOT NULL,
    from_schema     TEXT        NOT NULL,
    from_table      TEXT        NOT NULL,
    from_column     TEXT        NOT NULL,
    -- Target table
    to_db_alias     TEXT        NOT NULL,
    to_schema       TEXT        NOT NULL,
    to_table        TEXT        NOT NULL,
    to_column       TEXT        NOT NULL,
    -- Relation metadata
    relation_name   TEXT,               -- human-readable label
    relation_type   TEXT        NOT NULL DEFAULT 'fk',
    -- fk | analyzed | inferred | manual
    confidence      FLOAT       NOT NULL DEFAULT 1.0,
    call_count      INTEGER     NOT NULL DEFAULT 0,
    source_sql      TEXT,               -- SQL that produced this edge
    approved        BOOLEAN     NOT NULL DEFAULT FALSE,
    approved_by     TEXT,
    approved_at     TIMESTAMPTZ,
    -- Cross-DB: from_db_alias may differ from to_db_alias
    is_cross_db     BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (from_db_alias, from_schema, from_table, from_column,
            to_db_alias,   to_schema,   to_table,   to_column)
)
"""

_GRAPH_EDGES_IDX = [
    "CREATE INDEX IF NOT EXISTS idx_graph_edges_from ON graph_edges(from_db_alias, from_schema, from_table)",
    "CREATE INDEX IF NOT EXISTS idx_graph_edges_to   ON graph_edges(to_db_alias, to_schema, to_table)",
    "CREATE INDEX IF NOT EXISTS idx_graph_edges_cross ON graph_edges(is_cross_db) WHERE is_cross_db = TRUE",
]

_GRAPH_PATHS = """
CREATE TABLE IF NOT EXISTS graph_paths (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    -- Start and end table addresses (alias.schema.table)
    from_address    TEXT        NOT NULL,
    to_address      TEXT        NOT NULL,
    path_json       JSONB       NOT NULL,
    -- [{"db":"mydb","schema":"public","table":"orders","column":"customer_id"},
    --  {"db":"mydb","schema":"public","table":"customers","column":"id"}]
    hop_count       INTEGER     NOT NULL,
    total_weight    INTEGER     NOT NULL DEFAULT 0,
    join_hint       TEXT,               -- LLM-ready JOIN hint text
    is_cross_db     BOOLEAN     NOT NULL DEFAULT FALSE,
    computed_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (from_address, to_address)
)
"""

_GRAPH_PATHS_IDX = [
    "CREATE INDEX IF NOT EXISTS idx_graph_paths_from ON graph_paths(from_address)",
    "CREATE INDEX IF NOT EXISTS idx_graph_paths_to   ON graph_paths(to_address)",
    "CREATE INDEX IF NOT EXISTS idx_graph_paths_cross ON graph_paths(is_cross_db) WHERE is_cross_db = TRUE",
]

# ── Dialect rules ─────────────────────────────────────────────
_DIALECT_RULES = """
CREATE TABLE IF NOT EXISTS dialect_rules (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    rule_id         TEXT        NOT NULL UNIQUE,
    scope           TEXT        NOT NULL DEFAULT 'global',
    -- global | dialect | db | table | column
    dialect         TEXT        NOT NULL DEFAULT 'postgresql',
    db_alias        TEXT,               -- NULL = applies to all DBs
    schema_name     TEXT,
    table_name      TEXT,
    column_name     TEXT,
    condition_json  JSONB       NOT NULL DEFAULT '{}',
    forbidden_funcs         JSONB       NOT NULL DEFAULT '[]',
    forbidden_sql_patterns  JSONB       NOT NULL DEFAULT '[]',
    required_func   TEXT,
    instruction     TEXT        NOT NULL,
    example_bad     TEXT        NOT NULL DEFAULT '',
    example_good    TEXT        NOT NULL DEFAULT '',
    severity        TEXT        NOT NULL DEFAULT 'error',
    auto_detected   BOOLEAN     NOT NULL DEFAULT FALSE,
    enabled         BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
"""

# ── SQL patterns (dynamic pattern learning) ───────────────────
_SQL_PATTERNS = """
CREATE TABLE IF NOT EXISTS sql_patterns (
    id                UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    name              TEXT        NOT NULL UNIQUE,
    description       TEXT,
    scope             TEXT        NOT NULL DEFAULT 'global',
    dialect           TEXT        NOT NULL DEFAULT 'postgresql',
    db_alias          TEXT,
    detect_keywords   JSONB       NOT NULL DEFAULT '[]',
    detect_exclusions JSONB       NOT NULL DEFAULT '[]',
    detect_sql_check  TEXT,
    instruction       TEXT        NOT NULL,
    example_bad       TEXT        NOT NULL DEFAULT '',
    example_good      TEXT        NOT NULL DEFAULT '',
    auto_detected     BOOLEAN     NOT NULL DEFAULT FALSE,
    confidence        FLOAT       NOT NULL DEFAULT 1.0,
    hit_count         INTEGER     NOT NULL DEFAULT 0,
    success_count     INTEGER     NOT NULL DEFAULT 0,
    enabled           BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
"""

_PATTERN_APPLICATIONS = """
CREATE TABLE IF NOT EXISTS pattern_applications (
    id            UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    pattern_id    UUID        REFERENCES sql_patterns(id) ON DELETE CASCADE,
    db_alias      TEXT,
    question      TEXT        NOT NULL,
    generated_sql TEXT,
    was_correct   BOOLEAN,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
"""

# ── Verified queries (few-shot source) ────────────────────────
_VERIFIED_QUERIES = """
CREATE TABLE IF NOT EXISTS verified_queries (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    db_alias        TEXT        NOT NULL,
    question        TEXT        NOT NULL,
    sql             TEXT        NOT NULL,
    schema_hash     TEXT,               -- schema version at time of verification
    execution_ok    BOOLEAN     NOT NULL DEFAULT TRUE,
    gold_match      BOOLEAN,            -- NULL = unknown, TRUE/FALSE = EX eval result
    source          TEXT        NOT NULL DEFAULT 'pipeline',
    -- pipeline | user_feedback | bird_eval
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
"""

_VERIFIED_QUERIES_IDX = [
    "CREATE INDEX IF NOT EXISTS idx_verified_queries_db ON verified_queries(db_alias)",
]

# ── Pipeline execution log ────────────────────────────────────
_PIPELINE_LOGS = """
CREATE TABLE IF NOT EXISTS pipeline_logs (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    db_alias        TEXT,
    question        TEXT        NOT NULL,
    final_sql       TEXT,
    execution_ok    BOOLEAN,
    stage_logs      JSONB       NOT NULL DEFAULT '{}',
    -- {s1: {...}, s2: {...}, s3: {...}, s4: {...}}
    duration_ms     INTEGER,
    cache_hit       BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
"""

# ── Query history ────────────────────────────────────────────────
_QUERY_HISTORY = """
CREATE TABLE IF NOT EXISTS query_history (
    id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    db_alias    TEXT        NOT NULL,
    mode        TEXT        NOT NULL DEFAULT 'pipeline',  -- pipeline | direct
    input_text  TEXT        NOT NULL,    -- 사용자 입력 텍스트 (질문 또는 SQL)
    ok          BOOLEAN     NOT NULL DEFAULT TRUE,
    error       TEXT,
    duration_ms INTEGER,
    executed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
"""

_QUERY_HISTORY_IDX = [
    "CREATE INDEX IF NOT EXISTS idx_query_history_db ON query_history(db_alias, executed_at DESC)",
]

# ── Pgvector extension (installed separately) ─────────────────
_EXTENSIONS = [
    "CREATE EXTENSION IF NOT EXISTS pgcrypto",   # gen_random_uuid()
    "CREATE EXTENSION IF NOT EXISTS pg_trgm",     # trigram GIN
    # pgvector is optional — skip if not installed
    # "CREATE EXTENSION IF NOT EXISTS vector",
]

# ── pgvector tables (created only if vector extension available) ─
_SCHEMA_EMBEDDINGS = """
CREATE TABLE IF NOT EXISTS schema_embeddings (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    catalog_id      UUID        REFERENCES schema_catalog(id) ON DELETE CASCADE,
    embedding_text  TEXT        NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
"""

_QUESTION_EMBEDDINGS = """
CREATE TABLE IF NOT EXISTS question_embeddings (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    query_id        UUID        REFERENCES verified_queries(id) ON DELETE CASCADE,
    embedding_text  TEXT        NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
"""

# ── Ordered DDL list ──────────────────────────────────────────

INTERNAL_SCHEMA_SQL: list[str] = (
    _EXTENSIONS
    + [
        _DB_REGISTRY,
        _SCHEMA_CATALOG,
        _GRAPH_NODES,
        _GRAPH_EDGES,
        _GRAPH_PATHS,
        _DIALECT_RULES,
        _SQL_PATTERNS,
        _PATTERN_APPLICATIONS,
        _VERIFIED_QUERIES,
        _PIPELINE_LOGS,
        _QUERY_HISTORY,
        _SCHEMA_EMBEDDINGS,
        _QUESTION_EMBEDDINGS,
    ]
    + _SCHEMA_CATALOG_IDX
    + _GRAPH_EDGES_IDX
    + _GRAPH_PATHS_IDX
    + _VERIFIED_QUERIES_IDX
    + _QUERY_HISTORY_IDX
)

# ── incremental schema migrations ────────────────────────────
SCHEMA_ALTER_SQL: list[str] = [
    "ALTER TABLE pgxllm.dialect_rules ADD COLUMN IF NOT EXISTS forbidden_sql_patterns JSONB NOT NULL DEFAULT '[]'",
    """CREATE TABLE IF NOT EXISTS pgxllm.query_history (
        id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
        db_alias    TEXT        NOT NULL,
        mode        TEXT        NOT NULL DEFAULT 'pipeline',
        input_text  TEXT        NOT NULL,
        ok          BOOLEAN     NOT NULL DEFAULT TRUE,
        error       TEXT,
        duration_ms INTEGER,
        executed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )""",
    "CREATE INDEX IF NOT EXISTS idx_query_history_db ON pgxllm.query_history(db_alias, executed_at DESC)",
]

# ── pgvector column alter (run separately after vector extension check) ──
VECTOR_ALTER_SQL: list[str] = [
    "ALTER TABLE schema_embeddings   ADD COLUMN IF NOT EXISTS embedding vector(1536)",
    "ALTER TABLE question_embeddings ADD COLUMN IF NOT EXISTS embedding vector(1536)",
    "ALTER TABLE graph_paths         ADD COLUMN IF NOT EXISTS embedding vector(1536)",
    "CREATE INDEX IF NOT EXISTS idx_schema_emb_hnsw   ON schema_embeddings   USING hnsw (embedding vector_cosine_ops)",
    "CREATE INDEX IF NOT EXISTS idx_question_emb_hnsw ON question_embeddings USING hnsw (embedding vector_cosine_ops)",
    "CREATE INDEX IF NOT EXISTS idx_paths_emb_hnsw    ON graph_paths         USING hnsw (embedding vector_cosine_ops)",
]
