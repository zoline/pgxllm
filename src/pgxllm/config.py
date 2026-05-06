"""
pgxllm.config
-------------
Application configuration.

Loads from configs/default.yaml, then overrides with environment variables.

Key design:
  - internal_db  : single pgxllm metadata DB (can be any PG instance)
  - target_dbs   : N registered PostgreSQL target DBs, each with independent
                   host/port/credentials and per-DB schema include/exclude rules
  - cross_db_relations: allow JOIN relations across different target DBs
"""
from __future__ import annotations

import os
import re
from functools import lru_cache
from pathlib import Path
from typing import Literal, Optional

import yaml
from pydantic import BaseModel, Field, model_validator


# ── Helpers ───────────────────────────────────────────────────

def _expand_env(value: str) -> str:
    """
    Expand ${VAR:-default} and ${VAR} patterns in YAML string values.
    """
    def replacer(m: re.Match) -> str:
        var, _, default = m.group(1).partition(":-")
        return os.environ.get(var, default)
    return re.sub(r"\$\{([^}]+)\}", replacer, str(value))


def _expand_dict(d: dict) -> dict:
    """Recursively expand env vars in a dict."""
    result = {}
    for k, v in d.items():
        if isinstance(v, dict):
            result[k] = _expand_dict(v)
        elif isinstance(v, list):
            result[k] = [_expand_dict(i) if isinstance(i, dict)
                         else (_expand_env(i) if isinstance(i, str) else i)
                         for i in v]
        elif isinstance(v, str):
            result[k] = _expand_env(v)
        else:
            result[k] = v
    return result


# ── DB Connection Models ──────────────────────────────────────

class DBConn(BaseModel):
    """Base PostgreSQL connection parameters."""
    host:     str = "localhost"
    port:     int = 5432
    user:     str = "postgres"
    password: str = ""
    dbname:   str

    @property
    def dsn(self) -> str:
        """psycopg2-compatible DSN string."""
        pwd = f"password={self.password} " if self.password else ""
        return (
            f"host={self.host} port={self.port} "
            f"user={self.user} {pwd}dbname={self.dbname}"
        )

    @property
    def url(self) -> str:
        """SQLAlchemy-compatible URL."""
        pwd = f":{self.password}" if self.password else ""
        return f"postgresql://{self.user}{pwd}@{self.host}:{self.port}/{self.dbname}"

    def __str__(self) -> str:
        return f"{self.host}:{self.port}/{self.dbname}"


class InternalDBConfig(DBConn):
    """pgxllm internal metadata DB."""
    dbname:     str = "pgxllm"
    pg_schema:  str = Field(default="pgxllm", alias="schema")
    pool_min:   int = 1
    pool_max:   int = 5

    model_config = {"populate_by_name": True}

    @property
    def schema(self) -> str:
        return self.pg_schema


class TargetDBConfig(DBConn):
    """
    A registered target PostgreSQL DB.

    Schema resolution:
      schema_mode = "include"  → only scan listed schemas
      schema_mode = "exclude"  → scan all schemas EXCEPT listed
    """
    alias:       str                         # unique name, e.g. "mydb"
    dbname:      str   = ""
    schema_mode: Literal["include", "exclude"] = "exclude"
    schemas:     list[str] = Field(
        default_factory=lambda: ["pg_catalog", "information_schema", "pg_toast"]
    )
    pool_min:    int   = 1
    pool_max:    int   = 3

    # Per-DB sample data blacklist (merged with global blacklist)
    blacklist_tables:   list[str] = Field(default_factory=list)
    blacklist_columns:  list[str] = Field(default_factory=list)
    blacklist_patterns: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _default_dbname(self) -> "TargetDBConfig":
        if not self.dbname:
            self.dbname = self.alias
        return self

    def effective_schemas(self, all_schemas: list[str]) -> list[str]:
        """
        Given all_schemas from pg_catalog, return the schemas to analyse.
        """
        if self.schema_mode == "include":
            return [s for s in all_schemas if s in set(self.schemas)]
        else:
            excluded = set(self.schemas)
            return [s for s in all_schemas if s not in excluded]

    @property
    def qualified_id(self) -> str:
        """Unique identifier: alias (host:port/dbname)."""
        return f"{self.alias} ({self.host}:{self.port}/{self.dbname})"

    def __str__(self) -> str:
        return self.qualified_id


# ── Sub-config models ─────────────────────────────────────────

class LLMConfig(BaseModel):
    provider:    str   = "ollama"
    base_url:    str   = "http://localhost:11434"
    model:       str   = "qwen2.5-coder:32b"
    api_key:     str   = ""   # Anthropic / OpenAI / watsonx / vLLM
    project_id:  str   = ""   # watsonx.ai project ID
    username:    str   = ""   # watsonx CP4D 사용자명 (없으면 IBM Cloud IAM 사용)
    verify_ssl:  bool  = True  # False = self-signed 인증서 허용 (CP4D on-prem)
    timeout:     int   = 120
    max_tokens:  int   = 2048
    temperature: float = 0.0


class TfidfCacheConfig(BaseModel):
    db_path:              str   = "databases/pgxllm_cache.sqlite"
    top_k:                int   = 5
    similarity_threshold: float = 0.75


class EmbeddingCacheConfig(BaseModel):
    top_k:                int   = 5
    similarity_threshold: float = 0.85


class CacheConfig(BaseModel):
    backend:   Literal["tfidf", "embedding"] = "tfidf"
    tfidf:     TfidfCacheConfig              = Field(default_factory=TfidfCacheConfig)
    embedding: EmbeddingCacheConfig          = Field(default_factory=EmbeddingCacheConfig)


class GraphPostgreSQLConfig(BaseModel):
    pass


class GraphAGEConfig(BaseModel):
    graph_name: str = "pgxllm_graph"


class GraphNeo4jConfig(BaseModel):
    uri:      str = "bolt://localhost:7687"
    user:     str = "neo4j"
    password: str = ""


class GraphConfig(BaseModel):
    backend:               Literal["postgresql", "age", "neo4j"] = "postgresql"
    max_depth:             int  = 4
    precompute_on_refresh: bool = True
    postgresql:            GraphPostgreSQLConfig = Field(default_factory=GraphPostgreSQLConfig)
    age:                   GraphAGEConfig        = Field(default_factory=GraphAGEConfig)
    neo4j:                 GraphNeo4jConfig      = Field(default_factory=GraphNeo4jConfig)


class SampleDataThresholds(BaseModel):
    code_distinct_max: int = 50
    dimension_fk_min:  int = 2
    dimension_row_max: int = 1_000_000
    sample_limit:      int = 20


class GlobalBlacklist(BaseModel):
    tables:   list[str] = Field(default_factory=list)
    columns:  list[str] = Field(default_factory=list)
    patterns: list[str] = Field(
        default_factory=lambda: [
            "*_hash", "*_token", "*_password", "*_secret", "*_key"
        ]
    )


class SampleDataConfig(BaseModel):
    thresholds: SampleDataThresholds = Field(default_factory=SampleDataThresholds)
    blacklist:  GlobalBlacklist      = Field(default_factory=GlobalBlacklist)


class ValidationConfig(BaseModel):
    max_correction_loops: int = 3
    correction_timeout:   int = 30


class FewShotConfig(BaseModel):
    top_k: int = 3


class PipelineConfig(BaseModel):
    validation: ValidationConfig = Field(default_factory=ValidationConfig)
    few_shot:   FewShotConfig    = Field(default_factory=FewShotConfig)


class ParserConfig(BaseModel):
    max_depth: int = 5


class CrossDBConfig(BaseModel):
    enabled: bool = True


# ── Root AppConfig ────────────────────────────────────────────

class AppConfig(BaseModel):
    """
    Root application configuration.

    Loaded from configs/default.yaml with env var expansion.
    Target DBs can be added at runtime via CLI and are persisted
    in the internal DB's pgxllm.db_registry table.
    """
    internal_db:        InternalDBConfig  = Field(default_factory=InternalDBConfig)
    target_dbs:         list[TargetDBConfig] = Field(default_factory=list)
    cross_db_relations: CrossDBConfig     = Field(default_factory=CrossDBConfig)
    llm:                LLMConfig         = Field(default_factory=LLMConfig)
    cache:              CacheConfig       = Field(default_factory=CacheConfig)
    graph:              GraphConfig       = Field(default_factory=GraphConfig)
    parser:             ParserConfig      = Field(default_factory=ParserConfig)
    sample_data:        SampleDataConfig  = Field(default_factory=SampleDataConfig)
    pipeline:           PipelineConfig    = Field(default_factory=PipelineConfig)

    # Runtime: resolved config file path
    config_path: Optional[Path] = Field(default=None, exclude=True)

    def get_target_db(self, alias: str) -> Optional[TargetDBConfig]:
        """Look up a target DB by alias."""
        for db in self.target_dbs:
            if db.alias == alias:
                return db
        return None

    def get_target_db_required(self, alias: str) -> TargetDBConfig:
        db = self.get_target_db(alias)
        if db is None:
            aliases = [d.alias for d in self.target_dbs]
            raise KeyError(
                f"Target DB '{alias}' not registered. "
                f"Available: {aliases}. "
                f"Register with: pgxllm db register --alias {alias} ..."
            )
        return db

    def merge_blacklist(self, target: TargetDBConfig) -> GlobalBlacklist:
        """
        Merge global blacklist with per-DB blacklist.
        Used by SampleDataExtractor.
        """
        return GlobalBlacklist(
            tables=list(set(
                self.sample_data.blacklist.tables +
                target.blacklist_tables
            )),
            columns=list(set(
                self.sample_data.blacklist.columns +
                target.blacklist_columns
            )),
            patterns=list(set(
                self.sample_data.blacklist.patterns +
                target.blacklist_patterns
            )),
        )


# ── Loader ────────────────────────────────────────────────────

_DEFAULT_CONFIG_PATHS = [
    Path("configs/default.yaml"),
    Path("/etc/pgxllm/default.yaml"),
]

PGXLLM_ROOT = Path(os.environ.get("PGXLLM_ROOT", "."))


def load_config(path: Optional[Path] = None) -> AppConfig:
    """
    Load AppConfig from YAML file with environment variable expansion.

    Search order:
      1. Explicit path argument
      2. PGXLLM_CONFIG env var
      3. configs/default.yaml relative to PGXLLM_ROOT
      4. /etc/pgxllm/default.yaml

    Environment variables can override any YAML value using the
    ${VAR:-default} syntax inside YAML strings.
    """
    config_path: Optional[Path] = path

    if config_path is None:
        env_path = os.environ.get("PGXLLM_CONFIG")
        if env_path:
            config_path = Path(env_path)

    if config_path is None:
        for candidate in _DEFAULT_CONFIG_PATHS:
            full = PGXLLM_ROOT / candidate
            if full.exists():
                config_path = full
                break

    raw: dict = {}
    if config_path and config_path.exists():
        with open(config_path) as f:
            raw = yaml.safe_load(f) or {}
        raw = _expand_dict(raw)

    cfg = AppConfig.model_validate(raw)
    cfg.config_path = config_path
    return cfg


@lru_cache(maxsize=1)
def get_config() -> AppConfig:
    """
    Module-level cached config singleton.
    Call invalidate_config() to reload.
    """
    return load_config()


def invalidate_config() -> None:
    """Clear cached config (useful in tests or after CLI changes)."""
    get_config.cache_clear()
