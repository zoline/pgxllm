"""
pgxllm.web.app
--------------
FastAPI application — single-file server serving the query test UI.

Run:
    uvicorn pgxllm.web.app:app --reload --port 8000
    # or
    python -m pgxllm.web.app
"""
from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

log = logging.getLogger(__name__)

app = FastAPI(title="pgxllm", version="0.1.0", docs_url="/api/docs")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Lazy registry ─────────────────────────────────────────────

_registry = None
_config   = None


def _run_migrations(registry) -> None:
    """Apply incremental DDL migrations to existing internal DB."""
    stmts = [
        "ALTER TABLE db_registry ADD COLUMN IF NOT EXISTS db_type TEXT NOT NULL DEFAULT 'production'",
        "ALTER TABLE query_history ADD COLUMN IF NOT EXISTS is_benchmark BOOLEAN NOT NULL DEFAULT FALSE",
        """CREATE TABLE IF NOT EXISTS eval_results (
            id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
            db_alias        TEXT        NOT NULL,
            eval_name       TEXT        NOT NULL DEFAULT '',
            question_id     INTEGER,
            question        TEXT        NOT NULL,
            gold_sql        TEXT        NOT NULL,
            baseline_sql    TEXT,
            pipeline_sql    TEXT,
            baseline_ok     BOOLEAN,
            pipeline_ok     BOOLEAN,
            baseline_ex     BOOLEAN,
            pipeline_ex     BOOLEAN,
            error_baseline  TEXT,
            error_pipeline  TEXT,
            duration_ms     INTEGER,
            created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )""",
        "CREATE INDEX IF NOT EXISTS idx_eval_results_db ON eval_results(db_alias, created_at DESC)",
        "CREATE INDEX IF NOT EXISTS idx_eval_results_name ON eval_results(db_alias, eval_name)",
    ]
    for sql in stmts:
        try:
            with registry.internal.connection() as conn:
                conn.execute(sql)
        except Exception:
            pass


def get_registry():
    global _registry, _config
    if _registry is None:
        import os
        from pathlib import Path
        from pgxllm.config import load_config
        from pgxllm.db.connections import ConnectionRegistry
        from pgxllm.intelligence import DBRegistryService

        # .env 파일이 있으면 환경변수로 로드
        env_file = Path(__file__).parents[3] / ".env"
        if env_file.exists():
            for line in env_file.read_text().splitlines():
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, _, v = line.partition("=")
                    os.environ.setdefault(k.strip(), v.strip())

        _config   = load_config()
        _registry = ConnectionRegistry(_config)
        # incremental migrations for existing deployments
        _run_migrations(_registry)
        DBRegistryService(_registry).load_registered_to_config(_config)
    return _registry, _config


# ══════════════════════════════════════════════════════════════
# API — /api/db
# ══════════════════════════════════════════════════════════════

@app.get("/api/db/list")
def db_list():
    """등록된 target DB 목록."""
    try:
        registry, config = get_registry()
        from pgxllm.intelligence import DBRegistryService
        svc = DBRegistryService(registry)
        statuses = svc.list_all()
        return [
            {
                "alias":       s.alias,
                "host":        s.host,
                "port":        s.port,
                "dbname":      s.dbname,
                "db_type":     s.db_type,
                "schema_mode": s.schema_mode,
                "schemas":     s.schemas,
                "is_active":   s.is_active,
                "is_reachable": s.is_reachable,
                "schema_version_hash": s.schema_version_hash,
                "last_refresh_at": str(s.last_refresh_at) if s.last_refresh_at else None,
            }
            for s in statuses
        ]
    except Exception as e:
        log.warning("db_list error: %s", e)
        return []


class RegisterRequest(BaseModel):
    alias:       str
    host:        str
    port:        int = 5432
    user:        str = "postgres"
    password:    str = ""
    dbname:      Optional[str] = None
    db_type:     str = "production"   # production | benchmark
    schema_mode: str = "exclude"
    schemas:     list[str] = ["pg_catalog", "information_schema", "pg_toast"]


@app.post("/api/db/register")
def db_register(req: RegisterRequest):
    from pgxllm.config import TargetDBConfig
    from pgxllm.intelligence import DBRegistryService
    registry, config = get_registry()
    cfg = TargetDBConfig(
        alias=req.alias, host=req.host, port=req.port,
        user=req.user, password=req.password,
        dbname=req.dbname or req.alias,
        db_type=req.db_type,
        schema_mode=req.schema_mode, schemas=req.schemas,
    )
    try:
        DBRegistryService(registry).register(cfg, overwrite=True)
        return {"ok": True, "alias": req.alias}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/api/db/refresh/{alias}")
def db_refresh(alias: str):
    import datetime
    from pgxllm.intelligence import RefreshOrchestrator
    registry, config = get_registry()
    orch = RefreshOrchestrator(registry, config)
    result = orch.refresh(alias)
    return {
        "ok":              result.success,
        "summary":         result.summary(),
        "tables_scanned":  result.tables_scanned,
        "columns_scanned": result.columns_scanned,
        "rules_detected":  result.rules_detected,
        "fk_edges":        result.fk_edges_created,
        "schema_hash":     result.schema_hash,
        "error":           result.error,
        "refreshed_at":    datetime.datetime.now().isoformat(timespec='seconds'),
    }


# ══════════════════════════════════════════════════════════════
# API — /api/schema
# ══════════════════════════════════════════════════════════════

@app.get("/api/schema/{alias}")
def schema_list(alias: str, search: str = ""):
    """Schema catalog — 테이블/컬럼 목록."""
    try:
        registry, _ = get_registry()
        with registry.internal.connection() as conn:
            sql = """
                SELECT schema_name, table_name, column_name,
                       data_type, is_nullable, is_pk, is_fk,
                       fk_ref_table, fk_ref_column, comment_text, n_distinct, sample_values
                FROM schema_catalog
                WHERE db_alias = %s
            """
            params = [alias]
            if search:
                sql += " AND (table_name ILIKE %s OR column_name ILIKE %s OR comment_text ILIKE %s)"
                params += [f"%{search}%", f"%{search}%", f"%{search}%"]
            sql += " ORDER BY schema_name, table_name, column_name NULLS FIRST"
            rows = conn.execute(sql, params)

        # Group into tables
        tables: dict = {}
        for r in rows:
            key = f"{r['schema_name']}.{r['table_name']}"
            if key not in tables:
                tables[key] = {
                    "schema":  r["schema_name"],
                    "table":   r["table_name"],
                    "comment": None,
                    "columns": [],
                }
            if r["column_name"] is None:
                tables[key]["comment"] = r["comment_text"]
            else:
                tables[key]["columns"].append({
                    "name":       r["column_name"],
                    "type":       r["data_type"],
                    "nullable":   r["is_nullable"],
                    "pk":         r["is_pk"],
                    "fk":         r["is_fk"],
                    "fk_ref":     f"{r['fk_ref_table']}.{r['fk_ref_column']}" if r["is_fk"] else None,
                    "comment":    r["comment_text"],
                    "n_distinct": r["n_distinct"],
                    "samples":    r["sample_values"],
                })
        return list(tables.values())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/schema/{alias}/indexes")
def schema_indexes(alias: str):
    """타겟 DB에서 전체 인덱스 목록을 조회한다 (pg_index 기반)."""
    try:
        registry, _ = get_registry()
        mgr = registry.target(alias)
        with mgr.connection() as conn:
            rows = conn.execute(
                """
                SELECT
                    n.nspname                                      AS schema_name,
                    t.relname                                      AS table_name,
                    i.relname                                      AS index_name,
                    ix.indisunique                                 AS is_unique,
                    ix.indisprimary                                AS is_primary,
                    string_agg(
                        a.attname,
                        ', '
                        ORDER BY array_position(ix.indkey, a.attnum)
                    )                                              AS columns,
                    pg_get_indexdef(ix.indexrelid)                 AS index_def
                FROM pg_index     ix
                JOIN pg_class     i ON i.oid = ix.indexrelid
                JOIN pg_class     t ON t.oid = ix.indrelid
                JOIN pg_namespace n ON n.oid = t.relnamespace
                JOIN pg_attribute a ON a.attrelid = t.oid
                                    AND a.attnum = ANY(ix.indkey)
                                    AND a.attnum > 0
                WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                GROUP BY n.nspname, t.relname, i.relname,
                         ix.indisunique, ix.indisprimary, ix.indexrelid
                ORDER BY t.relname, ix.indisprimary DESC, ix.indisunique DESC, i.relname
                """
            )
        # key: "schema.table" → list of indexes
        result: dict = {}
        for r in rows:
            key = f"{r['schema_name']}.{r['table_name']}"
            result.setdefault(key, []).append({
                "name":       r["index_name"],
                "is_unique":  r["is_unique"],
                "is_primary": r["is_primary"],
                "columns":    r["columns"],
                "def":        r["index_def"],
            })
        return result
    except KeyError:
        raise HTTPException(status_code=404, detail=f"DB '{alias}' not registered.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ══════════════════════════════════════════════════════════════
# API — /api/query  (SQL 직접 실행)
# ══════════════════════════════════════════════════════════════

class QueryRequest(BaseModel):
    alias:  str
    sql:    str
    limit:  int  = 200
    mode:   str  = "direct"  # direct | pipeline
    debug:  bool = False


@app.post("/api/query/run")
def query_run(req: QueryRequest):
    """Target DB에 SQL을 실행하거나 Pipeline으로 질문을 처리한다."""
    if req.mode == "pipeline":
        return _run_pipeline(req.alias, req.sql, debug=req.debug)
    return _run_direct_sql(req.alias, req.sql, req.limit)


def _ensure_history_table(registry) -> None:
    """query_history 테이블이 없으면 생성 (마이그레이션 미적용 환경 대응)."""
    with registry.internal.connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS query_history (
                id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
                db_alias        TEXT        NOT NULL,
                mode            TEXT        NOT NULL DEFAULT 'pipeline',
                input_text      TEXT        NOT NULL,
                ok              BOOLEAN     NOT NULL DEFAULT TRUE,
                error           TEXT,
                duration_ms     INTEGER,
                is_benchmark    BOOLEAN     NOT NULL DEFAULT FALSE,
                executed_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_query_history_db "
            "ON query_history(db_alias, executed_at DESC)"
        )
        conn.execute(
            "ALTER TABLE query_history ADD COLUMN IF NOT EXISTS is_benchmark BOOLEAN NOT NULL DEFAULT FALSE"
        )


_history_table_ready = False  # 프로세스 당 한 번만 CREATE 실행


def _is_benchmark_db(registry, db_alias: str) -> bool:
    """db_registry에서 db_type이 benchmark인지 확인."""
    try:
        with registry.internal.connection() as conn:
            row = conn.execute_one(
                "SELECT db_type FROM db_registry WHERE alias=%s", [db_alias]
            )
        return (row or {}).get("db_type") == "benchmark"
    except Exception:
        return False


def _save_history(registry, db_alias: str, mode: str, input_text: str,
                  ok: bool, error, duration_ms, is_benchmark: bool = False) -> None:
    global _history_table_ready
    try:
        if not _history_table_ready:
            _ensure_history_table(registry)
            _history_table_ready = True
        with registry.internal.connection() as conn:
            conn.execute(
                """
                INSERT INTO query_history (db_alias, mode, input_text, ok, error, duration_ms, is_benchmark)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (db_alias, mode, input_text, ok, error, duration_ms, is_benchmark)
            )
    except Exception as e:
        log.warning("[history] save failed: %s", e)


def _run_pipeline(db_alias: str, question: str, *, debug: bool = False) -> dict:
    """Core Pipeline으로 자연어 질문을 처리한다."""
    try:
        from pgxllm.core.pipeline import PipelineRunner
        from pgxllm.core.models import PipelineRequest
        registry, config = get_registry()
        active_llm = get_active_llm_config()
        config = config.model_copy(update={"llm": active_llm})
        runner = PipelineRunner(registry, config)
        result = runner.run(PipelineRequest(question=question, db_alias=db_alias))
        _save_history(registry, db_alias, "pipeline", question,
                      result.ok, result.error, result.duration_ms,
                      is_benchmark=_is_benchmark_db(registry, db_alias))
        resp = {
            "mode":        "pipeline",
            "ok":          result.ok,
            "final_sql":   result.final_sql,
            "explanation": result.explanation,
            "duration_ms": result.duration_ms,
            "cache_hit":   result.cache_hit,
            "error":       result.error,
            "columns":     [],
            "rows":        [],
            "count":       0,
        }
        if debug:
            resp["stage_logs"] = [
                {"stage": s.stage, "ok": s.ok, "duration_ms": s.duration_ms, "detail": s.detail}
                for s in result.stage_logs
            ]
        return resp
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/query/cache")
def query_cache_delete(alias: str, question: str):
    """특정 질문의 캐시(verified_queries)를 삭제한다.

    cache.get()과 동일한 로직 사용:
      1. 정규화 키 exact match
      2. 원본 질문 exact match
      3. pg_trgm similarity >= threshold (유사 질문으로 캐시 히트된 경우 대응)
    """
    try:
        from pgxllm.parser.facade import SqlParser
        registry, config = get_registry()
        key = SqlParser().normalize(question)
        threshold = config.cache.tfidf.similarity_threshold
        with registry.internal.connection() as conn:
            rows = conn.execute(
                """
                DELETE FROM verified_queries
                WHERE db_alias = %s
                  AND (
                    question = %s
                    OR question = %s
                    OR similarity(question, %s) >= %s
                  )
                RETURNING id
                """,
                (alias, key, question, key, threshold)
            )
        return {"deleted": len(rows) if rows else 0, "alias": alias, "question": question}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/query/cache/all")
def query_cache_delete_all(alias: str):
    """DB alias의 모든 파이프라인 캐시를 삭제한다."""
    try:
        registry, _ = get_registry()
        with registry.internal.connection() as conn:
            rows = conn.execute(
                "DELETE FROM verified_queries WHERE db_alias=%s AND source='pipeline' RETURNING id",
                (alias,)
            )
        return {"deleted": len(rows) if rows else 0, "alias": alias}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/query/history")
def query_history_list(alias: str, limit: int = 50, mode: str = ""):
    """최근 실행 이력 조회."""
    try:
        registry, _ = get_registry()
        _ensure_history_table(registry)
        q = "SELECT id, db_alias, mode, input_text, ok, error, duration_ms, executed_at FROM query_history WHERE db_alias=%s"
        params: list = [alias]
        if mode:
            q += " AND mode=%s"
            params.append(mode)
        q += " ORDER BY executed_at DESC LIMIT %s"
        params.append(limit)
        with registry.internal.connection() as conn:
            rows = conn.execute(q, params)
        return [
            {**r, "executed_at": r["executed_at"].isoformat() if r.get("executed_at") else None}
            for r in rows
        ]
    except Exception as e:
        log.error("[history] list failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/query/history/{history_id}")
def query_history_delete(history_id: str, alias: str):
    """이력 항목 하나 삭제."""
    try:
        registry, _ = get_registry()
        with registry.internal.connection() as conn:
            conn.execute(
                "DELETE FROM query_history WHERE id=%s::uuid AND db_alias=%s",
                (history_id, alias)
            )
        return {"ok": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/query/history")
def query_history_clear(alias: str):
    """DB alias의 전체 이력 삭제."""
    try:
        registry, _ = get_registry()
        with registry.internal.connection() as conn:
            rows = conn.execute(
                "DELETE FROM query_history WHERE db_alias=%s RETURNING id", (alias,)
            )
        return {"deleted": len(rows) if rows else 0}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def _run_direct_sql(alias: str, sql: str, limit: int) -> dict:
    """Target DB에 SQL을 직접 실행한다 (SELECT 전용)."""
    sql = sql.strip().rstrip(";").strip()
    if not sql:
        raise HTTPException(status_code=400, detail="SQL이 비어있습니다.")
    first_word = sql.split()[0].upper() if sql.split() else ""
    if first_word not in ("SELECT", "WITH", "EXPLAIN"):
        raise HTTPException(status_code=400, detail="SELECT / WITH / EXPLAIN 만 허용됩니다.")
    import time as _time
    t0 = _time.monotonic()
    try:
        registry, _ = get_registry()
        mgr = registry.target(alias)
        with mgr.connection() as conn:
            rows, truncated = conn.execute_limited(sql, limit=limit)
        dur = int((_time.monotonic() - t0) * 1000)
        _save_history(registry, alias, "direct", sql, True, None, dur,
                      is_benchmark=_is_benchmark_db(registry, alias))
        if not rows:
            return {"columns": [], "rows": [], "count": 0, "truncated": False}
        columns = list(rows[0].keys())
        data    = [[str(v) if v is not None else None for v in row.values()] for row in rows]
        return {"columns": columns, "rows": data, "count": len(data), "truncated": truncated}
    except KeyError:
        raise HTTPException(status_code=404, detail=f"DB '{alias}' not registered.")
    except Exception as e:
        dur = int((_time.monotonic() - t0) * 1000)
        try:
            registry, _ = get_registry()
            _save_history(registry, alias, "direct", sql, False, str(e), dur)
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=str(e))


# ══════════════════════════════════════════════════════════════
# API — /api/graph
# ══════════════════════════════════════════════════════════════

@app.get("/api/graph/{alias}/paths")
def graph_paths_list(alias: str, from_table: str = "", to_table: str = ""):
    """graph_paths 목록 조회 (BFS 계산 결과)."""
    try:
        registry, _ = get_registry()
        q = """
            SELECT id::text, from_address, to_address,
                   hop_count, total_weight, join_hint, is_cross_db,
                   path_json
            FROM graph_paths
            WHERE from_address LIKE %s
        """
        params: list = [f"{alias}.%"]
        if from_table:
            q += " AND from_address ILIKE %s"
            params.append(f"%{from_table}%")
        if to_table:
            q += " AND to_address ILIKE %s"
            params.append(f"%{to_table}%")
        q += " ORDER BY hop_count, total_weight DESC LIMIT 200"
        with registry.internal.connection() as conn:
            rows = conn.execute(q, params)
        result = []
        for r in rows:
            path_json = r["path_json"]
            if isinstance(path_json, str):
                import json as _json
                path_json = _json.loads(path_json)
            result.append({
                "id":           r["id"],
                "from_address": r["from_address"],
                "to_address":   r["to_address"],
                "from_table":   r["from_address"].split(".")[-1],
                "to_table":     r["to_address"].split(".")[-1],
                "hop_count":    r["hop_count"],
                "total_weight": r["total_weight"],
                "join_hint":    r["join_hint"],
                "is_cross_db":  r["is_cross_db"],
                "path_json":    path_json,
            })
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/graph/{alias}")
def graph_edges(alias: str):
    """graph_edges 목록."""
    try:
        registry, _ = get_registry()
        with registry.internal.connection() as conn:
            rows = conn.execute(
                """
                SELECT id::text, from_schema, from_table, from_column,
                       to_db_alias, to_schema, to_table, to_column,
                       relation_name, relation_type, confidence, call_count,
                       approved, is_cross_db
                FROM graph_edges
                WHERE from_db_alias = %s
                ORDER BY approved, relation_type, from_table, from_column
                """,
                [alias]
            )
        return [dict(r) for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class CollectPgStatRequest(BaseModel):
    top:             int   = 100
    min_calls:       int   = 5
    auto_approve_at: float = 0.95


@app.post("/api/graph/{alias}/collect-pg-stat")
def graph_collect_pg_stat(alias: str, req: CollectPgStatRequest):
    """pg_stat_statements에서 JOIN 관계를 분석해 graph_edges에 저장."""
    try:
        registry, config = get_registry()
        from pgxllm.intelligence.relation_collector import RelationCollector
        collector = RelationCollector(registry, config)
        candidates = collector.from_pg_stat_statements(
            alias, top=req.top, min_calls=req.min_calls,
            auto_approve_at=req.auto_approve_at,
        )
        saved = collector.save(candidates, auto_approve_threshold=req.auto_approve_at)
        return {
            "ok": True,
            "candidates": len(candidates),
            "saved": saved,
            "auto_approved": sum(1 for c in candidates if c.auto_approve),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ══════════════════════════════════════════════════════════════
# API — /api/pgstat  (pg_stat_statements 분석 / 튜닝)
# ══════════════════════════════════════════════════════════════

_PGSTAT_EXCLUDE = (
    "pg_catalog", "pg_class", "pg_attribute", "pg_namespace",
    "pg_type", "pg_index", "pg_constraint", "pg_depend",
    "pg_statistic", "pg_stat", "pg_statio", "pg_locks",
    "pg_proc", "pg_trigger", "pg_rewrite", "pg_description",
    "pg_shdescription", "pg_database",
    "pg_am", "pg_toast", "pg_temp", "information_schema",
    # pgxllm 내부 테이블
    "graph_edges", "graph_nodes", "graph_paths",
    "schema_catalog", "schema_embeddings",
    "db_registry", "dialect_rules",
    "query_history", "question_embeddings",
    "sql_patterns", "pattern_applications",
    "pipeline_logs", "verified_queries",
)


@app.get("/api/pgstat/{alias}/queries")
def pgstat_list_queries(alias: str, top: int = 100, min_calls: int = 5):
    """pg_stat_statements에서 JOIN 쿼리 목록을 반환한다 (저장 없음)."""
    try:
        registry, _ = get_registry()
        mgr = registry.target(alias)
        with mgr.connection() as conn:
            rows = conn.execute(
                """
                SELECT queryid::text AS queryid,
                       query, calls,
                       ROUND(total_exec_time::numeric, 2) AS total_exec_time,
                       ROUND(mean_exec_time::numeric,  2) AS mean_exec_time
                FROM pg_stat_statements
                WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
                  AND calls >= %s
                  AND query ILIKE '%%JOIN%%'
                  AND query NOT ILIKE '%%pg_catalog%%'
                  AND query NOT ILIKE '%%information_schema%%'
                  AND query NOT ILIKE '%%pg_stat%%'
                  AND query NOT ILIKE '%%pg_class%%'
                  AND query NOT ILIKE '%%pgxllm_%%'
                ORDER BY calls DESC
                LIMIT %s
                """,
                [min_calls, top],
            )
        result = []
        for r in rows:
            sql_lower = (r["query"] or "").lower()
            if any(kw in sql_lower for kw in _PGSTAT_EXCLUDE):
                continue
            result.append({
                "queryid":         r["queryid"],
                "query":           r["query"],
                "calls":           r["calls"],
                "total_exec_time": r["total_exec_time"],
                "mean_exec_time":  r["mean_exec_time"],
            })
        return result
    except KeyError:
        raise HTTPException(status_code=404, detail=f"DB '{alias}' not registered.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/pgstat/{alias}/reset")
def pgstat_reset(alias: str):
    """pg_stat_statements_reset()을 실행해 해당 DB의 통계를 초기화한다."""
    try:
        registry, _ = get_registry()
        mgr = registry.target(alias)
        with mgr.connection() as conn:
            conn.execute("SELECT pg_stat_statements_reset()")
            conn.commit()
        return {"ok": True, "alias": alias}
    except KeyError:
        raise HTTPException(status_code=404, detail=f"DB '{alias}' not registered.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class QueryInferRequest(BaseModel):
    sql: str


@app.post("/api/pgstat/{alias}/query/infer")
def pgstat_infer_query(alias: str, req: QueryInferRequest):
    """SQL 텍스트에서 JOIN 관계를 추출한다 (graph_edges 저장 없음)."""
    try:
        registry, config = get_registry()
        from pgxllm.intelligence.relation_collector import RelationCollector
        from pgxllm.parser.facade import SqlParser

        collector   = RelationCollector(registry, config)
        schema_map  = collector._build_table_schema_map(alias)
        col_map     = collector._build_table_column_map(alias)
        known_tables = set(schema_map.keys())

        # Pre-check: detect parse failure (distinct from "no JOIN found")
        from pgxllm.parser.sql_parser import parse_sql as _parse_sql
        try:
            _stmts = _parse_sql(req.sql)
            parse_error = (len(_stmts) == 0)
        except Exception:
            parse_error = True

        parser    = SqlParser(max_depth=config.parser.max_depth)
        relations = parser.extract_relations(req.sql)

        seen: set[tuple] = set()
        candidates = []
        schema_available = bool(known_tables)
        for rel in relations:
            ft = rel.from_table.lower()
            tt = rel.to_table.lower()
            fc = rel.from_column.lower()
            tc = rel.to_column.lower()
            key = (ft, fc, tt, tc)
            if key in seen:
                continue
            seen.add(key)

            # Determine if this candidate is schema-verified
            tables_ok  = (not schema_available) or (ft in known_tables and tt in known_tables)
            columns_ok = (not schema_available) or (
                fc in col_map.get(ft, set()) and tc in col_map.get(tt, set())
            )
            unverified = not (tables_ok and columns_ok)

            candidates.append({
                "from_schema":  schema_map.get(ft, "public"),
                "from_table":   ft,
                "from_column":  fc,
                "to_schema":    schema_map.get(tt, "public"),
                "to_table":     tt,
                "to_column":    tc,
                "confidence":   0.8 if not unverified else 0.5,
                "unverified":   unverified,
                "already_saved": False,
                "approved":      False,
            })

        # graph_edges 존재 여부 확인 — 이미 저장된 관계에 표시
        with registry.internal.connection() as conn:
            for c in candidates:
                row = conn.execute_one(
                    """
                    SELECT approved FROM graph_edges
                    WHERE from_db_alias = %s
                      AND LOWER(from_table)  = %s AND LOWER(from_column) = %s
                      AND LOWER(to_table)    = %s AND LOWER(to_column)   = %s
                    """,
                    (alias, c["from_table"], c["from_column"],
                     c["to_table"],  c["to_column"]),
                )
                if row:
                    c["already_saved"] = True
                    c["approved"]      = bool(row["approved"])

        return {"ok": True, "candidates": candidates, "parse_error": parse_error}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class SaveCacheRequest(BaseModel):
    question: str
    sql: str


@app.post("/api/pgstat/{alias}/query/save-cache")
def pgstat_save_cache(alias: str, req: SaveCacheRequest):
    """자연어 질문과 SQL을 verified_queries 캐시에 저장한다."""
    try:
        registry, _ = get_registry()
        with registry.internal.connection() as conn:
            conn.execute(
                """
                INSERT INTO verified_queries (db_alias, question, sql, execution_ok, source)
                VALUES (%s, %s, %s, TRUE, 'user_feedback')
                """,
                (alias, req.question, req.sql),
            )
        return {"ok": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class SaveEdgeRequest(BaseModel):
    from_schema:   str
    from_table:    str
    from_column:   str
    to_schema:     str
    to_table:      str
    to_column:     str
    relation_name: Optional[str] = None
    confidence:    float = 0.8
    source_sql:    Optional[str] = None


@app.post("/api/pgstat/{alias}/query/save-edge")
def pgstat_save_edge(alias: str, req: SaveEdgeRequest):
    """추론된 관계 하나를 graph_edges에 analyzed 타입으로 저장한다."""
    try:
        registry, config = get_registry()
        from pgxllm.graph.base import TableEdge
        from pgxllm.graph.postgresql import PostgreSQLGraphStore

        graph = PostgreSQLGraphStore(registry, max_depth=config.graph.max_depth)
        edge  = TableEdge(
            from_db_alias=alias,
            from_schema=req.from_schema,
            from_table=req.from_table,
            from_column=req.from_column,
            to_db_alias=alias,
            to_schema=req.to_schema,
            to_table=req.to_table,
            to_column=req.to_column,
            relation_name=req.relation_name,
            relation_type="analyzed",
            confidence=req.confidence,
            call_count=1,
            approved=False,
            source_sql=req.source_sql,
            is_cross_db=False,
        )
        graph.add_edge(edge)
        return {"ok": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class QueryTuneRequest(BaseModel):
    sql: str


@app.post("/api/pgstat/{alias}/query/tune")
def pgstat_tune_query(alias: str, req: QueryTuneRequest):
    """LLM으로 쿼리 튜닝 제안을 생성한다."""
    try:
        registry, config = get_registry()
        from pgxllm.core.llm.factory import create_llm_provider
        from pgxllm.intelligence.relation_collector import RelationCollector
        from pgxllm.parser.facade import SqlParser

        collector  = RelationCollector(registry, config)
        schema_map = collector._build_table_schema_map(alias)

        parser    = SqlParser(max_depth=config.parser.max_depth)
        relations = parser.extract_relations(req.sql)
        tables    = {rel.from_table.lower() for rel in relations} | \
                    {rel.to_table.lower()   for rel in relations}

        table_lines: list[str] = []
        with registry.internal.connection() as conn:
            for tbl in sorted(tables):
                if tbl not in schema_map:
                    continue
                cols = conn.execute(
                    "SELECT column_name, data_type FROM schema_catalog "
                    "WHERE db_alias=%s AND table_name=%s AND column_name IS NOT NULL "
                    "ORDER BY column_name",
                    (alias, tbl),
                )
                col_strs = [f"{c['column_name']} {c['data_type']}" for c in cols[:20]]
                table_lines.append(
                    f"  {schema_map[tbl]}.{tbl}({', '.join(col_strs)})"
                )

        schema_ctx = "\n".join(table_lines) if table_lines else "(schema not available)"

        llm = create_llm_provider(get_active_llm_config())
        system = (
            "You are a PostgreSQL performance expert. "
            "Analyze the SQL query and provide specific, actionable optimization suggestions. "
            "Focus on: index usage, JOIN order, unnecessary subqueries, aggregation efficiency, N+1 patterns. "
            "Structure your response as: "
            "1) 문제점 (Issues found) "
            "2) 최적화된 쿼리 (Optimized query) "
            "3) 설명 (Explanation). "
            "Use Korean for explanations."
        )
        user = (
            f"Schema context:\n{schema_ctx}\n\n"
            f"Query to optimize:\n{req.sql}"
        )
        resp = llm.complete(system, user, temperature=0.3, max_tokens=1500)
        return {"ok": True, "suggestion": resp.text}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class QueryPlanRequest(BaseModel):
    sql:     str
    analyze: bool = False


@app.post("/api/pgstat/{alias}/query/plan")
def pgstat_query_plan(alias: str, req: QueryPlanRequest):
    """EXPLAIN (FORMAT JSON) 또는 EXPLAIN (ANALYZE, FORMAT JSON)으로 실행 계획을 반환한다.

    ANALYZE=True 시 PostgreSQL 기본 READ COMMITTED(MVCC)로 실행:
    - SELECT/WITH는 row lock 없이 실행되므로 rollback 불필요
    - INSERT/UPDATE/DELETE 등 DML은 rollback으로 부작용 방지
    """
    import json, re
    try:
        registry, _ = get_registry()
        mgr = registry.target(alias)
        analyze_clause = "ANALYZE true, BUFFERS false, " if req.analyze else ""
        explain_sql = f"EXPLAIN ({analyze_clause}FORMAT JSON, VERBOSE false) {req.sql}"

        with mgr.connection() as conn:
            rows = conn.execute(explain_sql)

            if req.analyze:
                stripped = req.sql.lstrip()
                is_select = bool(re.match(r'^\s*(SELECT|WITH)\b', stripped, re.IGNORECASE))
                if is_select:
                    conn.raw.commit()   # SELECT — MVCC 읽기, lock 없음, commit(no-op)
                else:
                    conn.raw.rollback() # DML — 롤백으로 부작용 방지

        if not rows:
            raise HTTPException(status_code=500, detail="EXPLAIN returned no rows")
        row       = rows[0]
        plan_data = row[list(row.keys())[0]]
        if isinstance(plan_data, str):
            plan_data = json.loads(plan_data)
        root = plan_data[0] if isinstance(plan_data, list) else plan_data
        return {
            "ok":             True,
            "plan":           root.get("Plan", root),
            "planning_time":  root.get("Planning Time"),
            "execution_time": root.get("Execution Time"),   # ANALYZE 시에만 존재
            "analyzed":       req.analyze,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Saved Query Plans ─────────────────────────────────────────────────────────



class QueryDescribeRequest(BaseModel):
    sql: str


@app.post("/api/pgstat/{alias}/query/describe")
def pgstat_describe_query(alias: str, req: QueryDescribeRequest):
    """SQL을 LLM으로 자연어 질문으로 변환한다 (reverse inference)."""
    try:
        registry, config = get_registry()
        from pgxllm.core.llm.factory import create_llm_provider

        llm = create_llm_provider(get_active_llm_config())
        system = (
            "You are a database expert. "
            "Given a SQL query, generate a concise natural language question that this query answers. "
            "$1, $2, $3 etc. are parameter placeholders — treat them as variable user inputs. "
            "Reply with ONLY the natural language question in Korean. No explanation, no SQL."
        )
        user = f"SQL:\n{req.sql}"
        resp = llm.complete(system, user, temperature=0.3, max_tokens=150)
        return {"ok": True, "description": resp.text.strip()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/graph/{alias}/edges/invalid")
def graph_delete_invalid_edges(alias: str):
    """
    analyzed 타입 엣지 중 schema_catalog에 없는 컬럼을 가진 것을 삭제한다.
    이미 저장된 잘못된 analyzed 엣지 정리용.
    """
    try:
        registry, _ = get_registry()
        with registry.internal.connection() as conn:
            rows = conn.execute(
                """
                DELETE FROM graph_edges
                WHERE from_db_alias = %s
                  AND relation_type = 'analyzed'
                  AND approved = FALSE
                  AND (
                      NOT EXISTS (
                          SELECT 1 FROM schema_catalog sc
                          WHERE sc.db_alias = graph_edges.from_db_alias
                            AND sc.table_name = graph_edges.from_table
                            AND sc.column_name = graph_edges.from_column
                      )
                      OR NOT EXISTS (
                          SELECT 1 FROM schema_catalog sc
                          WHERE sc.db_alias = graph_edges.to_db_alias
                            AND sc.table_name = graph_edges.to_table
                            AND sc.column_name = graph_edges.to_column
                      )
                  )
                RETURNING id
                """,
                (alias,)
            )
        return {"ok": True, "deleted": len(rows)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/graph/{alias}/paths")
def graph_delete_paths(alias: str):
    """해당 DB의 graph_paths 전체 삭제."""
    try:
        registry, _ = get_registry()
        with registry.internal.connection() as conn:
            rows = conn.execute(
                "DELETE FROM graph_paths WHERE from_address LIKE %s OR to_address LIKE %s RETURNING from_address",
                (f"{alias}.%", f"{alias}.%")
            )
        return {"ok": True, "deleted": len(rows)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/graph/{alias}/path/{path_id}")
def graph_delete_path(alias: str, path_id: str):
    """개별 graph_path 삭제."""
    try:
        registry, _ = get_registry()
        with registry.internal.connection() as conn:
            conn.execute(
                "DELETE FROM graph_paths WHERE id=%s::uuid AND (from_address LIKE %s OR to_address LIKE %s)",
                (path_id, f"{alias}.%", f"{alias}.%")
            )
        return {"ok": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def _refresh_paths_for(alias: str) -> int:
    """승인된 graph_edges 기반으로 BFS 경로를 재계산한다."""
    from pgxllm.graph.postgresql import PostgreSQLGraphStore
    registry, config = get_registry()
    store = PostgreSQLGraphStore(registry, max_depth=config.graph.max_depth)
    return store.refresh_paths(alias)


@app.post("/api/graph/{alias}/refresh-paths")
def graph_refresh_paths(alias: str):
    """승인된 graph_edges로 graph_paths(BFS 경로)를 재계산한다."""
    try:
        count = _refresh_paths_for(alias)
        return {"ok": True, "paths": count}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/graph/{alias}/approve/{edge_id}")
def graph_approve_edge(alias: str, edge_id: str):
    """특정 graph_edge 승인 (경로 재계산은 별도로 수행)."""
    try:
        registry, _ = get_registry()
        with registry.internal.connection() as conn:
            conn.execute(
                "UPDATE graph_edges SET approved=TRUE, updated_at=NOW() WHERE id=%s::uuid AND from_db_alias=%s",
                [edge_id, alias]
            )
        return {"ok": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/graph/{alias}/approve-all")
def graph_approve_all(alias: str):
    """pending 상태의 graph_edges 전체 승인 (경로 재계산은 별도로 수행)."""
    try:
        registry, _ = get_registry()
        with registry.internal.connection() as conn:
            rows = conn.execute(
                "UPDATE graph_edges SET approved=TRUE, updated_at=NOW() WHERE from_db_alias=%s AND approved=FALSE RETURNING id",
                [alias]
            )
        return {"ok": True, "approved": len(rows)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/graph/{alias}/edge/{edge_id}")
def graph_delete_edge(alias: str, edge_id: str):
    """graph_edge 삭제."""
    try:
        registry, _ = get_registry()
        with registry.internal.connection() as conn:
            conn.execute(
                "DELETE FROM graph_edges WHERE id=%s::uuid AND from_db_alias=%s",
                [edge_id, alias]
            )
        return {"ok": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class EdgeUpdateRequest(BaseModel):
    from_schema:    Optional[str] = None
    from_table:     Optional[str] = None
    from_column:    Optional[str] = None
    to_schema:      Optional[str] = None
    to_table:       Optional[str] = None
    to_column:      Optional[str] = None
    relation_name:  Optional[str] = None
    relation_type:  Optional[str] = None
    confidence:     Optional[float] = None


@app.post("/api/graph/{alias}/edge/{edge_id}/infer-name")
def graph_infer_relation_name(alias: str, edge_id: str):
    """
    단일 pending edge의 relation_name을 추정한다.
    1순위: LLM (테이블 컬럼 컨텍스트 + FK 방향 기반 의미론적 추론)
    2순위: 규칙 기반 (FK 방향 감지 → has_/belongs_to_ 패턴)
    3순위: from_column _id 제거 또는 to_table 이름
    """
    try:
        registry, config = get_registry()
        with registry.internal.connection() as conn:
            edge = conn.execute_one(
                "SELECT * FROM graph_edges WHERE id=%s::uuid AND from_db_alias=%s",
                [edge_id, alias]
            )
        if not edge:
            raise HTTPException(status_code=404, detail="Edge not found")

        from_table  = edge["from_table"]
        from_column = edge["from_column"]
        to_table    = edge["to_table"]
        to_column   = edge["to_column"]
        from_schema = edge["from_schema"]
        to_schema   = edge["to_schema"]

        # ── 컬럼 목록 조회 (schema_catalog) ──────────────────
        with registry.internal.connection() as conn:
            from_cols = [r["column_name"] for r in conn.execute(
                "SELECT column_name FROM schema_catalog WHERE db_alias=%s AND table_name=%s AND column_name IS NOT NULL ORDER BY column_name",
                (alias, from_table)
            )]
            to_cols = [r["column_name"] for r in conn.execute(
                "SELECT column_name FROM schema_catalog WHERE db_alias=%s AND table_name=%s AND column_name IS NOT NULL ORDER BY column_name",
                (alias, to_table)
            )]

        # ── FK 방향 감지 (from_table이 FK를 가지고 있으면 child) ──
        is_fk_direction = False  # from_table → to_table 이 FK 방향인지
        try:
            mgr = registry.target(alias)
            with mgr.connection() as conn:
                fk_row = conn.execute_one(
                    """
                    SELECT tc.constraint_name
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu
                      ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
                    JOIN information_schema.referential_constraints rc
                      ON rc.constraint_name = tc.constraint_name
                    JOIN information_schema.key_column_usage kcu2
                      ON kcu2.constraint_name = rc.unique_constraint_name
                    WHERE tc.constraint_type = 'FOREIGN KEY'
                      AND LOWER(tc.table_name)   = LOWER(%s)
                      AND LOWER(kcu.column_name) = LOWER(%s)
                      AND LOWER(kcu2.table_name) = LOWER(%s)
                    LIMIT 1
                    """,
                    [from_table, from_column, to_table]
                )
            is_fk_direction = bool(fk_row)
        except KeyError:
            pass

        # ── 0. 캐시 — 동일 컬럼 쌍으로 이미 relation_name이 있으면 재사용 ──
        suggested = None
        with registry.internal.connection() as conn:
            cached = conn.execute_one(
                """
                SELECT relation_name FROM graph_edges
                WHERE from_db_alias = %s
                  AND LOWER(from_table)  = LOWER(%s)
                  AND LOWER(from_column) = LOWER(%s)
                  AND LOWER(to_table)    = LOWER(%s)
                  AND LOWER(to_column)   = LOWER(%s)
                  AND relation_name IS NOT NULL
                  AND id != %s::uuid
                LIMIT 1
                """,
                (alias, from_table, from_column, to_table, to_column, edge_id)
            )
        if cached:
            return {"ok": True, "suggested_name": cached["relation_name"], "source": "cache"}

        # ── 1. LLM 추론 ───────────────────────────────────────
        try:
            from pgxllm.core.llm.factory import create_llm_provider
            llm = create_llm_provider(get_active_llm_config())

            direction_desc = (
                f"'{from_table}' has a foreign key '{from_column}' referencing '{to_table}.{to_column}' (child→parent direction)"
                if is_fk_direction else
                f"'{to_table}' has a foreign key referencing '{from_table}' (parent→child direction, reverse traversal)"
            )

            system = (
                "You are a database schema expert. "
                "Given two related tables and their join condition, suggest a concise snake_case relation name "
                "that describes how the source table relates to the target table. "
                "Examples: has_rentals, belongs_to_customer, rented_from, managed_by, contains_items. "
                "Reply with ONLY the relation name, nothing else."
            )
            user = (
                f"Source table: {from_table}\n"
                f"Source columns: {', '.join(from_cols[:20])}\n\n"
                f"Target table: {to_table}\n"
                f"Target columns: {', '.join(to_cols[:20])}\n\n"
                f"Join condition: {from_table}.{from_column} = {to_table}.{to_column}\n"
                f"Relationship direction: {direction_desc}\n\n"
                f"Suggest a relation name for: {from_table} → {to_table}"
            )
            resp = llm.complete(system, user, temperature=0.2, max_tokens=20)
            name = resp.text.strip().lower().split()[0] if resp.text.strip() else None
            # snake_case 검증 (영문자/숫자/언더스코어만 허용)
            import re as _re
            if name and _re.match(r'^[a-z][a-z0-9_]*$', name):
                suggested = name
                return {"ok": True, "suggested_name": suggested, "source": "llm"}
        except Exception as e:
            log.warning("[infer-name] LLM failed, falling back to rules: %s", e)

        # ── 2. 규칙 기반 fallback ─────────────────────────────
        if not suggested:
            if is_fk_direction:
                # child → parent: belongs_to 또는 컬럼명 기반
                col = from_column.lower()
                if col.endswith("_id") and col[:-3] != to_table.lower():
                    suggested = col[:-3]        # rental.inventory_id → inventory (의미상 from_inventory)
                else:
                    suggested = f"belongs_to_{to_table.lower()}"
            else:
                # parent → child (역방향): has_{child}
                suggested = f"has_{to_table.lower()}"

        return {"ok": True, "suggested_name": suggested, "source": "rules"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.patch("/api/graph/{alias}/edge/{edge_id}")
def graph_update_edge(alias: str, edge_id: str, req: EdgeUpdateRequest):
    """graph_edge 내용 수정."""
    try:
        registry, _ = get_registry()
        fields = {k: v for k, v in req.model_dump().items() if v is not None}
        if not fields:
            raise HTTPException(status_code=400, detail="수정할 필드가 없습니다.")
        set_clause = ", ".join(f"{k}=%s" for k in fields)
        values = list(fields.values()) + [edge_id, alias]
        with registry.internal.connection() as conn:
            conn.execute(
                f"UPDATE graph_edges SET {set_clause}, updated_at=NOW() WHERE id=%s::uuid AND from_db_alias=%s",
                values
            )
        return {"ok": True}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ══════════════════════════════════════════════════════════════
# API — /api/rules
# ══════════════════════════════════════════════════════════════

@app.get("/api/rules/{alias}")
def rules_list(alias: str):
    try:
        registry, _ = get_registry()
        is_bm = _is_benchmark_db(registry, alias)
        with registry.internal.connection() as conn:
            rows = conn.execute(
                """
                SELECT rule_id, scope, db_alias, table_name, column_name,
                       forbidden_funcs, forbidden_sql_patterns, required_func,
                       instruction, example_bad, example_good,
                       severity, auto_detected, enabled, created_at
                FROM dialect_rules
                WHERE db_alias = %s
                   OR scope IN ('global', 'dialect')
                   OR (scope IN ('table', 'column') AND db_alias IS NULL)
                   OR (scope = 'benchmark' AND (%s OR db_alias = %s))
                ORDER BY auto_detected DESC, scope, rule_id
                """,
                [alias, is_bm, alias]
            )
        return [dict(r) for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class RuleCreateRequest(BaseModel):
    rule_id:                str
    scope:                  str  = "global"
    db_alias:               Optional[str] = None
    table_name:             Optional[str] = None
    column_name:            Optional[str] = None
    instruction:            str
    forbidden_funcs:        list = []
    forbidden_sql_patterns: list = []
    required_func:          Optional[str] = None
    example_bad:            str  = ""
    example_good:           str  = ""
    severity:               str  = "warning"


@app.post("/api/rules/{alias}")
def rules_create(alias: str, req: RuleCreateRequest):
    """Dialect rule을 등록/갱신한다."""
    try:
        registry, _ = get_registry()
        from pgxllm.intelligence.rule_engine import RuleEngine
        engine = RuleEngine(registry)
        # global → db_alias 무조건 None
        # table/column → req.db_alias 우선, 없으면 None (모든 DB에 적용)
        # db → req.db_alias 우선, 없으면 현재 alias
        if req.scope == "global":
            db_alias = None
        elif req.scope == "db":
            db_alias = req.db_alias or alias
        else:
            db_alias = req.db_alias or None
        engine.add_rule(
            rule_id=req.rule_id,
            scope=req.scope,
            db_alias=db_alias,
            table_name=req.table_name,
            column_name=req.column_name,
            instruction=req.instruction,
            forbidden_funcs=req.forbidden_funcs,
            forbidden_sql_patterns=req.forbidden_sql_patterns,
            required_func=req.required_func,
            example_bad=req.example_bad,
            example_good=req.example_good,
            severity=req.severity,
            overwrite=True,
        )
        return {"ok": True, "rule_id": req.rule_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.patch("/api/rules/{alias}/{rule_id}")
def rules_toggle(alias: str, rule_id: str, enabled: bool):
    """Rule 활성/비활성 전환."""
    try:
        registry, _ = get_registry()
        from pgxllm.intelligence.rule_engine import RuleEngine
        engine = RuleEngine(registry)
        if enabled:
            engine.enable_rule(rule_id)
        else:
            engine.disable_rule(rule_id)
        return {"ok": True, "rule_id": rule_id, "enabled": enabled}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/rules/{alias}/{rule_id}")
def rules_delete(alias: str, rule_id: str):
    """Rule 삭제."""
    try:
        registry, _ = get_registry()
        from pgxllm.intelligence.rule_engine import RuleEngine
        RuleEngine(registry).delete_rule(rule_id)
        return {"ok": True, "rule_id": rule_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class PromoteRuleRequest(BaseModel):
    target_scope: str = "global"   # global | db
    target_db_alias: Optional[str] = None


@app.post("/api/rules/{alias}/{rule_id}/promote")
def rules_promote(alias: str, rule_id: str, req: PromoteRuleRequest):
    """benchmark 스코프 룰을 production scope(global/db)로 승격한다."""
    try:
        registry, _ = get_registry()
        if req.target_scope not in ("global", "db"):
            raise HTTPException(status_code=400, detail="target_scope must be 'global' or 'db'")
        new_db_alias = None if req.target_scope == "global" else (req.target_db_alias or alias)
        with registry.internal.connection() as conn:
            conn.execute(
                """
                UPDATE dialect_rules
                SET scope=%s, db_alias=%s, updated_at=NOW()
                WHERE rule_id=%s AND scope='benchmark'
                """,
                [req.target_scope, new_db_alias, rule_id]
            )
        return {"ok": True, "rule_id": rule_id, "new_scope": req.target_scope}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ══════════════════════════════════════════════════════════════
# API — /api/patterns
# ══════════════════════════════════════════════════════════════

@app.get("/api/patterns")
def patterns_list():
    try:
        registry, _ = get_registry()
        with registry.internal.connection() as conn:
            rows = conn.execute(
                "SELECT * FROM sql_patterns ORDER BY hit_count DESC"
            )
        return [dict(r) for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ══════════════════════════════════════════════════════════════
# API — /api/eval  (BIRD Benchmark 평가)
# ══════════════════════════════════════════════════════════════

import json as _json
import time as _time_mod


class EvalRunRequest(BaseModel):
    db_alias:   str
    eval_name:  str = ""
    items:      list   # [{question_id, question, gold_sql}, ...]
    run_baseline: bool = True   # LLM 직접 생성 (S1 only)
    run_pipeline: bool = True   # 전체 파이프라인


@app.post("/api/eval/run")
async def eval_run(req: EvalRunRequest):
    """BIRD 벤치마크 평가를 실행한다.

    items 형식: [{question_id: int, question: str, gold_sql: str}, ...]
    baseline: LLM이 스키마만 보고 직접 생성한 SQL
    pipeline: pgxllm 전체 파이프라인으로 생성한 SQL
    EX(Execution Accuracy): 결과셋이 gold_sql 결과와 동일한지 비교
    """
    from pgxllm.config import LLMConfig
    registry, config = get_registry()
    active_llm = get_active_llm_config()
    config = config.model_copy(update={"llm": active_llm})

    results = []
    eval_name = req.eval_name or f"eval_{int(_time_mod.time())}"

    for item in req.items:
        qid      = item.get("question_id")
        question = item.get("question", "")
        gold_sql = item.get("gold_sql", "")

        baseline_sql = pipeline_sql = None
        baseline_ok  = pipeline_ok  = None
        baseline_ex  = pipeline_ex  = None
        err_base = err_pipe = None
        t0 = _time_mod.monotonic()

        # ── Gold 결과 실행 ──────────────────────────────────────
        gold_rows = None
        try:
            mgr = registry.target(req.db_alias)
            with mgr.connection() as conn:
                gold_rows_raw, _ = conn.execute_limited(gold_sql.strip().rstrip(";"), limit=500)
                gold_rows = [tuple(r.values()) for r in gold_rows_raw]
        except Exception as e:
            gold_rows = None

        # ── Baseline: S1(Schema) + LLM 직접 생성 ───────────────
        if req.run_baseline:
            try:
                from pgxllm.core.pipeline import PipelineRunner
                from pgxllm.core.models import PipelineRequest
                runner = PipelineRunner(registry, config)
                # stage_mask: S1 only
                pr = PipelineRequest(question=question, db_alias=req.db_alias)
                res = runner.run_stage1_only(pr)
                baseline_sql = res.final_sql
                if baseline_sql:
                    with mgr.connection() as conn:
                        brows_raw, _ = conn.execute_limited(baseline_sql.strip().rstrip(";"), limit=500)
                        brows = [tuple(r.values()) for r in brows_raw]
                    baseline_ok = True
                    baseline_ex = (gold_rows is not None) and (sorted(map(str, brows)) == sorted(map(str, gold_rows)))
                else:
                    baseline_ok = False
            except Exception as e:
                baseline_ok = False
                err_base = str(e)

        # ── Pipeline: 전체 S1~S4 ───────────────────────────────
        if req.run_pipeline:
            try:
                from pgxllm.core.pipeline import PipelineRunner
                from pgxllm.core.models import PipelineRequest
                runner = PipelineRunner(registry, config)
                pr = PipelineRequest(question=question, db_alias=req.db_alias)
                res = runner.run(pr)
                pipeline_sql = res.final_sql
                if pipeline_sql and res.ok:
                    with mgr.connection() as conn:
                        prows_raw, _ = conn.execute_limited(pipeline_sql.strip().rstrip(";"), limit=500)
                        prows = [tuple(r.values()) for r in prows_raw]
                    pipeline_ok = True
                    pipeline_ex = (gold_rows is not None) and (sorted(map(str, prows)) == sorted(map(str, gold_rows)))
                else:
                    pipeline_ok = res.ok
                    err_pipe = res.error
            except Exception as e:
                pipeline_ok = False
                err_pipe = str(e)

        dur = int((_time_mod.monotonic() - t0) * 1000)

        # ── eval_results 저장 ──────────────────────────────────
        try:
            with registry.internal.connection() as conn:
                conn.execute(
                    """
                    INSERT INTO eval_results
                        (db_alias, eval_name, question_id, question, gold_sql,
                         baseline_sql, pipeline_sql,
                         baseline_ok, pipeline_ok, baseline_ex, pipeline_ex,
                         error_baseline, error_pipeline, duration_ms)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (req.db_alias, eval_name, qid, question, gold_sql,
                     baseline_sql, pipeline_sql,
                     baseline_ok, pipeline_ok, baseline_ex, pipeline_ex,
                     err_base, err_pipe, dur)
                )
        except Exception as e:
            log.warning("[eval] save failed: %s", e)

        results.append({
            "question_id":   qid,
            "question":      question,
            "gold_sql":      gold_sql,
            "baseline_sql":  baseline_sql,
            "pipeline_sql":  pipeline_sql,
            "baseline_ok":   baseline_ok,
            "pipeline_ok":   pipeline_ok,
            "baseline_ex":   baseline_ex,
            "pipeline_ex":   pipeline_ex,
            "error_baseline": err_base,
            "error_pipeline": err_pipe,
            "duration_ms":   dur,
        })

    total = len(results)
    b_ex  = sum(1 for r in results if r["baseline_ex"]) if req.run_baseline else None
    p_ex  = sum(1 for r in results if r["pipeline_ex"]) if req.run_pipeline else None

    return {
        "eval_name":     eval_name,
        "db_alias":      req.db_alias,
        "total":         total,
        "baseline_ex_count": b_ex,
        "pipeline_ex_count": p_ex,
        "baseline_ex_rate":  round(b_ex / total, 4) if (b_ex is not None and total) else None,
        "pipeline_ex_rate":  round(p_ex / total, 4) if (p_ex is not None and total) else None,
        "results":       results,
    }


@app.get("/api/eval/list")
def eval_list(db_alias: str = ""):
    """eval_results 요약 목록 (eval_name 별 집계)."""
    try:
        registry, _ = get_registry()
        where = "WHERE db_alias=%s" if db_alias else ""
        params = [db_alias] if db_alias else []
        with registry.internal.connection() as conn:
            rows = conn.execute(
                f"""
                SELECT db_alias, eval_name,
                       COUNT(*)                                    AS total,
                       SUM(CASE WHEN baseline_ex THEN 1 ELSE 0 END) AS baseline_ex,
                       SUM(CASE WHEN pipeline_ex THEN 1 ELSE 0 END) AS pipeline_ex,
                       MAX(created_at)                              AS last_run
                FROM eval_results
                {where}
                GROUP BY db_alias, eval_name
                ORDER BY MAX(created_at) DESC
                """,
                params
            )
        return [
            {**dict(r), "last_run": r["last_run"].isoformat() if r.get("last_run") else None}
            for r in rows
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/eval/results")
def eval_results(db_alias: str, eval_name: str):
    """eval_name에 해당하는 상세 결과 목록."""
    try:
        registry, _ = get_registry()
        with registry.internal.connection() as conn:
            rows = conn.execute(
                """
                SELECT id, question_id, question, gold_sql,
                       baseline_sql, pipeline_sql,
                       baseline_ok, pipeline_ok, baseline_ex, pipeline_ex,
                       error_baseline, error_pipeline, duration_ms, created_at
                FROM eval_results
                WHERE db_alias=%s AND eval_name=%s
                ORDER BY question_id
                """,
                [db_alias, eval_name]
            )
        return [
            {**dict(r), "created_at": r["created_at"].isoformat() if r.get("created_at") else None,
             "id": str(r["id"])}
            for r in rows
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/eval/results")
def eval_results_delete(db_alias: str, eval_name: str):
    """eval_name 결과 전체 삭제."""
    try:
        registry, _ = get_registry()
        with registry.internal.connection() as conn:
            rows = conn.execute(
                "DELETE FROM eval_results WHERE db_alias=%s AND eval_name=%s RETURNING id",
                [db_alias, eval_name]
            )
        return {"deleted": len(rows) if rows else 0}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ══════════════════════════════════════════════════════════════
# API — /api/llm  (LLM 설정 관리)
# ══════════════════════════════════════════════════════════════

_LLM_TABLE_READY = False

def _ensure_llm_settings_table(registry) -> None:
    global _LLM_TABLE_READY
    if _LLM_TABLE_READY:
        return
    with registry.internal.connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS llm_settings (
                id          SERIAL      PRIMARY KEY,
                provider    TEXT        NOT NULL,
                base_url    TEXT        NOT NULL DEFAULT '',
                model       TEXT        NOT NULL DEFAULT '',
                api_key     TEXT        NOT NULL DEFAULT '',
                project_id  TEXT        NOT NULL DEFAULT '',
                username    TEXT        NOT NULL DEFAULT '',
                verify_ssl  BOOLEAN     NOT NULL DEFAULT TRUE,
                timeout     INTEGER     NOT NULL DEFAULT 600,
                max_tokens  INTEGER     NOT NULL DEFAULT 2048,
                temperature FLOAT       NOT NULL DEFAULT 0.0,
                updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
        conn.execute("ALTER TABLE llm_settings ADD COLUMN IF NOT EXISTS username   TEXT    NOT NULL DEFAULT ''")
        conn.execute("ALTER TABLE llm_settings ADD COLUMN IF NOT EXISTS verify_ssl BOOLEAN NOT NULL DEFAULT TRUE")
    _LLM_TABLE_READY = True


def get_active_llm_config():
    """DB 저장 설정 우선, 없으면 YAML config.llm 반환."""
    from pgxllm.config import LLMConfig
    registry, config = get_registry()
    try:
        _ensure_llm_settings_table(registry)
        with registry.internal.connection() as conn:
            row = conn.execute_one(
                "SELECT * FROM llm_settings ORDER BY id DESC LIMIT 1"
            )
        if row:
            return LLMConfig(
                provider=row["provider"],
                base_url=row["base_url"]     or "",
                model=row["model"]           or "",
                api_key=row["api_key"]       or "",
                project_id=row["project_id"] or "",
                username=row.get("username", "") or "",
                verify_ssl=row.get("verify_ssl", True),
                timeout=row["timeout"],
                max_tokens=row["max_tokens"],
                temperature=row["temperature"],
            )
    except Exception as e:
        log.warning("[llm] fallback to yaml config: %s", e)
    return config.llm


class LLMSettingsRequest(BaseModel):
    provider:    str   = "ollama"
    base_url:    str   = ""
    model:       str   = ""
    api_key:     str   = ""
    project_id:  str   = ""
    username:    str   = ""   # CP4D on-prem 전용 (없으면 IBM Cloud IAM)
    verify_ssl:  bool  = True
    timeout:     int   = 600
    max_tokens:  int   = 2048
    temperature: float = 0.0


@app.get("/api/llm/config")
def llm_config_get():
    """현재 활성 LLM 설정을 반환한다. api_key는 마스킹."""
    cfg = get_active_llm_config()
    key = cfg.api_key
    masked = f"{'*' * (len(key) - 4)}{key[-4:]}" if len(key) > 4 else ("*" * len(key))
    return {
        "provider":    cfg.provider,
        "base_url":    cfg.base_url,
        "model":       cfg.model,
        "api_key":     masked,
        "api_key_set": bool(key),
        "project_id":  cfg.project_id,
        "username":    cfg.username,
        "verify_ssl":  cfg.verify_ssl,
        "timeout":     cfg.timeout,
        "max_tokens":  cfg.max_tokens,
        "temperature": cfg.temperature,
    }


@app.post("/api/llm/config")
def llm_config_save(req: LLMSettingsRequest):
    """LLM 설정을 DB에 저장한다. api_key가 '*' 로만 구성이면 기존 값 유지."""
    registry, _ = get_registry()
    try:
        _ensure_llm_settings_table(registry)
        # api_key가 마스킹된 값이면 기존 값 보존
        api_key = req.api_key
        if api_key and all(c == "*" for c in api_key):
            with registry.internal.connection() as conn:
                existing = conn.execute_one(
                    "SELECT api_key FROM llm_settings ORDER BY id DESC LIMIT 1"
                )
            api_key = (existing["api_key"] if existing else "") or ""

        with registry.internal.connection() as conn:
            conn.execute("DELETE FROM llm_settings")
            conn.execute(
                """
                INSERT INTO llm_settings
                    (provider, base_url, model, api_key, project_id, username,
                     verify_ssl, timeout, max_tokens, temperature)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (req.provider, req.base_url, req.model, api_key, req.project_id,
                 req.username, req.verify_ssl, req.timeout, req.max_tokens, req.temperature),
            )
        return {"ok": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/llm/test")
def llm_config_test(req: LLMSettingsRequest):
    """주어진 설정으로 LLM 연결을 테스트한다."""
    from pgxllm.config import LLMConfig
    from pgxllm.core.llm.factory import create_llm_provider

    # api_key 마스킹 처리
    api_key = req.api_key
    if api_key and all(c == "*" for c in api_key):
        existing = get_active_llm_config()
        api_key = existing.api_key

    cfg = LLMConfig(
        provider=req.provider,
        base_url=req.base_url,
        model=req.model,
        api_key=api_key,
        project_id=req.project_id,
        username=req.username,
        verify_ssl=req.verify_ssl,
        timeout=min(req.timeout, 30),  # 테스트는 30s 제한
        max_tokens=req.max_tokens,
        temperature=req.temperature,
    )
    try:
        provider = create_llm_provider(cfg)
        provider.complete("ping", "respond with 'ok'", max_tokens=10)
        return {"ok": True, "model": provider.model_name}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/llm/providers")
def llm_providers_list():
    """지원 provider 목록과 각 provider의 기본값/필수 필드를 반환한다."""
    return [
        {
            "id": "ollama",
            "label": "Ollama (Local)",
            "fields": ["base_url", "model"],
            "default_base_url": "http://localhost:11434",
            "default_model": "qwen2.5-coder:7b",
            "hint": "로컬에서 실행 중인 Ollama 서버",
        },
        {
            "id": "vllm",
            "label": "vLLM",
            "fields": ["base_url", "model", "api_key"],
            "default_base_url": "http://localhost:8001/v1",
            "default_model": "",
            "hint": "vLLM OpenAI-compatible 서버",
        },
        {
            "id": "lmstudio",
            "label": "LM Studio",
            "fields": ["base_url", "model", "api_key"],
            "default_base_url": "http://localhost:1234/v1",
            "default_model": "",
            "hint": "LM Studio 로컬 서버",
        },
        {
            "id": "openai",
            "label": "OpenAI",
            "fields": ["api_key", "model"],
            "default_base_url": "https://api.openai.com/v1",
            "default_model": "gpt-4o",
            "hint": "OpenAI API (OPENAI_API_KEY 또는 직접 입력)",
        },
        {
            "id": "anthropic",
            "label": "Anthropic Claude",
            "fields": ["api_key", "model"],
            "default_base_url": "",
            "default_model": "claude-3-5-sonnet-20241022",
            "hint": "Anthropic Claude API",
        },
        {
            "id": "watsonx",
            "label": "IBM watsonx.ai",
            "fields": ["base_url", "username", "api_key", "project_id", "model"],
            "default_base_url": "https://us-south.ml.cloud.ibm.com",
            "default_model": "ibm/granite-34b-code-instruct",
            "hint": "IBM Cloud: username 비워두기. CP4D on-prem: base_url + username + API key 입력",
        },
    ]


# ══════════════════════════════════════════════════════════════
# API — /api/status
# ══════════════════════════════════════════════════════════════

@app.get("/api/status")
def status():
    """Internal DB 연결 상태."""
    try:
        registry, config = get_registry()
        ok = registry.internal.test_connection()
        return {
            "internal_db": {
                "host":   config.internal_db.host,
                "dbname": config.internal_db.dbname,
                "ok":     ok,
            },
            "version": "0.1.0",
        }
    except Exception as e:
        return {"internal_db": {"ok": False, "error": str(e)}, "version": "0.1.0"}


# ══════════════════════════════════════════════════════════════
# SPA — Single Page Application (단일 HTML)
# ══════════════════════════════════════════════════════════════

_STATIC_DIR = Path(__file__).parent / "static"


# Serve static assets (JS, CSS, icons)
if _STATIC_DIR.exists():
    app.mount("/assets",   StaticFiles(directory=_STATIC_DIR / "assets"),   name="assets")
    app.mount("/favicon.svg", StaticFiles(directory=_STATIC_DIR),           name="favicon")


@app.get("/", response_class=HTMLResponse)
@app.get("/{full_path:path}", response_class=HTMLResponse)
def serve_spa(full_path: str = ""):
    if full_path.startswith("api/"):
        raise HTTPException(status_code=404)
    html_file = _STATIC_DIR / "index.html"
    if html_file.exists():
        return HTMLResponse(html_file.read_text())
    return HTMLResponse("<h1>pgxllm</h1><p>Run <code>npm run build</code> in frontend/</p>")


# ══════════════════════════════════════════════════════════════
# Dev entrypoint
# ══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "pgxllm.web.app:app",
        host="0.0.0.0",
        port=int(os.environ.get("PGXLLM_PORT", "8000")),
        reload=True,
        log_level="info",
    )
