"""
pgxllm CLI — automation & setup commands
Web UI handles exploration, analysis, and management.
"""
from __future__ import annotations
import json, logging, sys
from typing import Optional
import click

logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s: %(message)s")
log = logging.getLogger("pgxllm.cli")

def _get_registry():
    from pgxllm.config import load_config
    from pgxllm.db.connections import ConnectionRegistry
    from pgxllm.intelligence import DBRegistryService
    cfg = load_config()
    registry = ConnectionRegistry(cfg)
    DBRegistryService(registry).load_registered_to_config(cfg)
    return registry, cfg

@click.group()
@click.option("--verbose", "-v", is_flag=True)
def main(verbose):
    """pgxllm — PostgreSQL Text-to-SQL system."""
    if verbose: logging.getLogger("pgxllm").setLevel(logging.DEBUG)

# ── db ────────────────────────────────────────────────────────
@main.group("db")
def db_group(): """Target DB 등록 및 스키마 수집."""

@db_group.command("register")
@click.option("--alias", required=True)
@click.option("--host",  required=True)
@click.option("--port",  default=5432, type=int)
@click.option("--user",  default="postgres")
@click.option("--password", default="")
@click.option("--dbname",   default=None)
@click.option("--schema-mode", default="exclude", type=click.Choice(["include","exclude"]))
@click.option("--schemas",  default="pg_catalog,information_schema,pg_toast")
@click.option("--overwrite", is_flag=True)
def db_register(alias, host, port, user, password, dbname, schema_mode, schemas, overwrite):
    """Target DB를 pgxllm에 등록한다."""
    from pgxllm.config import TargetDBConfig
    from pgxllm.intelligence import DBRegistryService
    cfg = TargetDBConfig(alias=alias, host=host, port=port, user=user,
        password=password, dbname=dbname or alias, schema_mode=schema_mode,
        schemas=[s.strip() for s in schemas.split(",") if s.strip()])
    registry, _ = _get_registry()
    try:
        DBRegistryService(registry).register(cfg, overwrite=overwrite)
        click.echo(click.style(f"✅ Registered: {alias} ({host}:{port}/{cfg.dbname})", fg="green"))
    except ValueError as e:
        click.echo(click.style(f"❌ {e}", fg="red"), err=True); sys.exit(1)

@db_group.command("list")
def db_list():
    """등록된 Target DB 목록을 출력한다."""
    from pgxllm.intelligence import DBRegistryService
    registry, config = _get_registry()
    dbs = DBRegistryService(registry).list_all()
    if not dbs:
        click.echo("등록된 DB가 없습니다.")
    else:
        click.echo(f"등록된 DB ({len(dbs)}개):")
        for db in dbs:
            click.echo(f"  - {db}")

@db_group.command("refresh")
@click.option("--alias", default=None)
@click.option("--table", default=None)
@click.option("--all", "all_dbs", is_flag=True)
@click.option("--skip-samples", is_flag=True)
@click.option("--skip-rules",   is_flag=True)
@click.option("--skip-graph",   is_flag=True)
def db_refresh(alias, table, all_dbs, skip_samples, skip_rules, skip_graph):
    """pg_catalog 스캔 + 샘플 추출 + Rule 감지 + FK graph. (자동화/cron 용)"""
    registry, config = _get_registry()
    from pgxllm.intelligence import RefreshOrchestrator
    orch = RefreshOrchestrator(registry, config)
    if all_dbs:
        click.echo("Refreshing all ..."); results = orch.refresh_all(skip_samples=skip_samples, skip_rules=skip_rules, skip_graph=skip_graph)
    elif alias:
        click.echo(f"Refreshing [{alias}] ...")
        results = [orch.refresh(alias, table_filter=[table] if table else None,
                    skip_samples=skip_samples, skip_rules=skip_rules, skip_graph=skip_graph)]
    else:
        click.echo("--alias 또는 --all 을 지정하세요.", err=True); sys.exit(1)
    for r in results:
        click.echo(click.style(("✅ " if r.success else "❌ ") + r.summary(), fg="green" if r.success else "red"))

# ── eval ──────────────────────────────────────────────────────
@main.command("eval")
@click.option("--dataset",  default="bird", type=click.Choice(["bird"]))
@click.option("--file",     required=True)
@click.option("--alias",    required=True)
@click.option("--output",   default="results/eval.json")
@click.option("--limit",    default=None, type=int)
@click.option("--skip-baseline", is_flag=True)
def eval_cmd(dataset, file, alias, output, limit, skip_baseline):
    """BIRD benchmark 평가."""
    from pgxllm.eval import BIRDEvalRunner
    registry, config = _get_registry()
    runner = BIRDEvalRunner(registry, config)
    results = runner.run_file(file, alias, limit=limit, skip_baseline=skip_baseline)
    summary = BIRDEvalRunner.summarize(results)
    click.echo("\n" + click.style("=== 평가 결과 ===", fg="cyan", bold=True))
    click.echo(str(summary))
    BIRDEvalRunner.save_results(results, output)
    click.echo(click.style(f"\n결과 저장: {output}", fg="green"))

# ── web ───────────────────────────────────────────────────────
@main.command("web")
@click.option("--host",   default="0.0.0.0", show_default=True)
@click.option("--port",   default=8000,       show_default=True, type=int)
@click.option("--reload", "hot_reload", is_flag=True)
def web_cmd(host, port, hot_reload):
    """Query Test UI (FastAPI) 시작."""
    import uvicorn
    click.echo(click.style(f"✅ pgxllm Web UI: http://{host}:{port}", fg="green"))
    uvicorn.run("pgxllm.web.app:app", host=host, port=port, reload=hot_reload, log_level="info")

if __name__ == "__main__":
    main()
