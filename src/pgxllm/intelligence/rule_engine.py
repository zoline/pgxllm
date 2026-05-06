"""
pgxllm.intelligence.rule_engine
---------------------------------
RuleEngine — Dialect Rules를 관리하고
S3 SQL Generation 시 LLM prompt에 동적으로 주입한다.

  - 자동 감지 rule  (DialectRuleDetector → db refresh 시 생성)
  - 수동 등록 rule  (CLI / Web UI)
  - scope 우선순위: column > table > db > dialect > global
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Optional

from pgxllm.db.connections import ConnectionRegistry
from pgxllm.parser.validation_visitor import DialectRule

log = logging.getLogger(__name__)


# ── RuleEngine ────────────────────────────────────────────────

class RuleEngine:
    """
    Manages dialect rules and generates prompt injection text.

    Usage::

        engine = RuleEngine(registry)

        # 수동 rule 등록
        engine.add_rule(rule_id="my_rule", scope="column",
                        db_alias="mydb", ...)

        # S3 prompt injection용 rule 조회
        rules = engine.get_rules_for_query(
            db_alias="mydb",
            tables=["orders", "customers"],
            columns={"orders": ["order_date"], "customers": ["birth_date"]},
        )
        prompt_text = engine.build_prompt_injection(rules)

        # ValidationVisitor용 rule 변환
        dialect_rules = engine.to_validation_rules(rules)
    """

    def __init__(self, registry: ConnectionRegistry):
        self._registry = registry

    # ── CRUD ──────────────────────────────────────────────────

    def add_rule(
        self,
        *,
        rule_id:                str,
        scope:                  str = "column",
        dialect:                str = "postgresql",
        db_alias:               Optional[str] = None,
        schema_name:            Optional[str] = None,
        table_name:             Optional[str] = None,
        column_name:            Optional[str] = None,
        forbidden_funcs:        list[str],
        forbidden_sql_patterns: list[str] = [],
        required_func:          Optional[str] = None,
        instruction:            str,
        example_bad:            str = "",
        example_good:           str = "",
        severity:               str = "error",
        overwrite:              bool = False,
    ) -> None:
        with self._registry.internal.connection() as conn:
            existing = conn.execute_one(
                "SELECT rule_id FROM dialect_rules WHERE rule_id=%s", (rule_id,)
            )
            if existing and not overwrite:
                raise ValueError(
                    f"Rule '{rule_id}' already exists. Use overwrite=True."
                )
            conn.execute(
                """
                INSERT INTO dialect_rules (
                    rule_id, scope, dialect, db_alias, schema_name,
                    table_name, column_name, condition_json,
                    forbidden_funcs, forbidden_sql_patterns, required_func,
                    instruction, example_bad, example_good, severity,
                    auto_detected, enabled
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,'{}', %s,%s,%s,%s,%s,%s,%s,FALSE,TRUE)
                ON CONFLICT (rule_id) DO UPDATE SET
                    scope                   = EXCLUDED.scope,
                    forbidden_funcs         = EXCLUDED.forbidden_funcs,
                    forbidden_sql_patterns  = EXCLUDED.forbidden_sql_patterns,
                    required_func           = EXCLUDED.required_func,
                    instruction             = EXCLUDED.instruction,
                    example_bad             = EXCLUDED.example_bad,
                    example_good            = EXCLUDED.example_good,
                    severity                = EXCLUDED.severity,
                    enabled                 = TRUE,
                    updated_at              = NOW()
                """,
                (
                    rule_id, scope, dialect, db_alias, schema_name,
                    table_name, column_name,
                    json.dumps(forbidden_funcs),
                    json.dumps(forbidden_sql_patterns),
                    required_func,
                    instruction, example_bad, example_good, severity,
                )
            )
        log.info("Rule added/updated: %s", rule_id)

    def enable_rule(self, rule_id: str) -> None:
        with self._registry.internal.connection() as conn:
            conn.execute(
                "UPDATE dialect_rules SET enabled=TRUE, updated_at=NOW() WHERE rule_id=%s",
                (rule_id,)
            )

    def disable_rule(self, rule_id: str) -> None:
        with self._registry.internal.connection() as conn:
            conn.execute(
                "UPDATE dialect_rules SET enabled=FALSE, updated_at=NOW() WHERE rule_id=%s",
                (rule_id,)
            )

    def delete_rule(self, rule_id: str) -> None:
        with self._registry.internal.connection() as conn:
            conn.execute("DELETE FROM dialect_rules WHERE rule_id=%s", (rule_id,))

    # ── Query rules ───────────────────────────────────────────

    def get_rules_for_query(
        self,
        db_alias:  str,
        tables:    list[str],
        columns:   Optional[dict[str, list[str]]] = None,
        schema_name: Optional[str] = None,
    ) -> list[dict]:
        """
        Fetch applicable rules for the given query context.
        Ordered by scope specificity (column > table > db > dialect > global).

        Args:
            db_alias:    target DB alias
            tables:      list of table names in the query
            columns:     {table: [col, ...]} mapping
            schema_name: schema (optional, for more precise matching)
        """
        with self._registry.internal.connection() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM dialect_rules
                WHERE enabled = TRUE
                  AND (
                    scope = 'global'
                    OR (scope = 'dialect')
                    OR (scope = 'db'     AND db_alias = %s)
                    OR (scope = 'table'  AND (db_alias = %s OR db_alias IS NULL)
                                          AND table_name = ANY(%s))
                    OR (scope = 'column' AND (db_alias = %s OR db_alias IS NULL)
                                          AND table_name = ANY(%s))
                  )
                ORDER BY
                    CASE scope
                        WHEN 'column'  THEN 1
                        WHEN 'table'   THEN 2
                        WHEN 'db'      THEN 3
                        WHEN 'dialect' THEN 4
                        WHEN 'global'  THEN 5
                    END
                """,
                (db_alias, db_alias, tables, db_alias, tables)
            )

        # Filter column-scope rules to only matching columns
        result = []
        col_flat = set()
        if columns:
            for t, cols in columns.items():
                for c in cols:
                    col_flat.add((t.lower(), c.lower()))

        for row in rows:
            if row["scope"] == "column":
                tbl = (row["table_name"] or "").lower()
                col = (row["column_name"] or "").lower()
                if col and (tbl, col) not in col_flat:
                    continue
            result.append(row)

        return result

    def list_rules(
        self,
        db_alias: Optional[str] = None,
        scope: Optional[str] = None,
        enabled_only: bool = True,
    ) -> list[dict]:
        sql = "SELECT * FROM dialect_rules WHERE 1=1"
        params: list = []
        if enabled_only:
            sql += " AND enabled=TRUE"
        if db_alias:
            sql += " AND (db_alias=%s OR db_alias IS NULL)"
            params.append(db_alias)
        if scope:
            sql += " AND scope=%s"
            params.append(scope)
        sql += " ORDER BY scope, rule_id"
        with self._registry.internal.connection() as conn:
            return conn.execute(sql, params)

    # ── Prompt injection ──────────────────────────────────────

    def build_prompt_injection(self, rules: list[dict]) -> str:
        """
        Build LLM prompt injection text from a list of rule dicts.
        Returns empty string if no rules.
        """
        if not rules:
            return ""

        lines = ["## PostgreSQL 방언 규칙 (반드시 준수)"]
        lines.append("")

        for i, rule in enumerate(rules, 1):
            forbidden = json.loads(rule["forbidden_funcs"]) \
                if isinstance(rule["forbidden_funcs"], str) \
                else rule["forbidden_funcs"]

            scope_label = self._scope_label(rule)
            lines.append(f"### 규칙 {i}: {rule['rule_id']} ({scope_label})")
            lines.append(rule["instruction"])
            if forbidden:
                lines.append(f"❌ 금지 함수: {', '.join(forbidden)}")
            if rule.get("required_func"):
                lines.append(f"✅ 권장 함수: {rule['required_func']}")
            if rule.get("example_bad"):
                lines.append(f"나쁜 예: {rule['example_bad']}")
            if rule.get("example_good"):
                lines.append(f"좋은 예: {rule['example_good']}")
            lines.append("")

        return "\n".join(lines)

    # ── ValidationVisitor 변환 ────────────────────────────────

    def to_validation_rules(self, rules: list[dict]) -> list[DialectRule]:
        """Convert DB rule dicts to DialectRule objects for ValidationVisitor."""
        from pgxllm.parser.models import Severity
        result = []
        for r in rules:
            forbidden = json.loads(r["forbidden_funcs"]) \
                if isinstance(r["forbidden_funcs"], str) \
                else (r["forbidden_funcs"] or [])
            sev_str = (r.get("severity") or "error").lower()
            sev = Severity.ERROR if sev_str == "error" else Severity.WARNING

            result.append(DialectRule(
                rule_id=r["rule_id"],
                scope=r["scope"],
                dialect=r.get("dialect", "postgresql"),
                db_name=r.get("db_alias"),
                table_name=r.get("table_name"),
                column_name=r.get("column_name"),
                forbidden_funcs=forbidden,
                required_func=r.get("required_func"),
                instruction=r.get("instruction", ""),
                example_bad=r.get("example_bad", ""),
                example_good=r.get("example_good", ""),
                severity=sev,
            ))
        return result

    # ── Test ─────────────────────────────────────────────────

    def test_rule(self, db_alias: str, question: str) -> dict:
        """
        Simulate rule matching for a natural language question.
        Useful for CLI: pgxllm rules test --question "..."
        """
        from pgxllm.parser.facade import SqlParser
        # Extract tentative table/column mentions from question
        # (simplified — real S2 uses pgvector)
        words = [w.lower().strip(".,") for w in question.split()]
        rules = self.get_rules_for_query(db_alias, tables=words)
        prompt = self.build_prompt_injection(rules)
        return {
            "db_alias": db_alias,
            "question": question,
            "rules_matched": len(rules),
            "rules": [r["rule_id"] for r in rules],
            "prompt_injection": prompt,
        }

    # ── Helpers ───────────────────────────────────────────────

    @staticmethod
    def _scope_label(rule: dict) -> str:
        scope = rule.get("scope", "")
        if scope == "column":
            return f"{rule.get('table_name','')}.{rule.get('column_name','')}"
        if scope == "table":
            return rule.get("table_name", "")
        if scope == "db":
            return rule.get("db_alias", "")
        return scope
