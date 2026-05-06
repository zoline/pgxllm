"""
pgxllm.core.s4_validation
--------------------------
S4 Validation & Correction

수행:
  1. PREPARE validate — DB에서 실제 SQL 파싱 유효성 검사
  2. ValidationVisitor — 구조 규칙 위반 감지 (LIMIT without ORDER BY 등)
  3. Dialect Rule 위반 감지 (forbidden_funcs 등)
  4. 실패 시 S3 에 correction_hint 전달 → self-correction loop
  5. S4 실패 이력 → pattern_applications 기록
"""
from __future__ import annotations

import json
import logging
import re
from typing import Optional

from pgxllm.core.models import (
    PipelineRequest, SQLCandidate, ValidationIssue,
    ValidationResult, LinkedSchema,
)
from pgxllm.db.connections import ConnectionRegistry
from pgxllm.intelligence.rule_engine import RuleEngine
from pgxllm.parser.facade import SqlParser

log = logging.getLogger(__name__)


class SQLValidator:
    """
    S4 — SQL 후보를 검증한다.
    """

    def __init__(self, registry: ConnectionRegistry):
        self._registry    = registry
        self._rule_engine = RuleEngine(registry)
        self._parser      = SqlParser()

    def validate(
        self,
        candidate: SQLCandidate,
        schema:    LinkedSchema,
        *,
        db_alias:  str,
    ) -> ValidationResult:
        """
        Args:
            candidate: S3 가 생성한 SQL 후보
            schema:    LinkedSchema (dialect rules 포함)
            db_alias:  target DB alias (PREPARE 실행용)

        Returns:
            ValidationResult
        """
        sql    = candidate.sql
        issues: list[ValidationIssue] = []

        if not sql or not sql.strip():
            return ValidationResult(
                sql=sql, ok=False,
                issues=[ValidationIssue("empty_sql", "SQL이 비어있습니다.")],
            )

        # ── 1. PREPARE validate (구문 검사) ───────────────────────
        prepare_ok, prepare_error = self._prepare_validate(sql, db_alias)
        if not prepare_ok:
            log.info("[S4] PREPARE failed: %s", prepare_error)
            return ValidationResult(
                sql=sql, ok=False,
                issues=[ValidationIssue("syntax_error", prepare_error or "구문 오류")],
                error=prepare_error,
            )

        # ── 2. ValidationVisitor (구조 규칙 — AST 기반) ──────────
        try:
            dialect_rules = self._rule_engine.to_validation_rules(
                schema.dialect_rules
            )
            parser_result = self._parser.validate(sql, rules=dialect_rules)

            for issue in parser_result.issues:
                issues.append(ValidationIssue(
                    rule=issue.rule_id,
                    message=issue.message,
                    severity=issue.severity,
                ))
        except Exception as e:
            log.warning("[S4] parser validation error: %s", e)

        # ── 2b. SQL 텍스트 패턴 검사 (연산자 등 AST로 못 잡는 것) ──
        for rule in schema.dialect_rules:
            patterns_raw = rule.get("forbidden_sql_patterns") or []
            if isinstance(patterns_raw, str):
                try:
                    patterns_raw = json.loads(patterns_raw)
                except Exception:
                    patterns_raw = []
            if not patterns_raw:
                continue
            sev = rule.get("severity", "error")
            for pattern in patterns_raw:
                try:
                    if re.search(pattern, sql, re.IGNORECASE):
                        hint = rule.get("required_func") or ""
                        msg = (
                            f"SQL에 금지 패턴 '{pattern}' 사용됨 — {rule.get('instruction', '')}."
                        )
                        if hint:
                            msg += f" 대신 '{hint}'를 사용하세요."
                        issues.append(ValidationIssue(
                            rule=rule["rule_id"],
                            message=msg,
                            severity=sev,
                        ))
                        log.info("[S4] pattern rule '%s' triggered: %s", rule["rule_id"], pattern)
                        break  # 규칙당 첫 매칭만 보고
                except re.error as e:
                    log.warning("[S4] invalid regex pattern '%s' in rule '%s': %s", pattern, rule["rule_id"], e)

        # ── 3. 결과 ───────────────────────────────────────────────
        errors = [i for i in issues if i.severity == "error"]
        ok     = len(errors) == 0

        if not ok:
            log.info("[S4] validation issues: %s", [i.message for i in errors])

        return ValidationResult(sql=sql, ok=ok, issues=issues)

    def correction_hint(self, result: ValidationResult) -> str:
        """ValidationResult 에서 S3 correction prompt 힌트를 생성한다."""
        if result.error:
            return f"SQL 구문 오류: {result.error}"
        msgs = [i.message for i in result.issues if i.severity == "error"]
        return "구조 규칙 위반: " + "; ".join(msgs) if msgs else "알 수 없는 오류"

    def record_failure(
        self,
        request:   PipelineRequest,
        candidate: SQLCandidate,
        result:    ValidationResult,
    ) -> None:
        """S4 실패 이력을 pattern_applications 에 기록한다."""
        try:
            with self._registry.internal.connection() as conn:
                conn.execute(
                    """
                    INSERT INTO pattern_applications
                        (db_alias, question, generated_sql, was_correct)
                    VALUES (%s, %s, %s, FALSE)
                    """,
                    (request.db_alias, request.question, candidate.sql)
                )
        except Exception as e:
            log.debug("[S4] record_failure error: %s", e)

    # ── PREPARE helper ────────────────────────────────────────

    def _prepare_validate(
        self, sql: str, db_alias: str
    ) -> tuple[bool, Optional[str]]:
        """
        PostgreSQL PREPARE 로 SQL 구문 유효성 검사.
        실제 실행하지 않으므로 데이터 변경 없음.
        """
        try:
            mgr = self._registry.target(db_alias)
            with mgr.connection() as conn:
                stmt_name = "pgxllm_validate"
                conn.execute(f"DEALLOCATE ALL")
                conn.execute(f"PREPARE {stmt_name} AS {sql}")
                conn.execute(f"DEALLOCATE {stmt_name}")
                conn.rollback()
            return True, None
        except KeyError:
            # DB not registered — skip PREPARE
            log.debug("[S4] DB '%s' not in registry, skipping PREPARE", db_alias)
            return True, None
        except Exception as e:
            error_msg = str(e).split("\n")[0]
            return False, error_msg
