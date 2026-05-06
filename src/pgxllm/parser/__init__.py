"""
pgxllm.parser
-------------
SQL parsing subsystem.

Public API::

    from pgxllm.parser import SqlParser, ExtractedRelation, SqlStructure

    parser = SqlParser()
    relations = parser.extract_relations(sql)
    structure = parser.analyze_structure(sql)
    result    = parser.validate(sql, rules=rules)
"""

from .facade import SqlParser
from .models import (
    DateFuncType, DateFuncUsage, ExtractedRelation, JoinSource, JoinType,
    LimitPosition, Severity, SqlStructure, TopNPattern, ValidationResult, Violation,
)
from .validation_visitor import DialectRule

__all__ = [
    "SqlParser", "ExtractedRelation", "SqlStructure", "ValidationResult",
    "Violation", "DialectRule", "DateFuncUsage",
    "JoinType", "JoinSource", "LimitPosition", "TopNPattern", "DateFuncType", "Severity",
]
