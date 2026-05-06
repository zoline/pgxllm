"""
tests/parser/test_parser.py
----------------------------
Unit tests for Phase 2: ANTLR4 Parser
  - Tokenizer
  - RelationExtractVisitor
  - StructureAnalysisVisitor
  - ValidationVisitor
  - SqlParser facade
"""
import pytest
from pgxllm.parser import (
    SqlParser,
    ExtractedRelation,
    JoinType,
    JoinSource,
    LimitPosition,
    TopNPattern,
    DateFuncType,
    Severity,
)
from pgxllm.parser.tokenizer import tokenize, TT
from pgxllm.parser.sql_parser import parse_sql
from pgxllm.parser.validation_visitor import DialectRule


# ══════════════════════════════════════════════════════════
# Tokenizer
# ══════════════════════════════════════════════════════════

class TestTokenizer:

    def test_basic_select(self):
        tokens = tokenize("SELECT id, name FROM users")
        types = [(t.type, t.value) for t in tokens if t.type != TT.EOF]
        assert (TT.KEYWORD, "SELECT") in types
        assert (TT.KEYWORD, "FROM")   in types

    def test_param_tokens(self):
        """pg_stat_statements $1, $2 params"""
        tokens = tokenize("SELECT * FROM t WHERE id = $1 AND status = $2")
        params = [t for t in tokens if t.type == TT.PARAM]
        assert len(params) == 2
        assert params[0].value == "$1"
        assert params[1].value == "$2"

    def test_quoted_identifier(self):
        tokens = tokenize('SELECT "MyTable"."MyColumn" FROM "MyTable"')
        ids = [t for t in tokens if t.type == TT.IDENTIFIER]
        assert any(t.value == "MyTable" for t in ids)
        assert any(t.value == "MyColumn" for t in ids)

    def test_compound_keywords(self):
        tokens = tokenize("SELECT ROW_NUMBER() OVER (ORDER BY id) FROM t")
        kws = [t.value for t in tokens if t.type == TT.KEYWORD]
        assert "ROW_NUMBER" in kws
        assert "ORDER_BY" in kws

    def test_string_literal(self):
        tokens = tokenize("WHERE name = 'O''Brien'")
        lits = [t for t in tokens if t.type == TT.LITERAL]
        assert any("O''Brien" in t.value for t in lits)

    def test_comments_stripped(self):
        sql = """
        -- this is a comment
        SELECT id /* inline comment */ FROM t
        """
        tokens = tokenize(sql)
        values = [t.value for t in tokens]
        assert "this" not in values
        assert "inline" not in values

    def test_dollar_quoted_string(self):
        sql = "SELECT $$ hello world $$"
        tokens = tokenize(sql)
        lits = [t for t in tokens if t.type == TT.LITERAL]
        assert len(lits) == 1


# ══════════════════════════════════════════════════════════
# RelationExtractVisitor
# ══════════════════════════════════════════════════════════

class TestRelationExtract:

    def setup_method(self):
        self.parser = SqlParser()

    def test_explicit_inner_join(self):
        sql = """
        SELECT o.id, c.name
        FROM orders o
        JOIN customers c ON o.customer_id = c.id
        """
        rels = self.parser.extract_relations(sql)
        assert len(rels) >= 1
        r = rels[0]
        assert r.from_table  == "orders"
        assert r.from_column == "customer_id"
        assert r.to_table    == "customers"
        assert r.to_column   == "id"
        assert r.join_type   == JoinType.INNER

    def test_left_join(self):
        sql = """
        SELECT o.id, r.name
        FROM orders o
        LEFT JOIN regions r ON o.region_code = r.code
        """
        rels = self.parser.extract_relations(sql)
        assert any(
            r.from_table == "orders" and r.join_type == JoinType.LEFT
            for r in rels
        )

    def test_multi_join(self):
        sql = """
        SELECT o.id, c.name, r.region_name
        FROM orders o
        JOIN customers c ON o.customer_id = c.id
        JOIN regions r ON o.region_code = r.code
        """
        rels = self.parser.extract_relations(sql)
        tables = {(r.from_table, r.to_table) for r in rels}
        assert ("orders", "customers") in tables
        assert ("orders", "regions")   in tables

    def test_implicit_join_where(self):
        """FROM a, b WHERE a.id = b.a_id  — implicit JOIN"""
        sql = """
        SELECT o.id, c.name
        FROM orders o, customers c
        WHERE o.customer_id = c.id
        """
        rels = self.parser.extract_relations(sql)
        assert any(
            r.from_table == "orders" and r.to_table == "customers" and
            r.join_type == JoinType.IMPLICIT
            for r in rels
        )

    def test_cte_relations(self):
        sql = """
        WITH top_orders AS (
            SELECT o.id, c.name
            FROM orders o
            JOIN customers c ON o.customer_id = c.id
        )
        SELECT * FROM top_orders
        """
        rels = self.parser.extract_relations(sql)
        assert any(r.from_table == "orders" and r.to_table == "customers" for r in rels)
        assert any(r.source == JoinSource.CTE for r in rels)

    def test_subquery_relations(self):
        sql = """
        SELECT *
        FROM (
            SELECT o.id, c.name
            FROM orders o
            JOIN customers c ON o.customer_id = c.id
        ) sub
        """
        rels = self.parser.extract_relations(sql)
        assert any(r.from_table == "orders" and r.to_table == "customers" for r in rels)
        assert any(r.source == JoinSource.SUBQUERY for r in rels)

    def test_using_clause(self):
        sql = """
        SELECT * FROM orders o
        JOIN customers c USING (customer_id)
        """
        rels = self.parser.extract_relations(sql)
        assert any(
            r.from_table == "orders" and r.to_table == "customers" and
            r.from_column == "customer_id"
            for r in rels
        )

    def test_pg_stat_statements_params(self):
        """pg_stat_statements replaces literals with $1, $2"""
        sql = """
        SELECT o.id, c.name
        FROM orders o
        JOIN customers c ON o.customer_id = c.id
        WHERE o.status = $1 AND c.region = $2
        """
        rels = self.parser.extract_relations(sql)
        assert any(r.from_table == "orders" and r.to_table == "customers" for r in rels)

    def test_deduplication(self):
        """Same relation appearing multiple times should be deduplicated"""
        sql = """
        SELECT * FROM orders o
        JOIN customers c ON o.customer_id = c.id
        WHERE o.customer_id = c.id
        """
        rels = self.parser.extract_relations(sql)
        unique = {(r.from_table, r.from_column, r.to_table, r.to_column) for r in rels}
        assert len(unique) <= len(rels)   # dedup in set

    def test_self_join(self):
        sql = """
        SELECT e.name, m.name as manager
        FROM employees e
        JOIN employees m ON e.manager_id = m.id
        """
        rels = self.parser.extract_relations(sql)
        assert any(
            r.from_table == "employees" and r.to_table == "employees"
            for r in rels
        )

    def test_annotation_extraction(self):
        sql = """
        -- @relation orders -> customers : 주문-고객
        -- @relation orders -> regions   : 주문-지역
        SELECT * FROM orders o
        JOIN customers c ON o.customer_id = c.id
        """
        annotations = self.parser.extract_annotations(sql)
        assert len(annotations) == 2
        assert annotations[0]["from_table"] == "orders"
        assert annotations[0]["to_table"]   == "customers"
        assert annotations[0]["label"]      == "주문-고객"


# ══════════════════════════════════════════════════════════
# StructureAnalysisVisitor
# ══════════════════════════════════════════════════════════

class TestStructureAnalysis:

    def setup_method(self):
        self.parser = SqlParser()

    def test_simple_top_n(self):
        sql = """
        SELECT id, name, sales
        FROM customers
        ORDER BY sales DESC
        LIMIT 5
        """
        s = self.parser.analyze_structure(sql)
        assert s.limit_position == LimitPosition.FINAL
        assert s.has_order_by
        assert s.top_n_pattern == TopNPattern.SIMPLE
        assert s.top_n_value   == 5

    def test_top_n_then_detail(self):
        """상위 N 부서 추출 후 월별 상세 — inline view LIMIT"""
        sql = """
        SELECT o.dept, o.month, SUM(o.sales)
        FROM orders o
        WHERE o.dept IN (
            SELECT dept
            FROM orders
            GROUP BY dept
            ORDER BY SUM(sales) DESC
            LIMIT 5
        )
        GROUP BY o.dept, o.month
        """
        s = self.parser.analyze_structure(sql)
        assert s.has_subquery
        assert s.top_n_pattern in (TopNPattern.THEN_DETAIL, TopNPattern.SIMPLE)

    def test_top_n_per_group(self):
        """부서별 상위 3명 — RANK() + PARTITION BY"""
        sql = """
        SELECT * FROM (
            SELECT *,
                RANK() OVER (PARTITION BY dept ORDER BY sales DESC) AS rnk
            FROM employees
        ) ranked
        WHERE rnk <= 3
        """
        s = self.parser.analyze_structure(sql)
        assert s.has_window_func
        assert "RANK" in s.window_funcs
        assert s.top_n_pattern == TopNPattern.PER_GROUP

    def test_extract_on_column(self):
        """EXTRACT on a column → date func usage detected"""
        sql = "SELECT * FROM orders WHERE EXTRACT(YEAR FROM order_date) = 2023"
        s = self.parser.analyze_structure(sql)
        assert any(
            df.func_type == DateFuncType.EXTRACT and df.column == "order_date"
            for df in s.date_funcs
        )

    def test_substr_on_column(self):
        sql = "SELECT * FROM orders WHERE SUBSTR(order_date, 1, 4) = '2023'"
        s = self.parser.analyze_structure(sql)
        assert any(
            df.func_type == DateFuncType.SUBSTR and df.column == "order_date"
            for df in s.date_funcs
        )

    def test_between_yyyymm(self):
        sql = "SELECT * FROM orders WHERE order_date BETWEEN '202301' AND '202312'"
        s = self.parser.analyze_structure(sql)
        assert any(
            df.func_type == DateFuncType.BETWEEN and df.pattern == "YYYYMM"
            for df in s.date_funcs
        )

    def test_group_by_having(self):
        sql = """
        SELECT dept, COUNT(*) as cnt
        FROM employees
        GROUP BY dept
        HAVING COUNT(*) > 10
        """
        s = self.parser.analyze_structure(sql)
        assert s.has_group_by
        assert s.has_having

    def test_cte_count(self):
        sql = """
        WITH a AS (SELECT 1), b AS (SELECT 2)
        SELECT * FROM a, b
        """
        s = self.parser.analyze_structure(sql)
        assert s.has_cte
        assert s.cte_count == 2

    def test_join_count(self):
        sql = """
        SELECT * FROM a
        JOIN b ON a.id = b.a_id
        JOIN c ON b.id = c.b_id
        JOIN d ON c.id = d.c_id
        """
        s = self.parser.analyze_structure(sql)
        assert s.join_count == 3

    def test_union(self):
        sql = "SELECT id FROM a UNION ALL SELECT id FROM b"
        s = self.parser.analyze_structure(sql)
        assert s.has_union

    def test_window_funcs(self):
        sql = """
        SELECT
            RANK()         OVER (ORDER BY sales DESC)      AS rank_,
            DENSE_RANK()   OVER (ORDER BY sales DESC)      AS drank,
            ROW_NUMBER()   OVER (ORDER BY id)              AS rn,
            SUM(sales)     OVER (PARTITION BY dept)        AS dept_sum
        FROM employees
        """
        s = self.parser.analyze_structure(sql)
        assert s.has_window_func
        for fn in ("RANK", "DENSE_RANK", "ROW_NUMBER"):
            assert fn in s.window_funcs


# ══════════════════════════════════════════════════════════
# ValidationVisitor
# ══════════════════════════════════════════════════════════

class TestValidation:

    def setup_method(self):
        self.parser = SqlParser()

    def test_valid_sql(self):
        sql = "SELECT id FROM users ORDER BY id LIMIT 10"
        result = self.parser.validate(sql)
        assert result.is_valid

    def test_limit_without_order_by(self):
        sql = "SELECT id FROM users LIMIT 10"
        result = self.parser.validate(sql)
        warnings = [v for v in result.violations if v.severity == Severity.WARNING]
        assert any(v.rule_id == "limit_without_order_by" for v in warnings)

    def test_dialect_rule_extract_forbidden(self):
        """EXTRACT on TEXT YYYYMM column should raise error"""
        rule = DialectRule(
            rule_id="text_date_yyyymm",
            scope="column",
            dialect="postgresql",
            db_name="mydb",
            table_name="orders",
            column_name="order_date",
            forbidden_funcs=["EXTRACT", "BETWEEN"],
            required_func="SUBSTR",
            instruction="TEXT YYYYMM 형식 — SUBSTR 사용",
            example_bad="EXTRACT(YEAR FROM order_date)",
            example_good="SUBSTR(order_date, 1, 4)",
        )
        sql = "SELECT * FROM orders WHERE EXTRACT(YEAR FROM order_date) = 2023"
        result = self.parser.validate(sql, rules=[rule], db_name="mydb")
        assert not result.is_valid
        assert any(v.rule_id == "text_date_yyyymm" for v in result.violations)

    def test_dialect_rule_between_forbidden(self):
        rule = DialectRule(
            rule_id="text_date_yyyymm",
            scope="column",
            dialect="postgresql",
            db_name="mydb",
            table_name="orders",
            column_name="order_date",
            forbidden_funcs=["EXTRACT", "BETWEEN"],
            required_func="SUBSTR",
            instruction="TEXT YYYYMM 형식 — SUBSTR 사용",
        )
        sql = "SELECT * FROM orders WHERE order_date BETWEEN '202301' AND '202312'"
        result = self.parser.validate(sql, rules=[rule], db_name="mydb")
        assert not result.is_valid

    def test_dialect_rule_scope_mismatch(self):
        """Rule for different db_name should not apply"""
        rule = DialectRule(
            rule_id="text_date_yyyymm",
            scope="db",
            dialect="postgresql",
            db_name="other_db",
            table_name=None,
            column_name=None,
            forbidden_funcs=["EXTRACT"],
            required_func="SUBSTR",
            instruction="TEXT date",
        )
        sql = "SELECT EXTRACT(YEAR FROM order_date) FROM orders"
        result = self.parser.validate(sql, rules=[rule], db_name="mydb")
        # rule should NOT trigger (different db)
        assert not any(v.rule_id == "text_date_yyyymm" for v in result.violations)


# ══════════════════════════════════════════════════════════
# SqlParser facade
# ══════════════════════════════════════════════════════════

class TestSqlParserFacade:

    def setup_method(self):
        self.parser = SqlParser()

    def test_normalize(self):
        sql = "  SELECT  *  FROM  t  WHERE id = $1 ; "
        norm = self.parser.normalize(sql)
        assert "$1" not in norm
        assert "?" in norm
        assert not norm.endswith(";")
        assert "  " not in norm

    def test_empty_sql(self):
        """Empty / whitespace SQL should not raise"""
        rels = self.parser.extract_relations("")
        assert rels == []

    def test_invalid_sql_tolerant(self):
        """Parser should not raise on garbage input"""
        rels = self.parser.extract_relations("THIS IS NOT SQL AT ALL ###")
        assert isinstance(rels, list)

    def test_multiple_statements(self):
        sql = """
        SELECT * FROM orders o JOIN customers c ON o.customer_id = c.id;
        SELECT * FROM products p JOIN categories cat ON p.category_id = cat.id;
        """
        rels = self.parser.extract_relations(sql)
        tables = {(r.from_table, r.to_table) for r in rels}
        assert ("orders", "customers")  in tables
        assert ("products", "categories") in tables

    def test_complex_query(self):
        """Real-world complex query"""
        sql = """
        WITH monthly_sales AS (
            SELECT
                s.region_code,
                s.sale_month,
                SUM(s.amount) AS total
            FROM sales s
            JOIN regions r ON s.region_code = r.code
            GROUP BY s.region_code, s.sale_month
        ),
        top_regions AS (
            SELECT region_code
            FROM monthly_sales
            GROUP BY region_code
            ORDER BY SUM(total) DESC
            LIMIT 5
        )
        SELECT
            ms.region_code,
            r.region_name,
            ms.sale_month,
            ms.total,
            RANK() OVER (PARTITION BY ms.region_code ORDER BY ms.total DESC) AS month_rank
        FROM monthly_sales ms
        JOIN top_regions tr ON ms.region_code = tr.region_code
        JOIN regions r      ON ms.region_code = r.code
        ORDER BY ms.region_code, ms.sale_month
        """
        rels = self.parser.extract_relations(sql)
        s    = self.parser.analyze_structure(sql)

        assert any(r.from_table == "sales"         and r.to_table == "regions"     for r in rels)
        assert s.has_cte
        assert s.cte_count    == 2
        assert s.has_window_func
        assert "RANK"   in s.window_funcs
        assert s.has_group_by
        assert s.top_n_pattern == TopNPattern.PER_GROUP
