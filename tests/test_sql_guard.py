"""Exhaustive security tests for SQL analysis and classification."""

from __future__ import annotations

import pytest

from pgmcp.errors import UnsafeQueryError
from pgmcp.sql_guard import (
    StatementKind,
    analyze,
    assert_read_only,
    assert_single_statement,
    split_statements,
    validate_fragment,
)


class TestSplitStatements:
    """Test statement splitting on semicolons, handling string literals."""

    def test_single_select(self) -> None:
        """A plain SELECT is returned as one statement."""
        statements = split_statements("SELECT 1")
        assert len(statements) == 1
        assert statements[0] == "SELECT 1"

    def test_multiple_statements(self) -> None:
        """Multiple statements are split."""
        statements = split_statements("SELECT 1; SELECT 2")
        assert len(statements) == 2
        assert "1" in statements[0]
        assert "2" in statements[1]

    def test_semicolon_in_string_literal(self) -> None:
        """Semicolons inside string literals are not statement separators."""
        statements = split_statements("SELECT 'hello;world'")
        assert len(statements) == 1

    def test_semicolon_in_dollar_quoted_string(self) -> None:
        """Dollar-quoted strings can contain semicolons."""
        statements = split_statements("CREATE FUNCTION f() AS $$SELECT 1;$$")
        assert len(statements) == 1

    def test_empty_input(self) -> None:
        """Empty input returns no statements."""
        assert split_statements("") == []
        assert split_statements("   ") == []
        assert split_statements("\n\n") == []

    def test_comment_only(self) -> None:
        """Comment-only input returns no statements."""
        statements = split_statements("-- a comment\n/* multi\nline */")
        assert len(statements) == 0

    def test_leading_comments_stripped(self) -> None:
        """Leading comments are removed from returned statement."""
        statements = split_statements("-- comment\nSELECT 1")
        assert len(statements) == 1
        assert statements[0] == "SELECT 1"


class TestAnalyzeReadOnly:
    """Test classification of read-only statements."""

    def test_select(self) -> None:
        """SELECT is read-only."""
        info = analyze("SELECT 1")
        assert info.kind == StatementKind.READ
        assert info.is_read_only is True

    def test_select_with_cte(self) -> None:
        """WITH SELECT (non-mutating CTE) is read-only."""
        info = analyze("WITH x AS (SELECT 1) SELECT * FROM x")
        assert info.kind == StatementKind.READ
        assert info.is_read_only is True

    def test_table(self) -> None:
        """TABLE is read-only."""
        info = analyze("TABLE my_table")
        assert info.kind == StatementKind.READ
        assert info.is_read_only is True

    def test_values(self) -> None:
        """VALUES is read-only."""
        info = analyze("VALUES (1), (2), (3)")
        assert info.kind == StatementKind.READ
        assert info.is_read_only is True

    def test_show(self) -> None:
        """SHOW is read-only."""
        info = analyze("SHOW max_connections")
        assert info.kind == StatementKind.READ
        assert info.is_read_only is True

    def test_explain(self) -> None:
        """EXPLAIN without ANALYZE is read-only."""
        info = analyze("EXPLAIN SELECT 1")
        assert info.kind == StatementKind.READ
        assert info.is_read_only is True

    def test_explain_with_options(self) -> None:
        """EXPLAIN with options (but not ANALYZE) is read-only."""
        info = analyze("EXPLAIN (FORMAT JSON, VERBOSE) SELECT 1")
        assert info.kind == StatementKind.READ
        assert info.is_read_only is True

    def test_copy_select_to_stdout(self) -> None:
        """COPY (query) TO STDOUT is read-only."""
        info = analyze("COPY (SELECT id FROM users) TO STDOUT")
        assert info.kind == StatementKind.READ
        assert info.is_read_only is True


class TestAnalyzeDML:
    """Test classification of data-modifying statements."""

    def test_insert(self) -> None:
        """INSERT is DML and not read-only."""
        info = analyze("INSERT INTO t (x) VALUES (1)")
        assert info.kind == StatementKind.DML
        assert info.is_read_only is False

    def test_update(self) -> None:
        """UPDATE is DML."""
        info = analyze("UPDATE t SET x = 1")
        assert info.kind == StatementKind.DML
        assert info.is_read_only is False

    def test_delete(self) -> None:
        """DELETE is DML."""
        info = analyze("DELETE FROM t WHERE x > 0")
        assert info.kind == StatementKind.DML
        assert info.is_read_only is False

    def test_merge(self) -> None:
        """MERGE is DML."""
        info = analyze("MERGE INTO t USING source ON t.id = source.id DO UPDATE SET x = source.x")
        assert info.kind == StatementKind.DML
        assert info.is_read_only is False

    def test_with_data_modifying_cte(self) -> None:
        """WITH clause containing DELETE/UPDATE/INSERT is DML."""
        info = analyze("WITH x AS (DELETE FROM t RETURNING *) SELECT * FROM x")
        assert info.kind == StatementKind.DML
        assert info.is_read_only is False
        assert "data-modifying" in (info.reason or "").lower()


class TestAnalyzeDDL:
    """Test classification of DDL statements."""

    def test_create_table(self) -> None:
        """CREATE TABLE is DDL."""
        info = analyze("CREATE TABLE t (id INT)")
        assert info.kind == StatementKind.DDL
        assert info.is_read_only is False

    def test_create_view(self) -> None:
        """CREATE VIEW is DDL."""
        info = analyze("CREATE VIEW v AS SELECT 1")
        assert info.kind == StatementKind.DDL
        assert info.is_read_only is False

    def test_alter_table(self) -> None:
        """ALTER TABLE is DDL."""
        info = analyze("ALTER TABLE t ADD COLUMN x INT")
        assert info.kind == StatementKind.DDL
        assert info.is_read_only is False

    def test_drop_table(self) -> None:
        """DROP TABLE is DDL."""
        info = analyze("DROP TABLE t")
        assert info.kind == StatementKind.DDL
        assert info.is_read_only is False

    def test_truncate(self) -> None:
        """TRUNCATE is DDL."""
        info = analyze("TRUNCATE TABLE t")
        assert info.kind == StatementKind.DDL
        assert info.is_read_only is False

    def test_grant(self) -> None:
        """GRANT is DDL."""
        info = analyze("GRANT SELECT ON t TO user")
        assert info.kind == StatementKind.DDL
        assert info.is_read_only is False


class TestAnalyzeMaintenance:
    """Test maintenance statements that do not modify data."""

    def test_vacuum(self) -> None:
        """VACUUM is maintenance."""
        info = analyze("VACUUM t")
        assert info.kind == StatementKind.MAINTENANCE
        assert info.is_read_only is False

    def test_analyze(self) -> None:
        """ANALYZE is maintenance."""
        info = analyze("ANALYZE t")
        assert info.kind == StatementKind.MAINTENANCE
        assert info.is_read_only is False

    def test_cluster(self) -> None:
        """CLUSTER is DDL (classified with CREATE/ALTER/DROP)."""
        info = analyze("CLUSTER t USING idx")
        assert info.kind == StatementKind.DDL
        assert info.is_read_only is False


class TestAnalyzeRefused:
    """Test statements that are always refused."""

    def test_begin_transaction(self) -> None:
        """BEGIN is a transaction control statement."""
        info = analyze("BEGIN")
        assert info.kind == StatementKind.TRANSACTION
        assert info.is_read_only is False

    def test_commit(self) -> None:
        """COMMIT is refused."""
        info = analyze("COMMIT")
        assert info.kind == StatementKind.TRANSACTION
        assert info.is_read_only is False

    def test_rollback(self) -> None:
        """ROLLBACK is refused."""
        info = analyze("ROLLBACK")
        assert info.kind == StatementKind.TRANSACTION
        assert info.is_read_only is False

    def test_set_session(self) -> None:
        """SET is session control and refused."""
        info = analyze("SET work_mem TO '256MB'")
        assert info.kind == StatementKind.SESSION
        assert info.is_read_only is False

    def test_set_local(self) -> None:
        """SET LOCAL is session control and refused."""
        info = analyze("SET LOCAL work_mem TO '256MB'")
        assert info.kind == StatementKind.SESSION
        assert info.is_read_only is False

    def test_reset(self) -> None:
        """RESET is session control."""
        info = analyze("RESET work_mem")
        assert info.kind == StatementKind.SESSION
        assert info.is_read_only is False

    def test_copy_from_file(self) -> None:
        """COPY FROM FILE is not read-only."""
        info = analyze("COPY t FROM '/tmp/data.csv'")
        assert info.kind == StatementKind.COPY
        assert info.is_read_only is False

    def test_copy_to_file(self) -> None:
        """COPY TO FILE is not read-only."""
        info = analyze("COPY t TO '/tmp/out.csv'")
        assert info.kind == StatementKind.COPY
        assert info.is_read_only is False


class TestDangerousFunctions:
    """Test detection of functions that escape read-only guarantees."""

    def test_pg_read_file(self) -> None:
        """pg_read_file() is dangerous."""
        info = analyze("SELECT pg_read_file('/etc/passwd')")
        assert not info.is_read_only
        assert "pg_read_file" in (info.reason or "").lower()

    def test_pg_read_file_case_insensitive(self) -> None:
        """pg_read_file detection is case-insensitive."""
        info = analyze("SELECT PG_READ_FILE('/etc/passwd')")
        assert not info.is_read_only

    def test_pg_read_file_with_whitespace(self) -> None:
        """Whitespace before paren is handled."""
        info = analyze("SELECT pg_read_file ('/etc/passwd')")
        assert not info.is_read_only

    def test_lo_export(self) -> None:
        """lo_export() is dangerous."""
        info = analyze("SELECT lo_export(123, '/tmp/data')")
        assert not info.is_read_only
        assert "lo_export" in (info.reason or "").lower()

    def test_dblink(self) -> None:
        """dblink() is dangerous."""
        info = analyze("SELECT dblink('host=localhost', 'SELECT 1')")
        assert not info.is_read_only

    def test_pg_terminate_backend(self) -> None:
        """pg_terminate_backend() is dangerous."""
        info = analyze("SELECT pg_terminate_backend(123)")
        assert not info.is_read_only

    def test_set_config(self) -> None:
        """set_config() is dangerous."""
        info = analyze("SELECT set_config('key', 'value', false)")
        assert not info.is_read_only

    def test_pg_sleep(self) -> None:
        """pg_sleep() is dangerous."""
        info = analyze("SELECT pg_sleep(10)")
        assert not info.is_read_only

    def test_system_function(self) -> None:
        """system() is dangerous."""
        info = analyze("SELECT system('ls')")
        assert not info.is_read_only


class TestCopyProgram:
    """Test COPY ... PROGRAM detection."""

    def test_copy_from_program(self) -> None:
        """COPY FROM PROGRAM is dangerous."""
        info = analyze("COPY t FROM PROGRAM 'cat /tmp/data.csv'")
        assert not info.is_read_only
        assert "program" in (info.reason or "").lower()

    def test_copy_to_program(self) -> None:
        """COPY TO PROGRAM is dangerous."""
        info = analyze("COPY t TO PROGRAM 'gzip > /tmp/out.csv.gz'")
        assert not info.is_read_only

    def test_copy_program_case_insensitive(self) -> None:
        """COPY PROGRAM detection is case-insensitive."""
        info = analyze("copy t from program 'cat /tmp/data'")
        assert not info.is_read_only


class TestExplainAnalyze:
    """Test EXPLAIN ANALYZE (which runs the statement)."""

    def test_explain_analyze_select(self) -> None:
        """EXPLAIN ANALYZE SELECT is read-only."""
        info = analyze("EXPLAIN ANALYZE SELECT 1")
        assert info.is_read_only is True

    def test_explain_analyse_select(self) -> None:
        """EXPLAIN ANALYSE (British spelling) SELECT is read-only."""
        info = analyze("EXPLAIN ANALYSE SELECT 1")
        assert info.is_read_only is True

    def test_explain_analyze_insert(self) -> None:
        """EXPLAIN ANALYZE INSERT is not read-only (runs the insert)."""
        info = analyze("EXPLAIN ANALYZE INSERT INTO t (x) VALUES (1)")
        assert info.is_read_only is False
        assert "explain" in (info.reason or "").lower()

    def test_explain_analyze_delete(self) -> None:
        """EXPLAIN ANALYZE DELETE is not read-only."""
        info = analyze("EXPLAIN ANALYZE DELETE FROM t WHERE x > 0")
        assert info.is_read_only is False

    def test_explain_analyze_options(self) -> None:
        """EXPLAIN (FORMAT JSON, ANALYZE) is evaluated correctly."""
        info = analyze("EXPLAIN (FORMAT JSON, ANALYZE) SELECT 1")
        assert info.is_read_only is True


class TestEmptyAndComments:
    """Test empty and comment-only inputs."""

    def test_empty_string(self) -> None:
        """Empty string is classified as EMPTY."""
        info = analyze("")
        assert info.kind == StatementKind.EMPTY
        assert info.is_read_only is False

    def test_whitespace_only(self) -> None:
        """Whitespace-only input is EMPTY."""
        info = analyze("   \n\n   ")
        assert info.kind == StatementKind.EMPTY

    def test_comment_only_input(self) -> None:
        """Comment-only input is EMPTY."""
        info = analyze("-- this is a comment")
        assert info.kind == StatementKind.EMPTY

    def test_comment_and_whitespace(self) -> None:
        """Comments and whitespace together are EMPTY."""
        info = analyze("/* block */ -- line\n  ")
        assert info.kind == StatementKind.EMPTY


class TestUnknownKeywords:
    """Test handling of unrecognized keywords."""

    def test_unknown_keyword(self) -> None:
        """Unrecognized leading keyword is UNKNOWN."""
        info = analyze("GIBBERISH some stuff")
        assert info.kind == StatementKind.UNKNOWN
        assert info.is_read_only is False


class TestAssertSingleStatement:
    """Test assert_single_statement() helper."""

    def test_single_statement_allowed(self) -> None:
        """A single statement is returned normalized."""
        result = assert_single_statement("SELECT 1")
        assert result == "SELECT 1"

    def test_multiple_statements_rejected(self) -> None:
        """Multiple statements raise UnsafeQueryError."""
        with pytest.raises(UnsafeQueryError) as exc_info:
            assert_single_statement("SELECT 1; SELECT 2")
        assert "single" in str(exc_info.value).lower()
        assert "2" in str(exc_info.value)


class TestAssertReadOnly:
    """Test assert_read_only() helper."""

    def test_read_only_select_allowed(self) -> None:
        """A read-only SELECT is allowed."""
        info = assert_read_only("SELECT 1")
        assert info.is_read_only is True

    def test_insert_rejected(self) -> None:
        """INSERT is rejected with UnsafeQueryError."""
        with pytest.raises(UnsafeQueryError) as exc_info:
            assert_read_only("INSERT INTO t (x) VALUES (1)")
        assert "read-only" in str(exc_info.value).lower()

    def test_update_rejected(self) -> None:
        """UPDATE is rejected."""
        with pytest.raises(UnsafeQueryError):
            assert_read_only("UPDATE t SET x = 1")

    def test_dangerous_function_rejected(self) -> None:
        """A read with a dangerous function is rejected."""
        with pytest.raises(UnsafeQueryError) as exc_info:
            assert_read_only("SELECT pg_sleep(1)")
        assert "read-only" in str(exc_info.value).lower()


class TestMultiLineAndFormatted:
    """Test handling of multi-line and formatted SQL."""

    def test_multi_line_select(self) -> None:
        """Multi-line SELECT is parsed correctly."""
        sql = """
            SELECT
                id,
                name,
                email
            FROM users
            WHERE active = true
        """
        info = analyze(sql)
        assert info.kind == StatementKind.READ
        assert info.is_read_only is True

    def test_formatted_insert(self) -> None:
        """Formatted INSERT is parsed correctly."""
        sql = """
            INSERT INTO users (
                name,
                email
            ) VALUES (
                'John',
                'john@example.com'
            )
        """
        info = analyze(sql)
        assert info.kind == StatementKind.DML

    def test_indented_leading_keyword(self) -> None:
        """Leading keyword with leading whitespace is found."""
        info = analyze("    SELECT 1")
        assert info.kind == StatementKind.READ


class TestStatementInfoProperties:
    """Test StatementInfo properties."""

    def test_read_does_not_mutate(self) -> None:
        """READ statements don't mutate."""
        info = analyze("SELECT 1")
        assert info.mutates is False

    def test_dml_mutates(self) -> None:
        """DML statements mutate."""
        info = analyze("DELETE FROM t")
        assert info.mutates is True

    def test_ddl_mutates(self) -> None:
        """DDL statements mutate."""
        info = analyze("CREATE TABLE t (id INT)")
        assert info.mutates is True

    def test_call_mutates(self) -> None:
        """CALL statements mutate."""
        info = analyze("CALL some_proc()")
        assert info.mutates is True

    def test_maintenance_does_not_mutate(self) -> None:
        """MAINTENANCE statements don't mutate via mutates property."""
        info = analyze("VACUUM t")
        assert info.mutates is False


class TestCallAndDo:
    """Test CALL and DO statements."""

    def test_call_procedure(self) -> None:
        """CALL is treated as a write."""
        info = analyze("CALL my_procedure()")
        assert info.kind == StatementKind.CALL
        assert info.is_read_only is False

    def test_do_block(self) -> None:
        """DO is treated as a write."""
        info = analyze("DO $$BEGIN RAISE NOTICE 'hello'; END$$")
        assert info.kind == StatementKind.CALL
        assert info.is_read_only is False


class TestFragmentValidation:
    """`where` / `order_by` fragments get spliced into a statement pgmcp builds.

    The read-only transaction stops them writing, but not from restructuring the
    query to read past a schema allow-list, so they are held to a bare expression.
    """

    @pytest.mark.parametrize(
        "fragment",
        [
            "status = 'paid'",
            "id > 100",
            "id BETWEEN 1 AND 10",
            "lower(email) LIKE '%@example.com'",
            "created_at > now() - interval '1 day'",
            "a = 1 AND b <> 2 OR NOT c",
            "tags @> ARRAY['x']",
            "metadata->>'k' = 'v'",
            "id IS NOT NULL",
            "status IN ('a', 'b')",
            "coalesce(a, 0) > 0",
            "(a > 1 AND b < 2)",
            "s = 'a;b'",
            "s = 'contains the word SELECT and UNION'",
            "id::text = '1'",
        ],
    )
    def test_plain_expressions_are_accepted(self, fragment: str) -> None:
        assert validate_fragment(fragment)

    @pytest.mark.parametrize(
        "fragment",
        [
            "id DESC",
            "placed_at DESC NULLS LAST",
            "lower(name) ASC",
        ],
    )
    def test_ordering_expressions_are_accepted(self, fragment: str) -> None:
        assert validate_fragment(fragment, kind="ORDER BY")

    @pytest.mark.parametrize(
        "fragment",
        [
            "true UNION SELECT 1",
            "1=1 UNION ALL SELECT email FROM shop.customers",
            "true INTERSECT SELECT 1",
            "true EXCEPT SELECT 1",
            "(SELECT count(*) FROM shop.customers) > 0",
            "id IN (SELECT id FROM secret.t)",
            "1=1; DROP TABLE t",
            "1=1 /* x */ ; DELETE FROM t",
            "pg_read_file('/etc/passwd') = ''",
            "id > 0) UNION SELECT (1",
            "a = 1 LIMIT 1",
            "a = 1 OFFSET 5",
            "1=1 GROUP BY x",
            "x FROM t",
            "",
            "   ",
            "-- only a comment",
        ],
    )
    def test_structural_injection_is_refused(self, fragment: str) -> None:
        with pytest.raises(UnsafeQueryError):
            validate_fragment(fragment)

    def test_trailing_comment_is_stripped_not_honoured(self) -> None:
        """A trailing `--` must not comment out the rest of the built statement."""
        assert validate_fragment("1=1 --") == "1=1"
