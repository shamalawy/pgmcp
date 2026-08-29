"""Tests for error types and secret redaction."""

from __future__ import annotations

from pgmcp.errors import QueryExecutionError, redact_dsn, redact_text


class TestRedactDSNUriForm:
    """Test redact_dsn with URI form DSNs."""

    def test_simple_uri_with_password(self) -> None:
        """Password in URI form is redacted."""
        dsn = "postgresql://user:hunter2@localhost/db"
        result = redact_dsn(dsn)
        assert "***" in result
        assert "hunter2" not in result

    def test_uri_without_password(self) -> None:
        """URI without password is unchanged."""
        dsn = "postgresql://user@localhost/db"
        result = redact_dsn(dsn)
        assert result == dsn

    def test_uri_with_port(self) -> None:
        """Port is preserved when redacting password."""
        dsn = "postgresql://user:secret@localhost:5433/db"
        result = redact_dsn(dsn)
        assert "***" in result
        assert "5433" in result
        assert "secret" not in result

    def test_uri_password_with_special_chars(self) -> None:
        """Passwords with special characters are redacted."""
        dsn = "postgresql://user:p%40ssw%3Drd@localhost/db"
        result = redact_dsn(dsn)
        assert "***" in result

    def test_uri_password_containing_at(self) -> None:
        """Passwords containing @ are handled correctly."""
        dsn = "postgresql://user:p@ssword@localhost/db"
        result = redact_dsn(dsn)
        assert "***" in result
        assert "p@ssword" not in result

    def test_uri_password_containing_colon(self) -> None:
        """Passwords containing : are handled correctly."""
        dsn = "postgresql://user:p:ssword@localhost/db"
        result = redact_dsn(dsn)
        assert "***" in result

    def test_uri_with_path_query_fragment(self) -> None:
        """Path, query, and fragment are preserved."""
        dsn = "postgresql://user:secret@localhost/db?sslmode=require#params"
        result = redact_dsn(dsn)
        assert "***" in result
        assert "sslmode=require" in result

    def test_uri_with_username_no_password(self) -> None:
        """URI with username but no password is unchanged."""
        dsn = "postgresql://user@localhost/db"
        result = redact_dsn(dsn)
        assert result == dsn

    def test_postgresql_scheme(self) -> None:
        """postgresql:// scheme is handled."""
        dsn = "postgresql://user:secret@host/db"
        result = redact_dsn(dsn)
        assert "***" in result

    def test_postgres_scheme(self) -> None:
        """postgres:// scheme (legacy) is handled."""
        dsn = "postgres://user:secret@host/db"
        result = redact_dsn(dsn)
        assert "***" in result


class TestRedactDSNKeywordForm:
    """Test redact_dsn with keyword/value form DSNs."""

    def test_keyword_form_password(self) -> None:
        """password= in keyword form is redacted."""
        dsn = "host=localhost password=secret user=alice"
        result = redact_dsn(dsn)
        assert "***" in result
        assert "secret" not in result

    def test_keyword_form_password_with_spaces(self) -> None:
        """password with spaces around = is redacted."""
        dsn = "host=localhost password = secret user=alice"
        result = redact_dsn(dsn)
        assert "***" in result

    def test_keyword_form_quoted_password(self) -> None:
        """Single-quoted passwords are redacted."""
        dsn = "host=localhost password='secret with spaces' user=alice"
        result = redact_dsn(dsn)
        assert "***" in result
        assert "secret with spaces" not in result

    def test_keyword_form_password_with_quote(self) -> None:
        """Passwords containing single quotes (escaped as '') are handled."""
        dsn = "host=localhost password='secret''s' user=alice"
        result = redact_dsn(dsn)
        assert "***" in result

    def test_keyword_form_password_no_quotes(self) -> None:
        """Unquoted password in keyword form is redacted."""
        dsn = "host=localhost password=hunter2 user=alice dbname=db"
        result = redact_dsn(dsn)
        assert "***" in result
        assert "hunter2" not in result

    def test_keyword_form_case_insensitive(self) -> None:
        """Password keyword is case-insensitive."""
        dsn = "host=localhost PASSWORD=secret user=alice"
        result = redact_dsn(dsn)
        assert "***" in result

    def test_keyword_form_no_password(self) -> None:
        """Keyword form without password is unchanged."""
        dsn = "host=localhost user=alice dbname=db"
        result = redact_dsn(dsn)
        assert result == dsn


class TestRedactDSNMalformed:
    """Test redact_dsn with malformed input."""

    def test_empty_string(self) -> None:
        """Empty string is returned unchanged."""
        result = redact_dsn("")
        assert result == ""

    def test_malformed_uri(self) -> None:
        """Unparseable URI is either masked or returned as-is."""
        # urlsplit is very lenient and doesn't raise errors for malformed URIs
        result = redact_dsn("ht!tp:///broken:url:://host")
        # Either masked or returned unchanged (since there's no password)
        assert result in ("***", "ht!tp:///broken:url:://host")

    def test_malformed_keyword_form(self) -> None:
        """Malformed keyword form returns original or masked."""
        # Keyword form is regex-based so unlikely to fail, but test robustness
        result = redact_dsn("host=localhost user=alice")
        assert result == "host=localhost user=alice"  # No password to redact


class TestRedactText:
    """Test redact_text() secret removal."""

    def test_redact_single_secret(self) -> None:
        """A secret is replaced with ***."""
        text = "The password is hunter2"
        result = redact_text(text, "hunter2")
        assert "***" in result
        assert "hunter2" not in result

    def test_redact_multiple_secrets(self) -> None:
        """Multiple secrets are all redacted."""
        text = "user alice password hunter2"
        result = redact_text(text, "alice", "hunter2")
        assert result.count("***") == 2
        assert "alice" not in result
        assert "hunter2" not in result

    def test_redact_none_secret(self) -> None:
        """None secrets are skipped."""
        text = "The password is hunter2"
        result = redact_text(text, None, "hunter2")
        assert "***" in result
        assert "hunter2" not in result

    def test_redact_empty_secret(self) -> None:
        """Empty string secrets are skipped."""
        text = "The password is hunter2"
        result = redact_text(text, "", "hunter2")
        assert "***" in result
        assert "hunter2" not in result

    def test_redact_non_matching_secret(self) -> None:
        """Secrets not in text don't change it."""
        text = "hello world"
        result = redact_text(text, "notfound")
        assert result == text

    def test_redact_case_sensitive(self) -> None:
        """Redaction is case-sensitive."""
        text = "The password is Hunter2"
        result = redact_text(text, "hunter2")
        assert "Hunter2" in result  # Not redacted

    def test_redact_repeated_secret(self) -> None:
        """Repeated secrets are all redacted."""
        text = "password1 password1 password1"
        result = redact_text(text, "password1")
        assert result.count("***") == 3


class TestQueryExecutionError:
    """Test QueryExecutionError and its as_dict() method."""

    def test_basic_error(self) -> None:
        """Basic error message is stored."""
        exc = QueryExecutionError("syntax error")
        assert str(exc) == "syntax error"

    def test_as_dict_minimal(self) -> None:
        """as_dict with just a message."""
        exc = QueryExecutionError("syntax error")
        d = exc.as_dict()
        assert d["error"] == "syntax error"
        assert "sqlstate" not in d

    def test_as_dict_with_sqlstate(self) -> None:
        """sqlstate is included when present."""
        exc = QueryExecutionError("syntax error", sqlstate="42601")
        d = exc.as_dict()
        assert d["error"] == "syntax error"
        assert d["sqlstate"] == "42601"

    def test_as_dict_with_detail(self) -> None:
        """detail is included when present."""
        exc = QueryExecutionError(
            "foreign key violation",
            detail="Key (user_id)=(999) is not present in table users.",
        )
        d = exc.as_dict()
        assert d["detail"] == "Key (user_id)=(999) is not present in table users."

    def test_as_dict_with_hint(self) -> None:
        """hint is included when present."""
        exc = QueryExecutionError(
            "column does not exist",
            hint="Perhaps you meant to reference the alias 'u'.",
        )
        d = exc.as_dict()
        assert d["hint"] == "Perhaps you meant to reference the alias 'u'."

    def test_as_dict_with_position(self) -> None:
        """position is included when present."""
        exc = QueryExecutionError(
            "syntax error",
            position=42,
        )
        d = exc.as_dict()
        assert d["position"] == 42

    def test_as_dict_all_fields(self) -> None:
        """All fields are included when present."""
        exc = QueryExecutionError(
            "division by zero",
            sqlstate="22012",
            detail="An attempt was made to divide by zero.",
            hint="Consider adding a check for division by zero.",
            position=123,
        )
        d = exc.as_dict()
        assert d["error"] == "division by zero"
        assert d["sqlstate"] == "22012"
        assert d["detail"] == "An attempt was made to divide by zero."
        assert d["hint"] == "Consider adding a check for division by zero."
        assert d["position"] == 123

    def test_as_dict_excludes_none_values(self) -> None:
        """None values are excluded from the dict."""
        exc = QueryExecutionError(
            "error",
            sqlstate="42P01",
            detail=None,
            hint=None,
            position=None,
        )
        d = exc.as_dict()
        assert "detail" not in d
        assert "hint" not in d
        assert "position" not in d
        assert d["sqlstate"] == "42P01"

    def test_as_dict_position_zero(self) -> None:
        """Position of 0 is falsy but should still be included if set."""
        exc = QueryExecutionError(
            "error",
            position=0,
        )
        d = exc.as_dict()
        assert d["position"] == 0

    def test_as_dict_json_serializable(self) -> None:
        """as_dict returns JSON-serializable types."""
        import json

        exc = QueryExecutionError(
            "error message",
            sqlstate="XXXXX",
            detail="detail text",
            hint="hint text",
            position=42,
        )
        d = exc.as_dict()
        # Should not raise
        json.dumps(d)
