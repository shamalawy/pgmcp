"""Configuration parsing and validation tests."""

from __future__ import annotations

import pytest

from pgmcp.config import AccessMode, Settings, Toolset
from pgmcp.errors import ConfigurationError


class TestAccessModeNormalization:
    """Test access_mode parsing with aliases."""

    def test_read_only_value(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Explicit read-only value is accepted."""
        s = Settings(dsn="postgresql://localhost/test", access_mode="read-only")
        assert s.access_mode == AccessMode.READ_ONLY

    def test_readonly_alias(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """readonly alias maps to read-only."""
        s = Settings(dsn="postgresql://localhost/test", access_mode="readonly")
        assert s.access_mode == AccessMode.READ_ONLY

    def test_read_only_underscore_alias(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """read_only (underscore) alias maps to read-only."""
        s = Settings(dsn="postgresql://localhost/test", access_mode="read_only")
        assert s.access_mode == AccessMode.READ_ONLY

    def test_ro_alias(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """ro alias maps to read-only."""
        s = Settings(dsn="postgresql://localhost/test", access_mode="ro")
        assert s.access_mode == AccessMode.READ_ONLY

    def test_write_value(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """write value is accepted."""
        s = Settings(dsn="postgresql://localhost/test", access_mode="write")
        assert s.access_mode == AccessMode.WRITE

    def test_rw_alias(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """rw alias maps to write."""
        s = Settings(dsn="postgresql://localhost/test", access_mode="rw")
        assert s.access_mode == AccessMode.WRITE

    def test_read_write_alias(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """read-write alias maps to write."""
        s = Settings(dsn="postgresql://localhost/test", access_mode="read-write")
        assert s.access_mode == AccessMode.WRITE

    def test_access_mode_case_insensitive(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Access mode parsing is case-insensitive."""
        s = Settings(dsn="postgresql://localhost/test", access_mode="WRITE")
        assert s.access_mode == AccessMode.WRITE


class TestAllowBlockedSchemas:
    """Test allowed_schemas and blocked_schemas parsing."""

    def test_comma_separated_allowed(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Comma-separated allowed_schemas are parsed."""
        s = Settings(
            dsn="postgresql://localhost/test",
            allowed_schemas="public,reporting,staging",
        )
        assert s.allowed_schemas == ["public", "reporting", "staging"]

    def test_json_list_allowed(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """JSON list for allowed_schemas is parsed."""
        s = Settings(
            dsn="postgresql://localhost/test",
            allowed_schemas='["public", "reporting"]',
        )
        assert s.allowed_schemas == ["public", "reporting"]

    def test_empty_string_allowed(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Empty string for allowed_schemas results in empty list."""
        s = Settings(dsn="postgresql://localhost/test", allowed_schemas="")
        assert s.allowed_schemas == []

    def test_blocked_schemas_parsing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """blocked_schemas parsing."""
        s = Settings(
            dsn="postgresql://localhost/test",
            blocked_schemas="pg_temp,test_schema",
        )
        assert s.blocked_schemas == ["pg_temp", "test_schema"]

    def test_spaces_around_commas(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Spaces around commas are stripped."""
        s = Settings(
            dsn="postgresql://localhost/test",
            allowed_schemas="public , reporting , staging",
        )
        assert s.allowed_schemas == ["public", "reporting", "staging"]

    def test_json_with_commas_in_name(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """JSON list allows schema names containing commas (if quoted)."""
        # This tests the JSON parsing path
        s = Settings(
            dsn="postgresql://localhost/test",
            allowed_schemas='["public,weird", "normal"]',
        )
        assert s.allowed_schemas == ["public,weird", "normal"]

    def test_malformed_json_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Malformed JSON raises ValueError."""
        # Pydantic validation raises ValueError, which may be caught and converted
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            Settings(
                dsn="postgresql://localhost/test",
                allowed_schemas='["unclosed"',
            )


class TestEnabledToolsets:
    """Test enabled_toolsets parsing."""

    def test_all_by_default(self) -> None:
        """All toolsets are enabled by default."""
        s = Settings(dsn="postgresql://localhost/test")
        assert Toolset.SCHEMA in s.enabled_toolsets
        assert Toolset.QUERY in s.enabled_toolsets
        assert Toolset.HEALTH in s.enabled_toolsets
        assert Toolset.WRITE in s.enabled_toolsets

    def test_comma_separated(self) -> None:
        """Comma-separated toolset names are parsed."""
        s = Settings(
            dsn="postgresql://localhost/test",
            enabled_toolsets="schema,query",
        )
        assert s.enabled_toolsets == [Toolset.SCHEMA, Toolset.QUERY]

    def test_json_list(self) -> None:
        """JSON list of toolsets is parsed."""
        s = Settings(
            dsn="postgresql://localhost/test",
            enabled_toolsets='["schema", "health"]',
        )
        assert s.enabled_toolsets == [Toolset.SCHEMA, Toolset.HEALTH]

    def test_all_keyword(self) -> None:
        """'all' keyword enables all toolsets."""
        s = Settings(
            dsn="postgresql://localhost/test",
            enabled_toolsets="all",
        )
        assert len(s.enabled_toolsets) == 4

    def test_empty_string(self) -> None:
        """Empty string disables all toolsets."""
        s = Settings(
            dsn="postgresql://localhost/test",
            enabled_toolsets="",
        )
        assert s.enabled_toolsets == []

    def test_case_insensitive(self) -> None:
        """Toolset names are case-insensitive."""
        s = Settings(
            dsn="postgresql://localhost/test",
            enabled_toolsets="SCHEMA,Query",
        )
        assert s.enabled_toolsets == [Toolset.SCHEMA, Toolset.QUERY]

    def test_write_disabled_in_read_only_mode(self) -> None:
        """Write toolset is disabled in read-only mode even if enabled."""
        s = Settings(
            dsn="postgresql://localhost/test",
            access_mode="read-only",
            enabled_toolsets="schema,query,write",
        )
        assert Toolset.WRITE in s.enabled_toolsets  # It's in the list
        assert not s.toolset_enabled(Toolset.WRITE)  # But not actually enabled


class TestResolveDSN:
    """Test DSN resolution from various sources."""

    def test_explicit_dsn(self) -> None:
        """Explicit dsn parameter is used first."""
        s = Settings(dsn="postgresql://explicit@host/db")
        assert s.resolve_dsn() == "postgresql://explicit@host/db"

    def test_database_url_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """DATABASE_URL environment variable is used if dsn not set."""
        monkeypatch.setenv("DATABASE_URL", "postgresql://user@host/db")
        s = Settings()
        assert s.resolve_dsn() == "postgresql://user@host/db"

    def test_postgres_url_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """POSTGRES_URL environment variable is used."""
        monkeypatch.setenv("POSTGRES_URL", "postgresql://user@host/db")
        s = Settings()
        assert s.resolve_dsn() == "postgresql://user@host/db"

    def test_postgres_dsn_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """POSTGRES_DSN environment variable is used."""
        monkeypatch.setenv("POSTGRES_DSN", "postgresql://user@host/db")
        s = Settings()
        assert s.resolve_dsn() == "postgresql://user@host/db"

    def test_pghost_pguser_pgdatabase(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """PG* environment variables are used to assemble DSN."""
        monkeypatch.delenv("DATABASE_URL", raising=False)
        monkeypatch.delenv("POSTGRES_URL", raising=False)
        monkeypatch.delenv("POSTGRES_DSN", raising=False)
        monkeypatch.setenv("PGHOST", "localhost")
        monkeypatch.setenv("PGUSER", "alice")
        monkeypatch.setenv("PGDATABASE", "mydb")
        s = Settings()
        dsn = s.resolve_dsn()
        assert "alice" in dsn
        assert "localhost" in dsn
        assert "mydb" in dsn

    def test_pgpassword_included(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """PGPASSWORD is included in assembled DSN."""
        monkeypatch.delenv("DATABASE_URL", raising=False)
        monkeypatch.delenv("POSTGRES_URL", raising=False)
        monkeypatch.delenv("POSTGRES_DSN", raising=False)
        monkeypatch.setenv("PGHOST", "localhost")
        monkeypatch.setenv("PGUSER", "alice")
        monkeypatch.setenv("PGPASSWORD", "secret")
        monkeypatch.setenv("PGDATABASE", "mydb")
        s = Settings()
        dsn = s.resolve_dsn()
        assert "secret" in dsn

    def test_pgport_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """PGPORT defaults to 5432."""
        monkeypatch.delenv("DATABASE_URL", raising=False)
        monkeypatch.delenv("POSTGRES_URL", raising=False)
        monkeypatch.delenv("POSTGRES_DSN", raising=False)
        monkeypatch.setenv("PGHOST", "localhost")
        monkeypatch.setenv("PGUSER", "alice")
        monkeypatch.setenv("PGDATABASE", "mydb")
        s = Settings()
        dsn = s.resolve_dsn()
        assert ":5432" in dsn

    def test_pgport_custom(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """PGPORT custom value is used."""
        monkeypatch.delenv("DATABASE_URL", raising=False)
        monkeypatch.delenv("POSTGRES_URL", raising=False)
        monkeypatch.delenv("POSTGRES_DSN", raising=False)
        monkeypatch.setenv("PGHOST", "localhost")
        monkeypatch.setenv("PGUSER", "alice")
        monkeypatch.setenv("PGPORT", "5433")
        monkeypatch.setenv("PGDATABASE", "mydb")
        s = Settings()
        dsn = s.resolve_dsn()
        assert ":5433" in dsn

    def test_no_dsn_raises_configuration_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """ConfigurationError is raised when no DSN is available."""
        monkeypatch.delenv("DATABASE_URL", raising=False)
        monkeypatch.delenv("POSTGRES_URL", raising=False)
        monkeypatch.delenv("POSTGRES_DSN", raising=False)
        monkeypatch.delenv("PGHOST", raising=False)
        monkeypatch.delenv("PGUSER", raising=False)
        monkeypatch.delenv("PGDATABASE", raising=False)
        with pytest.raises(ConfigurationError):
            Settings().resolve_dsn()

    def test_pghost_alone_triggers_assembly(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Just PGHOST is enough to trigger DSN assembly."""
        monkeypatch.delenv("DATABASE_URL", raising=False)
        monkeypatch.delenv("POSTGRES_URL", raising=False)
        monkeypatch.delenv("POSTGRES_DSN", raising=False)
        monkeypatch.setenv("PGHOST", "localhost")
        s = Settings()
        dsn = s.resolve_dsn()
        assert "localhost" in dsn


class TestValidation:
    """Test Settings validation."""

    def test_pool_sizes_valid(self) -> None:
        """Valid pool sizes are accepted."""
        s = Settings(
            dsn="postgresql://localhost/test",
            pool_min_size=2,
            pool_max_size=5,
        )
        assert s.pool_min_size == 2
        assert s.pool_max_size == 5

    def test_pool_max_less_than_min_raises(self) -> None:
        """pool_max_size < pool_min_size raises ValueError."""
        with pytest.raises(ValueError):
            Settings(
                dsn="postgresql://localhost/test",
                pool_min_size=5,
                pool_max_size=2,
            )

    def test_max_rows_bounds(self) -> None:
        """max_rows has bounds."""
        # Valid
        s = Settings(dsn="postgresql://localhost/test", max_rows=100)
        assert s.max_rows == 100
        # Too small
        with pytest.raises(ValueError):
            Settings(dsn="postgresql://localhost/test", max_rows=0)
        # Too large
        with pytest.raises(ValueError):
            Settings(dsn="postgresql://localhost/test", max_rows=200_000)

    def test_bad_log_level(self) -> None:
        """Invalid log level raises ValueError/ValidationError."""
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            Settings(dsn="postgresql://localhost/test", log_level="GIBBERISH")

    def test_valid_log_levels(self) -> None:
        """Valid log levels are accepted."""
        for level in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
            s = Settings(dsn="postgresql://localhost/test", log_level=level)
            assert s.log_level == level

    def test_log_level_case_normalized(self) -> None:
        """Log level is uppercased."""
        s = Settings(dsn="postgresql://localhost/test", log_level="debug")
        assert s.log_level == "DEBUG"

    def test_port_bounds(self) -> None:
        """Port must be in valid range."""
        # Valid
        s = Settings(dsn="postgresql://localhost/test", port=8000)
        assert s.port == 8000
        # Too low
        with pytest.raises(ValueError):
            Settings(dsn="postgresql://localhost/test", port=0)
        # Too high
        with pytest.raises(ValueError):
            Settings(dsn="postgresql://localhost/test", port=70000)

    def test_mount_path_slash_prefix(self) -> None:
        """mount_path gets a leading slash if missing."""
        s1 = Settings(dsn="postgresql://localhost/test", mount_path="/mcp")
        assert s1.mount_path == "/mcp"
        s2 = Settings(dsn="postgresql://localhost/test", mount_path="mcp")
        assert s2.mount_path == "/mcp"


class TestToolsetEnabled:
    """Test toolset_enabled() logic."""

    def test_enabled_toolset_in_list(self) -> None:
        """A toolset in the list is enabled."""
        s = Settings(
            dsn="postgresql://localhost/test",
            enabled_toolsets="schema,query",
        )
        assert s.toolset_enabled(Toolset.SCHEMA)
        assert s.toolset_enabled(Toolset.QUERY)
        assert not s.toolset_enabled(Toolset.HEALTH)

    def test_write_blocked_in_read_only_mode(self) -> None:
        """Write toolset is blocked in read-only mode."""
        s = Settings(
            dsn="postgresql://localhost/test",
            access_mode="read-only",
            enabled_toolsets="all",
        )
        assert not s.toolset_enabled(Toolset.WRITE)
        assert s.toolset_enabled(Toolset.SCHEMA)

    def test_write_enabled_in_write_mode(self) -> None:
        """Write toolset is enabled in write mode if in the list."""
        s = Settings(
            dsn="postgresql://localhost/test",
            access_mode="write",
            enabled_toolsets="write",
        )
        assert s.toolset_enabled(Toolset.WRITE)


class TestSafeDSN:
    """Test safe_dsn property redaction."""

    def test_safe_dsn_with_password(self) -> None:
        """Passwords are redacted in safe_dsn."""
        s = Settings(dsn="postgresql://user:password@host/db")
        assert "password" not in s.safe_dsn
        assert "***" in s.safe_dsn

    def test_safe_dsn_without_password(self) -> None:
        """DSN without password is unchanged."""
        s = Settings(dsn="postgresql://user@host/db")
        assert s.safe_dsn == s.resolve_dsn()

    def test_safe_dsn_unconfigured(self) -> None:
        """unconfigured DSN returns placeholder."""
        s = Settings()
        assert "<unconfigured>" in s.safe_dsn


class TestDescribe:
    """Test the describe() method."""

    def test_describe_includes_settings(self) -> None:
        """describe() returns configuration as dict."""
        s = Settings(
            dsn="postgresql://user:password@host/db",
            access_mode="write",
            max_rows=200,
        )
        desc = s.describe()
        assert "dsn" in desc
        assert "password" not in desc["dsn"]
        assert desc["access_mode"] == "write"
        assert desc["max_rows"] == 200
        assert "enabled_toolsets" in desc

    def test_describe_omits_empty_lists(self) -> None:
        """Null values are omitted from describe."""
        s = Settings(
            dsn="postgresql://localhost/test",
            allowed_schemas=[],
            blocked_schemas=[],
        )
        desc = s.describe()
        assert desc.get("allowed_schemas") is None
        assert desc.get("blocked_schemas") is None


class TestHiddenSchemas:
    """Test hidden_schemas() method."""

    def test_hidden_schemas_default(self) -> None:
        """By default, system schemas are hidden."""
        s = Settings(dsn="postgresql://localhost/test", include_system_schemas=False)
        hidden = s.hidden_schemas()
        assert "pg_catalog" in hidden
        assert "information_schema" in hidden
        assert "pg_toast" in hidden

    def test_hidden_schemas_include_system(self) -> None:
        """When include_system_schemas=True, they are not hidden by that."""
        s = Settings(
            dsn="postgresql://localhost/test",
            include_system_schemas=True,
            blocked_schemas=[],
        )
        hidden = s.hidden_schemas()
        assert "pg_catalog" not in hidden

    def test_hidden_schemas_blocked(self) -> None:
        """blocked_schemas are always hidden."""
        s = Settings(
            dsn="postgresql://localhost/test",
            blocked_schemas=["custom_schema"],
        )
        hidden = s.hidden_schemas()
        assert "custom_schema" in hidden
        assert "pg_catalog" in hidden  # System schemas still hidden by default
