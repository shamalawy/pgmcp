"""Tests for CLI argument parsing and handling."""

from __future__ import annotations

import pytest

from pgmcp.__main__ import build_parser, settings_from_args
from pgmcp.config import AccessMode, OutputFormat
from pgmcp.errors import ConfigurationError


class TestBuildParser:
    """Test build_parser() creates a valid argument parser."""

    def test_parser_accepts_version(self) -> None:
        """Parser has --version flag."""
        parser = build_parser()
        # The --version action will raise SystemExit on success
        with pytest.raises(SystemExit):
            parser.parse_args(["--version"])

    def test_parser_accepts_help(self) -> None:
        """Parser has --help flag."""
        parser = build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["--help"])

    def test_parser_accepts_dsn(self) -> None:
        """Parser accepts --dsn."""
        parser = build_parser()
        args = parser.parse_args(["--dsn", "postgresql://localhost/test"])
        assert args.dsn == "postgresql://localhost/test"

    def test_parser_accepts_access_mode(self) -> None:
        """Parser accepts --access-mode."""
        parser = build_parser()
        args = parser.parse_args(["--access-mode", "write"])
        assert args.access_mode == "write"

    def test_parser_accepts_read_only_shorthand(self) -> None:
        """Parser accepts --read-only shorthand."""
        parser = build_parser()
        args = parser.parse_args(["--read-only"])
        assert args.access_mode == "read-only"

    def test_parser_accepts_max_rows(self) -> None:
        """Parser accepts --max-rows."""
        parser = build_parser()
        args = parser.parse_args(["--max-rows", "1000"])
        assert args.max_rows == 1000

    def test_parser_accepts_statement_timeout(self) -> None:
        """Parser accepts --statement-timeout."""
        parser = build_parser()
        args = parser.parse_args(["--statement-timeout", "60000"])
        assert args.statement_timeout_ms == 60000

    def test_parser_accepts_allowed_schemas(self) -> None:
        """Parser accepts --allowed-schemas."""
        parser = build_parser()
        args = parser.parse_args(["--allowed-schemas", "public,reporting"])
        assert args.allowed_schemas == "public,reporting"

    def test_parser_accepts_blocked_schemas(self) -> None:
        """Parser accepts --blocked-schemas."""
        parser = build_parser()
        args = parser.parse_args(["--blocked-schemas", "test,temp"])
        assert args.blocked_schemas == "test,temp"

    def test_parser_accepts_format(self) -> None:
        """Parser accepts --format."""
        parser = build_parser()
        args = parser.parse_args(["--format", "csv"])
        assert args.default_format == "csv"

    def test_parser_accepts_transport(self) -> None:
        """Parser accepts --transport."""
        parser = build_parser()
        args = parser.parse_args(["--transport", "streamable-http"])
        assert args.transport == "streamable-http"

    def test_parser_accepts_host(self) -> None:
        """Parser accepts --host."""
        parser = build_parser()
        args = parser.parse_args(["--host", "0.0.0.0"])
        assert args.host == "0.0.0.0"

    def test_parser_accepts_port(self) -> None:
        """Parser accepts --port."""
        parser = build_parser()
        args = parser.parse_args(["--port", "8080"])
        assert args.port == 8080

    def test_parser_accepts_check(self) -> None:
        """Parser accepts --check."""
        parser = build_parser()
        args = parser.parse_args(["--check"])
        assert args.check is True

    def test_parser_accepts_log_level(self) -> None:
        """Parser accepts --log-level."""
        parser = build_parser()
        args = parser.parse_args(["--log-level", "DEBUG"])
        assert args.log_level == "DEBUG"

    def test_parser_accepts_toolsets(self) -> None:
        """Parser accepts --toolsets."""
        parser = build_parser()
        args = parser.parse_args(["--toolsets", "schema,query"])
        assert args.enabled_toolsets == "schema,query"

    def test_parser_unknown_arg_fails(self) -> None:
        """Parser rejects unknown arguments."""
        parser = build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["--unknown-arg"])

    def test_parser_multiple_args(self) -> None:
        """Parser accepts multiple arguments together."""
        parser = build_parser()
        args = parser.parse_args(
            [
                "--dsn",
                "postgresql://localhost/test",
                "--access-mode",
                "write",
                "--max-rows",
                "1000",
                "--format",
                "json",
            ]
        )
        assert args.dsn == "postgresql://localhost/test"
        assert args.access_mode == "write"
        assert args.max_rows == 1000
        assert args.default_format == "json"


class TestSettingsFromArgs:
    """Test settings_from_args() conversion."""

    def test_settings_from_minimal_args(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Minimal args with DSN create valid settings."""
        parser = build_parser()
        args = parser.parse_args(["--dsn", "postgresql://localhost/test"])
        settings = settings_from_args(args)
        assert settings.resolve_dsn() == "postgresql://localhost/test"

    def test_settings_from_args_access_mode(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Access mode from args is applied."""
        parser = build_parser()
        args = parser.parse_args(
            [
                "--dsn",
                "postgresql://localhost/test",
                "--access-mode",
                "write",
            ]
        )
        settings = settings_from_args(args)
        assert settings.access_mode == AccessMode.WRITE

    def test_settings_from_args_max_rows(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """max_rows from args is applied."""
        parser = build_parser()
        args = parser.parse_args(
            [
                "--dsn",
                "postgresql://localhost/test",
                "--max-rows",
                "2000",
            ]
        )
        settings = settings_from_args(args)
        assert settings.max_rows == 2000

    def test_settings_from_args_format(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Format from args is applied."""
        parser = build_parser()
        args = parser.parse_args(
            [
                "--dsn",
                "postgresql://localhost/test",
                "--format",
                "csv",
            ]
        )
        settings = settings_from_args(args)
        assert settings.default_format == OutputFormat.CSV

    def test_settings_from_args_transport(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Transport from args is applied."""
        parser = build_parser()
        args = parser.parse_args(
            [
                "--dsn",
                "postgresql://localhost/test",
                "--transport",
                "sse",
            ]
        )
        settings = settings_from_args(args)
        from pgmcp.config import Transport

        assert settings.transport == Transport.SSE

    def test_settings_from_args_none_values_ignored(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """None values from args are ignored (environment wins)."""
        parser = build_parser()
        args = parser.parse_args(["--dsn", "postgresql://localhost/test"])
        # access_mode should be None if not specified
        assert args.access_mode is None
        settings = settings_from_args(args)
        # Should get the default (read-only)
        assert settings.access_mode == AccessMode.READ_ONLY

    def test_settings_from_args_invalid_raises_configuration_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Invalid args raise ConfigurationError."""
        parser = build_parser()
        args = parser.parse_args(
            [
                "--dsn",
                "postgresql://localhost/test",
                "--max-rows",
                "999999999",  # Exceeds bounds
            ]
        )
        with pytest.raises(ConfigurationError):
            settings_from_args(args)

    def test_settings_from_args_check_not_passed(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The 'check' flag is not passed to Settings."""
        parser = build_parser()
        args = parser.parse_args(
            [
                "--dsn",
                "postgresql://localhost/test",
                "--check",
            ]
        )
        # Should not raise; check is filtered out
        settings = settings_from_args(args)
        assert settings is not None


class TestParserEdgeCases:
    """Test edge cases and error handling."""

    def test_parser_with_equals_syntax(self) -> None:
        """Parser accepts --arg=value syntax."""
        parser = build_parser()
        args = parser.parse_args(["--dsn=postgresql://localhost/test"])
        assert args.dsn == "postgresql://localhost/test"

    def test_parser_missing_required_value(self) -> None:
        """Parser rejects --arg without value."""
        parser = build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["--dsn"])

    def test_parser_boolean_flags(self) -> None:
        """Boolean flags work correctly."""
        parser = build_parser()
        args = parser.parse_args(
            [
                "--check",
                "--include-system-schemas",
            ]
        )
        assert args.check is True
        assert args.include_system_schemas is True

    def test_parser_no_args_is_valid(self) -> None:
        """Parser works with no arguments (relies on env vars)."""
        parser = build_parser()
        args = parser.parse_args([])
        # Should successfully create args with all defaults/None
        assert args is not None


class TestAccessModeOptions:
    """Test access mode related options."""

    def test_parser_has_read_only_shorthand(self) -> None:
        """--read-only is accepted as shorthand."""
        parser = build_parser()
        args = parser.parse_args(["--read-only"])
        assert args.access_mode == "read-only"

    def test_parser_has_access_mode_choices(self) -> None:
        """Parser restricts access_mode to valid choices."""
        parser = build_parser()
        # Valid
        args = parser.parse_args(["--access-mode", "read-only"])
        assert args.access_mode == "read-only"
        # Invalid
        with pytest.raises(SystemExit):
            parser.parse_args(["--access-mode", "invalid"])


class TestConnectionOptions:
    """Test connection-related options."""

    def test_parser_application_name(self) -> None:
        """Parser accepts --application-name."""
        parser = build_parser()
        args = parser.parse_args(["--application-name", "my-app"])
        assert args.application_name == "my-app"

    def test_parser_pool_options(self) -> None:
        """Parser accepts pool options."""
        parser = build_parser()
        args = parser.parse_args(
            [
                "--pool-min-size",
                "2",
                "--pool-max-size",
                "10",
                "--connect-timeout",
                "30",
            ]
        )
        assert args.pool_min_size == 2
        assert args.pool_max_size == 10
        assert args.connect_timeout == 30


class TestTransportOptions:
    """Test transport-related options."""

    def test_parser_mount_path(self) -> None:
        """Parser accepts --mount-path."""
        parser = build_parser()
        args = parser.parse_args(["--mount-path", "/api/mcp"])
        assert args.mount_path == "/api/mcp"

    def test_parser_json_response(self) -> None:
        """Parser accepts --json-response."""
        parser = build_parser()
        args = parser.parse_args(["--json-response"])
        assert args.json_response is True

    def test_parser_stateless(self) -> None:
        """Parser accepts --stateless."""
        parser = build_parser()
        args = parser.parse_args(["--stateless"])
        assert args.stateless_http is True
