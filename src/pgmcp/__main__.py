"""Command-line entry point for pgmcp.

Uses ``argparse`` rather than a CLI framework so that installing pgmcp pulls in
nothing beyond the MCP SDK and the PostgreSQL driver — it is frequently launched
by ``uvx``/``pipx`` where every extra dependency is startup latency.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from typing import Any

from . import __version__
from .config import AccessMode, OutputFormat, Settings, Toolset, Transport
from .errors import ConfigurationError, PgMcpError

__all__ = ["build_parser", "main", "run"]

_EPILOG = """\
environment variables:
  PGMCP_DSN / DATABASE_URL     libpq connection string (required)
  PGMCP_ACCESS_MODE            read-only (default) | write
  PGMCP_MAX_ROWS               hard cap on rows per call (default 500)
  PGMCP_STATEMENT_TIMEOUT_MS   per-statement timeout (default 30000)
  PGMCP_ALLOWED_SCHEMAS        comma-separated allow-list
  PGMCP_BLOCKED_SCHEMAS        comma-separated deny-list
  PGMCP_ENABLED_TOOLSETS       schema,query,health,write

examples:
  pgmcp --dsn postgresql://user:pw@localhost:5432/app
  pgmcp --access-mode write --allowed-schemas public,reporting
  pgmcp --transport streamable-http --host 0.0.0.0 --port 8000
  pgmcp --check
"""


def build_parser() -> argparse.ArgumentParser:
    """The full argument parser, also used to render documentation."""
    parser = argparse.ArgumentParser(
        prog="pgmcp",
        description="A fully featured Model Context Protocol server for PostgreSQL.",
        epilog=_EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--version", action="version", version=f"pgmcp {__version__}")

    connection = parser.add_argument_group("connection")
    connection.add_argument("--dsn", help="libpq connection string; defaults to $PGMCP_DSN/$DATABASE_URL")
    connection.add_argument("--application-name", help="value reported in pg_stat_activity")
    connection.add_argument("--pool-min-size", type=int, help="minimum pooled connections")
    connection.add_argument("--pool-max-size", type=int, help="maximum pooled connections")
    connection.add_argument("--connect-timeout", type=int, help="connect timeout in seconds")

    safety = parser.add_argument_group("safety")
    safety.add_argument(
        "--access-mode",
        choices=[m.value for m in AccessMode],
        help="read-only (default) hides every mutating tool; write enables them",
    )
    safety.add_argument(
        "--read-only",
        dest="access_mode",
        action="store_const",
        const=AccessMode.READ_ONLY.value,
        help="shorthand for --access-mode=read-only",
    )
    safety.add_argument("--max-rows", type=int, help="hard cap on rows returned by any tool call")
    safety.add_argument("--max-result-bytes", type=int, help="byte budget for a serialised result")
    safety.add_argument(
        "--statement-timeout",
        type=int,
        dest="statement_timeout_ms",
        help="per-statement timeout in milliseconds",
    )
    safety.add_argument("--allowed-schemas", help="comma-separated schemas to expose (default: all)")
    safety.add_argument("--blocked-schemas", help="comma-separated schemas to hide")
    safety.add_argument(
        "--include-system-schemas",
        action="store_true",
        default=None,
        help="expose pg_catalog and information_schema in listings",
    )
    safety.add_argument("--allowed-databases", help="comma-separated databases reachable cross-database")
    safety.add_argument(
        "--no-cross-database",
        dest="allow_cross_database",
        action="store_false",
        default=None,
        help="refuse tool calls that target another database",
    )
    safety.add_argument(
        "--toolsets",
        dest="enabled_toolsets",
        help=f"comma-separated subset of: {', '.join(t.value for t in Toolset)}",
    )

    output = parser.add_argument_group("output")
    output.add_argument(
        "--format",
        dest="default_format",
        choices=[f.value for f in OutputFormat],
        help="default serialisation for tabular results",
    )
    output.add_argument("--default-row-limit", type=int, help="row limit when a tool call omits one")

    transport = parser.add_argument_group("transport")
    transport.add_argument(
        "--transport", choices=[t.value for t in Transport], help="stdio (default), streamable-http, or sse"
    )
    transport.add_argument("--host", help="bind address for HTTP transports")
    transport.add_argument("--port", type=int, help="port for HTTP transports")
    transport.add_argument("--mount-path", help="URL path for the streamable-http endpoint")
    transport.add_argument(
        "--json-response", action="store_true", default=None, help="return plain JSON instead of SSE framing"
    )
    transport.add_argument(
        "--stateless",
        dest="stateless_http",
        action="store_true",
        default=None,
        help="run streamable-http without server-side session state",
    )

    parser.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])
    parser.add_argument(
        "--check",
        action="store_true",
        help="connect, print a configuration and connectivity report, then exit",
    )
    return parser


def settings_from_args(args: argparse.Namespace) -> Settings:
    """Build :class:`Settings`, letting explicit flags win over the environment."""
    overrides: dict[str, Any] = {
        key: value for key, value in vars(args).items() if value is not None and key not in {"check"}
    }
    try:
        return Settings(**overrides)
    except ConfigurationError:
        raise
    except ValueError as exc:
        raise ConfigurationError(str(exc)) from exc


async def _check(settings: Settings) -> int:
    """Verify connectivity and print a report; returns a process exit code."""
    import json

    from .database import Database

    report: dict[str, Any] = {"pgmcp_version": __version__, "settings": settings.describe()}
    database = Database(settings)
    try:
        await database.connect()
        row = await database.fetch_one(
            "SELECT version() AS version, current_database() AS database, "
            "current_user AS user, pg_is_in_recovery() AS in_recovery"
        )
        report["connection"] = {"ok": True, **(row or {})}
        schemas = await database.fetch_all(
            "SELECT nspname FROM pg_namespace WHERE nspname !~ '^pg_' "
            "AND nspname <> 'information_schema' ORDER BY nspname"
        )
        report["visible_schemas"] = [s["nspname"] for s in schemas]
        exit_code = 0
    except PgMcpError as exc:
        report["connection"] = {"ok": False, "error": str(exc)}
        exit_code = 1
    finally:
        await database.close()

    print(json.dumps(report, indent=2, default=str))
    return exit_code


def run(argv: list[str] | None = None) -> int:
    """Parse ``argv``, then either run the check or serve. Returns an exit code."""
    args = build_parser().parse_args(argv)

    try:
        settings = settings_from_args(args)
    except ConfigurationError as exc:
        print(f"pgmcp: {exc}", file=sys.stderr)
        return 2

    logging.basicConfig(
        level=getattr(logging, settings.log_level, logging.INFO),
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
        # stdio transport owns stdout; anything we print there corrupts the protocol.
        stream=sys.stderr,
    )

    if args.check:
        return asyncio.run(_check(settings))

    from .server import build_server

    server = build_server(settings)
    kwargs: dict[str, Any] = {}
    if settings.transport is Transport.STREAMABLE_HTTP:
        kwargs = {
            "host": settings.host,
            "port": settings.port,
            "streamable_http_path": settings.mount_path,
            "json_response": settings.json_response,
            "stateless_http": settings.stateless_http,
        }
    elif settings.transport is Transport.SSE:
        kwargs = {"host": settings.host, "port": settings.port}

    try:
        server.run(transport=settings.transport.value, **kwargs)
    except KeyboardInterrupt:  # pragma: no cover - interactive
        return 130
    except PgMcpError as exc:
        print(f"pgmcp: {exc}", file=sys.stderr)
        return 1
    return 0


def main() -> None:
    """Console-script entry point."""
    raise SystemExit(run())


if __name__ == "__main__":  # pragma: no cover
    main()
