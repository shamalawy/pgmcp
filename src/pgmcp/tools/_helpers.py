"""Shared plumbing for tool modules.

Every tool in pgmcp is a thin async function that:

1. resolves its output format and row limit against the server settings,
2. builds a catalog query that binds schema/table names as **parameters** (never
   as interpolated identifiers),
3. runs it through :class:`~pgmcp.database.Database`, and
4. renders the result with :func:`~pgmcp.formatting.render`.

The :func:`tool` decorator handles the fourth thing every tool needs: turning a
:class:`~pgmcp.errors.PgMcpError` into an MCP ``ToolError``, which reaches the
model as readable text it can act on instead of an opaque crash.
"""

from __future__ import annotations

import functools
import json
import logging
from collections.abc import Awaitable, Callable, Sequence
from typing import Any, TypeVar

from mcp.server.mcpserver.exceptions import ToolError
from psycopg import sql

from ..config import OutputFormat, Settings
from ..database import Database, get_database
from ..errors import PgMcpError, QueryExecutionError, SchemaNotAllowedError
from ..formatting import QueryResult, json_default, render, to_jsonable, truncate

logger = logging.getLogger("pgmcp.tools")

T = TypeVar("T")

__all__ = [
    "as_json",
    "current_settings",
    "db",
    "ident",
    "in_clause",
    "list_param",
    "regclass",
    "resolve_format",
    "schema_predicate",
    "settings_of",
    "show",
    "tool",
    "validate_schema",
]


def db() -> Database:
    """The active :class:`~pgmcp.database.Database`."""
    return get_database()


def current_settings() -> Settings:
    """The active :class:`~pgmcp.config.Settings`."""
    return get_database().settings


def settings_of(database: Database) -> Settings:
    return database.settings


def tool(fn: Callable[..., Awaitable[str]]) -> Callable[..., Awaitable[str]]:
    """Convert pgmcp errors into ``ToolError`` so the model sees a usable message.

    Anything that is *not* a :class:`~pgmcp.errors.PgMcpError` is left to
    propagate: an unexpected exception is a bug, and the SDK logs its traceback
    server-side while telling the client only the tool name.
    """

    @functools.wraps(fn)
    async def wrapper(*args: Any, **kwargs: Any) -> str:
        try:
            return await fn(*args, **kwargs)
        except QueryExecutionError as exc:
            raise ToolError(
                json.dumps(exc.as_dict(), indent=2, ensure_ascii=False, default=json_default)
            ) from exc
        except PgMcpError as exc:
            raise ToolError(str(exc)) from exc

    return wrapper


def resolve_format(fmt: str | None, settings: Settings | None = None) -> OutputFormat:
    """Coerce a caller-supplied format name, falling back to the configured default."""
    active = settings or current_settings()
    if not fmt:
        return active.default_format
    try:
        return OutputFormat(fmt.strip().lower())
    except ValueError as exc:
        allowed = ", ".join(f.value for f in OutputFormat)
        raise ToolError(f"unknown format {fmt!r}; expected one of: {allowed}") from exc


def show(result: QueryResult, fmt: str | None = None, settings: Settings | None = None) -> str:
    """Render a :class:`QueryResult` honouring the format and byte budget."""
    active = settings or current_settings()
    return render(result, resolve_format(fmt, active), max_bytes=active.max_result_bytes)


def as_json(payload: Any, settings: Settings | None = None) -> str:
    """Serialise an arbitrary structure with the same byte budget as a query result."""
    active = settings or current_settings()
    text = json.dumps(to_jsonable(payload), indent=2, ensure_ascii=False, default=json_default)
    return truncate(text, active.max_result_bytes)


def validate_schema(schema: str, settings: Settings | None = None) -> str:
    """Raise unless ``schema`` is visible under the current allow/deny configuration."""
    active = settings or current_settings()
    name = (schema or "").strip()
    if not name:
        raise SchemaNotAllowedError("a schema name is required")
    if active.allowed_schemas and name not in active.allowed_schemas:
        raise SchemaNotAllowedError(
            f"schema {name!r} is not in the allow-list ({', '.join(active.allowed_schemas)})"
        )
    if name in active.blocked_schemas:
        raise SchemaNotAllowedError(f"schema {name!r} is blocked by server configuration")
    if name in ("pg_catalog", "information_schema", "pg_toast") and not active.include_system_schemas:
        raise SchemaNotAllowedError(
            f"schema {name!r} is a system schema; start the server with "
            "PGMCP_INCLUDE_SYSTEM_SCHEMAS=true to inspect it"
        )
    return name


def schema_predicate(
    column: str,
    settings: Settings | None = None,
    *,
    include_system: bool = False,
) -> tuple[str, list[Any]]:
    """Build a SQL fragment restricting ``column`` to the visible schemas.

    ``column`` is a *trusted* catalog column reference supplied by pgmcp's own
    code (e.g. ``"n.nspname"``), never client input. The values it is compared
    against are always bound as parameters.

    Returns ``(fragment, params)`` where ``fragment`` always begins with
    ``AND`` and may be empty.
    """
    active = settings or current_settings()
    clauses: list[str] = []
    params: list[Any] = []

    if active.allowed_schemas:
        clauses.append(f"{column} = ANY(%s)")
        params.append(list(active.allowed_schemas))

    # ``include_system`` relaxes only the built-in system-schema filter; an
    # operator's explicit blocked_schemas list is never relaxed.
    hidden = list(active.blocked_schemas) if include_system else list(active.hidden_schemas())
    if hidden:
        clauses.append(f"NOT ({column} = ANY(%s))")
        params.append(hidden)

    if not include_system:
        # Per-session temp schemas are noise in every listing.
        clauses.append(f"{column} !~ '^pg_temp_' AND {column} !~ '^pg_toast_temp_'")

    fragment = ("AND " + " AND ".join(clauses)) if clauses else ""
    return fragment, params


def ident(*parts: str) -> sql.Identifier:
    """Quote one or more identifier parts, e.g. ``ident("public", "users")``.

    Use this — never an f-string — whenever a client-supplied name has to appear
    in a position where PostgreSQL will not accept a bind parameter.
    """
    return sql.Identifier(*parts)


def regclass(schema: str, name: str) -> str:
    """A dotted, *unquoted* relation name for binding to ``to_regclass(%s)``.

    ``to_regclass`` parses its argument, so each part is quoted here to keep
    names with capitals, dots, spaces or embedded quotes intact. Quoting goes
    through psycopg's own identifier escaping rather than an f-string, so a name
    containing ``"`` cannot break out of the quoted literal.
    """
    return sql.Identifier(schema, name).as_string(None)


def in_clause(count: int) -> str:
    """``%s, %s, ...`` for ``count`` bind parameters."""
    return ", ".join(["%s"] * count)


def list_param(values: Sequence[str] | None) -> list[str] | None:
    """Normalise an optional list argument, treating an empty list as unset."""
    if not values:
        return None
    return [v for v in values if v]
