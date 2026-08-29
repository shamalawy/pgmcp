"""Assembly of the pgmcp MCP server.

:func:`build_server` wires the configured toolsets, resources and prompts onto a
single :class:`~mcp.server.mcpserver.MCPServer`.  The connection pool is opened
in the server lifespan and installed as a process-wide singleton, so tool
functions can be called directly in tests without constructing an MCP session.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from mcp.server.mcpserver import MCPServer

from . import __version__
from .config import AccessMode, Settings, Toolset
from .database import Database, set_database

logger = logging.getLogger("pgmcp")

__all__ = ["build_server", "server_instructions"]


def server_instructions(settings: Settings) -> str:
    """The guidance string sent to clients during initialisation.

    Worth being specific here: it is the cheapest place to stop a model from
    guessing table names or pasting an unbounded ``SELECT *``.
    """
    mode = settings.access_mode
    lines = [
        "pgmcp gives you structured access to a PostgreSQL database.",
        "",
        "How to work effectively:",
        "- Start from `list_schemas` and `list_tables` to learn what exists; do not guess names.",
        "- Call `describe_table` before writing SQL against a table so you use real column names,"
        " types and foreign keys.",
        "- `search_objects` finds tables, columns and functions by pattern when you only know"
        " roughly what you are looking for.",
        "- Use `query` for SELECTs. Pass values through the `params` argument rather than"
        " formatting them into the SQL string.",
        "- Results are capped: check `truncated` in the response and page with LIMIT/OFFSET"
        " instead of raising the limit blindly.",
        "- `explain_query` shows the plan; `index_usage` and `table_stats` explain why a query is slow.",
        "",
    ]
    if mode.allows_write:
        lines += [
            "This server is in WRITE mode: `execute_sql` and `execute_transaction` can change data"
            " and schema. Confirm intent with the user before running them, prefer"
            " `execute_transaction` with `dry_run=true` first, and always scope UPDATE/DELETE"
            " with a WHERE clause.",
        ]
    else:
        lines += [
            "This server is READ-ONLY: every statement runs in a read-only transaction and any"
            " attempt to write will be refused. Do not try to work around it; tell the user the"
            " server must be restarted with write access instead.",
        ]
    return "\n".join(lines)


def build_server(settings: Settings | None = None) -> MCPServer:
    """Construct the MCP server for ``settings`` (or settings read from the environment)."""
    active = settings or Settings()

    @asynccontextmanager
    async def lifespan(_server: MCPServer) -> AsyncIterator[Database]:
        database = Database(active)
        set_database(database)
        try:
            await database.connect()
            yield database
        finally:
            await database.close()
            set_database(None)

    mcp = MCPServer(
        name="pgmcp",
        title="PostgreSQL",
        version=__version__,
        instructions=server_instructions(active),
        website_url="https://github.com/shamalawy/pgmcp",
        lifespan=lifespan,
    )

    register_all(mcp, active)
    return mcp


def register_all(mcp: MCPServer, settings: Settings) -> None:
    """Register every enabled toolset, plus resources and prompts."""
    # Imported lazily so that `pgmcp.server` stays importable without a database.
    from . import prompts, resources
    from .tools import health, query, schema, write

    registered: list[str] = []
    if settings.toolset_enabled(Toolset.SCHEMA):
        schema.register(mcp, settings)
        registered.append(Toolset.SCHEMA.value)
    if settings.toolset_enabled(Toolset.QUERY):
        query.register(mcp, settings)
        registered.append(Toolset.QUERY.value)
    if settings.toolset_enabled(Toolset.HEALTH):
        health.register(mcp, settings)
        registered.append(Toolset.HEALTH.value)
    if settings.toolset_enabled(Toolset.WRITE):
        write.register(mcp, settings)
        registered.append(Toolset.WRITE.value)

    resources.register(mcp, settings)
    prompts.register(mcp, settings)

    logger.info(
        "registered toolsets: %s (access_mode=%s)",
        ", ".join(registered) or "none",
        settings.access_mode.value,
    )
    if settings.access_mode is AccessMode.WRITE:
        logger.warning(
            "pgmcp is running in WRITE mode against %s — clients can modify this database",
            settings.safe_dsn,
        )
