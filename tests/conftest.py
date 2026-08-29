"""Shared fixtures.

Integration tests need a live PostgreSQL seeded with ``tests/fixtures/seed.sql``.
Point ``PGMCP_TEST_DSN`` at it; when it is unreachable the integration tests skip
so that the unit suite still runs (and still means something) without a database.
"""

from __future__ import annotations

import contextlib
import os
import uuid
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
from mcp.server.mcpserver import MCPServer
from mcp.types import CallToolResult

from pgmcp.config import AccessMode, Settings
from pgmcp.database import Database, set_database

TEST_DSN = os.getenv(
    "PGMCP_TEST_DSN",
    "postgresql://postgres:postgres@localhost:55432/pgmcp_demo",
)

# A separate, disposable database for tests that actually mutate something.
WRITE_TEST_DSN = os.getenv(
    "PGMCP_WRITE_TEST_DSN",
    "postgresql://postgres:postgres@localhost:55432/pgmcp_write",
)


def text_of(result: CallToolResult) -> str:
    """The text payload of a tool result, whether it succeeded or errored."""
    return "".join(block.text for block in result.content if getattr(block, "text", None))


@pytest_asyncio.fixture(scope="session")
async def database_available() -> bool:
    """Skip the whole integration set when the fixture database is unreachable."""
    probe = Database(Settings(dsn=TEST_DSN))
    try:
        await probe.connect()
    except Exception as exc:  # any connection failure means "skip", not "fail"
        pytest.skip(f"integration database unreachable ({TEST_DSN}): {exc}")
    finally:
        await probe.close()
    return True


@pytest.fixture
def settings() -> Settings:
    """Read-only settings pointed at the fixture database."""
    return Settings(dsn=TEST_DSN, access_mode=AccessMode.READ_ONLY)


@pytest_asyncio.fixture
async def database(settings: Settings, database_available: bool) -> AsyncIterator[Database]:
    """A connected, read-only database installed as the process-wide singleton."""
    db = Database(settings)
    await db.connect()
    set_database(db)
    try:
        yield db
    finally:
        await db.close()
        set_database(None)


@pytest_asyncio.fixture
async def server(database: Database) -> MCPServer:
    """A fully built MCP server backed by the read-only fixture database."""
    from pgmcp.server import build_server

    return build_server(database.settings)


@pytest_asyncio.fixture
async def write_database(database_available: bool) -> AsyncIterator[tuple[Database, str]]:
    """A write-mode database plus a uniquely named schema, dropped on teardown.

    Yields ``(database, schema_name)``. Every object a test creates belongs in
    that schema so concurrent runs cannot collide.
    """
    schema_name = f"t_{uuid.uuid4().hex[:12]}"
    db = Database(Settings(dsn=WRITE_TEST_DSN, access_mode=AccessMode.WRITE))
    try:
        await db.connect()
    except Exception as exc:
        pytest.skip(f"write test database unreachable ({WRITE_TEST_DSN}): {exc}")

    set_database(db)
    await db.run_user_query(f'CREATE SCHEMA "{schema_name}"')
    try:
        yield db, schema_name
    finally:
        with contextlib.suppress(Exception):
            await db.run_user_query(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE')
        await db.close()
        set_database(None)


@pytest_asyncio.fixture
async def write_server(write_database: tuple[Database, str]) -> tuple[MCPServer, str]:
    """An MCP server in write mode, plus the scratch schema name."""
    from pgmcp.server import build_server

    db, schema_name = write_database
    return build_server(db.settings), schema_name
